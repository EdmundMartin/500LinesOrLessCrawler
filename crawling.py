import asyncio
import cgi
from collections import namedtuple
import logging
import re
import time
import urllib.parse
from asyncio import Queue
from typing import List, Optional, Tuple, Set

import aiohttp
import bs4


LOGGER = logging.getLogger(__name__)


def lenient_host(host):
    parts = host.split(".")[-2:]
    return "".join(parts)


def is_redirect(status: int) -> bool:
    return status >= 300 < 400


def extract_urls(url: str, html: str) -> List[str]:
    found = set()
    soup = bs4.BeautifulSoup(html, "lxml")
    for href in soup.find_all("a", href=True):
        url = urllib.parse.urljoin(url, href["href"])
        found.add(url)
    return list(found)


FetchStatistic = namedtuple(
    "FetchStatistic",
    [
        "url",
        "next_url",
        "status",
        "exception",
        "size",
        "content_type",
        "encoding",
        "num_urls",
        "num_new_urls",
    ],
)


class Crawler:
    def __init__(
        self,
        roots: List[str],
        exclude=None,
        strict: bool = True,
        max_redirect: int = 10,
        max_tries: int = 4,
        max_tasks: int = 10,
        *,
        loop: Optional[asyncio.AbstractEventLoop] = None,
    ):
        self.loop = loop or asyncio.get_event_loop()
        self.roots = roots
        self.exclude = exclude
        self.strict = strict
        self.max_redirect = max_redirect
        self.max_tries = max_tries
        self.max_tasks = max_tasks
        self.q = Queue(loop=self.loop)
        self.seen_urls = set()
        self.done = []
        self.session = aiohttp.ClientSession(loop=self.loop)
        self.root_domains = set()
        self._setup_roots(roots)
        self.t0 = None
        self.t1 = None

    def _setup_roots(self, roots: List[str]) -> None:
        for root in roots:
            parts = urllib.parse.urlparse(root)
            host, port = urllib.parse.splitport(parts.netloc)
            self.add_url(root)
            if not host:
                continue
            if re.match(r"\A[\d\.]*\Z", host):
                self.root_domains.add(host)
            else:
                host = host.lower()
                if self.strict:
                    self.root_domains.add(host)
                else:
                    self.root_domains.add(lenient_host(host))

    async def close(self):
        await self.session.close()

    def host_okay(self, host):
        host = host.lower()
        if host in self.root_domains:
            return True
        if re.match(r"\A[\d\.]*\Z", host):
            return False
        if self.strict:
            return self._host_okay_strictish(host)
        else:
            return self._host_okay_lenient(host)

    def _host_okay_strictish(self, host):
        host = host[4:] if host.startswith("www.") else f"www.{host}"
        return host in self.root_domains

    def _host_okay_lenient(self, host):
        return lenient_host(host) in self.root_domains

    def record_statistic(self, fetch_statistic: FetchStatistic):
        self.done.append(fetch_statistic)

    async def parse_links(
        self, response: aiohttp.ClientResponse
    ) -> Tuple[FetchStatistic, Set[str]]:
        links = set()
        content_type = None
        encoding = None
        body = await response.read()
        if response.status == 200:
            content_type = response.headers.get("content-type")
            pdict = {}
            if content_type:
                content_type, pdict = cgi.parse_header(content_type)

            encoding = pdict.get("charset", "utf-8")
            if content_type in ("text/html", "application/xml", "text/html;"):
                text = await response.text(encoding=encoding, errors="ignore")
                urls = extract_urls(str(response.url), text)
                if urls:
                    LOGGER.info(
                        "got {} distinct urls from {}".format(
                            len(urls), str(response.url)
                        )
                    )
                for url in urls:
                    defrag, _ = urllib.parse.urldefrag(url)
                    if self.url_allowed(defrag):
                        links.add(defrag)

        stat = FetchStatistic(
            url=response.url,
            next_url=None,
            status=response.status,
            exception=None,
            size=len(body),
            content_type=content_type,
            encoding=encoding,
            num_urls=len(links),
            num_new_urls=len(links - self.seen_urls),
        )

        return stat, links

    async def fetch(self, url: str, max_redirect: int):
        tries: int = 0
        exception: Optional[Exception] = None
        while tries < self.max_tries:
            try:
                response: aiohttp.ClientResponse = await self.session.get(
                    url, allow_redirects=False
                )

                if tries > 1:
                    LOGGER.info("try {} for {}".format(tries, url))

                break
            except aiohttp.ClientError as client_error:
                LOGGER.info("try {} for {} raised {}", tries, url, client_error)
                exception = client_error

            tries += 1

        else:
            LOGGER.error("{} failed after {} tries".format(url, self.max_tries))

            self.record_statistic(
                FetchStatistic(
                    url=url,
                    next_url=None,
                    status=None,
                    exception=exception,
                    size=0,
                    content_type=None,
                    encoding=None,
                    num_urls=0,
                    num_new_urls=0,
                )
            )
            return

        try:
            if is_redirect(response.status):
                location = response.headers.get("location")
                if not location:
                    return
                next_url = urllib.parse.urljoin(url, location)
                self.record_statistic(
                    FetchStatistic(
                        url=url,
                        next_url=next_url,
                        status=response.status,
                        exception=None,
                        size=0,
                        content_type=None,
                        encoding=None,
                        num_urls=0,
                        num_new_urls=0,
                    )
                )

                if next_url in self.seen_urls:
                    return
                if max_redirect > 0:
                    LOGGER.info("redirect to {} from {}".format(next_url, url))
                    self.add_url(next_url, max_redirect - 1)
                else:
                    LOGGER.error(
                        "redirect limit reached for {} from {}".format(next_url, url)
                    )

            else:
                stat, links = await self.parse_links(response)
                self.record_statistic(stat)
                for link in links.difference(self.seen_urls):
                    self.q.put_nowait((link, self.max_redirect))
                self.seen_urls.update(links)
        finally:
            response.close()

    async def work(self):
        try:
            while True:
                url, max_redirect = await self.q.get()
                await self.fetch(url, max_redirect)
                self.q.task_done()
        except asyncio.CancelledError:
            pass

    def url_allowed(self, url: str):
        parts = urllib.parse.urlparse(url)
        if parts.scheme not in ("http", "https"):
            LOGGER.debug("skipping non-http scheme in {}".format(url))
            return False
        host, port = urllib.parse.splitport(parts.netloc)
        if not self.host_okay(host):
            LOGGER.debug("skipping non-root host in {}".format(url))
            return False
        return True

    def add_url(self, url: str, max_redirect: Optional[int] = None):
        if max_redirect is None:
            max_redirect = self.max_redirect
        LOGGER.debug("adding {} {}".format(url, max_redirect))
        self.seen_urls.add(url)
        self.q.put_nowait((url, max_redirect))

    async def crawl(self):
        workers = [
            asyncio.Task(self.work(), loop=self.loop) for _ in range(self.max_tasks)
        ]
        self.t0 = time.time()
        await self.q.join()
        self.t1 = time.time()
        for w in workers:
            w.cancel()
