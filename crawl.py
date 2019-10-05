import asyncio
import sys
import logging

from crawling import Crawler


if __name__ == "__main__":
    try:
        import uvloop

        uvloop.install()
    except ImportError:
        print("Using standard event loop")
    levels = [logging.ERROR, logging.WARN, logging.INFO, logging.DEBUG]
    logging.basicConfig(level=levels[-1])

    loop = asyncio.get_event_loop()
    crawler = Crawler(["https://www.theguardian.com/uk"])

    try:
        loop.run_until_complete(crawler.crawl())
    except KeyboardInterrupt:
        sys.stderr.flush()
        print("\nInterrupted\n")
    finally:
        loop.run_until_complete(crawler.close())
        loop.close()
