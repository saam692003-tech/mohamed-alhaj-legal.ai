import os
import logging
from official_sources import crawl_all

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

if __name__ == "__main__":
    result = crawl_all(
        max_pages=int(os.getenv("CRAWL_MAX_PAGES", "1200")),
        delay=float(os.getenv("CRAWL_DELAY", "1.0")),
    )
    logging.info("Yemeni legal library refresh: %s", result)
