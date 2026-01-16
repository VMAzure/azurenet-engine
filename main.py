import time
import logging
from dotenv import load_dotenv
from pathlib import Path


ENV_PATH = Path(__file__).resolve().parent / ".env"
load_dotenv(dotenv_path=ENV_PATH)

import os

from app.scheduler import build_scheduler

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

def main():
    logging.info("🚀 azurenet-engine starting")

    scheduler = build_scheduler()
    scheduler.start()

    logging.info("🕒 scheduler started")

    try:
        while True:
            time.sleep(60)
    except KeyboardInterrupt:
        logging.info("🛑 shutdown requested")
    finally:
        scheduler.shutdown(wait=True)
        logging.info("✅ scheduler stopped")

if __name__ == "__main__":
    main()
