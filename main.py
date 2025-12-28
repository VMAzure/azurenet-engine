import time
import logging

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
