from __future__ import annotations

import logging

from apscheduler.schedulers.blocking import BlockingScheduler

from .config import settings
from .sync import sync_latest_if_needed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def job() -> None:
    try:
        trade_date = sync_latest_if_needed()
        log.info("sync check completed, latest due trade date=%s", trade_date)
    except Exception:
        log.exception("daily sync failed; keeping last known-good data")


def main() -> None:
    # Catch up after a restart. If today's data is not due yet, this only checks the
    # prior completed trading day.
    job()
    scheduler = BlockingScheduler(timezone=settings.timezone)
    # Three idempotent attempts. A successful first run makes later attempts no-op.
    scheduler.add_job(job, "cron", day_of_week="mon-fri", hour=17, minute=20, id="close-1720")
    scheduler.add_job(job, "cron", day_of_week="mon-fri", hour=18, minute=5, id="close-1805")
    scheduler.add_job(job, "cron", day_of_week="mon-fri", hour=19, minute=0, id="close-1900")
    scheduler.start()


if __name__ == "__main__":
    main()
