import logging
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger

from app.settings import (
    SCHEDULER_TIMEZONE,
    ENABLE_NUOVO_SYNC,
    ENABLE_USATO_SYNC,
    ENABLE_VIC_SYNC,
)

from app.jobs.nuovo import sync_nuovo
from app.jobs.usato import sync_usato
from app.jobs.vic import sync_vic


def build_scheduler() -> BackgroundScheduler:
    scheduler = BackgroundScheduler(timezone=SCHEDULER_TIMEZONE)

    if ENABLE_NUOVO_SYNC:
        scheduler.add_job(
            sync_nuovo,
            CronTrigger(hour=2, minute=0),
            id="sync_nuovo",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    if ENABLE_USATO_SYNC:
        scheduler.add_job(
            sync_usato,
            CronTrigger(hour=3, minute=0),
            id="sync_usato",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    if ENABLE_VIC_SYNC:
        scheduler.add_job(
            sync_vic,
            CronTrigger(hour=4, minute=0),
            id="sync_vic",
            replace_existing=True,
            max_instances=1,
            coalesce=True,
        )

    return scheduler
