from apscheduler.triggers.cron import CronTrigger
from app.jobs.vic import (
    sync_vic_marche,
    sync_vic_modelli,
    sync_vic_versioni,
    sync_vic_dettagli,
)

def schedule_vcom_jobs(scheduler):
    # --------------------------------------------------
    # VCOM — VEICOLI COMMERCIALI (DELTA-ONLY)
    # --------------------------------------------------

    # Marche + Modelli (struttura, raramente cambia)
    scheduler.add_job(
        func=sync_vic_marche,
        trigger=CronTrigger(day=1, hour=3, minute=0),
        id="vcom_marche",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_vic_modelli,
        trigger=CronTrigger(day=1, hour=3, minute=10),
        id="vcom_modelli",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Versioni (delta)
    scheduler.add_job(
        func=sync_vic_versioni,
        trigger=CronTrigger(day=1, hour=3, minute=40),
        id="vcom_versioni",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    # Dettagli (solo per versioni nuove)
    scheduler.add_job(
        func=sync_vic_dettagli,
        trigger=CronTrigger(day=1, hour=4, minute=40),
        id="vcom_dettagli",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

from apscheduler.schedulers.background import BackgroundScheduler
import logging


def build_scheduler():
    """
    Entry point per lo scheduler.
    Usato da main.py
    """
    logging.info("[SCHEDULER] building scheduler")

    scheduler = BackgroundScheduler()

    # registra job VCOM
    schedule_vcom_jobs(scheduler)

    logging.info("[SCHEDULER] VCOM jobs registered")

    return scheduler
