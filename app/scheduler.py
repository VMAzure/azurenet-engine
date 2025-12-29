from apscheduler.triggers.cron import CronTrigger
from app.jobs.vic import (
    sync_vic_marche,
    sync_vic_modelli,
    sync_vic_versioni,
    sync_vic_dettagli,
)
from app.jobs.nuovo import (
    sync_nuovo_marche,
    sync_nuovo_modelli,
    sync_nuovo_allestimenti,
    sync_nuovo_dettagli,
)

from app.jobs.usato import (
    sync_usato_marche,
    sync_usato_anni,
    sync_usato_modelli,
    sync_usato_allestimenti,
    sync_usato_dettagli,
)


def schedule_nuovo_jobs(scheduler):
    # --------------------------------------------------
    # NUOVO — AUTO NUOVE (DELTA-ONLY)
    # --------------------------------------------------

    scheduler.add_job(
        func=sync_nuovo_marche,
        trigger=CronTrigger(day=11, hour=5, minute=0),
        id="nuovo_marche",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_nuovo_modelli,
        trigger=CronTrigger(day=11, hour=5, minute=15),
        id="nuovo_modelli",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_nuovo_allestimenti,
        trigger=CronTrigger(day=11, hour=5, minute=40),
        id="nuovo_allestimenti",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_nuovo_dettagli,
        trigger=CronTrigger(day=11, hour=6, minute=20),
        id="nuovo_dettagli",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

def schedule_usato_jobs(scheduler):
    # --------------------------------------------------
    # USATO — AUTO USATE (DELTA-ONLY)
    # --------------------------------------------------

    scheduler.add_job(
        func=sync_usato_marche,
        trigger=CronTrigger(day=21, hour=5, minute=0),
        id="usato_marche",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_usato_anni,
        trigger=CronTrigger(day=21, hour=5, minute=10),
        id="usato_anni",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_usato_modelli,
        trigger=CronTrigger(day=21, hour=5, minute=25),
        id="usato_modelli",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_usato_allestimenti,
        trigger=CronTrigger(day=21, hour=5, minute=55),
        id="usato_allestimenti",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=sync_usato_dettagli,
        trigger=CronTrigger(day=21, hour=6, minute=30),
        id="usato_dettagli",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
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

    # registra job NUOVO
    schedule_nuovo_jobs(scheduler)
    logging.info("[SCHEDULER] NUOVO jobs registered")

    # registra job USATO
    schedule_usato_jobs(scheduler)
    logging.info("[SCHEDULER] USATO jobs registered")

    return scheduler


