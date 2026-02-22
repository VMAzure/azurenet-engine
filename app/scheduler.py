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
    sync_vehicle_versions_cm_from_stock, 
)


from app.jobs.wltp_enrichment import wltp_enrichment_worker
from app.jobs.vehicle_stock_csv_import import vehicle_stock_csv_import_job
from app.jobs.sync_google_reviews import google_reviews_sync_job

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
        trigger=CronTrigger(day=5, hour=1, minute=1),
        id="usato_dettagli",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    # --------------------------------------------------
    # USATO — MAPPING STOCK → VEHICLE_VERSIONS_CM
    # --------------------------------------------------
    scheduler.add_job(
        func=sync_vehicle_versions_cm_from_stock,
        trigger=CronTrigger(
            minute="*/30",
            hour="9-19"
        ),
        id="usato_vehicle_versions_cm",
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

    

from app.jobs.autoscout_sync import autoscout_sync_job

def schedule_autoscout_jobs(scheduler):
    # --------------------------------------------------
    # AUTOSCOUT24 — SYNC AUTO USATE (CREATE / UPDATE / DELETE)
    # --------------------------------------------------

    scheduler.add_job(
        func=autoscout_sync_job,
        trigger=CronTrigger(minute="*/1"),  # ogni 1 minuto
        id="autoscout_sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    logging.info("[SCHEDULER] AUTOSCOUT SYNC job registered")

def schedule_wltp_jobs(scheduler):
    # --------------------------------------------------
    # WLTP — ARRICCHIMENTO NORMATIVA EURO (AUTO + VCOM)
    # --------------------------------------------------

    scheduler.add_job(
        func=wltp_enrichment_worker,
        trigger=CronTrigger(minute="*/10"),  # ogni 10 minuti
        id="wltp_enrichment",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    logging.info("[SCHEDULER] WLTP enrichment job registered")


    # --------------------------------------------------
    # VEHICLE STOCK — CSV IMPORT (FULL SYNC)
    # --------------------------------------------------
    scheduler.add_job(
        func=vehicle_stock_csv_import_job,
        trigger=CronTrigger(
            minute=0,          # allo scoccare dell'ora
            hour="9-19",       # solo dalle 09 alle 19
        ),
        id="vehicle_stock_csv_import",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

def schedule_reviews_jobs(scheduler):
    scheduler.add_job(
        func=google_reviews_sync_job,
        trigger=CronTrigger(hour=3, minute=30),  # ogni giorno 03:30
        id="google_reviews_sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    logging.info("[SCHEDULER] GOOGLE REVIEWS job registered")

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

    # Pubblica AS24    
    schedule_autoscout_jobs(scheduler)
    logging.info("[SCHEDULER] AUTOSCOUT jobs registered")

    # registra WLTP (normativa euro)
    schedule_wltp_jobs(scheduler)
    logging.info("[SCHEDULER] WLTP jobs registered")

    # registra GOOGLE REVIEWS
    schedule_reviews_jobs(scheduler)
    logging.info("[SCHEDULER] GOOGLE REVIEWS jobs registered")


    return scheduler


