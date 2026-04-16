from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
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
from app.jobs.sync_news import sync_news_job
from app.jobs.rewrite_news import rewrite_news_job
from app.jobs.vehicle_podcast_worker import vehicle_podcast_worker
from app.jobs.dealer_podcast_worker import dealer_podcast_worker
from app.jobs.audit_rescan_worker import audit_rescan_batch

from app.jobs.sync_motornet_immagini_fill import run as sync_nuovo_immagini_fill
from app.jobs.queue_modelli_missing import run as queue_modelli_missing
from app.jobs.nlt_disattiva_fuori_catalogo import run as nlt_disattiva_fuori_catalogo


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

    scheduler.add_job(
        func=sync_nuovo_immagini_fill,
        trigger=CronTrigger(day=11, hour=7, minute=30),
        id="nuovo_immagini_fill",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=queue_modelli_missing,
        trigger=CronTrigger(day=11, hour=9, minute=0),  # 30 min dopo
        id="nuovo_queue_modelli_missing",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )

    scheduler.add_job(
        func=nlt_disattiva_fuori_catalogo,
        trigger=CronTrigger(day=11, hour=17, minute=40),  
        id="nlt_disattiva_fuori_catalogo",
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
from app.jobs.asm_sync import asm_sync_job

def schedule_asm_jobs(scheduler):
    scheduler.add_job(
        func=asm_sync_job,
        trigger=CronTrigger(minute="*/2"),  # ogni 2 minuti
        id="asm_sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logging.info("[SCHEDULER] ASM SYNC job registered")

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

    # Pubblica ASM (AutoSuperMarket)
    schedule_asm_jobs(scheduler)
    logging.info("[SCHEDULER] ASM jobs registered")

    # registra WLTP (normativa euro)
    schedule_wltp_jobs(scheduler)
    logging.info("[SCHEDULER] WLTP jobs registered")

    # registra GOOGLE REVIEWS
    schedule_reviews_jobs(scheduler)
    logging.info("[SCHEDULER] GOOGLE REVIEWS jobs registered")

    # registra NEWS SYNC
    scheduler.add_job(
        func=sync_news_job,
        trigger=CronTrigger(hour=2, minute=30),  # ogni notte 02:30
        id="news_sync",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logging.info("[SCHEDULER] NEWS SYNC job registered")

    # registra REWRITE NEWS (1 ora dopo il fetch)
    scheduler.add_job(
        func=rewrite_news_job,
        trigger=CronTrigger(hour=3, minute=30),
        id="news_rewrite",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
    )
    logging.info("[SCHEDULER] NEWS REWRITE job registered")

    # PODCAST VEICOLO (coda async): poll ogni 60s, processa BATCH_SIZE righe.
    # core_api_v2 accoda status='pending' al click del dealer, questo worker
    # pesca e genera (gpt-5 + Gemini TTS + upload Supabase).
    scheduler.add_job(
        func=vehicle_podcast_worker,
        trigger=IntervalTrigger(seconds=60),
        id="vehicle_podcast_worker",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    logging.info("[SCHEDULER] VEHICLE PODCAST WORKER job registered (every 60s)")

    # DEALER PODCAST (coda async): poll ogni 60s, 1 dealer per run.
    scheduler.add_job(
        func=dealer_podcast_worker,
        trigger=IntervalTrigger(seconds=60),
        id="dealer_podcast_worker",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=30,
    )
    logging.info("[SCHEDULER] DEALER PODCAST WORKER job registered (every 60s)")

    # AUDIT RESCAN: processa batch di 50 domini pending ogni 5 minuti.
    # Quando non ci sono più pending, gira a vuoto (0 operazioni).
    # Una volta completato il backfill iniziale, può essere rimosso o
    # trasformato in scan settimanale per monitoraggio continuo.
    scheduler.add_job(
        func=audit_rescan_batch,
        trigger=IntervalTrigger(minutes=5),
        id="audit_rescan_batch",
        replace_existing=True,
        max_instances=1,
        coalesce=True,
        misfire_grace_time=120,
    )
    logging.info("[SCHEDULER] AUDIT RESCAN WORKER job registered (every 5min, batch 50)")

    return scheduler


