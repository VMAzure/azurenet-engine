import time
import uuid
import logging
from sqlalchemy import text
from app.database import SessionLocal

PROMPT = """
Fotografia cinematografica realistica in città metropolitana elegante,
ristoranti e movida serale sullo sfondo, alberi verdi e lampioni stile parigi.
Scatto 150mm, luce cinematografica, riflessi realistici.
Auto in primo piano al 100%.
No rendering, Remove license
""".strip()

SYSTEM_USER_ID = 13
POLL_INTERVAL = 10          # secondi
MAX_WAIT_SECONDS = 20 * 60  # 20 minuti


def process_cdn_previews():
    logging.info("[CDN→GIGI] START")

    db = SessionLocal()

    try:
        # 1️⃣ PRENDE SOLO QUELLI NON ANCORA PROCESSATI
        previews = db.execute(text("""
            SELECT codice_modello, url_cdn
            FROM public.mnet_modelli_cdn_preview
            WHERE is_valid = false
            ORDER BY codice_modello
        """)).fetchall()

        if not previews:
            logging.info("[CDN→GIGI] NOTHING TO DO")
            return

        for codice_modello, url_cdn in previews:
            logging.info("[CDN→GIGI] START modello=%s", codice_modello)

            job_id = uuid.uuid4()

            # 2️⃣ CREA JOB AI
            db.execute(text("""
                INSERT INTO public.gigi_gorilla_jobs (
                    id,
                    user_id,
                    subject_url,
                    prompt,
                    orientation,
                    aspect_ratio,
                    status
                )
                VALUES (
                    :id,
                    :user_id,
                    :subject_url,
                    :prompt,
                    '5:4',
                    '5:4',
                    'queued'
                )
            """), {
                "id": job_id,
                "user_id": SYSTEM_USER_ID,
                "subject_url": url_cdn,
                "prompt": PROMPT,
            })
            db.commit()

            # 3️⃣ ATTESA OUTPUT
            start = time.time()
            output_url = None

            while time.time() - start < MAX_WAIT_SECONDS:
                row = db.execute(text("""
                    SELECT public_url
                    FROM public.gigi_gorilla_job_outputs
                    WHERE job_id = :job_id
                      AND status = 'completed'
                      AND public_url IS NOT NULL
                """), {"job_id": job_id}).fetchone()

                if row:
                    output_url = row[0]
                    break

                time.sleep(POLL_INTERVAL)

            if not output_url:
                logging.error(
                    "[CDN→GIGI] TIMEOUT modello=%s job=%s",
                    codice_modello,
                    job_id,
                )
                # ❌ NON segniamo is_valid
                # ❌ verrà ripreso al prossimo run
                continue

            # 4️⃣ SCRITTURA IMMAGINE FINALE (DELTA-ONLY)
            updated = db.execute(text("""
                UPDATE public.mnet_modelli
                SET default_img = :img
                WHERE codice_modello = :codice
                  AND default_img IS NULL
            """), {
                "img": output_url,
                "codice": codice_modello,
            }).rowcount

            if updated == 0:
                logging.warning(
                    "[CDN→GIGI] SKIP WRITE modello=%s (default_img già presente)",
                    codice_modello,
                )

            # 5️⃣ SEGNA COME PROCESSATO (CHIAVE IDMPOTENZA)
            db.execute(text("""
                UPDATE public.mnet_modelli_cdn_preview
                SET
                    is_valid = true,
                    checked_at = now(),
                    checked_by = 'gigi_gorilla'
                WHERE codice_modello = :codice
            """), {
                "codice": codice_modello,
            })

            db.commit()

            logging.info("[CDN→GIGI] DONE modello=%s", codice_modello)

        logging.info("[CDN→GIGI] END")

    except Exception:
        db.rollback()
        logging.exception("[CDN→GIGI] FAILED")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    process_cdn_previews()
