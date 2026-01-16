"""
CONSUMER — APPLY AI OUTPUTS TO mnet_modelli.default_img

Rules:
- Read only completed AI outputs (idx = 0)
- Overwrite default_img intentionally
- Mark state as done
- Idempotent and safe to rerun
"""

import logging
from sqlalchemy import text

from app.database import SessionLocal

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

# ============================================================
# CONSUMER
# ============================================================

def process_modelli_img_old_ai_consumer():
    logging.info("[OLD→AI][CONSUMER] START")

    db = SessionLocal()

    try:
        rows = db.execute(text("""
            SELECT
                r.codice_modello,
                r.job_id,
                o.public_url
            FROM mnet_modelli_img_old_ai r
            JOIN gigi_gorilla_job_outputs o
              ON o.job_id = r.job_id
            WHERE r.status = 'queued'
              AND o.status = 'completed'
              AND o.idx = 0
              AND o.public_url IS NOT NULL
            ORDER BY r.created_at
        """)).fetchall()

        if not rows:
            logging.info("[OLD→AI][CONSUMER] NOTHING TO DO")
            return

        for codice_modello, job_id, public_url in rows:
            logging.info(
                "[OLD→AI][CONSUMER] APPLY modello=%s job=%s",
                codice_modello,
                job_id,
            )

            # 1️⃣ WRITE default_img (OVERWRITE INTENTIONAL)
            db.execute(text("""
                UPDATE mnet_modelli
                SET default_img = :url,
                    ultima_modifica = now()
                WHERE codice_modello = :codice
            """), {
                "url": public_url,
                "codice": codice_modello,
            })

            # 2️⃣ MARK STATE DONE
            db.execute(text("""
                UPDATE mnet_modelli_img_old_ai
                SET status = 'done',
                    updated_at = now()
                WHERE codice_modello = :codice
            """), {
                "codice": codice_modello,
            })

            db.commit()

        logging.info(
            "[OLD→AI][CONSUMER] DONE applied=%d",
            len(rows),
        )

    except Exception:
        db.rollback()
        logging.exception("[OLD→AI][CONSUMER] FAILED")
        raise

    finally:
        db.close()
        logging.info("[OLD→AI][CONSUMER] END")


if __name__ == "__main__":
    process_modelli_img_old_ai_consumer()
