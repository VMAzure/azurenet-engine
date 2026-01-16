"""
ONE-SHOT WORKER — BRAND IMAGES REGEN (AI via Gigi Gorilla)

Rules:
- Loop su TUTTI i brand
- Inserisce job in gigi_gorilla_jobs
- Nessun polling
- Nessuna generazione sincrona
- Overwrite finale img_url gestito dal worker AI
"""

import uuid
import logging
from sqlalchemy import text

from app.database import SessionLocal

# ============================================================
# CONFIG
# ============================================================

SYSTEM_USER_ID = 13
AI_MODEL_ID = "gemini-3-pro-image-preview"
ORIENTATION = "16:9"
ASPECT_RATIO = "16:9"

PROMPT_TEMPLATE = """
Fotografia professionale ravvicinata di un dettaglio particolarmente moderno
e tecnologicamente avanzato di un veicolo del brand {brand_name}.

Scatto macro o close-up su elementi di design contemporaneo
(fari, firma luminosa, superfici, materiali, dettagli tecnologici),
con forte impatto visivo ed effetto “wow”.

Stile premium corporate, fotografia reale ad alta qualità,
illuminazione controllata, profondità di campo ridotta,
look moderno, pulito e futuristico ma coerente con il brand.

Ambientazione elegante e professionale.
Nessun testo, nessun logo, nessun watermark,
nessun elemento grafico sovraimpresso.
""".strip()

# ============================================================
# LOGGING
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)

logger = logging.getLogger("brand-gigi-jobs")

# ============================================================
# MAIN WORKER
# ============================================================

def process_brand_images_with_gigi():
    logger.info("[BRAND→GIGI] START ONE-SHOT WORKER")

    db = SessionLocal()

    try:
        rows = db.execute(text("""
            SELECT
                m.acronimo AS brand_id,
                m.nome     AS brand_name
            FROM mnet_marche_usato m
            ORDER BY m.acronimo
        """)).fetchall()


        logger.info("[BRAND→GIGI] Brands found: %s", len(rows))

        for brand_id, brand_name in rows:
            job_id = uuid.uuid4()

            prompt = PROMPT_TEMPLATE.format(brand_name=brand_name)

            logger.info(
                "[BRAND→GIGI] CREATE JOB brand=%s job_id=%s",
                brand_id,
                job_id,
            )

            # 1) CREATE GIGI GORILLA JOB
            db.execute(text("""
                INSERT INTO gigi_gorilla_jobs (
                    id,
                    user_id,
                    subject_url,
                    prompt,
                    orientation,
                    aspect_ratio,
                    model_id,
                    status
                )
                VALUES (
                    :id,
                    :user_id,
                    NULL,
                    :prompt,
                    :orientation,
                    :aspect_ratio,
                    :model_id,
                    'queued'
                )
            """), {
                "id": job_id,
                "user_id": SYSTEM_USER_ID,
                "prompt": prompt,
                "orientation": ORIENTATION,
                "aspect_ratio": ASPECT_RATIO,
                "model_id": AI_MODEL_ID,
            })


            # 🔥 COMMIT IMMEDIATO: rende il job visibile al worker AI
            db.commit()

        logger.info("[BRAND→GIGI] ALL JOBS QUEUED")

    except Exception:
        db.rollback()
        logger.exception("[BRAND→GIGI] FATAL ERROR")
        raise

    finally:
        db.close()
        logger.info("[BRAND→GIGI] END")


if __name__ == "__main__":
    process_brand_images_with_gigi()
