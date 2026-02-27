import logging
from sqlalchemy import text
from app.database import SessionLocal


# ==========================================================
# CONFIG
# ==========================================================

BATCH_SIZE = 200

# ==========================================================
# SQL
# ==========================================================

SQL_GET_MODELLI_MISSING = """
SELECT
    m.codice_modello,
    v.url AS mnet_image_url,
    m.default_img,
    m.default_img_9_16
FROM public.mnet_modelli m
JOIN public.v_model_best_image v
  ON v.codice_modello = m.codice_modello
WHERE
    (m.default_img IS NULL OR m.default_img_9_16 IS NULL)
LIMIT :limit;
"""

SQL_QUEUE_EXISTS = """
SELECT 1
FROM public.mnet_modelli_image_queue
WHERE codice_modello = :codice
  AND orientation = :orientation
  AND status IN ('queued', 'processing')
LIMIT 1;
"""

SQL_INSERT_QUEUE = """
INSERT INTO public.mnet_modelli_image_queue (
    codice_modello,
    mnet_image_url,
    orientation,
    status,
    retry_count,
    created_at,
    updated_at
)
VALUES (
    :codice,
    :url,
    :orientation,
    'queued',
    0,
    NOW(),
    NOW()
);
"""


# ==========================================================
# WORKER
# ==========================================================

def run():
    logging.info("🧩 MODELLI_MISSING_QUEUE — scan modelli")

    with SessionLocal() as db:

        rows = db.execute(
            text(SQL_GET_MODELLI_MISSING),
            {"limit": BATCH_SIZE}
        ).mappings().all()

        if not rows:
            logging.info("ℹ️ Nessun modello da accodare")
            return

        logging.info(f"▶️ Trovati {len(rows)} modelli incompleti")

        for r in rows:

            codice = r["codice_modello"]
            image_url = r["mnet_image_url"]

            # --------------------------------------------------
            # 5:4
            # --------------------------------------------------
            if r["default_img"] is None:

                exists = db.execute(
                    text(SQL_QUEUE_EXISTS),
                    {"codice": codice, "orientation": "5:4"}
                ).scalar()

                if not exists:
                    db.execute(
                        text(SQL_INSERT_QUEUE),
                        {
                            "codice": codice,
                            "url": image_url,
                            "orientation": "5:4",
                        }
                    )
                    logging.info(f"➕ Accodato 5:4 → {codice}")

            # --------------------------------------------------
            # 9:16
            # --------------------------------------------------
            if r["default_img_9_16"] is None:

                exists = db.execute(
                    text(SQL_QUEUE_EXISTS),
                    {"codice": codice, "orientation": "9:16"}
                ).scalar()

                if not exists:
                    db.execute(
                        text(SQL_INSERT_QUEUE),
                        {
                            "codice": codice,
                            "url": image_url,
                            "orientation": "9:16",
                        }
                    )
                    logging.info(f"➕ Accodato 9:16 → {codice}")

        db.commit()

    logging.info("✅ MODELLI_MISSING_QUEUE — completato")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()