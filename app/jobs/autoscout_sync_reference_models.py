import logging
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import SessionLocal
from app.external.autoscout import get_makes

logger = logging.getLogger(__name__)


def autoscout_sync_reference_models():
    session = SessionLocal()

    inserted = 0
    skipped = 0

    try:
        data = get_makes()
        makes = data.get("makes", [])

        logger.info(
            "[AUTOSCOUT_REF_SYNC] Makes ricevute: %s",
            len(makes),
        )

        for make in makes:
            make_id = make["id"]
            make_name = make["name"].strip()

            for model in make.get("models", []):
                model_id = model["id"]
                model_name = model["name"].strip()
                vehicle_type = model.get("vehicleType")

                if vehicle_type not in ("C", "X"):
                    continue

                result = session.execute(
                    text("""
                        INSERT INTO autoscout_reference_models (
                            autoscout_make_id,
                            autoscout_make_name,
                            autoscout_model_id,
                            autoscout_model_name,
                            vehicle_type
                        ) VALUES (
                            :make_id,
                            :make_name,
                            :model_id,
                            :model_name,
                            :vehicle_type
                        )
                        ON CONFLICT (autoscout_make_id, autoscout_model_id)
                        DO NOTHING
                    """),
                    {
                        "make_id": make_id,
                        "make_name": make_name,
                        "model_id": model_id,
                        "model_name": model_name,
                        "vehicle_type": vehicle_type,
                    },
                )

                if result.rowcount == 1:
                    inserted += 1
                else:
                    skipped += 1

                if (inserted + skipped) % 500 == 0:
                    session.commit()
                    logger.info(
                        "[AUTOSCOUT_REF_SYNC] progress inserted=%s skipped=%s",
                        inserted,
                        skipped,
                    )

        session.commit()

        logger.info(
            "[AUTOSCOUT_REF_SYNC] Completato | inseriti=%s | skip=%s",
            inserted,
            skipped,
        )

    except SQLAlchemyError:
        session.rollback()
        logger.exception("[AUTOSCOUT_REF_SYNC] ERRORE DB")
        raise

    finally:
        session.close()

