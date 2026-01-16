import logging
import requests
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import SessionLocal
from app.external.autoscout import (
    resolve_customer_id,
    upload_image,
    update_listing_images,
    AutoScoutClientError,
)

logger = logging.getLogger(__name__)


# ============================================================
# JOB: AUTOSCOUT PRE-UPLOAD + ATTACH IMAGES (PRODUCTION)
# ============================================================

def autoscout_preupload_images_job():
    session = SessionLocal()

    try:
        # ------------------------------------------------------------
        # 1️⃣ Pick listing valido (prima o dopo create)
        # ------------------------------------------------------------
        listing = session.execute(
            text("""
                SELECT *
                FROM autoscout_listings
                WHERE status IN ('PENDING_CREATE', 'CREATED')
                  AND listing_id IS NOT NULL
                ORDER BY requested_at
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            """)
        ).mappings().first()

        if not listing:
            logger.info("[AUTOSCOUT_PREUPLOAD] Nessun listing processabile")
            session.commit()
            return

        listing_pk = listing["id"]
        id_auto = listing["id_auto"]
        dealer_id = listing["dealer_id"]
        listing_id_remote = listing["listing_id"]

        logger.info(
            "[AUTOSCOUT_PREUPLOAD] Start job | listing_pk=%s listing_id=%s id_auto=%s",
            listing_pk,
            listing_id_remote,
            id_auto,
        )

        # ------------------------------------------------------------
        # 2️⃣ Config dealer
        # ------------------------------------------------------------
        config = session.execute(
            text("""
                SELECT *
                FROM autoscout_dealer_config
                WHERE dealer_id = :dealer_id
                  AND enabled = true
            """),
            {"dealer_id": dealer_id},
        ).mappings().first()

        if not config:
            raise RuntimeError("Configurazione AutoScout dealer mancante")

        test_mode = bool(config.get("test_mode"))
        customer_id = resolve_customer_id(config["customer_id"])

        # ------------------------------------------------------------
        # 3️⃣ Load vetrina (foto + ai, NO video)
        #     Ordine: priority → created_at
        # ------------------------------------------------------------
        rows = session.execute(
            text("""
                SELECT
                    v.media_type,
                    v.media_id,
                    v.priority,
                    v.created_at,
                    CASE v.media_type
                        WHEN 'foto' THEN img.foto
                        WHEN 'ai'   THEN leo.public_url
                    END AS media_url
                FROM usato_vetrina v
                LEFT JOIN azlease_usatoimg img ON img.id = v.media_id
                LEFT JOIN usato_leonardo leo ON leo.id = v.media_id
                WHERE v.id_auto = :id_auto
                  AND v.media_type IN ('foto', 'ai')
                  AND (
                        (v.media_type = 'foto' AND img.foto IS NOT NULL)
                     OR (v.media_type = 'ai'   AND leo.public_url IS NOT NULL)
                  )
                ORDER BY
                    v.priority ASC NULLS LAST,
                    v.created_at ASC
            """),
            {"id_auto": str(id_auto)},
        ).mappings().all()

        if not rows:
            logger.info(
                "[AUTOSCOUT_PREUPLOAD] Nessuna immagine in vetrina | id_auto=%s",
                id_auto,
            )
            session.commit()
            return

        # ------------------------------------------------------------
        # 4️⃣ Pre-upload immagini (ordine vetrina)
        # ------------------------------------------------------------
        image_ids = []

        for idx, r in enumerate(rows, start=1):
            try:
                resp = requests.get(r["media_url"], timeout=15)
                resp.raise_for_status()

                image_id = upload_image(
                    customer_id=customer_id,
                    image_bytes=resp.content,
                    content_type=resp.headers.get("Content-Type", "image/jpeg"),
                    test_mode=test_mode,
                )

                image_ids.append(image_id)

                logger.info(
                    "[AUTOSCOUT_PREUPLOAD] Pre-upload OK (%d/%d) | media_id=%s",
                    idx,
                    len(rows),
                    r["media_id"],
                )

            except AutoScoutClientError:
                logger.exception(
                    "[AUTOSCOUT_PREUPLOAD] Errore AS24 | media_id=%s",
                    r["media_id"],
                )
                continue

            except requests.RequestException:
                logger.exception(
                    "[AUTOSCOUT_PREUPLOAD] Errore download | media_id=%s",
                    r["media_id"],
                )
                continue

        # ------------------------------------------------------------
        # 5️⃣ Attach immagini al listing (PUT)
        # ------------------------------------------------------------
        if image_ids:
            update_listing_images(
                customer_id=customer_id,
                listing_id=listing_id_remote,
                image_ids=image_ids,
            )
            logger.info(
                "[AUTOSCOUT_PREUPLOAD] Immagini agganciate | count=%d listing_id=%s",
                len(image_ids),
                listing_id_remote,
            )
        else:
            logger.info(
                "[AUTOSCOUT_PREUPLOAD] Nessuna immagine valida da agganciare | id_auto=%s",
                id_auto,
            )

        session.commit()

    except Exception as exc:
        session.rollback()
        logger.exception("[AUTOSCOUT_PREUPLOAD] ERRORE JOB")

        try:
            session.execute(
                text("""
                    UPDATE autoscout_listings
                    SET
                        status = 'ERROR',
                        last_error = :error,
                        retry_count = retry_count + 1,
                        updated_at = now()
                    WHERE id = :id
                """),
                {
                    "error": str(exc),
                    "id": listing_pk,
                },
            )
            session.commit()
        except SQLAlchemyError:
            session.rollback()
            logger.exception(
                "[AUTOSCOUT_PREUPLOAD] Errore nel salvataggio stato ERROR"
            )

    finally:
        session.close()


# ============================================================
# ENTRYPOINT
# ============================================================

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("[AUTOSCOUT_PREUPLOAD] Avvio manuale job")
    autoscout_preupload_images_job()
