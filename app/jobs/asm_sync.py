"""AutoSuperMarket (ASM) — sync job per annunci usato.

Polling DB queue: PENDING_CREATE / UPDATE_REQUIRED / DELETE_REQUIRED
Pattern identico a autoscout_sync.py ma senza model mapping e image pre-upload.
"""

import logging
from datetime import datetime

from sqlalchemy import text

from app.database import SessionLocal
from app.external.autosupermarket import (
    create_listing,
    update_listing,
    delete_listing,
    AsmClientError,
)
from app.external.autosupermarket_payload import build_asm_payload

logger = logging.getLogger(__name__)

BATCH_SIZE = 10


def asm_sync_job():
    session = SessionLocal()

    try:
        # ============================================================
        # 0. DELETE_REQUIRED — priorità assoluta
        # ============================================================
        delete_rows = session.execute(
            text("""
                SELECT *
                FROM asm_listings
                WHERE status = 'DELETE_REQUIRED'
                ORDER BY requested_at
                FOR UPDATE SKIP LOCKED
                LIMIT :limit
            """),
            {"limit": BATCH_SIZE},
        ).mappings().all()

        for row in delete_rows:
            row_id = row["id"]
            asm_listing_id = row.get("asm_listing_id")

            # Carica config dealer
            config = session.execute(
                text("""
                    SELECT *
                    FROM asm_dealer_config
                    WHERE dealer_id = :dealer_id AND enabled = true
                """),
                {"dealer_id": row["dealer_id"]},
            ).mappings().first()

            if not config:
                logger.warning("[ASM_DELETE] Config mancante, elimino record locale | id=%s", row_id)
                session.execute(text("DELETE FROM asm_listings WHERE id = :id"), {"id": row_id})
                session.commit()
                continue

            if config.get("disable_asm_listing_sync"):
                logger.info("[ASM_DELETE] Skip API (solo pubblicazione) | id=%s", row_id)
                session.execute(text("DELETE FROM asm_listings WHERE id = :id"), {"id": row_id})
                session.commit()
                continue

            if asm_listing_id:
                try:
                    delete_listing(token=config["api_token"], listing_id=asm_listing_id)
                except AsmClientError as exc:
                    logger.warning("[ASM_DELETE] Errore API | id=%s err=%s", row_id, exc)
                    session.execute(
                        text("""
                            UPDATE asm_listings
                            SET status = 'ERROR', last_error = :err,
                                retry_count = retry_count + 1, last_attempt_at = NOW()
                            WHERE id = :id
                        """),
                        {"id": row_id, "err": str(exc)},
                    )
                    session.commit()
                    continue

            session.execute(text("DELETE FROM asm_listings WHERE id = :id"), {"id": row_id})
            session.commit()

        # ============================================================
        # 1. PENDING_CREATE + UPDATE_REQUIRED
        # ============================================================
        listings = session.execute(
            text("""
                SELECT *
                FROM asm_listings
                WHERE status IN ('PENDING_CREATE', 'UPDATE_REQUIRED')
                ORDER BY requested_at
                FOR UPDATE SKIP LOCKED
                LIMIT :limit
            """),
            {"limit": BATCH_SIZE},
        ).mappings().all()

        if not listings:
            session.commit()
            return

        for listing in listings:
            row_id = listing["id"]
            dealer_id = listing["dealer_id"]
            id_auto = listing["id_auto"]
            asm_listing_id = listing.get("asm_listing_id")

            logger.info("[ASM_SYNC] Processing | id=%s dealer=%s auto=%s status=%s",
                        row_id, dealer_id, id_auto, listing["status"])

            # Skip UPDATE se sync disabilitato
            if listing["status"] == "UPDATE_REQUIRED":
                skip_cfg = session.execute(
                    text("""
                        SELECT COALESCE(disable_asm_listing_sync, false) AS v
                        FROM asm_dealer_config
                        WHERE dealer_id = :dealer_id AND enabled = true
                    """),
                    {"dealer_id": dealer_id},
                ).mappings().first()

                if skip_cfg and skip_cfg.get("v"):
                    logger.info("[ASM_SYNC] Skip UPDATE (solo pubblicazione) | id=%s", row_id)
                    session.execute(
                        text("""
                            UPDATE asm_listings
                            SET status = 'PUBLISHED', last_attempt_at = NOW(), last_error = NULL
                            WHERE id = :id
                        """),
                        {"id": row_id},
                    )
                    session.commit()
                    continue

            try:
                # Carica auto
                auto = session.execute(
                    text("SELECT * FROM azlease_usatoauto WHERE id = :id"),
                    {"id": id_auto},
                ).mappings().first()

                if not auto:
                    raise RuntimeError("Auto tecnica non trovata")

                # Carica usatoin
                usatoin = session.execute(
                    text("SELECT * FROM azlease_usatoin WHERE id = :id"),
                    {"id": auto["id_usatoin"]},
                ).mappings().first()

                if not usatoin:
                    raise RuntimeError("Contesto usatoin non trovato")

                # Carica dettagli Motornet (vista unificata)
                det_base = session.execute(
                    text("""
                        SELECT * FROM v_mnet_dettagli_unificati
                        WHERE codice_motornet_uni = :codice
                    """),
                    {"codice": auto["codice_motornet"]},
                ).mappings().first()

                if not det_base:
                    raise RuntimeError(f"Dettagli Motornet non trovati: {auto['codice_motornet']}")

                # Dettagli AUTO (solo per catalog=auto)
                det_auto = None
                if det_base.get("catalog") == "auto":
                    det_auto = session.execute(
                        text("""
                            SELECT tipo, alimentazione, cambio, kw, cilindrata,
                                   cilindri, peso_vuoto, posti, porte
                            FROM mnet_dettagli_usato
                            WHERE codice_motornet = :codice
                        """),
                        {"codice": auto["codice_motornet"]},
                    ).mappings().first()

                # Config ASM dealer
                config = session.execute(
                    text("""
                        SELECT * FROM asm_dealer_config
                        WHERE dealer_id = :dealer_id AND enabled = true
                    """),
                    {"dealer_id": dealer_id},
                ).mappings().first()

                if not config:
                    raise RuntimeError("Configurazione ASM dealer mancante")

                # Immagini dalla vetrina
                images_rows = session.execute(
                    text("""
                        SELECT media_url
                        FROM usato_vetrina
                        WHERE id_auto = :id_auto
                        ORDER BY sort_order, created_at
                    """),
                    {"id_auto": id_auto},
                ).fetchall()

                image_urls = [r[0] for r in images_rows if r[0]] if images_rows else []

                # Build payload
                payload = build_asm_payload(
                    auto=dict(auto),
                    usatoin=dict(usatoin),
                    det_base=dict(det_base),
                    det_auto=dict(det_auto) if det_auto else None,
                    dealer_asm_id=config["dealer_asm_id"],
                    images=image_urls or None,
                )

                # CREATE o UPDATE
                if not asm_listing_id:
                    # POST → crea annuncio
                    new_id = create_listing(token=config["api_token"], payload=payload)
                    session.execute(
                        text("""
                            UPDATE asm_listings
                            SET status = 'PUBLISHED', asm_listing_id = :asm_id,
                                last_attempt_at = NOW(), last_error = NULL, retry_count = 0
                            WHERE id = :id
                        """),
                        {"id": row_id, "asm_id": new_id},
                    )
                    logger.info("[ASM_SYNC] Creato listing ASM | id=%s asm_listing_id=%s", row_id, new_id)
                else:
                    # PATCH → aggiorna annuncio
                    update_listing(token=config["api_token"], listing_id=asm_listing_id, payload=payload)
                    session.execute(
                        text("""
                            UPDATE asm_listings
                            SET status = 'PUBLISHED', last_attempt_at = NOW(),
                                last_error = NULL, retry_count = 0
                            WHERE id = :id
                        """),
                        {"id": row_id},
                    )
                    logger.info("[ASM_SYNC] Aggiornato listing ASM | id=%s asm_listing_id=%s", row_id, asm_listing_id)

                session.commit()

            except Exception as exc:
                logger.exception("[ASM_SYNC] Errore processing | id=%s", row_id)
                session.rollback()
                session.execute(
                    text("""
                        UPDATE asm_listings
                        SET status = 'ERROR', last_error = :err,
                            retry_count = retry_count + 1, last_attempt_at = NOW()
                        WHERE id = :id
                    """),
                    {"id": row_id, "err": str(exc)[:500]},
                )
                session.commit()

    except Exception:
        logger.exception("[ASM_SYNC] Errore fatale nel job")
        session.rollback()
    finally:
        session.close()


if __name__ == "__main__":
    asm_sync_job()
