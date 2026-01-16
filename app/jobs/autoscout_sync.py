import logging
import json
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import SessionLocal

from app.external.autoscout import (
    resolve_customer_id,
    create_listing,
    AutoScoutClientError,
)
import requests
from app.external.autoscout import upload_image, update_listing

from app.external.autoscout_payload import build_minimal_payload


logger = logging.getLogger(__name__)

BATCH_SIZE = 5 


# ============================================================
# JOB: AUTOSCOUT CREATE (STEP B)
# ============================================================

def autoscout_sync_job():
    session = SessionLocal()

    try:
        # ------------------------------------------------------------
        # 1️⃣ Preleva batch record in PENDING_CREATE (lock-safe)
        # ------------------------------------------------------------
        listings = session.execute(
            text("""
                SELECT *
                FROM autoscout_listings
                WHERE status IN (
                    'PENDING_CREATE',
                    'UPDATE_REQUIRED',
                    'DELETE_REQUIRED'
                )
                ORDER BY requested_at
                FOR UPDATE SKIP LOCKED
                LIMIT :limit
            """),
            {"limit": BATCH_SIZE},
        ).mappings().all()

        if not listings:
            logger.info("[AUTOSCOUT_CREATE] Nessun record PENDING_CREATE")
            session.commit()
            return

        for listing in listings:
            listing_id = listing["id"]
            dealer_id = listing["dealer_id"]
            id_auto = listing["id_auto"]

            logger.info(
                "[AUTOSCOUT_CREATE] Preso record | listing_id=%s dealer_id=%s id_auto=%s",
                listing_id,
                dealer_id,
                id_auto,
            )

            try:
                # ------------------------------------------------------------
                # 2️⃣ Carica auto tecnica
                # ------------------------------------------------------------
                auto = session.execute(
                    text("""
                        SELECT *
                        FROM azlease_usatoauto
                        WHERE id = :id_auto
                    """),
                    {"id_auto": id_auto},
                ).mappings().first()

                if not auto:
                    raise RuntimeError("Auto tecnica non trovata")

                # ------------------------------------------------------------
                # 3️⃣ Carica contesto commerciale (usatoin)
                # ------------------------------------------------------------
                usatoin = session.execute(
                    text("""
                        SELECT *
                        FROM azlease_usatoin
                        WHERE id = :id_usatoin
                    """),
                    {"id_usatoin": auto["id_usatoin"]},
                ).mappings().first()

                if not usatoin:
                    raise RuntimeError("Contesto usatoin non trovato")

                # ------------------------------------------------------------
                # 3.5️⃣ Carica dettagli MNET (bodyType, fuel, transmission)
                # ------------------------------------------------------------
                det = session.execute(
                    text("""
                        SELECT
                            tipo,
                            segmento,
                            alimentazione,
                            cambio,
                            kw,
                            cilindrata,
                            cilindri,
                            peso_vuoto,
                            posti,
                            porte
                        FROM mnet_dettagli_usato
                        WHERE codice_motornet_uni = :codice
                    """),
                    {"codice": auto["codice_motornet"]},
                ).mappings().first()

                if not det:
                    raise RuntimeError(
                        f"Dettagli MNET non trovati (mnet_dettagli_usato): codice={auto['codice_motornet']}"
                    )

                # ------------------------------------------------------------
                # 3.6️⃣ Resolve dati tecnici veicolo (normalizzazione robusta)
                # ------------------------------------------------------------
                def _to_int(val):
                    try:
                        if val is None:
                            return None
                        return int(str(val).strip())
                    except (ValueError, TypeError):
                        return None

                as24_power = _to_int(det.get("kw"))
                as24_cylinder_capacity = _to_int(det.get("cilindrata"))
                as24_cylinder_count = _to_int(det.get("cilindri"))
                as24_empty_weight = _to_int(det.get("peso_vuoto"))
                as24_seat_count = _to_int(det.get("posti"))
                as24_door_count = _to_int(det.get("porte"))

                last_service_date = auto.get("data_ultimo_intervento")
                as24_last_service_date = (
                    last_service_date.strftime("%Y-%m")
                    if last_service_date
                    else None
                )

                description = usatoin.get("descrizione")
                as24_description = description.strip() if description and description.strip() else None

                logger.info(
                    "[AUTOSCOUT_DEBUG_TECH] power=%s cyl_cap=%s cyl=%s weight=%s seats=%s doors=%s",
                    as24_power,
                    as24_cylinder_capacity,
                    as24_cylinder_count,
                    as24_empty_weight,
                    as24_seat_count,
                    as24_door_count,
                )

                # ------------------------------------------------------------
                # 4️⃣ Config dealer
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

                # ------------------------------------------------------------
                # 5️⃣ Resolve customerId from sellId
                # ------------------------------------------------------------
                sell_id = config["customer_id"]
                customer_id = resolve_customer_id(sell_id)

                # ------------------------------------------------------------
                # 5.5 Create listing AutoScout24
                # ------------------------------------------------------------
                mapping = session.execute(
                    text("""
                        SELECT
                            as24_make_id,
                            as24_model_id
                        FROM autoscout_model_map_v2
                        WHERE codice_motornet_uni = :codice
                    """),
                    {"codice": auto["codice_motornet"]},
                ).mappings().first()

                if not mapping:
                    raise RuntimeError("Mapping AutoScout24 non trovato (worker)")

                as24_make_id = mapping["as24_make_id"]
                as24_model_id = mapping["as24_model_id"]

                if not as24_make_id or not as24_model_id:
                    raise RuntimeError("Mapping AutoScout24 incompleto (worker)")

                # ------------------------------------------------------------
                # 5.6️⃣ Resolve bodyType AutoScout24 (DB-driven, production-safe)
                # ------------------------------------------------------------

                mnet_tipo = det.get("tipo")
                mnet_segmento = det.get("segmento")


                as24_bodytype_id = None

                if mnet_tipo is not None:
                    bodytype_row = session.execute(
                        text("""
                            SELECT as24_bodytype_id
                            FROM autoscout_bodytype_map
                            WHERE mnet_tipo = :mnet_tipo
                        """),
                        {"mnet_tipo": mnet_tipo},
                    ).mappings().first()

                    if not bodytype_row:
                        raise RuntimeError(
                            f"BodyType MNET non mappato (autoscout_bodytype_map): tipo={mnet_tipo}"
                        )

                    as24_bodytype_id = bodytype_row["as24_bodytype_id"]

                else:
                    # Fallback guidato per tipo NULL
                    if mnet_segmento in ("Pick-up", "Fuoristrada"):
                        as24_bodytype_id = 4  # SUV/Fuoristrada/Pick-up
                    else:
                        as24_bodytype_id = 7  # Altro

                if not as24_bodytype_id:
                    raise RuntimeError(
                        f"BodyType AutoScout24 non risolto | tipo={mnet_tipo} segmento={mnet_segmento}"
                    )
    
                # ------------------------------------------------------------
                # 5.7️⃣ Resolve Fuel AutoScout24 (primaryFuelType + fuelCategory)
                # ------------------------------------------------------------

                mnet_alimentazione = det.get("alimentazione")

                if not mnet_alimentazione:
                    raise RuntimeError("Alimentazione MNET mancante")

                fuel_row = session.execute(
                    text("""
                        SELECT
                            as24_primary_fuel_type,
                            as24_fuel_category
                        FROM autoscout_fuel_map
                        WHERE mnet_alimentazione = :alimentazione
                    """),
                    {"alimentazione": mnet_alimentazione},
                ).mappings().first()

                if not fuel_row:
                    raise RuntimeError(
                        f"Fuel MNET non mappato (autoscout_fuel_map): alimentazione={mnet_alimentazione}"
                    )

                as24_primary_fuel_type = fuel_row["as24_primary_fuel_type"]
                as24_fuel_category = fuel_row["as24_fuel_category"]

                if not as24_primary_fuel_type or not as24_fuel_category:
                    raise RuntimeError(
                        f"Fuel AutoScout24 non risolto | alimentazione={mnet_alimentazione}"
                    )

                # ------------------------------------------------------------
                # 5.8️⃣ Resolve Transmission AutoScout24 (AS24 enum)
                # ------------------------------------------------------------

                mnet_cambio = det.get("cambio")

                if mnet_cambio in ("Manuale", "Manuale sequenziale", "Sequenziale"):
                    as24_transmission = "M"
                elif mnet_cambio in (
                    "Automatico",
                    "Automatico sequenziale",
                    "Automatico doppia frizione",
                    "CVT",
                ):
                    as24_transmission = "A"
                else:
                    # Fallback conservativo
                    as24_transmission = "M"

                # ------------------------------------------------------------
                # 5.9️⃣ Load Equipment AutoScout24 (DB-driven, definitivo)
                # ------------------------------------------------------------

                equipment_rows = session.execute(
                    text("""
                        SELECT DISTINCT as24_equipment_id
                        FROM public.autousato_equipaggiamenti
                        WHERE id_auto = :id_auto
                            AND presente = true
                            AND as24_equipment_id IS NOT NULL
                    """),
                    {"id_auto": id_auto},
                ).fetchall()

                as24_equipment_ids = [row[0] for row in equipment_rows]

                if not as24_equipment_ids:
                    logger.info(
                        "[AUTOSCOUT_CREATE] Nessun equipment AS24 per auto %s",
                        id_auto,
                    )
                else:
                    logger.info(
                        "[AUTOSCOUT_CREATE] Equipment AS24 (%d): %s",
                        len(as24_equipment_ids),
                        as24_equipment_ids,
                    )

                # ------------------------------------------------------------
                # 5.x️⃣ Resolve Full Service History (AS24)
                # ------------------------------------------------------------

                cronologia_tagliandi = auto.get("cronologia_tagliandi")

                as24_has_full_service_history = None
                if cronologia_tagliandi is True:
                    as24_has_full_service_history = True
                elif cronologia_tagliandi is False:
                    as24_has_full_service_history = False
 

                # ------------------------------------------------------------
                # 5.10️⃣ Pre-upload immagini AS24 (C: dentro CREATE)
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

                image_ids = []

                for idx, r in enumerate(rows, start=1):
                    try:
                        resp = requests.get(r["media_url"], timeout=15)
                        resp.raise_for_status()

                        image_id = upload_image(
                            customer_id=customer_id,
                            image_bytes=resp.content,
                            content_type=resp.headers.get("Content-Type", "image/jpeg"),
                            test_mode=config["test_mode"],
                        )

                        image_ids.append(image_id)

                        logger.info(
                            "[AUTOSCOUT_CREATE] Pre-upload image OK (%d/%d) | media_id=%s",
                            idx,
                            len(rows),
                            r["media_id"],
                        )

                    except AutoScoutClientError:
                        logger.exception(
                            "[AUTOSCOUT_CREATE] Errore AS24 pre-upload | media_id=%s",
                            r["media_id"],
                        )
                        continue

                    except requests.RequestException:
                        logger.exception(
                            "[AUTOSCOUT_CREATE] Errore download immagine | media_id=%s",
                            r["media_id"],
                        )
                        continue



                payload = build_minimal_payload(
                    auto=auto,
                    usatoin=usatoin,
                    as24_make_id=as24_make_id,
                    as24_model_id=as24_model_id,
                    as24_bodytype_id=as24_bodytype_id,
                    as24_primary_fuel_type=as24_primary_fuel_type,
                    as24_fuel_category=as24_fuel_category,
                    as24_transmission=as24_transmission,

                    # Dati tecnici
                    as24_power=as24_power,
                    as24_cylinder_capacity=as24_cylinder_capacity,
                    as24_cylinder_count=as24_cylinder_count,
                    as24_empty_weight=as24_empty_weight,
                    as24_seat_count=as24_seat_count,
                    as24_door_count=as24_door_count,
                    as24_last_service_date=as24_last_service_date,
                    as24_description=as24_description,

                    # Equipment
                    as24_equipment_ids=as24_equipment_ids,

                    as24_has_full_service_history=as24_has_full_service_history,
                )

                if "publication" not in payload:
                    payload["publication"] = {}

                payload["publication"]["status"] = (
                    "Active" if usatoin.get("visibile") else "Inactive"
                )
                if image_ids:
                    payload["images"] = [{"id": img_id} for img_id in image_ids]  

                listing_id_remote = listing.get("listing_id")
                if listing_id_remote:
                    logger.info(
                        "[AUTOSCOUT_UPSERT] UPDATE listing AS24 | listing_id=%s",
                        listing_id_remote,
                    )

                    update_listing(
                        customer_id=customer_id,
                        listing_id=listing_id_remote,
                        payload=payload,
                        test_mode=config["test_mode"],
                    )

                else:
                    logger.info(
                        "[AUTOSCOUT_UPSERT] CREATE listing AS24 | id_auto=%s",
                        id_auto,
                    )

                    listing_id_remote = create_listing(
                        customer_id=customer_id,
                        payload=payload,
                        test_mode=config["test_mode"],
                    )

                    session.execute(
                        text("""
                            UPDATE autoscout_listings
                            SET listing_id = :listing_id
                            WHERE id = :id
                        """),
                        {"listing_id": listing_id_remote, "id": listing_id},
                    )



        
                # ------------------------------------------------------------
                # 6️⃣ Update stato → CREATED
                # ------------------------------------------------------------
                session.execute(
                    text("""
                        UPDATE autoscout_listings
                        SET
                            listing_id = :listing_id,
                            status = 'PUBLISHED',
                            last_attempt_at = now(),
                            retry_count = 0
                        WHERE id = :id

                    """),
                    {
                        "listing_id": listing_id_remote,
                        "id": listing_id,
                    },
                )

                session.commit()

            except Exception as exc:
                session.rollback()
                logger.exception(
                    "[AUTOSCOUT_CREATE] ERRORE su listing_id=%s",
                    listing_id,
                )

                try:
                    session.execute(
                        text("""
                            UPDATE autoscout_listings
                            SET
                                status = 'ERROR',
                                last_error = :error,
                                retry_count = retry_count + 1,
                                last_attempt_at = :now
                            WHERE id = :id
                        """),
                        {
                            "error": str(exc),
                            "now": datetime.utcnow(),
                            "id": listing_id,
                        },
                    )
                    session.commit()
                except SQLAlchemyError:
                    session.rollback()
                    logger.exception(
                        "[AUTOSCOUT_CREATE] Errore nel salvataggio stato ERROR | listing_id=%s",
                        listing_id,
                    )

                continue

    except Exception:
        logger.exception("[AUTOSCOUT_CREATE] ERRORE FATALE JOB")
        session.rollback()

    finally:
        session.close()


# ---- fine del file, FUORI dalla funzione ----

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    logger.info("[AUTOSCOUT_CREATE] Avvio manuale job")
    autoscout_create_job()


