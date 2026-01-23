import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import SessionLocal

from app.external.autoscout import (
    resolve_customer_id,
    create_listing,
    delete_listing,
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
            # ------------------------------------------------------------
            # 🗑️ DELETE REQUIRED — auto venduta
            # ------------------------------------------------------------
            if listing["status"] == "DELETE_REQUIRED":

                # 🔴 carica config dealer (serve per test_mode + sellId)
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
                    raise RuntimeError("Configurazione AutoScout dealer mancante (DELETE)")

                sell_id = config["customer_id"]
                customer_id = resolve_customer_id(sell_id)

                listing_id_remote = listing.get("listing_id")

                if not listing_id_remote:
                    logger.info(
                        "[AUTOSCOUT_DELETE] Nessun listing remoto da eliminare | id_auto=%s",
                        id_auto,
                    )

                    session.execute(
                        text("""
                            DELETE FROM autoscout_listings
                            WHERE id = :id
                        """),
                        {"id": listing_id},
                    )
                    session.commit()
                    continue

                logger.info(
                    "[AUTOSCOUT_DELETE] DELETE listing AS24 | listing_id=%s test_mode=%s",
                    listing_id_remote,
                    config["test_mode"],
                )

                delete_listing(
                    customer_id=customer_id,
                    listing_id=listing_id_remote,
                    test_mode=config["test_mode"],
                )

                session.execute(
                    text("""
                        DELETE FROM autoscout_listings
                        WHERE id = :id
                    """),
                    {"id": listing_id},
                )

                session.commit()
                continue


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

                alloy_wheel_size = auto.get("alloy_wheel_size")

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
                # 3️⃣.5️⃣ Carica dettagli MNET BASE (vista unificata) — C e X
                # ------------------------------------------------------------
                det_base = session.execute(
                    text("""
                        SELECT *
                        FROM v_mnet_dettagli_unificati
                        WHERE codice_motornet_uni = :codice
                    """),
                    {"codice": auto["codice_motornet"]},
                ).mappings().first()



                if not det_base:
                    raise RuntimeError(
                        f"Dettagli Motornet non trovati (v_mnet_dettagli_unificati): codice={auto['codice_motornet']}"
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
                # 5️.1 Resolve Mapping AutoScout24 (make / model / vehicle type)
                # ------------------------------------------------------------
                mapping = session.execute(
                    text("""
                        SELECT
                            as24_make_id,
                            as24_model_id,
                            as24_vehicle_type
                        FROM public.autoscout_model_map_v2
                        WHERE codice_motornet_uni = :codice
                    """),
                    {"codice": auto["codice_motornet"]},
                ).mappings().first()

                if not mapping:
                    raise RuntimeError("Mapping AutoScout24 mancante")

                if mapping["as24_vehicle_type"] not in ("C", "X"):
                    raise RuntimeError("as24_vehicle_type non valido o mancante")

                as24_make_id = mapping["as24_make_id"]
                as24_model_id = mapping["as24_model_id"]

                if not as24_make_id or not as24_model_id:
                    raise RuntimeError("Mapping AutoScout24 incompleto (make/model)")

                # ------------------------------------------------------------
                # 5.2️⃣ Guardia coerenza catalog Motornet vs mapping AS24
                # ------------------------------------------------------------
                if mapping["as24_vehicle_type"] == "C" and det_base["catalog"] != "auto":
                    raise RuntimeError(
                        "Mismatch Motornet catalog vs AS24 vehicle_type (atteso auto)"
                    )

                if mapping["as24_vehicle_type"] == "X" and det_base["catalog"] not in ("vic",):
                    raise RuntimeError(
                        "Mismatch Motornet catalog vs AS24 vehicle_type (atteso vic)"
                    )


                # ------------------------------------------------------------
                # 5.5️⃣ Arricchimento AUTO (obbligatorio per C)
                # ------------------------------------------------------------
                det_auto = None

                if mapping["as24_vehicle_type"] == "C":
                    det_auto = session.execute(
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

                    if not det_auto:
                        raise RuntimeError(
                            "Dettagli AUTO mancanti (mnet_dettagli_usato)"
                        )

                logger.info(
                    "[AUTOSCOUT_CTX] vehicle_type=%s catalog=%s codice=%s",
                    mapping["as24_vehicle_type"],
                    det_base["catalog"],
                    auto["codice_motornet"],
                )

                # ------------------------------------------------------------
                # 5.6️⃣ Resolve dati tecnici veicolo (normalizzazione robusta)
                # ------------------------------------------------------------
                def _to_int(val):
                    try:
                        if val is None:
                            return None
                        return int(str(val).strip())
                    except (ValueError, TypeError):
                        return None
                
                as24_power = None
                as24_cylinder_capacity = None
                as24_cylinder_count = None
                as24_empty_weight = None
                as24_seat_count = None
                as24_door_count = None

                if mapping["as24_vehicle_type"] == "C":

                    as24_power = _to_int(det_auto["kw"])
                    as24_cylinder_capacity = _to_int(det_auto["cilindrata"])
                    as24_cylinder_count = _to_int(det_auto["cilindri"])
                    as24_empty_weight = _to_int(det_auto["peso_vuoto"])
                    as24_seat_count = _to_int(det_auto["posti"])
                    as24_door_count = _to_int(det_auto["porte"])

                last_service_date = auto.get("data_ultimo_intervento")
                as24_last_service_date = (
                    last_service_date.strftime("%Y-%m")
                    if last_service_date
                    else None
                )

                description = usatoin.get("descrizione")
                as24_description = description.strip() if description and description.strip() else None
               
                # ------------------------------------------------------------
                # 5.6.x️⃣ Resolve modelVersion AutoScout24 (ALIAS → MNET)
                # ------------------------------------------------------------
                as24_model_version = None

                alias_allestimento = usatoin.get("alias_allestimento")
                mnet_allestimento = det_base.get("allestimento") if det_base else None

                if alias_allestimento and alias_allestimento.strip():
                    as24_model_version = alias_allestimento.strip()
                elif mnet_allestimento and mnet_allestimento.strip():
                    as24_model_version = mnet_allestimento.strip()

                logger.info(
                    "[AUTOSCOUT_MODEL_VERSION] alias=%s mnet=%s resolved=%s",
                    alias_allestimento,
                    mnet_allestimento,
                    as24_model_version,
                )

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
                # 5.6️⃣ Resolve bodyType AutoScout24 (DB-driven, production-safe)
                # ------------------------------------------------------------
                as24_bodytype_id = None
                as24_primary_fuel_type = None
                as24_fuel_category = None
                as24_transmission = None

                
                if mapping["as24_vehicle_type"] == "C":

                    mnet_tipo = det_auto["tipo"]
                    mnet_segmento = det_auto["segmento"]


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
                # 5.6.1️⃣ BodyType AutoScout24 per VIC (vehicleType = X)
                # ------------------------------------------------------------
                if mapping["as24_vehicle_type"] == "X":
                    # AS24 reference: bodyType 7 = "Altro" è valido per X
                    as24_bodytype_id = 7
                # ------------------------------------------------------------
                # 5.6.y️⃣ Resolve Drivetrain AutoScout24 (from Motornet)
                # ------------------------------------------------------------

                def map_mnet_trazione_to_as24(trazione: str | None) -> str | None:
                    if not trazione:
                        return None

                    return {
                        "Anteriore": "F",
                        "Posteriore": "R",
                        "Integrale": "4",
                    }.get(trazione)


                as24_drivetrain = None

                if mapping["as24_vehicle_type"] == "C":
                    as24_drivetrain = map_mnet_trazione_to_as24(det_base.get("trazione"))

                    if as24_drivetrain:
                        logger.info(
                            "[AUTOSCOUT_DRIVETRAIN] trazione MNET='%s' → AS24='%s'",
                            det_base.get("trazione"),
                            as24_drivetrain,
                        )
                    else:
                        logger.info(
                            "[AUTOSCOUT_DRIVETRAIN] trazione MNET='%s' non mappata → campo escluso",
                            det_base.get("trazione"),
                        )

                # ------------------------------------------------------------
                # 5.7️⃣ Resolve Fuel + Transmission AutoScout24 (solo C)
                # ------------------------------------------------------------
                if mapping["as24_vehicle_type"] == "C":

                    mnet_alimentazione = det_auto["alimentazione"]

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

                    mnet_cambio = det_auto["cambio"]

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

                ALLOWED_AS24_IMAGE_TYPES = {
                    "image/jpeg",
                    "image/png",
                    "image/gif",
                }


                for idx, r in enumerate(rows, start=1):
                    try:
                        resp = requests.get(r["media_url"], timeout=15)
                        resp.raise_for_status()

                        content_type = resp.headers.get("Content-Type", "").split(";")[0].lower()

                        if content_type not in ALLOWED_AS24_IMAGE_TYPES:
                            logger.warning(
                                "[AUTOSCOUT_CREATE] Media saltato (content-type non valido AS24) | media_id=%s type=%s",
                                r["media_id"],
                                content_type,
                            )
                            continue

                        image_id = upload_image(
                            customer_id=customer_id,
                            image_bytes=resp.content,
                            content_type=content_type,
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

                # ------------------------------------------------------------
                # 5.8.1️⃣ Normalizzazione payload per vehicleType = X
                # ------------------------------------------------------------
                if mapping["as24_vehicle_type"] == "X":
                    as24_primary_fuel_type = None
                    as24_fuel_category = None
                    as24_transmission = None


                payload = build_minimal_payload(
                    auto=auto,
                    usatoin=usatoin,
                    as24_make_id=as24_make_id,
                    as24_model_id=as24_model_id,
                    as24_model_version=as24_model_version,

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
                    as24_drivetrain=as24_drivetrain,


                    # Equipment
                    as24_equipment_ids=as24_equipment_ids,
                    alloy_wheel_size=alloy_wheel_size,
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
                err_str = str(exc)

                # ------------------------------------------------------------
                # 🩹 RIPARAZIONE AUTOMATICA:
                # listing cancellato su AS24 → serve CREATE, non PUT
                # ------------------------------------------------------------
                if "listing-does-not-exist" in err_str:
                    logger.warning(
                        "[AUTOSCOUT_REPAIR] Listing non esistente su AS24, forzo CREATE | listing_id=%s",
                        listing_id,
                    )

                    session.execute(
                        text("""
                            UPDATE autoscout_listings
                            SET
                                status = 'PENDING_CREATE',
                                listing_id = NULL,
                                last_error = :error,
                                requested_at = now(),
                                retry_count = 0
                            WHERE id = :id
                        """),
                        {
                            "id": listing_id,
                            "error": err_str,
                        },
                    )
                    session.commit()
                    continue

                # ------------------------------------------------------------
                # ❌ ERRORE GENERICO
                # ------------------------------------------------------------
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
                            "error": err_str,
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
    autoscout_sync_job()


