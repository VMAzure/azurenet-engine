import csv
import io
import logging
from datetime import datetime

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from app.database import engine
from app.storage import download_bytes

logger = logging.getLogger(__name__)

from datetime import datetime

import re

def normalize_cod_versione_cm(raw: str | None) -> str | None:
    """
    Normalizza il codice versione CM:
    - None / vuoto → None
    - strip
    - uppercase
    - rimuove tutto tranne A–Z0–9
    """
    if not raw:
        return None

    raw = str(raw).strip().upper()
    norm = re.sub(r'[^A-Z0-9]', '', raw)
    return norm or None


def _parse_date(value):
    """
    Normalizza date CSV italiane (DD/MM/YYYY) → date ISO.
    Ritorna None se vuoto o invalido.
    """
    try:
        if not value:
            return None
        return datetime.strptime(value.strip(), "%d/%m/%Y").date()
    except Exception:
        return None

def vehicle_stock_csv_import_job():
    """
    Job schedulato:
    - prende 1 CSV pending
    - lo importa in vehicles_stock_sale
    - aggiorna vehicle_stock_imports
    """

    logger.info("[CSV IMPORT] job start")

    with engine.begin() as conn:
        # --------------------------------------------------
        # 1️⃣ Trova un import pending
        # --------------------------------------------------
        import_row = conn.execute(
            text("""
                select *
                from public.vehicle_stock_imports
                where status = 'pending'
                order by created_at
                limit 1
            """)
        ).mappings().first()

        if not import_row:
            logger.debug("[CSV IMPORT] no pending import found")
            return

        import_id = import_row["id"]
        file_path = import_row["file_path"]

        logger.info(f"[CSV IMPORT] processing import {import_id}")

        # --------------------------------------------------
        # 2️⃣ Lock logico
        # --------------------------------------------------
        conn.execute(
            text("""
                update public.vehicle_stock_imports
                set status = 'processing'
                where id = :id
            """),
            {"id": import_id},
        )

    # --------------------------------------------------
    # 3️⃣ Scarica CSV (fuori dalla transazione)
    # --------------------------------------------------
    try:
        csv_bytes = download_bytes(
            bucket="vehicle-stock-imports",
            path=file_path,
        )
    except Exception as e:
        _fail_import(import_id, f"CSV download failed: {e}")
        return

    # --------------------------------------------------
    # 4️⃣ Parse CSV
    # --------------------------------------------------
    rows_total = 0
    rows_inserted = 0
    rows_updated = 0
    rows_skipped = 0

    try:
        csv_file = io.StringIO(csv_bytes.decode("utf-8-sig"))
        reader = csv.DictReader(csv_file)
    except Exception as e:
        _fail_import(import_id, f"CSV parse failed: {e}")
        return

    with engine.begin() as conn:
        for row in reader:
            rows_total += 1

            if not any(row.values()):
                rows_skipped += 1
                continue


            try:
                external_id = row.get("ID MyGarage")
                if not external_id:
                    rows_skipped += 1
                    continue

                with conn.begin_nested():
                    result = conn.execute(
                        text("""
                            insert into public.vehicles_stock_sale (
                                external_id,
                                vid,
                                targa,
                                vin,
                                source,
                                raw_linea,
                                raw_status,
                                raw_stato,
                                cod_versione_cm,
                                brand,
                                model,
                                version,
                                description,
                                vehicle_category,
                                kilometers,
                                first_registration_date,
                                fuel_type,
                                body_type,
                                color_ext,
                                color_int,
                                price_public,
                                price_showroom,
                                price_internal,
                                location,
                                arrival_date,
                                expected_arrival_date,
                                images_count,
                                main_image_url,
                                last_import_id,
                                last_seen_at,
                                is_active,
                                dealer,
                                order_number,
                                stock_flag,
                                vehicle_type_raw,
                                price_reserved_1,
                                price_reserved_2,
                                confirmation_week

                            )
                            values (
                                :external_id,
                                :vid,
                                :targa,
                                :vin,
                                'mygarage',
                                :raw_linea,
                                :raw_status,
                                :raw_stato,
                                :cod_versione_cm,
                                :brand,
                                :model,
                                :version,
                                :description,
                                :vehicle_category,
                                :kilometers,
                                :first_registration_date,
                                :fuel_type,
                                :body_type,
                                :color_ext,
                                :color_int,
                                :price_public,
                                :price_showroom,
                                :price_internal,
                                :location,
                                :arrival_date,
                                :expected_arrival_date,
                                :images_count,
                                :main_image_url,
                                :last_import_id,
                                now(),
                                true,
                                :dealer,
                                :order_number,
                                :stock_flag,
                                :vehicle_type_raw,
                                :price_reserved_1,
                                :price_reserved_2,
                                :confirmation_week

                            )
                            on conflict (external_id) do update set
                                vid = excluded.vid,
                                targa = excluded.targa,
                                vin = excluded.vin,
                                raw_linea = excluded.raw_linea,
                                raw_status = excluded.raw_status,
                                raw_stato = excluded.raw_stato,
                                cod_versione_cm = excluded.cod_versione_cm,
                                brand = excluded.brand,
                                model = excluded.model,
                                version = excluded.version,
                                description = excluded.description,
                                vehicle_category = excluded.vehicle_category,
                                kilometers = excluded.kilometers,
                                first_registration_date = excluded.first_registration_date,
                                fuel_type = excluded.fuel_type,
                                body_type = excluded.body_type,
                                color_ext = excluded.color_ext,
                                color_int = excluded.color_int,
                                price_public = excluded.price_public,
                                price_showroom = excluded.price_showroom,
                                price_internal = excluded.price_internal,
                                location = excluded.location,
                                arrival_date = excluded.arrival_date,
                                expected_arrival_date = excluded.expected_arrival_date,
                                images_count = excluded.images_count,
                                main_image_url = excluded.main_image_url,
                                last_import_id = excluded.last_import_id,
                                last_seen_at = excluded.last_seen_at,
                                dealer = excluded.dealer,
                                order_number = excluded.order_number,
                                stock_flag = excluded.stock_flag,
                                vehicle_type_raw = excluded.vehicle_type_raw,
                                price_reserved_1 = excluded.price_reserved_1,
                                price_reserved_2 = excluded.price_reserved_2,
                                confirmation_week = excluded.confirmation_week,

                                is_active = true
                            returning (xmax = 0) as inserted
                        """),
                        {
                            "external_id": row.get("ID MyGarage"),
                            "vid": row.get("VID"),
                            "targa": row.get("Targa"),
                            "vin": row.get("Telaio"),
                            "raw_linea": row.get("Linea"),
                            "raw_status": row.get("Status"),
                            "raw_stato": row.get("Stato"),
                            "cod_versione_cm": normalize_cod_versione_cm(
                                row.get("Cod.Versione CM")
                            ),
                            "brand": row.get("Marca"),
                            "model": row.get("Modello"),
                            "version": row.get("Versione"),
                            "description": row.get("Descrizione"),
                            "vehicle_category": row.get("Veicolo comm."),
                            "kilometers": _parse_int(row.get("Chilometri")),
                            "first_registration_date": _parse_date(row.get("Immatricolazione")),
                            "fuel_type": row.get("Alimentazione"),
                            "body_type": row.get("Tipo carrozzeria"),
                            "color_ext": row.get("Colore esterni"),
                            "color_int": row.get("Interni"),
                            "price_public": _parse_price(row.get("Prezzo Internet")),
                            "price_showroom": _parse_price(row.get("Prezzo Showroom")),
                            "price_internal": _parse_price(row.get("Prezzo veicolo")),
                            "location": row.get("Ubicazione"),
                            "arrival_date": _parse_date(row.get("Data di arrivo")),
                            "expected_arrival_date": _parse_date(row.get("Data prev. di arrivo")),
                            "images_count": _parse_int(row.get("Immagini")) or 0,
                            "main_image_url": row.get("Immagine"),
                            "last_import_id": import_id,
                            "dealer": row.get("Dealer"),
                            "order_number": row.get("Nr.Ordine"),
                            "stock_flag": row.get("Stock"),
                            "vehicle_type_raw": row.get("Tipo"),
                            "price_reserved_1": _parse_price(row.get("Prezzo riservato 1")),
                            "price_reserved_2": _parse_price(row.get("Prezzo riservato 2")),
                            "confirmation_week": row.get("Sett.di conferma"),

                        },
                    ).scalar()



                    if result:
                        rows_inserted += 1
                    else:
                        rows_updated += 1

            except Exception:
                rows_skipped += 1
                logger.exception(
                    f"[CSV IMPORT] row skipped (external_id={external_id})"
                )


    # --------------------------------------------------
    # 5️⃣ Finalizza import
    # --------------------------------------------------
    with engine.begin() as conn:

        # DELETE auto non più presenti nel CSV
        conn.execute(
            text("""
                delete from public.vehicles_stock_sale
                where last_import_id is distinct from :import_id
            """),
            {"import_id": import_id},
        )

        # Finalizza import
        conn.execute(
            text("""
                update public.vehicle_stock_imports
                set status = 'done',
                    rows_total = :rows_total,
                    rows_inserted = :rows_inserted,
                    rows_updated = :rows_updated,
                    rows_skipped = :rows_skipped,
                    processed_at = now()
                where id = :id
            """),
            {
                "id": import_id,
                "rows_total": rows_total,
                "rows_inserted": rows_inserted,
                "rows_updated": rows_updated,
                "rows_skipped": rows_skipped,
            },
        )

    logger.info(
        f"[CSV IMPORT] import {import_id} completed: "
        f"total={rows_total}, inserted={rows_inserted}, "
        f"updated={rows_updated}, skipped={rows_skipped}"
    )


# --------------------------------------------------
# Helpers (robusti, zero ipotesi)
# --------------------------------------------------

def _parse_int(value):
    """
    Parsing robusto interi da CSV eterogenei (km, contatori, immagini).

    Regola:
    - prende tutto
    - rimuove . , spazi
    - tiene solo cifre
    - ritorna int
    """
    try:
        if value is None:
            return None

        s = str(value).strip()
        if not s:
            return None

        # rimuove separatori e spazi
        s = s.replace(".", "").replace(",", "").replace(" ", "")

        if not s.isdigit():
            return None

        return int(s)
    except Exception:
        return None



def _parse_price(value):
    """
    Parsing robusto prezzi CSV italiani:
    es. '26.400,00' -> 26400.0
    """
    try:
        if value is None:
            return None

        s = str(value).strip()
        if not s:
            return None

        # rimuove simbolo euro e spazi
        s = s.replace("€", "").replace(" ", "")

        # rimuove separatore migliaia
        s = s.replace(".", "")

        # converte separatore decimale
        s = s.replace(",", ".")

        return float(s)
    except Exception:
        return None


def _fail_import(import_id, message):
    logger.error(f"[CSV IMPORT] import {import_id} failed: {message}")
    with engine.begin() as conn:
        conn.execute(
            text("""
                update public.vehicle_stock_imports
                set status = 'error',
                    error = :error,
                    processed_at = now()
                where id = :id
            """),
            {"id": import_id, "error": message},
        )



if __name__ == "__main__":
    logger.info("[CSV IMPORT] module executed as script")
    vehicle_stock_csv_import_job()
