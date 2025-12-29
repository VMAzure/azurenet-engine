import asyncio
import logging
import time
from sqlalchemy import text

from app.database import DBSession
from app.external.motornet import motornet_get

# ============================================================
# ENDPOINTS
# ============================================================

VCOM_MARCHE_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/usato/vcom/marche"
)

VCOM_MODELLI_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/usato/vcom/marca/modelli"
)

VCOM_VERSIONI_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/usato/vcom/modello/versioni"
)

VCOM_DETTAGLIO_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/usato/vcom/dettaglio"
)


# ============================================================
# CONFIG
# ============================================================

MAX_RETRY = 3
RETRY_DELAY_SEC = 3


# ============================================================
# VIC → MARCHE (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_vic_marche():
    logging.info("[VIC][MARCHE] START")

    data = asyncio.run(motornet_get(VCOM_MARCHE_URL))
    marche = data.get("marche", [])

    if not marche:
        logging.warning("[VIC][MARCHE] EMPTY RESPONSE")
        return

    inserted = 0

    with DBSession() as db:
        for m in marche:
            result = db.execute(
                text("""
                    INSERT INTO mnet_vcom_marche (
                        acronimo,
                        nome,
                        logo,
                        updated_at
                    )
                    SELECT
                        :acronimo,
                        :nome,
                        :logo,
                        now()
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM mnet_vcom_marche
                        WHERE acronimo = :acronimo
                    )
                """),
                {
                    "acronimo": m["acronimo"],
                    "nome": m["nome"],
                    "logo": m.get("logo"),
                },
            )

            # rowcount = 1 solo se INSERT avvenuto
            if result.rowcount == 1:
                inserted += 1
                logging.info(
                    "[VIC][MARCHE] inserted %s",
                    m["acronimo"],
                )

    logging.info(
        "[VIC][MARCHE] DONE (new=%d, total_seen=%d)",
        inserted,
        len(marche),
    )


# ============================================================
# VIC → MODELLI (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_vic_modelli():
    logging.info("[VIC][MODELLI] START")

    # 1. Carico marche dal DB (fonte di verità)
    with DBSession() as db:
        result = db.execute(
            text("SELECT acronimo FROM mnet_vcom_marche ORDER BY acronimo")
        ).fetchall()

    marche = [r[0] for r in result]

    if not marche:
        logging.warning("[VIC][MODELLI] ABORT: no marche in DB")
        return

    inserted = 0
    seen = 0

    # 2. Loop per marca (delta)
    for acronimo in marche:
        logging.info("[VIC][MODELLI] marca=%s", acronimo)

        try:
            data = asyncio.run(
                motornet_get(
                    f"{VCOM_MODELLI_URL}?codice_marca={acronimo}"
                )
            )

            modelli = data.get("modelli", [])
            seen += len(modelli)

            if not modelli:
                continue

            with DBSession() as db:
                for m in modelli:
                    gamma = m.get("gammaModello") or {}
                    gruppo = m.get("gruppoStorico") or {}
                    serie = m.get("serieGamma") or {}

                    codice_modello = gamma.get("codice")
                    if not codice_modello:
                        continue

                    res = db.execute(
                        text("""
                            INSERT INTO mnet_vcom_modelli (
                                codice_modello,
                                marca_acronimo,
                                descrizione,
                                gruppo_storico_codice,
                                gruppo_storico_descrizione,
                                serie_gamma_codice,
                                serie_gamma_descrizione,
                                inizio_produzione,
                                fine_produzione,
                                inizio_commercializzazione,
                                fine_commercializzazione,
                                modello_breve_carrozzeria,
                                foto,
                                prezzo_minimo,
                                updated_at
                            )
                            SELECT
                                :codice_modello,
                                :marca_acronimo,
                                :descrizione,
                                :gruppo_storico_codice,
                                :gruppo_storico_descrizione,
                                :serie_gamma_codice,
                                :serie_gamma_descrizione,
                                :inizio_produzione,
                                :fine_produzione,
                                :inizio_commercializzazione,
                                :fine_commercializzazione,
                                :modello_breve_carrozzeria,
                                :foto,
                                :prezzo_minimo,
                                now()
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM mnet_vcom_modelli
                                WHERE codice_modello = :codice_modello
                            )
                        """),
                        {
                            "codice_modello": codice_modello,
                            "marca_acronimo": acronimo,
                            "descrizione": gamma.get("descrizione"),
                            "gruppo_storico_codice": gruppo.get("codice"),
                            "gruppo_storico_descrizione": gruppo.get("descrizione"),
                            "serie_gamma_codice": serie.get("codice"),
                            "serie_gamma_descrizione": serie.get("descrizione"),
                            "inizio_produzione": m.get("inizioProduzione"),
                            "fine_produzione": m.get("fineProduzione"),
                            "inizio_commercializzazione": m.get("inizioCommercializzazione"),
                            "fine_commercializzazione": m.get("fineCommercializzazione"),
                            "modello_breve_carrozzeria": m.get("modelloBreveCarrozzeria"),
                            "foto": m.get("foto"),
                            "prezzo_minimo": m.get("prezzoMinimo"),
                        },
                    )

                    if res.rowcount == 1:
                        inserted += 1
                        logging.info(
                            "[VIC][MODELLI] inserted %s",
                            codice_modello,
                        )

        except Exception as exc:
            logging.exception(
                "[VIC][MODELLI] marca=%s FAILED",
                acronimo,
            )
            continue

    logging.info(
        "[VIC][MODELLI] DONE (new=%d, total_seen=%d)",
        inserted,
        seen,
    )


# ============================================================
# VIC → VERSIONI (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_vic_versioni():
    logging.info("[VIC][VERSIONI] START")

    # 1. Carico modelli dal DB (fonte di verità)
    with DBSession() as db:
        rows = db.execute(
            text("""
                SELECT codice_modello, marca_acronimo
                FROM mnet_vcom_modelli
                ORDER BY codice_modello
            """)
        ).fetchall()

    modelli = [(r[0], r[1]) for r in rows]

    if not modelli:
        logging.warning("[VIC][VERSIONI] ABORT: no modelli in DB")
        return

    inserted = 0
    seen = 0

    # 2. Loop per modello
    for codice_modello, marca_acronimo in modelli:
        logging.info("[VIC][VERSIONI] modello=%s", codice_modello)

        try:
            data = asyncio.run(
                motornet_get(
                    f"{VCOM_VERSIONI_URL}?codice_modello={codice_modello}"
                )
            )

            versioni = data.get("versioni", [])
            seen += len(versioni)

            if not versioni:
                continue

            with DBSession() as db:
                for v in versioni:
                    codice_uni = v.get("codiceMotornetUnivoco")
                    if not codice_uni:
                        continue

                    res = db.execute(
                        text("""
                            INSERT INTO mnet_vcom_versioni (
                                codice_motornet_uni,
                                codice_modello,
                                nome,
                                data_da,
                                data_a,
                                inizio_produzione,
                                fine_produzione,
                                marca_acronimo,
                                updated_at
                            )
                            SELECT
                                :codice_uni,
                                :codice_modello,
                                :nome,
                                :data_da,
                                :data_a,
                                :inizio_produzione,
                                :fine_produzione,
                                :marca_acronimo,
                                now()
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM mnet_vcom_versioni
                                WHERE codice_motornet_uni = :codice_uni
                            )
                        """),
                        {
                            "codice_uni": codice_uni,
                            "codice_modello": codice_modello,
                            "nome": v.get("nome"),
                            "data_da": v.get("da"),
                            "data_a": v.get("a"),
                            "inizio_produzione": v.get("inizioProduzione"),
                            "fine_produzione": v.get("fineProduzione"),
                            "marca_acronimo": marca_acronimo,
                        },
                    )

                    if res.rowcount == 1:
                        inserted += 1
                        logging.info(
                            "[VIC][VERSIONI] inserted %s",
                            codice_uni,
                        )

        except Exception as exc:
            logging.exception(
                "[VIC][VERSIONI] modello=%s FAILED",
                codice_modello,
            )
            # errore isolato: continuiamo
            with DBSession() as db:
                db.execute(
                    text("""
                        INSERT INTO mnet_vcom_sync_errors (
                            job_name, key, error
                        ) VALUES (
                            'vic_versioni', :key, :error
                        )
                    """),
                    {
                        "key": codice_modello,
                        "error": str(exc),
                    },
                )
            continue

    logging.info(
        "[VIC][VERSIONI] DONE (new=%d, total_seen=%d)",
        inserted,
        seen,
    )


# ============================================================
# VIC → DETTAGLI (CRASH-SAFE, RIPRENDIBILE)
# ============================================================

VCOM_DETTAGLIO_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/usato/vcom/dettaglio"
)

SYNC_DETTAGLI_JOB = "vic_dettagli"
MAX_RETRY = 3
RETRY_DELAY_SEC = 3


def sync_vic_dettagli():
    logging.info("[VIC][DETTAGLI] START")

    # --------------------------------------------------
    # 1. Recupero checkpoint
    # --------------------------------------------------
    with DBSession() as db:
        row = db.execute(
            text("""
                SELECT last_key
                FROM mnet_vcom_sync_state
                WHERE job_name = :job
            """),
            {"job": SYNC_DETTAGLI_JOB},
        ).fetchone()

    last_key = row[0] if row else None

    # --------------------------------------------------
    # 2. Recupero versioni da processare
    # --------------------------------------------------
    with DBSession() as db:
        if last_key:
            result = db.execute(
                text("""
                    SELECT codice_motornet_uni
                    FROM mnet_vcom_versioni
                    WHERE codice_motornet_uni > :last_key
                    ORDER BY codice_motornet_uni
                """),
                {"last_key": last_key},
            ).fetchall()
        else:
            result = db.execute(
                text("""
                    SELECT codice_motornet_uni
                    FROM mnet_vcom_versioni
                    ORDER BY codice_motornet_uni
                """)
            ).fetchall()

    codici = [r[0] for r in result]

    logging.info(
        "[VIC][DETTAGLI] %d versioni da processare (resume=%s)",
        len(codici),
        last_key,
    )

    processed = 0

    # --------------------------------------------------
    # 3. Loop versioni
    # --------------------------------------------------
    for codice_uni in codici:
        attempt = 0
        success = False

        while attempt < MAX_RETRY and not success:
            attempt += 1
            try:
                logging.info(
                    "[VIC][DETTAGLI] codice=%s attempt=%d/%d",
                    codice_uni,
                    attempt,
                    MAX_RETRY,
                )

                data = asyncio.run(
                    motornet_get(
                        f"{VCOM_DETTAGLIO_URL}?codice_motornet_uni={codice_uni}"
                    )
                )

                modello = data.get("modello")
                if not modello:
                    raise RuntimeError("Payload dettaglio vuoto")

                with DBSession() as db:
                    db.execute(
                        text("""
                            INSERT INTO mnet_vcom_dettagli (
                                codice_motornet_uni,
                                marca_acronimo,
                                marca_nome,
                                codice_modello,
                                descrizione_modello,
                                allestimento,
                                immagine,
                                codice_costruttore,
                                codice_motore,
                                alimentazione_codice,
                                alimentazione_descrizione,
                                tipo_codice,
                                tipo_descrizione,
                                categoria_codice,
                                categoria_descrizione,
                                cilindrata,
                                hp,
                                kw,
                                euro,
                                prezzo_listino,
                                prezzo_accessori,
                                data_listino,
                                cambio_codice,
                                cambio_descrizione,
                                trazione_codice,
                                trazione_descrizione,
                                lunghezza,
                                larghezza,
                                altezza,
                                passo,
                                porte,
                                posti,
                                autonomia_media,
                                autonomia_massima,
                                peso,
                                peso_vuoto,
                                peso_totale_terra,
                                portata,
                                accessi_disponibili,
                                accessori_serie,
                                accessori_opzionali,
                                updated_at
                            ) VALUES (
                                :codice,
                                :marca_acronimo,
                                :marca_nome,
                                :codice_modello,
                                :descrizione_modello,
                                :allestimento,
                                :immagine,
                                :codice_costruttore,
                                :codice_motore,
                                :alimentazione_codice,
                                :alimentazione_descrizione,
                                :tipo_codice,
                                :tipo_descrizione,
                                :categoria_codice,
                                :categoria_descrizione,
                                :cilindrata,
                                :hp,
                                :kw,
                                :euro,
                                :prezzo_listino,
                                :prezzo_accessori,
                                :data_listino,
                                :cambio_codice,
                                :cambio_descrizione,
                                :trazione_codice,
                                :trazione_descrizione,
                                :lunghezza,
                                :larghezza,
                                :altezza,
                                :passo,
                                :porte,
                                :posti,
                                :autonomia_media,
                                :autonomia_massima,
                                :peso,
                                :peso_vuoto,
                                :peso_totale_terra,
                                :portata,
                                :accessi_disponibili,
                                :accessori_serie,
                                :accessori_opzionali,
                                now()
                            )
                            ON CONFLICT (codice_motornet_uni)
                            DO UPDATE SET
                                marca_acronimo = EXCLUDED.marca_acronimo,
                                marca_nome = EXCLUDED.marca_nome,
                                codice_modello = EXCLUDED.codice_modello,
                                descrizione_modello = EXCLUDED.descrizione_modello,
                                allestimento = EXCLUDED.allestimento,
                                immagine = EXCLUDED.immagine,
                                codice_costruttore = EXCLUDED.codice_costruttore,
                                codice_motore = EXCLUDED.codice_motore,
                                alimentazione_codice = EXCLUDED.alimentazione_codice,
                                alimentazione_descrizione = EXCLUDED.alimentazione_descrizione,
                                tipo_codice = EXCLUDED.tipo_codice,
                                tipo_descrizione = EXCLUDED.tipo_descrizione,
                                categoria_codice = EXCLUDED.categoria_codice,
                                categoria_descrizione = EXCLUDED.categoria_descrizione,
                                cilindrata = EXCLUDED.cilindrata,
                                hp = EXCLUDED.hp,
                                kw = EXCLUDED.kw,
                                euro = EXCLUDED.euro,
                                prezzo_listino = EXCLUDED.prezzo_listino,
                                prezzo_accessori = EXCLUDED.prezzo_accessori,
                                data_listino = EXCLUDED.data_listino,
                                cambio_codice = EXCLUDED.cambio_codice,
                                cambio_descrizione = EXCLUDED.cambio_descrizione,
                                trazione_codice = EXCLUDED.trazione_codice,
                                trazione_descrizione = EXCLUDED.trazione_descrizione,
                                lunghezza = EXCLUDED.lunghezza,
                                larghezza = EXCLUDED.larghezza,
                                altezza = EXCLUDED.altezza,
                                passo = EXCLUDED.passo,
                                porte = EXCLUDED.porte,
                                posti = EXCLUDED.posti,
                                autonomia_media = EXCLUDED.autonomia_media,
                                autonomia_massima = EXCLUDED.autonomia_massima,
                                peso = EXCLUDED.peso,
                                peso_vuoto = EXCLUDED.peso_vuoto,
                                peso_totale_terra = EXCLUDED.peso_totale_terra,
                                portata = EXCLUDED.portata,
                                accessi_disponibili = EXCLUDED.accessi_disponibili,
                                accessori_serie = EXCLUDED.accessori_serie,
                                accessori_opzionali = EXCLUDED.accessori_opzionali,
                                updated_at = now()
                        """),
                        {
                            "codice": codice_uni,
                            "marca_acronimo": (modello.get("marca") or {}).get("acronimo"),
                            "marca_nome": (modello.get("marca") or {}).get("nome"),
                            "codice_modello": (modello.get("codDescModello") or {}).get("codice"),
                            "descrizione_modello": (modello.get("codDescModello") or {}).get("descrizione"),
                            "allestimento": modello.get("allestimento"),
                            "immagine": modello.get("immagine"),
                            "codice_costruttore": modello.get("codiceCostruttore"),
                            "codice_motore": modello.get("codiceMotore"),
                            "alimentazione_codice": (modello.get("alimentazione") or {}).get("codice"),
                            "alimentazione_descrizione": (modello.get("alimentazione") or {}).get("descrizione"),
                            "tipo_codice": (modello.get("tipo") or {}).get("codice"),
                            "tipo_descrizione": (modello.get("tipo") or {}).get("descrizione"),
                            "categoria_codice": (modello.get("categoria") or {}).get("codice"),
                            "categoria_descrizione": (modello.get("categoria") or {}).get("descrizione"),
                            "cilindrata": modello.get("cilindrata"),
                            "hp": modello.get("hp"),
                            "kw": modello.get("kw"),
                            "euro": modello.get("euro"),
                            "prezzo_listino": modello.get("prezzoListino"),
                            "prezzo_accessori": modello.get("prezzoAccessori"),
                            "data_listino": modello.get("dataListino"),
                            "cambio_codice": modello.get("codiceCambio"),
                            "cambio_descrizione": modello.get("descrizioneCambio"),
                            "trazione_codice": (modello.get("trazione") or {}).get("codice"),
                            "trazione_descrizione": (modello.get("trazione") or {}).get("descrizione"),
                            "lunghezza": modello.get("lunghezza"),
                            "larghezza": modello.get("larghezza"),
                            "altezza": modello.get("altezza"),
                            "passo": modello.get("passo"),
                            "porte": modello.get("porte"),
                            "posti": modello.get("posti"),
                            "autonomia_media": modello.get("autonomiaMedia"),
                            "autonomia_massima": modello.get("autonomiaMassima"),
                            "peso": modello.get("peso"),
                            "peso_vuoto": modello.get("pesoVuoto"),
                            "peso_totale_terra": modello.get("pesoTotaleTerra"),
                            "portata": modello.get("portata"),
                            "accessi_disponibili": data.get("accessiDisponibili"),
                            "accessori_serie": modello.get("accessoriSerie"),
                            "accessori_opzionali": modello.get("accessoriOpzionali"),
                        }
                    )

                # --------------------------------------------------
                # checkpoint
                # --------------------------------------------------
                with DBSession() as db:
                    db.execute(
                        text("""
                            INSERT INTO mnet_vcom_sync_state (job_name, last_key, updated_at)
                            VALUES (:job, :key, now())
                            ON CONFLICT (job_name)
                            DO UPDATE SET
                                last_key = EXCLUDED.last_key,
                                updated_at = now()
                        """),
                        {
                            "job": SYNC_DETTAGLI_JOB,
                            "key": codice_uni,
                        },
                    )

                processed += 1
                success = True

            except Exception as exc:
                logging.exception(
                    "[VIC][DETTAGLI] codice=%s FAILED attempt=%d",
                    codice_uni,
                    attempt,
                )
                if attempt < MAX_RETRY:
                    time.sleep(RETRY_DELAY_SEC)

        if not success:
            logging.error(
                "[VIC][DETTAGLI] codice=%s SKIPPED after %d attempts",
                codice_uni,
                MAX_RETRY,
            )

            # registra errore
            with DBSession() as db:
                db.execute(
                    text("""
                        INSERT INTO mnet_vcom_sync_errors (
                            job_name, key, error
                        ) VALUES (
                            :job, :key, :error
                        )
                    """),
                    {
                        "job": SYNC_DETTAGLI_JOB,
                        "key": codice_uni,
                        "error": "Network/endpoint failure after retries",
                    },
                )

            # aggiorna checkpoint COMUNQUE
            with DBSession() as db:
                db.execute(
                    text("""
                        INSERT INTO mnet_vcom_sync_state (job_name, last_key, updated_at)
                        VALUES (:job, :key, now())
                        ON CONFLICT (job_name)
                        DO UPDATE SET
                            last_key = EXCLUDED.last_key,
                            updated_at = now()
                    """),
                    {
                        "job": SYNC_DETTAGLI_JOB,
                        "key": codice_uni,
                    },
                )

            # 🔑 CONTINUA col prossimo codice
            continue


    logging.info(
        "[VIC][DETTAGLI] DONE processed=%d last=%s",
        processed,
        codice_uni if processed else last_key,
    )

# ============================================================
# VIC → DETTAGLI (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_vic_dettagli():
    logging.info("[VIC][DETTAGLI] START")

    # 1) Seleziona SOLO versioni senza dettaglio (DB = filtro)
    with DBSession() as db:
        rows = db.execute(
            text("""
                SELECT v.codice_motornet_uni
                FROM mnet_vcom_versioni v
                LEFT JOIN mnet_vcom_dettagli d
                  ON d.codice_motornet_uni = v.codice_motornet_uni
                WHERE d.codice_motornet_uni IS NULL
                ORDER BY v.codice_motornet_uni
            """)
        ).fetchall()

    codici = [r[0] for r in rows]

    if not codici:
        logging.info("[VIC][DETTAGLI] NOTHING TO DO (no missing details)")
        return

    inserted = 0
    seen = len(codici)

    # 2) Loop SOLO sui mancanti
    for codice_uni in codici:
        try:
            logging.info("[VIC][DETTAGLI] fetching %s", codice_uni)

            data = asyncio.run(
                motornet_get(
                    f"{VCOM_DETTAGLIO_URL}?codice_motornet_uni={codice_uni}"
                )
            )

            modello = data.get("modello")
            if not modello:
                raise RuntimeError("Empty dettaglio payload")

            # INSERT-ONLY (no update)
            with DBSession() as db:
                res = db.execute(
                    text("""
                        INSERT INTO mnet_vcom_dettagli (
                            codice_motornet_uni,
                            marca_acronimo,
                            marca_nome,
                            codice_modello,
                            descrizione_modello,
                            allestimento,
                            immagine,
                            codice_costruttore,
                            codice_motore,
                            alimentazione_codice,
                            alimentazione_descrizione,
                            tipo_codice,
                            tipo_descrizione,
                            categoria_codice,
                            categoria_descrizione,
                            cilindrata,
                            hp,
                            kw,
                            euro,
                            prezzo_listino,
                            prezzo_accessori,
                            data_listino,
                            cambio_codice,
                            cambio_descrizione,
                            trazione_codice,
                            trazione_descrizione,
                            lunghezza,
                            larghezza,
                            altezza,
                            passo,
                            porte,
                            posti,
                            autonomia_media,
                            autonomia_massima,
                            peso,
                            peso_vuoto,
                            peso_totale_terra,
                            portata,
                            accessi_disponibili,
                            accessori_serie,
                            accessori_opzionali,
                            updated_at
                        )
                        SELECT
                            :codice,
                            :marca_acronimo,
                            :marca_nome,
                            :codice_modello,
                            :descrizione_modello,
                            :allestimento,
                            :immagine,
                            :codice_costruttore,
                            :codice_motore,
                            :alimentazione_codice,
                            :alimentazione_descrizione,
                            :tipo_codice,
                            :tipo_descrizione,
                            :categoria_codice,
                            :categoria_descrizione,
                            :cilindrata,
                            :hp,
                            :kw,
                            :euro,
                            :prezzo_listino,
                            :prezzo_accessori,
                            :data_listino,
                            :cambio_codice,
                            :cambio_descrizione,
                            :trazione_codice,
                            :trazione_descrizione,
                            :lunghezza,
                            :larghezza,
                            :altezza,
                            :passo,
                            :porte,
                            :posti,
                            :autonomia_media,
                            :autonomia_massima,
                            :peso,
                            :peso_vuoto,
                            :peso_totale_terra,
                            :portata,
                            :accessi_disponibili,
                            :accessori_serie,
                            :accessori_opzionali,
                            now()
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM mnet_vcom_dettagli
                            WHERE codice_motornet_uni = :codice
                        )
                    """),
                    {
                        "codice": codice_uni,
                        "marca_acronimo": (modello.get("marca") or {}).get("acronimo"),
                        "marca_nome": (modello.get("marca") or {}).get("nome"),
                        "codice_modello": (modello.get("codDescModello") or {}).get("codice"),
                        "descrizione_modello": (modello.get("codDescModello") or {}).get("descrizione"),
                        "allestimento": modello.get("allestimento"),
                        "immagine": modello.get("immagine"),
                        "codice_costruttore": modello.get("codiceCostruttore"),
                        "codice_motore": modello.get("codiceMotore"),
                        "alimentazione_codice": (modello.get("alimentazione") or {}).get("codice"),
                        "alimentazione_descrizione": (modello.get("alimentazione") or {}).get("descrizione"),
                        "tipo_codice": (modello.get("tipo") or {}).get("codice"),
                        "tipo_descrizione": (modello.get("tipo") or {}).get("descrizione"),
                        "categoria_codice": (modello.get("categoria") or {}).get("codice"),
                        "categoria_descrizione": (modello.get("categoria") or {}).get("descrizione"),
                        "cilindrata": modello.get("cilindrata"),
                        "hp": modello.get("hp"),
                        "kw": modello.get("kw"),
                        "euro": modello.get("euro"),
                        "prezzo_listino": modello.get("prezzoListino"),
                        "prezzo_accessori": modello.get("prezzoAccessori"),
                        "data_listino": modello.get("dataListino"),
                        "cambio_codice": modello.get("codiceCambio"),
                        "cambio_descrizione": modello.get("descrizioneCambio"),
                        "trazione_codice": (modello.get("trazione") or {}).get("codice"),
                        "trazione_descrizione": (modello.get("trazione") or {}).get("descrizione"),
                        "lunghezza": modello.get("lunghezza"),
                        "larghezza": modello.get("larghezza"),
                        "altezza": modello.get("altezza"),
                        "passo": modello.get("passo"),
                        "porte": modello.get("porte"),
                        "posti": modello.get("posti"),
                        "autonomia_media": modello.get("autonomiaMedia"),
                        "autonomia_massima": modello.get("autonomiaMassima"),
                        "peso": modello.get("peso"),
                        "peso_vuoto": modello.get("pesoVuoto"),
                        "peso_totale_terra": modello.get("pesoTotaleTerra"),
                        "portata": modello.get("portata"),
                        "accessi_disponibili": data.get("accessiDisponibili"),
                        "accessori_serie": modello.get("accessoriSerie"),
                        "accessori_opzionali": modello.get("accessoriOpzionali"),
                    }
                )

                if res.rowcount == 1:
                    inserted += 1
                    logging.info("[VIC][DETTAGLI] inserted %s", codice_uni)

        except Exception as exc:
            logging.exception("[VIC][DETTAGLI] FAILED %s", codice_uni)
            # audit errore, ma NON blocchiamo
            with DBSession() as db:
                db.execute(
                    text("""
                        INSERT INTO mnet_vcom_sync_errors (job_name, key, error)
                        VALUES ('vic_dettagli', :key, :error)
                    """),
                    {"key": codice_uni, "error": str(exc)},
                )
            continue

    logging.info(
        "[VIC][DETTAGLI] DONE (new=%d, total_missing_seen=%d)",
        inserted,
        seen,
    )
