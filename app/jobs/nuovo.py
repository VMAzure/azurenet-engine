import asyncio
import logging
from datetime import datetime
from sqlalchemy import text

from app.database import DBSession
from app.external.motornet import motornet_get

# ============================================================
# ENDPOINTS — NUOVO
# ============================================================

NUOVO_MARCHE_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/nuovo/auto/marche"
)

NUOVO_MODELLI_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/nuovo/auto/modelli"
)

NUOVO_VERSIONI_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/nuovo/auto/versioni"
)

NUOVO_DETTAGLIO_URL = (
    "https://webservice.motornet.it/api/v3_0/rest/public/nuovo/auto/dettaglio"
)

# ============================================================
# CONFIG
# ============================================================

ANNO_RIF = datetime.now().year

# ============================================================
# NUOVO → MARCHE (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_nuovo_marche():
    logging.info("[NUOVO][MARCHE] START")

    data = asyncio.run(motornet_get(NUOVO_MARCHE_URL))
    marche = data.get("marche", [])

    if not marche:
        logging.warning("[NUOVO][MARCHE] EMPTY RESPONSE")
        return

    inserted = 0

    with DBSession() as db:
        for m in marche:
            res = db.execute(
                text("""
                    INSERT INTO mnet_marche (
                        acronimo,
                        nome,
                        logo,
                        utile
                    )
                    SELECT
                        CAST(:acronimo AS varchar),
                        CAST(:nome AS varchar),
                        CAST(:logo AS text),
                        FALSE
                    WHERE NOT EXISTS (
                        SELECT 1
                        FROM mnet_marche
                        WHERE acronimo = CAST(:acronimo AS varchar)
                    )
                """),
                {
                    "acronimo": m["acronimo"],
                    "nome": m["nome"],
                    "logo": m.get("logo"),
                },
            )




            if res.rowcount == 1:
                inserted += 1
                logging.info("[NUOVO][MARCHE] inserted %s", m["acronimo"])

    logging.info(
        "[NUOVO][MARCHE] DONE (new=%d, total_seen=%d)",
        inserted,
        len(marche),
    )



# ============================================================
# NUOVO → MODELLI (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_nuovo_modelli():
    logging.info("[NUOVO][MODELLI] START")

    with DBSession() as db:
        rows = db.execute(
            text("SELECT acronimo FROM mnet_marche WHERE utile IS TRUE ORDER BY acronimo")
        ).fetchall()

    marche = [r[0] for r in rows]

    if not marche:
        logging.warning("[NUOVO][MODELLI] ABORT: no marche utili")
        return

    inserted = 0
    seen = 0

    for acronimo in marche:
        logging.info("[NUOVO][MODELLI] marca=%s", acronimo)

        try:
            data = asyncio.run(
                motornet_get(
                    f"{NUOVO_MODELLI_URL}?codice_marca={acronimo}&anno={ANNO_RIF}"
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
                            INSERT INTO mnet_modelli (
                                codice_modello,
                                descrizione,
                                marca_acronimo,
                                inizio_produzione,
                                fine_produzione,
                                gruppo_storico_codice,
                                gruppo_storico_descrizione,
                                serie_gamma_codice,
                                serie_gamma_descrizione,
                                inizio_commercializzazione,
                                fine_commercializzazione
                            )
                            SELECT
                                CAST(:codice_modello AS varchar),
                                CAST(:descrizione AS varchar),
                                CAST(:marca_acronimo AS varchar),
                                CAST(:inizio_produzione AS date),
                                CAST(:fine_produzione AS date),
                                CAST(:gruppo_storico_codice AS varchar),
                                CAST(:gruppo_storico_descrizione AS varchar),
                                CAST(:serie_gamma_codice AS varchar),
                                CAST(:serie_gamma_descrizione AS varchar),
                                CAST(:inizio_commercializzazione AS date),
                                CAST(:fine_commercializzazione AS date)
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM mnet_modelli
                                WHERE codice_modello = CAST(:codice_modello AS varchar)
                            )
                        """),
                        {
                            "codice_modello": codice_modello,
                            "descrizione": gamma.get("descrizione"),
                            "marca_acronimo": acronimo,
                            "inizio_produzione": m.get("inizioProduzione"),
                            "fine_produzione": m.get("fineProduzione"),
                            "gruppo_storico_codice": gruppo.get("codice"),
                            "gruppo_storico_descrizione": gruppo.get("descrizione"),
                            "serie_gamma_codice": serie.get("codice"),
                            "serie_gamma_descrizione": serie.get("descrizione"),
                            "inizio_commercializzazione": m.get("inizioCommercializzazione"),
                            "fine_commercializzazione": m.get("fineCommercializzazione"),
                        },
                    )


                    if res.rowcount == 1:
                        inserted += 1
                        logging.info(
                            "[NUOVO][MODELLI] inserted %s",
                            codice_modello,
                        )

        except Exception:
            logging.exception("[NUOVO][MODELLI] marca=%s FAILED", acronimo)
            continue

    logging.info(
        "[NUOVO][MODELLI] DONE (new=%d, total_seen=%d)",
        inserted,
        seen,
    )


# ============================================================
# NUOVO → ALLESTIMENTI (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_nuovo_allestimenti():
    logging.info("[NUOVO][ALLESTIMENTI] START")

    with DBSession() as db:
        rows = db.execute(
            text("""
                SELECT m.codice_modello
                FROM mnet_modelli m
                JOIN mnet_marche ma ON ma.acronimo = m.marca_acronimo
                WHERE ma.utile IS TRUE
                ORDER BY m.codice_modello
            """)
        ).fetchall()

    modelli = [r[0] for r in rows]

    if not modelli:
        logging.warning("[NUOVO][ALLESTIMENTI] ABORT: no modelli")
        return

    inserted = 0
    seen = 0

    for codice_modello in modelli:
        logging.info("[NUOVO][ALLESTIMENTI] modello=%s", codice_modello)

        try:
            data = asyncio.run(
                motornet_get(
                    f"{NUOVO_VERSIONI_URL}?codice_modello={codice_modello}&anno={ANNO_RIF}"
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
                            INSERT INTO mnet_allestimenti (
                                codice_modello,
                                codice_motornet_uni,
                                nome,
                                data_da,
                                data_a
                            )
                            SELECT
                                CAST(:codice_modello AS varchar),
                                CAST(:codice_uni AS varchar),
                                CAST(:nome AS varchar),
                                CAST(:data_da AS date),
                                CAST(:data_a AS date)
                            WHERE NOT EXISTS (
                                SELECT 1
                                FROM mnet_allestimenti
                                WHERE codice_motornet_uni = CAST(:codice_uni AS varchar)
                            )
                        """),
                        {
                            "codice_modello": codice_modello,
                            "codice_uni": codice_uni,
                            "nome": v.get("nome"),
                            "data_da": v.get("da"),
                            "data_a": v.get("a"),
                        },
                    )


                    if res.rowcount == 1:
                        inserted += 1
                        logging.info(
                            "[NUOVO][ALLESTIMENTI] inserted %s",
                            codice_uni,
                        )

        except Exception:
            logging.exception(
                "[NUOVO][ALLESTIMENTI] modello=%s FAILED",
                codice_modello,
            )
            continue

    logging.info(
        "[NUOVO][ALLESTIMENTI] DONE (new=%d, total_seen=%d)",
        inserted,
        seen,
    )


# ============================================================
# NUOVO → DETTAGLI (DELTA-ONLY, PRODUZIONE)
# ============================================================

def sync_nuovo_dettagli():
    logging.info("[NUOVO][DETTAGLI] START")

    # 1) SOLO allestimenti senza dettagli
    with DBSession() as db:
        rows = db.execute(
            text("""
                SELECT a.codice_motornet_uni
                FROM mnet_allestimenti a
                LEFT JOIN mnet_dettagli d
                  ON d.codice_motornet_uni = a.codice_motornet_uni
                WHERE d.codice_motornet_uni IS NULL
                ORDER BY a.codice_motornet_uni
            """)
        ).fetchall()

    codici = [r[0] for r in rows]

    if not codici:
        logging.info("[NUOVO][DETTAGLI] NOTHING TO DO")
        return

    inserted = 0
    seen = len(codici)

    for codice_uni in codici:
        try:
            logging.info("[NUOVO][DETTAGLI] fetching %s", codice_uni)

            data = asyncio.run(
                motornet_get(
                    f"{NUOVO_DETTAGLIO_URL}?codice_motornet_uni={codice_uni}"
                )
            )

            modello = data.get("modello")
            if not modello:
                raise RuntimeError("Empty dettaglio payload")

            with DBSession() as db:
                res = db.execute(
                    text("""
                        INSERT INTO mnet_dettagli (
                            codice_motornet_uni,
                            alimentazione,
                            cilindrata,
                            hp,
                            kw,
                            euro,
                            consumo_medio,
                            consumo_urbano,
                            consumo_extraurbano,
                            emissioni_co2,
                            tipo_cambio,
                            trazione,
                            porte,
                            posti,
                            lunghezza,
                            larghezza,
                            altezza,
                            altezza_minima,
                            peso,
                            peso_vuoto,
                            peso_potenza,
                            portata,
                            velocita,
                            accelerazione,
                            bagagliaio,
                            descrizione_breve,
                            foto,
                            prezzo_listino,
                            prezzo_accessori,
                            data_listino,
                            neo_patentati,
                            architettura,
                            coppia,
                            coppia_ibrido,
                            coppia_totale,
                            numero_giri,
                            numero_giri_ibrido,
                            numero_giri_totale,
                            valvole,
                            passo,
                            cilindri,
                            cavalli_fiscali,
                            pneumatici_anteriori,
                            pneumatici_posteriori,
                            massa_p_carico,
                            indice_carico,
                            codice_velocita,
                            cap_serb_litri,
                            cap_serb_kg,
                            paese_prod,
                            tipo_guida,
                            tipo_motore,
                            descrizione_motore,
                            cambio_descrizione,
                            nome_cambio,
                            marce,
                            codice_costruttore,
                            modello_breve_carrozzeria,
                            tipo,
                            tipo_descrizione,
                            segmento,
                            segmento_descrizione,
                            garanzia_km,
                            garanzia_tempo,
                            guado,
                            pendenza_max,
                            sosp_pneum,
                            tipo_batteria,
                            traino,
                            volumi,
                            cavalli_ibrido,
                            cavalli_totale,
                            potenza_ibrido,
                            potenza_totale,
                            motore_elettrico,
                            motore_ibrido,
                            capacita_nominale_batteria,
                            capacita_netta_batteria,
                            cavalli_elettrico_max,
                            cavalli_elettrico_boost_max,
                            potenza_elettrico_max,
                            potenza_elettrico_boost_max,
                            autonomia_media,
                            autonomia_massima,
                            equipaggiamento,
                            hc,
                            nox,
                            pm10,
                            wltp,
                            ridotte,
                            freni
                        )
                        SELECT
                            CAST(:codice AS varchar),
                            :alimentazione,
                            :cilindrata,
                            :hp,
                            :kw,
                            :euro,
                            :consumo_medio,
                            :consumo_urbano,
                            :consumo_extraurbano,
                            :emissioni_co2,
                            :tipo_cambio,
                            :trazione,
                            :porte,
                            :posti,
                            :lunghezza,
                            :larghezza,
                            :altezza,
                            :altezza_minima,
                            :peso,
                            :peso_vuoto,
                            :peso_potenza,
                            :portata,
                            :velocita,
                            :accelerazione,
                            :bagagliaio,
                            :descrizione_breve,
                            :foto,
                            :prezzo_listino,
                            :prezzo_accessori,
                            :data_listino,
                            :neo_patentati,
                            :architettura,
                            :coppia,
                            :coppia_ibrido,
                            :coppia_totale,
                            :numero_giri,
                            :numero_giri_ibrido,
                            :numero_giri_totale,
                            :valvole,
                            :passo,
                            :cilindri,
                            :cavalli_fiscali,
                            :pneumatici_anteriori,
                            :pneumatici_posteriori,
                            :massa_p_carico,
                            :indice_carico,
                            :codice_velocita,
                            :cap_serb_litri,
                            :cap_serb_kg,
                            :paese_prod,
                            :tipo_guida,
                            :tipo_motore,
                            :descrizione_motore,
                            :cambio_descrizione,
                            :nome_cambio,
                            :marce,
                            :codice_costruttore,
                            :modello_breve_carrozzeria,
                            :tipo,
                            :tipo_descrizione,
                            :segmento,
                            :segmento_descrizione,
                            :garanzia_km,
                            :garanzia_tempo,
                            :guado,
                            :pendenza_max,
                            :sosp_pneum,
                            :tipo_batteria,
                            :traino,
                            :volumi,
                            :cavalli_ibrido,
                            :cavalli_totale,
                            :potenza_ibrido,
                            :potenza_totale,
                            :motore_elettrico,
                            :motore_ibrido,
                            :capacita_nominale_batteria,
                            :capacita_netta_batteria,
                            :cavalli_elettrico_max,
                            :cavalli_elettrico_boost_max,
                            :potenza_elettrico_max,
                            :potenza_elettrico_boost_max,
                            :autonomia_media,
                            :autonomia_massima,
                            :equipaggiamento,
                            :hc,
                            :nox,
                            :pm10,
                            :wltp,
                            :ridotte,
                            :freni
                        WHERE NOT EXISTS (
                            SELECT 1
                            FROM mnet_dettagli
                            WHERE codice_motornet_uni = CAST(:codice AS varchar)
                        )
                    """),
                    {
                        "codice": codice_uni,
                        "alimentazione": (modello.get("alimentazione") or {}).get("descrizione"),
                        "cilindrata": modello.get("cilindrata"),
                        "hp": modello.get("hp"),
                        "kw": modello.get("kw"),
                        "euro": modello.get("euro"),
                        "consumo_medio": modello.get("consumoMedio"),
                        "consumo_urbano": modello.get("consumoUrbano"),
                        "consumo_extraurbano": modello.get("consumoExtraurbano"),
                        "emissioni_co2": modello.get("emissioniCo2"),
                        "tipo_cambio": (modello.get("cambio") or {}).get("descrizione"),
                        "trazione": (modello.get("trazione") or {}).get("descrizione"),
                        "porte": modello.get("porte"),
                        "posti": modello.get("posti"),
                        "lunghezza": modello.get("lunghezza"),
                        "larghezza": modello.get("larghezza"),
                        "altezza": modello.get("altezza"),
                        "altezza_minima": modello.get("altezzaMinima"),
                        "peso": modello.get("peso"),
                        "peso_vuoto": modello.get("pesoVuoto"),
                        "peso_potenza": modello.get("pesoPotenza"),
                        "portata": modello.get("portata"),
                        "velocita": modello.get("velocita"),
                        "accelerazione": modello.get("accelerazione"),
                        "bagagliaio": modello.get("bagagliaio"),
                        "descrizione_breve": modello.get("descrizioneBreve"),
                        "foto": modello.get("immagine"),
                        "prezzo_listino": modello.get("prezzoListino"),
                        "prezzo_accessori": modello.get("prezzoAccessori"),
                        "data_listino": modello.get("dataListino"),
                        "neo_patentati": modello.get("neoPatentati"),
                        "architettura": (modello.get("architettura") or {}).get("descrizione"),
                        "coppia": modello.get("coppia"),
                        "coppia_ibrido": modello.get("coppiaIbrido"),
                        "coppia_totale": modello.get("coppiaTotale"),
                        "numero_giri": modello.get("numeroGiri"),
                        "numero_giri_ibrido": modello.get("numeroGiriIbrido"),
                        "numero_giri_totale": modello.get("numeroGiriTotale"),
                        "valvole": modello.get("valvole"),
                        "passo": modello.get("passo"),
                        "cilindri": modello.get("cilindri"),
                        "cavalli_fiscali": modello.get("cavalliFiscali"),
                        "pneumatici_anteriori": modello.get("pneumaticiAnteriori"),
                        "pneumatici_posteriori": modello.get("pneumaticiPosteriori"),
                        "massa_p_carico": modello.get("massaPCarico"),
                        "indice_carico": modello.get("indiceCarico"),
                        "codice_velocita": modello.get("codVel"),
                        "cap_serb_litri": modello.get("capSerbLitri"),
                        "cap_serb_kg": modello.get("capSerbKg"),
                        "paese_prod": modello.get("paeseProd"),
                        "tipo_guida": modello.get("tipoGuida"),
                        "tipo_motore": modello.get("tipoMotore"),
                        "descrizione_motore": modello.get("descrizioneMotore"),
                        "cambio_descrizione": (modello.get("cambio") or {}).get("descrizione"),
                        "nome_cambio": modello.get("nomeCambio"),
                        "marce": modello.get("descrizioneMarce"),
                        "codice_costruttore": modello.get("codiceCostruttore"),
                        "modello_breve_carrozzeria": ((modello.get("modelloBreveCarrozzeria") or {}).get("descrizione")),
                        "tipo": (modello.get("tipo") or {}).get("codice"),
                        "tipo_descrizione": (modello.get("tipo") or {}).get("descrizione"),
                        "segmento": (modello.get("segmento") or {}).get("codice"),
                        "segmento_descrizione": (modello.get("segmento") or {}).get("descrizione"),
                        "garanzia_km": modello.get("garanziaKm"),
                        "garanzia_tempo": modello.get("garanziaTempo"),
                        "guado": modello.get("guado"),
                        "pendenza_max": modello.get("pendenzaMax"),
                        "sosp_pneum": bool(modello.get("sospPneum")) if modello.get("sospPneum") is not None else None,
                        "tipo_batteria": modello.get("tipoBatteria"),
                        "traino": modello.get("traino"),
                        "volumi": modello.get("volumi"),
                        "cavalli_ibrido": modello.get("cavalliIbrido"),
                        "cavalli_totale": modello.get("cavalliTotale"),
                        "potenza_ibrido": modello.get("potenzaIbrido"),
                        "potenza_totale": modello.get("potenzaTotale"),
                        "motore_elettrico": (modello.get("motoreElettrico") or {}).get("descrizione"),
                        "motore_ibrido": (modello.get("motoreIbrido") or {}).get("descrizione"),
                        "capacita_nominale_batteria": modello.get("capacitaNominaleBatteria"),
                        "capacita_netta_batteria": modello.get("capacitaNettaBatteria"),
                        "cavalli_elettrico_max": modello.get("cavalliElettricoMax"),
                        "cavalli_elettrico_boost_max": modello.get("cavalliElettricoBoostMax"),
                        "potenza_elettrico_max": modello.get("potenzaElettricoMax"),
                        "potenza_elettrico_boost_max": modello.get("potenzaElettricoBoostMax"),
                        "autonomia_media": modello.get("autonomiaMedia"),
                        "autonomia_massima": modello.get("autonomiaMassima"),
                        "equipaggiamento": modello.get("equipaggiamento"),
                        "hc": modello.get("hc"),
                        "nox": modello.get("nox"),
                        "pm10": modello.get("pm10"),
                        "wltp": modello.get("wltp"),
                        "ridotte": modello.get("ridotte"),
                        "freni": (modello.get("freni") or {}).get("descrizione"),
                    }
                )

                if res.rowcount == 1:
                    inserted += 1
                    logging.info("[NUOVO][DETTAGLI] inserted %s", codice_uni)

        except Exception:
            logging.exception("[NUOVO][DETTAGLI] FAILED %s", codice_uni)
            continue

    logging.info(
        "[NUOVO][DETTAGLI] DONE (new=%d, total_missing_seen=%d)",
        inserted,
        seen,
    )
