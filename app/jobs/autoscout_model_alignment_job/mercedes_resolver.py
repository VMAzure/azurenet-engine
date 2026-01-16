import re
from typing import Optional, Tuple
from sqlalchemy import text
from app.database import SessionLocal


# ============================================================
# MERCEDES RESOLVER (DEDICATO)
# ============================================================

def match_mercedes_model(
    allestimento: str,
    as24_model_names: list[str]
) -> tuple[str | None, str, float]:

    if not allestimento:
        return None, "NONE", 0.0

    text = allestimento.upper().strip()

    # normalizza modelli AS24
    as24_norm = {m.upper(): m for m in as24_model_names}

    # --------------------------------------------------
    # 0️⃣ ESCLUSIONE VEICOLI COMMERCIALI
    # --------------------------------------------------
    if re.match(r"^(V|VIANO|VITO|SPRINTER)\b", text):
        return None, "COMMERCIAL_VEHICLE", 0.0

    # --------------------------------------------------
    # 1️⃣ MATCH SIGLA + NUMERO (caso standard, PRIORITÀ MASSIMA)
    # es: C 220, E 350, A 250, GLC 300, EQE 350
    # --------------------------------------------------
    m = re.match(r"^([A-Z]{1,4})\s+(?:SW|COUPE|CABRIO|SHOOTING BRAKE|SUV)?\s*(\d{3})\b", text)
    if m:
        sigla = m.group(1)
        numero = m.group(2)
        candidate = f"{sigla} {numero}"

        if candidate in as24_norm:
            return as24_norm[candidate], "MERCEDES_SIGLA_NUMERO", 0.99

    # --------------------------------------------------
    # 2️⃣ EQ con varianti (EQE SUV 500, CLA EQ 250+)
    # --------------------------------------------------
    m_eq = re.match(r"^(EQ[A-Z]{0,2}|CLA EQ)\s+(?:SUV\s+)?(\d{3})\+?\b", text)
    if m_eq:
        sigla = m_eq.group(1).replace(" ", "")
        numero = m_eq.group(2)
        candidate = f"{sigla} {numero}"

        if candidate in as24_norm:
            return as24_norm[candidate], "MERCEDES_EQ", 0.99

    # --------------------------------------------------
    # 3️⃣ Fallback famiglia (solo sigle >= 3 lettere)
    # es: CLK, CLS, SLK, SLS
    # --------------------------------------------------
    m_fam = re.match(r"^([A-Z]{3,4})\b", text)
    if m_fam:
        sigla = m_fam.group(1)
        if sigla in as24_norm:
            return as24_norm[sigla], "MERCEDES_FAMILY", 0.90

    return None, "NONE", 0.0




# ============================================================
# JOB PRINCIPALE
# ============================================================

def populate_autoscout_model_map_v2():
    db = SessionLocal()

    # --------------------------------------------------------
    # 1) CARICA MODELLI AS24 — SOLO MERCEDES-BENZ
    # --------------------------------------------------------
    as24_rows = db.execute(text("""
        SELECT
            autoscout_make_id,
            autoscout_model_id,
            autoscout_model_name
        FROM autoscout_reference_models
        WHERE is_active = true
          AND autoscout_make_name = 'Mercedes-Benz'
    """)).mappings().all()

    if not as24_rows:
        print("[ABORT] Nessun modello AS24 Mercedes-Benz trovato")
        return

    as24_model_names = [r["autoscout_model_name"] for r in as24_rows]
    as24_by_name = {r["autoscout_model_name"]: r for r in as24_rows}

    print(f"[INIT] AS24 Mercedes-Benz models loaded: {len(as24_model_names)}")

    # --------------------------------------------------------
    # 2) CARICA MOTORNEL — SOLO MERCEDES (MER%)
    # --------------------------------------------------------
    rows = db.execute(text("""
        SELECT
            du.codice_motornet_uni,
            du.allestimento
        FROM mnet_dettagli_usato du
        WHERE du.codice_motornet_uni LIKE 'MER%'
          AND du.allestimento IS NOT NULL
          AND trim(du.allestimento) <> ''
    """)).mappings().all()

    print(f"[INIT] MNET Mercedes rows loaded: {len(rows)}")

    # --------------------------------------------------------
    # 3) LOOP + INSERT
    # --------------------------------------------------------
    inserted = 0

    for i, row in enumerate(rows, start=1):
        codice_uni = row["codice_motornet_uni"]
        allestimento = row["allestimento"]

        model_name, reason, confidence = match_mercedes_model(
            allestimento,
            as24_model_names
        )

        if not model_name:
            continue

        as24_model = as24_by_name[model_name]

        db.execute(text("""
            INSERT INTO autoscout_model_map_v2 (
                codice_motornet_uni,
                mnet_marca,
                mnet_allestimento,
                as24_make_id,
                as24_model_id,
                source,
                confidence
            )
            VALUES (
                :codice_uni,
                'mercedes',
                :allestimento,
                :make_id,
                :model_id,
                'mercedes_resolver_v1',
                :confidence
            )
            ON CONFLICT (codice_motornet_uni)
            DO NOTHING
        """), {
            "codice_uni": codice_uni,
            "allestimento": allestimento,
            "make_id": as24_model["autoscout_make_id"],
            "model_id": as24_model["autoscout_model_id"],
            "confidence": confidence
        })

        inserted += 1

        if i % 500 == 0:
            db.commit()
            print(f"[PROGRESS] processed={i} inserted≈{inserted}")

    db.commit()
    db.close()

    print(f"[DONE] processed={len(rows)} inserted≈{inserted}")


if __name__ == "__main__":
    populate_autoscout_model_map_v2()
