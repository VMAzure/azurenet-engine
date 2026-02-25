import logging
from sqlalchemy import text
from app.database import DBSession


def run():
    logging.info("🔎 NLT — disattivazione offerte fuori catalogo")

    with DBSession() as db:

        # 1️⃣ Offerte con modello non più presente nel dominio NUOVO
        result_missing = db.execute(text("""
            UPDATE public.nlt_offerte o
            SET attivo = FALSE
            WHERE o.attivo = TRUE
              AND o.codice_modello IS NOT NULL
              AND o.codice_modello NOT IN (
                  SELECT m.codice_modello
                  FROM public.mnet_modelli m
              )
            RETURNING o.id_offerta;
        """))

        missing_ids = [row.id_offerta for row in result_missing]

        # 2️⃣ Offerte con modello presente ma fuori commercializzazione
        result_expired = db.execute(text("""
            UPDATE public.nlt_offerte o
            SET attivo = FALSE
            FROM public.mnet_modelli m
            WHERE o.attivo = TRUE
              AND o.codice_modello = m.codice_modello
              AND m.fine_commercializzazione IS NOT NULL
              AND m.fine_commercializzazione < CURRENT_DATE
            RETURNING o.id_offerta;
        """))

        expired_ids = [row.id_offerta for row in result_expired]

        db.commit()

    logging.info(
        f"✅ NLT disattivate: {len(missing_ids)} fuori dominio + "
        f"{len(expired_ids)} fine commercializzazione"
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run()