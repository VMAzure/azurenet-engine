"""
apply_faq_rewrites.py — applica TUTTE le riscritture pending in produzione.

One-shot: dopo che rewrite_faq.py ha finito di generare le riscritture,
questo script copia ogni new_answer in dealer_faq.answer e riporta il
flag needs_human_review, così l'admin DealerMax vede direttamente le
nuove FAQ nella lista e può modificarle inline.

Le FAQ con needs_human_review=True vengono mostrate con bordo colorato
nella UI admin (bordo arancio).

Uso:
  python app/jobs/apply_faq_rewrites.py --run              # applica tutte
  python app/jobs/apply_faq_rewrites.py --run --dry-run    # anteprima
"""

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from dotenv import load_dotenv
from sqlalchemy import text

from app.database import SessionLocal

load_dotenv(_PROJECT_ROOT / ".env")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def apply_rewrites(dry_run: bool = False):
    db = SessionLocal()
    try:
        # Seleziona rewrites pending più recenti per ogni FAQ
        rows = db.execute(
            text("""
                SELECT DISTINCT ON (r.faq_id)
                    r.id AS rewrite_id,
                    r.faq_id,
                    r.new_answer,
                    r.needs_human_review,
                    r.confidence,
                    f.question
                FROM dealer_faq_rewrites r
                JOIN dealer_faq f ON f.id = r.faq_id
                WHERE r.status = 'pending'
                ORDER BY r.faq_id, r.created_at DESC
            """)
        ).fetchall()

        if not rows:
            logging.info("[APPLY] nessun rewrite pending")
            return

        logging.info(f"[APPLY] {len(rows)} rewrites da applicare")
        flagged = sum(1 for r in rows if r.needs_human_review)
        high = sum(1 for r in rows if r.confidence == "high")
        medium = sum(1 for r in rows if r.confidence == "medium")
        low = sum(1 for r in rows if r.confidence == "low")
        logging.info(
            f"[APPLY] distribuzione: high={high} medium={medium} low={low} | "
            f"flagged per review={flagged}"
        )

        if dry_run:
            for r in rows[:10]:
                flag = " [FLAGGED]" if r.needs_human_review else ""
                logging.info(f"  {r.confidence}{flag} {r.question[:80]}")
            if len(rows) > 10:
                logging.info(f"  ... e altri {len(rows) - 10}")
            return

        applied = 0
        for r in rows:
            try:
                # Applica alla FAQ + riporta il flag
                db.execute(
                    text("""
                        UPDATE dealer_faq
                        SET answer = :answer,
                            needs_human_review = :flag,
                            manual_version = manual_version + 1,
                            updated_at = NOW()
                        WHERE id = :faq_id
                    """),
                    {
                        "answer": r.new_answer,
                        "flag": r.needs_human_review,
                        "faq_id": str(r.faq_id),
                    },
                )
                # Marca il rewrite come applied
                db.execute(
                    text("""
                        UPDATE dealer_faq_rewrites
                        SET status = 'applied',
                            applied_at = NOW(),
                            reviewer_notes = COALESCE(reviewer_notes, '') || ' [bulk auto-apply]'
                        WHERE id = :id
                    """),
                    {"id": str(r.rewrite_id)},
                )
                db.commit()
                applied += 1
            except Exception:
                db.rollback()
                logging.exception(f"[APPLY] failed id={r.faq_id}")
                continue

        # Eventuali altri rewrite pending (non più recenti) li scartiamo come superseded
        db.execute(
            text("""
                UPDATE dealer_faq_rewrites
                SET status = 'rejected',
                    reviewed_at = NOW(),
                    reviewer_notes = COALESCE(reviewer_notes, '') || ' [superseded by bulk apply]'
                WHERE status = 'pending'
            """)
        )
        db.commit()

        logging.info(f"[APPLY] done={applied} flagged_for_review={flagged}")

    finally:
        db.close()


def main():
    parser = argparse.ArgumentParser(description="Applica tutte le riscritture FAQ pending in produzione")
    parser.add_argument("--run", action="store_true", help="Esegui l'applicazione")
    parser.add_argument("--dry-run", action="store_true", help="Solo anteprima")
    args = parser.parse_args()

    if not args.run and not args.dry_run:
        parser.print_help()
        sys.exit(0)

    apply_rewrites(dry_run=args.dry_run)


if __name__ == "__main__":
    main()
