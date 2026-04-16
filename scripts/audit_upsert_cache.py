"""
audit_upsert_cache.py — Upsert in batch dei domini dalla seed_expand_cache.
Riconnette al DB per ogni batch per evitare timeout del pooler Supabase.
"""
from __future__ import annotations
import json, os, sys
from pathlib import Path

import psycopg
from dotenv import load_dotenv

_HERE = Path(__file__).resolve()
_ROOT = _HERE.parents[2]
load_dotenv(_ROOT / "core_api_v2" / ".env")

sys.path.insert(0, str(_HERE.parent))
from audit_seed_and_run import upsert_watchlist

BATCH = 200


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    db_url = os.environ["DATABASE_URL"]
    cache = _HERE.parent / "seed_expand_cache" / "merged_new.json"
    if not cache.exists():
        print(f"Cache mancante: {cache}", file=sys.stderr); sys.exit(1)

    rows = json.loads(cache.read_text(encoding="utf-8"))
    print(f"[upsert] {len(rows)} record dalla cache")

    total = 0
    for i in range(0, len(rows), BATCH):
        batch = rows[i:i + BATCH]
        conn = psycopg.connect(db_url, prepare_threshold=None, autocommit=False)
        try:
            upsert_watchlist(conn, batch)
            total += len(batch)
            print(f"[upsert] {total}/{len(rows)}", flush=True)
        finally:
            conn.close()
    print(f"[upsert] DONE — {total} record in DB")


if __name__ == "__main__":
    main()
