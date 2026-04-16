"""
audit_pending.py — Esegue audit su tutti i domini in audit_watchlist
che non sono ancora stati scansionati (last_scanned_at IS NULL).
Persiste in audit_scans. Re-connect per batch contro timeout pooler.
"""
from __future__ import annotations

import concurrent.futures as cf
import os, sys, time
from pathlib import Path

import psycopg
from dotenv import load_dotenv

_HERE = Path(__file__).resolve()
_ROOT = _HERE.parents[2]
load_dotenv(_ROOT / "core_api_v2" / ".env")

sys.path.insert(0, str(_HERE.parents[1]))
from app.jobs.audit_scanner import audit_domain  # noqa: E402

sys.path.insert(0, str(_HERE.parent))
from audit_seed_and_run import insert_scan  # noqa: E402

WORKERS = int(os.environ.get("AUDIT_WORKERS", "10"))
BATCH = 50


def fetch_pending(conn: psycopg.Connection, limit: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain FROM public.audit_watchlist
            WHERE is_active = TRUE AND last_scanned_at IS NULL
            ORDER BY scan_priority DESC, domain
            LIMIT %s
            """, (limit,)
        )
        return [r[0] for r in cur.fetchall()]


def process_batch(db_url: str, domains: list[str]) -> tuple[int, int]:
    ok = err = 0
    # Audit in parallelo
    results = []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        fut_map = {ex.submit(audit_domain, d): d for d in domains}
        for fut in cf.as_completed(fut_map):
            d = fut_map[fut]
            try:
                results.append((d, fut.result()))
            except Exception as e:
                err += 1
                print(f"  [audit-err] {d}: {e}", file=sys.stderr)
    # Persist batch (single connection per batch)
    conn = psycopg.connect(db_url, prepare_threshold=None, autocommit=False)
    try:
        for d, res in results:
            try:
                insert_scan(conn, res, triggered_by="worker_backfill")
                ok += 1
            except Exception as e:
                err += 1
                print(f"  [db-err] {d}: {e}", file=sys.stderr)
    finally:
        conn.close()
    return ok, err


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    db_url = os.environ["DATABASE_URL"]

    # Recupera TUTTI i pending in una sola query rapida
    conn = psycopg.connect(db_url, prepare_threshold=None)
    pending = fetch_pending(conn, limit=100_000)
    conn.close()

    total = len(pending)
    print(f"[start] {total} domini pending, workers={WORKERS}, batch={BATCH}")

    t0 = time.time()
    done = ok_tot = err_tot = 0
    for i in range(0, total, BATCH):
        batch = pending[i:i + BATCH]
        ok, err = process_batch(db_url, batch)
        ok_tot += ok; err_tot += err; done += len(batch)
        elapsed = time.time() - t0
        rate = done / elapsed if elapsed > 0 else 0
        eta_s = (total - done) / rate if rate > 0 else 0
        print(f"[{done:>5}/{total}] ok={ok_tot} err={err_tot} | {rate:.1f}/s | ETA {eta_s/60:.1f}m", flush=True)

    print(f"[DONE] ok={ok_tot} err={err_tot} in {(time.time()-t0)/60:.1f}m")


if __name__ == "__main__":
    main()
