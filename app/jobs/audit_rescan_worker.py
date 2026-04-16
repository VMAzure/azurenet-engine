"""
audit_rescan_worker.py — Job APScheduler per rescan domini pending (score_machine IS NULL).

Gira come job periodico (ogni 5 min). Ad ogni run processa un batch di 50 domini.
Quando non ci sono più pending, il job gira a vuoto (0 operazioni, costo zero).

Pensato per girare su Railway dove la connettività al DB è locale (~1ms).
"""
from __future__ import annotations

import concurrent.futures as cf
import logging
import os
import time

import psycopg

from app.jobs.audit_scanner import audit_domain

logger = logging.getLogger(__name__)

BATCH_SIZE = 50
WORKERS = 10


def _get_db_url() -> str:
    return os.environ["DATABASE_URL"]


def _fetch_pending(conn: psycopg.Connection, limit: int) -> list[str]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT domain FROM public.audit_watchlist
            WHERE is_active = TRUE AND last_scanned_at IS NULL
            ORDER BY scan_priority DESC, domain
            LIMIT %s
            """,
            (limit,),
        )
        return [r[0] for r in cur.fetchall()]


def _insert_scan(conn: psycopg.Connection, res, triggered_by: str = "worker_rescan") -> None:
    """Insert scan con 4 assi (score_machine + score_ainative)."""
    from app.jobs.audit_scanner import AuditResult

    def _flag(cid: str) -> bool | None:
        for c in res.checks:
            if c.id == cid:
                return c.status == "pass"
        return None

    def _ev_int(cid: str, key: str) -> int:
        for c in res.checks:
            if c.id == cid and isinstance(c.evidence, dict):
                v = c.evidence.get(key)
                return int(v) if isinstance(v, (int, float)) else 0
        return 0

    def _jsonld_types() -> list[str]:
        for c in res.checks:
            if c.id == "machine.jsonld" and isinstance(c.evidence, dict):
                return c.evidence.get("types_found") or []
        return []

    import json
    from psycopg.types.json import Jsonb

    machine = res.scores.get("machine")
    ainative = res.scores.get("ainative")

    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO public.audit_scans (
                domain, triggered_by,
                score_tech, score_seo, score_machine, score_ainative,
                score_ai, score_total,
                platform_name, platform_confidence,
                http_status, http_ttfb_ms, html_bytes, cdn_hint,
                has_llms_txt, has_llms_full, has_ai_txt, has_ai_plugin, has_ai_sitemap,
                has_dataset, has_speakable, ai_ua_allowlisted,
                jsonld_types, evidences, errors, www_fallback_used
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                res.domain, triggered_by,
                res.scores.get("tech"), res.scores.get("seo"), machine, ainative,
                machine, res.scores.get("total"),
                res.platform.get("name"), res.platform.get("confidence"),
                res.http.get("status"), res.http.get("ttfb_ms"),
                res.http.get("html_bytes"), res.http.get("cdn_hint"),
                _flag("ainative.llms_txt"),
                _flag("ainative.llms_full"),
                _flag("ainative.ai_txt"),
                _flag("ainative.plugin_manifest"),
                _flag("ainative.ai_sitemap"),
                _flag("ainative.dataset"),
                _flag("ainative.speakable"),
                _ev_int("ainative.robots_ai_ua", "count") or 0,
                _jsonld_types(),
                Jsonb({c.id: {"status": c.status, "score": c.score, "evidence": c.evidence} for c in res.checks}),
                res.errors or None,
                bool(res.http.get("www_fallback_used", False)),
            ),
        )
        cur.execute(
            "UPDATE public.audit_watchlist SET last_scanned_at = NOW() WHERE domain = %s",
            (res.domain,),
        )
    conn.commit()


def audit_rescan_batch():
    """Processa un batch di domini pending. Chiamato dallo scheduler."""
    db_url = _get_db_url()

    conn = psycopg.connect(db_url, prepare_threshold=None)
    pending = _fetch_pending(conn, BATCH_SIZE)
    conn.close()

    if not pending:
        return  # niente da fare

    logger.info(f"[audit-rescan] processing {len(pending)} pending domains")
    t0 = time.time()

    # Audit in parallelo
    results = []
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        fut_map = {ex.submit(audit_domain, d): d for d in pending}
        for fut in cf.as_completed(fut_map):
            d = fut_map[fut]
            try:
                results.append(fut.result())
            except Exception as e:
                logger.warning(f"[audit-rescan] {d}: {e}")

    # Persist batch
    ok = err = 0
    conn = psycopg.connect(db_url, prepare_threshold=None)
    for res in results:
        try:
            _insert_scan(conn, res)
            ok += 1
        except Exception as e:
            err += 1
            logger.warning(f"[audit-rescan] db insert {res.domain}: {e}")
    conn.close()

    elapsed = time.time() - t0
    logger.info(f"[audit-rescan] batch done: ok={ok} err={err} in {elapsed:.1f}s")
