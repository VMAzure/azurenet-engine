"""
audit_seed_and_run.py — Seeder Google Places (province IT) + audit + persistenza DB.

Field mask esteso: oltre dominio/città, recupera phone, rating Google, coords,
maps URL, business_status, types. Tutto usato per enrichment DB benchmark.

Query variants per città:
  - "concessionaria auto {city}"
  - "autosalone {city}"
  - "rivenditore auto usate {city}"

De-dup per dominio. Filtra CLOSED_PERMANENTLY.

Uso:
  python scripts/audit_seed_and_run.py --mode provinces --workers 10
  python scripts/audit_seed_and_run.py --cities Milano,Roma --count 30
"""
from __future__ import annotations

import argparse
import concurrent.futures as cf
import json
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import httpx
import psycopg
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

_HERE = Path(__file__).resolve()
_ROOT = _HERE.parents[2]
load_dotenv(_ROOT / "core_api_v2" / ".env")

sys.path.insert(0, str(_HERE.parents[1]))
from app.jobs.audit_scanner import audit_domain, AuditResult  # noqa: E402

PLACES_ENDPOINT = "https://places.googleapis.com/v1/places:searchText"
FIELD_MASK = (
    "places.displayName,"
    "places.formattedAddress,"
    "places.websiteUri,"
    "places.addressComponents,"
    "places.nationalPhoneNumber,"
    "places.internationalPhoneNumber,"
    "places.rating,"
    "places.userRatingCount,"
    "places.googleMapsUri,"
    "places.location,"
    "places.businessStatus,"
    "places.types"
)

# 107 capoluoghi di provincia italiani
PROVINCES = [
    ("Agrigento", "AG", "Sicilia"), ("Alessandria", "AL", "Piemonte"),
    ("Ancona", "AN", "Marche"), ("Aosta", "AO", "Valle d'Aosta"),
    ("Arezzo", "AR", "Toscana"), ("Ascoli Piceno", "AP", "Marche"),
    ("Asti", "AT", "Piemonte"), ("Avellino", "AV", "Campania"),
    ("Bari", "BA", "Puglia"), ("Barletta", "BT", "Puglia"),
    ("Belluno", "BL", "Veneto"), ("Benevento", "BN", "Campania"),
    ("Bergamo", "BG", "Lombardia"), ("Biella", "BI", "Piemonte"),
    ("Bologna", "BO", "Emilia-Romagna"), ("Bolzano", "BZ", "Trentino-Alto Adige"),
    ("Brescia", "BS", "Lombardia"), ("Brindisi", "BR", "Puglia"),
    ("Cagliari", "CA", "Sardegna"), ("Caltanissetta", "CL", "Sicilia"),
    ("Campobasso", "CB", "Molise"), ("Caserta", "CE", "Campania"),
    ("Catania", "CT", "Sicilia"), ("Catanzaro", "CZ", "Calabria"),
    ("Chieti", "CH", "Abruzzo"), ("Como", "CO", "Lombardia"),
    ("Cosenza", "CS", "Calabria"), ("Cremona", "CR", "Lombardia"),
    ("Crotone", "KR", "Calabria"), ("Cuneo", "CN", "Piemonte"),
    ("Enna", "EN", "Sicilia"), ("Fermo", "FM", "Marche"),
    ("Ferrara", "FE", "Emilia-Romagna"), ("Firenze", "FI", "Toscana"),
    ("Foggia", "FG", "Puglia"), ("Forlì", "FC", "Emilia-Romagna"),
    ("Frosinone", "FR", "Lazio"), ("Genova", "GE", "Liguria"),
    ("Gorizia", "GO", "Friuli-Venezia Giulia"), ("Grosseto", "GR", "Toscana"),
    ("Imperia", "IM", "Liguria"), ("Isernia", "IS", "Molise"),
    ("La Spezia", "SP", "Liguria"), ("L'Aquila", "AQ", "Abruzzo"),
    ("Latina", "LT", "Lazio"), ("Lecce", "LE", "Puglia"),
    ("Lecco", "LC", "Lombardia"), ("Livorno", "LI", "Toscana"),
    ("Lodi", "LO", "Lombardia"), ("Lucca", "LU", "Toscana"),
    ("Macerata", "MC", "Marche"), ("Mantova", "MN", "Lombardia"),
    ("Massa", "MS", "Toscana"), ("Matera", "MT", "Basilicata"),
    ("Messina", "ME", "Sicilia"), ("Milano", "MI", "Lombardia"),
    ("Modena", "MO", "Emilia-Romagna"), ("Monza", "MB", "Lombardia"),
    ("Napoli", "NA", "Campania"), ("Novara", "NO", "Piemonte"),
    ("Nuoro", "NU", "Sardegna"), ("Oristano", "OR", "Sardegna"),
    ("Padova", "PD", "Veneto"), ("Palermo", "PA", "Sicilia"),
    ("Parma", "PR", "Emilia-Romagna"), ("Pavia", "PV", "Lombardia"),
    ("Perugia", "PG", "Umbria"), ("Pesaro", "PU", "Marche"),
    ("Pescara", "PE", "Abruzzo"), ("Piacenza", "PC", "Emilia-Romagna"),
    ("Pisa", "PI", "Toscana"), ("Pistoia", "PT", "Toscana"),
    ("Pordenone", "PN", "Friuli-Venezia Giulia"), ("Potenza", "PZ", "Basilicata"),
    ("Prato", "PO", "Toscana"), ("Ragusa", "RG", "Sicilia"),
    ("Ravenna", "RA", "Emilia-Romagna"), ("Reggio Calabria", "RC", "Calabria"),
    ("Reggio Emilia", "RE", "Emilia-Romagna"), ("Rieti", "RI", "Lazio"),
    ("Rimini", "RN", "Emilia-Romagna"), ("Roma", "RM", "Lazio"),
    ("Rovigo", "RO", "Veneto"), ("Salerno", "SA", "Campania"),
    ("Sassari", "SS", "Sardegna"), ("Savona", "SV", "Liguria"),
    ("Siena", "SI", "Toscana"), ("Siracusa", "SR", "Sicilia"),
    ("Sondrio", "SO", "Lombardia"), ("Taranto", "TA", "Puglia"),
    ("Teramo", "TE", "Abruzzo"), ("Terni", "TR", "Umbria"),
    ("Torino", "TO", "Piemonte"), ("Trapani", "TP", "Sicilia"),
    ("Trento", "TN", "Trentino-Alto Adige"), ("Treviso", "TV", "Veneto"),
    ("Trieste", "TS", "Friuli-Venezia Giulia"), ("Udine", "UD", "Friuli-Venezia Giulia"),
    ("Varese", "VA", "Lombardia"), ("Venezia", "VE", "Veneto"),
    ("Verbania", "VB", "Piemonte"), ("Vercelli", "VC", "Piemonte"),
    ("Verona", "VR", "Veneto"), ("Vibo Valentia", "VV", "Calabria"),
    ("Vicenza", "VI", "Veneto"), ("Viterbo", "VT", "Lazio"),
]

QUERY_VARIANTS = [
    "concessionaria auto {}",
    "autosalone {}",
    "rivenditore auto usate {}",
]


def _normalize_domain(url: str) -> str | None:
    if not url:
        return None
    try:
        p = urlparse(url if url.startswith("http") else f"https://{url}")
        host = (p.netloc or p.path).lower().strip("/")
        if host.startswith("www."):
            host = host[4:]
        blacklist = {
            "facebook.com", "instagram.com", "google.com", "autoscout24.it",
            "subito.it", "automoto.it", "autouncle.it", "youtube.com", "linkedin.com",
            "wa.me", "whatsapp.com", "m.facebook.com", "tiktok.com", "x.com", "twitter.com",
        }
        if any(host.endswith(b) for b in blacklist):
            return None
        if "." not in host:
            return None
        return host
    except Exception:
        return None


def _extract_region_from_components(address_components, fallback: str | None) -> str | None:
    if address_components:
        for c in address_components:
            if "administrative_area_level_1" in (c.get("types") or []):
                return c.get("longText") or c.get("shortText")
    return fallback


def _extract_province_from_components(address_components, fallback: str | None) -> str | None:
    if address_components:
        for c in address_components:
            if "administrative_area_level_2" in (c.get("types") or []):
                return c.get("shortText") or c.get("longText")
    return fallback


def places_search(
    client: httpx.Client,
    api_key: str,
    query: str,
    page_size: int = 20,
) -> list[dict]:
    try:
        r = client.post(
            PLACES_ENDPOINT,
            headers={
                "Content-Type": "application/json",
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": FIELD_MASK,
            },
            json={
                "textQuery": query,
                "pageSize": page_size,
                "languageCode": "it",
                "regionCode": "IT",
            },
        )
        if r.status_code != 200:
            print(f"[places] {query!r}: HTTP {r.status_code} {r.text[:150]}", file=sys.stderr)
            return []
        return r.json().get("places") or []
    except Exception as e:
        print(f"[places] {query!r}: {e}", file=sys.stderr)
        return []


def seed_provinces(api_key: str, provinces: list[tuple[str, str, str]], variants: list[str]) -> list[dict]:
    results: list[dict] = []
    seen: set[str] = set()
    n_queries = len(provinces) * len(variants)
    done = 0
    with httpx.Client(timeout=20.0) as client:
        for name, prov_code, region in provinces:
            for variant in variants:
                done += 1
                query = variant.format(name)
                places = places_search(client, api_key, query)
                new_in_query = 0
                for p in places:
                    # Filtra chiusi
                    if p.get("businessStatus") == "CLOSED_PERMANENTLY":
                        continue
                    uri = p.get("websiteUri")
                    if not uri:
                        continue
                    dom = _normalize_domain(uri)
                    if not dom or dom in seen:
                        continue
                    seen.add(dom)
                    new_in_query += 1
                    loc = p.get("location") or {}
                    results.append({
                        "domain": dom,
                        "dealer_name": (p.get("displayName") or {}).get("text"),
                        "address": p.get("formattedAddress"),
                        "city": name,
                        "province": _extract_province_from_components(p.get("addressComponents"), prov_code),
                        "region": _extract_region_from_components(p.get("addressComponents"), region),
                        "phone": p.get("nationalPhoneNumber"),
                        "phone_intl": p.get("internationalPhoneNumber"),
                        "google_rating": p.get("rating"),
                        "google_rating_count": p.get("userRatingCount"),
                        "google_maps_url": p.get("googleMapsUri"),
                        "latitude": loc.get("latitude"),
                        "longitude": loc.get("longitude"),
                        "business_status": p.get("businessStatus"),
                        "google_types": p.get("types"),
                    })
                if done % 20 == 0 or done == n_queries:
                    print(f"[seeder] {done}/{n_queries} query ({len(results)} dealer univoci)", flush=True)
                time.sleep(0.1)
    return results


def seed_cities(api_key: str, cities: list[str], per_city: int = 20) -> list[dict]:
    # Subset di PROVINCES per le città indicate
    prov_map = {name.lower(): (name, pc, r) for name, pc, r in PROVINCES}
    chosen = [prov_map.get(c.strip().lower()) for c in cities if c.strip().lower() in prov_map]
    chosen = [c for c in chosen if c]
    if not chosen:
        chosen = [(c, "", "") for c in cities]
    return seed_provinces(api_key, chosen, QUERY_VARIANTS[:1])


def upsert_watchlist(conn: psycopg.Connection, rows: list[dict]) -> int:
    if not rows:
        return 0
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO public.audit_watchlist
                (domain, dealer_name, city, province, region, address,
                 phone, phone_intl, google_rating, google_rating_count,
                 google_maps_url, latitude, longitude, business_status, google_types,
                 source)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'google_places')
            ON CONFLICT (domain) DO UPDATE SET
                dealer_name         = COALESCE(EXCLUDED.dealer_name, public.audit_watchlist.dealer_name),
                city                = COALESCE(EXCLUDED.city, public.audit_watchlist.city),
                province            = COALESCE(EXCLUDED.province, public.audit_watchlist.province),
                region              = COALESCE(EXCLUDED.region, public.audit_watchlist.region),
                address             = COALESCE(EXCLUDED.address, public.audit_watchlist.address),
                phone               = COALESCE(EXCLUDED.phone, public.audit_watchlist.phone),
                phone_intl          = COALESCE(EXCLUDED.phone_intl, public.audit_watchlist.phone_intl),
                google_rating       = COALESCE(EXCLUDED.google_rating, public.audit_watchlist.google_rating),
                google_rating_count = COALESCE(EXCLUDED.google_rating_count, public.audit_watchlist.google_rating_count),
                google_maps_url     = COALESCE(EXCLUDED.google_maps_url, public.audit_watchlist.google_maps_url),
                latitude            = COALESCE(EXCLUDED.latitude, public.audit_watchlist.latitude),
                longitude           = COALESCE(EXCLUDED.longitude, public.audit_watchlist.longitude),
                business_status     = COALESCE(EXCLUDED.business_status, public.audit_watchlist.business_status),
                google_types        = COALESCE(EXCLUDED.google_types, public.audit_watchlist.google_types)
            """,
            [(
                r["domain"], r["dealer_name"], r["city"], r["province"], r["region"], r["address"],
                r["phone"], r["phone_intl"], r["google_rating"], r["google_rating_count"],
                r["google_maps_url"], r["latitude"], r["longitude"], r["business_status"], r["google_types"],
            ) for r in rows],
        )
    conn.commit()
    return len(rows)


def _audit_flag(res: AuditResult, cid: str) -> bool | None:
    for c in res.checks:
        if c.id == cid:
            return c.status == "pass"
    return None


def _audit_evidence_int(res: AuditResult, cid: str, key: str) -> int | None:
    for c in res.checks:
        if c.id == cid and isinstance(c.evidence, dict):
            v = c.evidence.get(key)
            return int(v) if isinstance(v, (int, float)) else None
    return None


def _audit_jsonld_types(res: AuditResult) -> list[str]:
    for c in res.checks:
        if c.id == "machine.jsonld" and isinstance(c.evidence, dict):
            return c.evidence.get("types_found") or []
    return []


def insert_scan(conn: psycopg.Connection, res: AuditResult, triggered_by: str = "worker_seed") -> None:
    with conn.cursor() as cur:
        # score_ai legacy: popolato = score_machine (retrocompatibilità con
        # query esistenti mentre si completa la migrazione ai 4 assi).
        machine = res.scores.get("machine")
        ainative = res.scores.get("ainative")
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
                _audit_flag(res, "ainative.llms_txt"),
                _audit_flag(res, "ainative.llms_full"),
                _audit_flag(res, "ainative.ai_txt"),
                _audit_flag(res, "ainative.plugin_manifest"),
                _audit_flag(res, "ainative.ai_sitemap"),
                _audit_flag(res, "ainative.dataset"),
                _audit_flag(res, "ainative.speakable"),
                _audit_evidence_int(res, "ainative.robots_ai_ua", "count") or 0,
                _audit_jsonld_types(res),
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


def run_audits_with_progress(
    domains: list[dict],
    conn: psycopg.Connection | None,
    max_workers: int = 8,
    triggered_by: str = "worker_seed",
) -> tuple[int, int]:
    ok = err = 0
    total = len(domains)
    with cf.ThreadPoolExecutor(max_workers=max_workers) as ex:
        fut_map = {ex.submit(audit_domain, d["domain"]): d for d in domains}
        for i, fut in enumerate(cf.as_completed(fut_map), 1):
            meta = fut_map[fut]
            try:
                res = fut.result()
                if conn is not None:
                    try:
                        insert_scan(conn, res, triggered_by=triggered_by)
                    except Exception as e:
                        print(f"[db] insert {res.domain}: {e}", file=sys.stderr)
                ok += 1
            except Exception as e:
                err += 1
                print(f"[audit] {meta['domain']}: {e}", file=sys.stderr)
            if i % 25 == 0 or i == total:
                print(f"[audit] {i}/{total} — ok={ok} err={err}", flush=True)
    return ok, err


def print_summary(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*), COUNT(last_scanned_at) FROM public.audit_watchlist")
        wl_total, wl_scanned = cur.fetchone()
        cur.execute("""
            SELECT COUNT(*), ROUND(AVG(score_total)::numeric, 2),
                   ROUND(AVG(score_ai)::numeric, 2),
                   SUM(CASE WHEN has_llms_txt THEN 1 ELSE 0 END),
                   SUM(CASE WHEN has_dataset THEN 1 ELSE 0 END),
                   SUM(CASE WHEN ai_ua_allowlisted >= 5 THEN 1 ELSE 0 END)
            FROM public.audit_scans_latest
        """)
        sc_total, avg_tot, avg_ai, w_llms, w_ds, w_ua = cur.fetchone()
        cur.execute("""
            SELECT platform_name, COUNT(*) AS n,
                   ROUND(AVG(score_total)::numeric, 2) AS avg_tot,
                   ROUND(AVG(score_ai)::numeric, 2) AS avg_ai
            FROM public.audit_scans_latest
            GROUP BY platform_name
            ORDER BY n DESC LIMIT 10
        """)
        platforms = cur.fetchall()
        cur.execute("""
            SELECT region, COUNT(*) AS n, ROUND(AVG(score_total)::numeric, 2) AS avg_tot
            FROM public.audit_scans_latest
            WHERE region IS NOT NULL
            GROUP BY region ORDER BY n DESC LIMIT 10
        """)
        regions = cur.fetchall()
    print()
    print("=" * 80)
    print(f"DB SUMMARY — watchlist {wl_total} (scanned {wl_scanned}), scans_latest {sc_total}")
    print(f"  Media: TOT {avg_tot}  AI {avg_ai}  | llms.txt {w_llms}/{sc_total}  Dataset {w_ds}/{sc_total}  UA_AI {w_ua}/{sc_total}")
    print("\nTop piattaforme:")
    for p in platforms:
        print(f"  {p[0] or '-':<30} n={p[1]:>4}  avg_tot={p[2]}  avg_ai={p[3]}")
    print("\nTop regioni:")
    for r in regions:
        print(f"  {r[0]:<25} n={r[1]:>4}  avg_tot={r[2]}")
    print("=" * 80)


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["provinces", "cities"], default="provinces")
    p.add_argument("--cities", type=str, default=None)
    p.add_argument("--count", type=int, default=0, help="Cap max dealer per seed (0=no limit)")
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--variants", type=int, default=3, help="Numero query variants per città (1-3)")
    p.add_argument("--limit-provinces", type=int, default=0, help="Per test: limita a N province")
    p.add_argument("--no-db", action="store_true")
    p.add_argument("--seed-cache", type=str, default=str(_HERE.parent / "seed_cache.json"),
                   help="JSON file per caching risultati Google Places (evita di ripagare)")
    p.add_argument("--skip-seed", action="store_true",
                   help="Usa SOLO il seed cache, NON chiama Google Places (0€)")
    p.add_argument("--skip-audit", action="store_true",
                   help="Solo seed+upsert watchlist, salta audit")
    args = p.parse_args()

    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    api_key = os.environ.get("GOOGLE_PLACES_API_KEY") or os.environ.get("GOOGLE_MAPS_API_KEY")
    db_url = os.environ.get("DATABASE_URL")
    if not api_key:
        print("ERROR: GOOGLE_PLACES_API_KEY missing", file=sys.stderr); sys.exit(1)
    if not db_url and not args.no_db:
        print("ERROR: DATABASE_URL missing", file=sys.stderr); sys.exit(1)

    # SEED (con cache JSON — evita di ri-pagare Google Places)
    cache_path = Path(args.seed_cache)
    if args.skip_seed:
        if not cache_path.exists():
            print(f"ERROR: --skip-seed ma {cache_path} non esiste. Lancia senza --skip-seed prima.", file=sys.stderr)
            sys.exit(1)
        seeded = json.loads(cache_path.read_text(encoding="utf-8"))
        print(f"[run] SEED CACHE: {len(seeded)} dealer da {cache_path} (0€ Google Places)")
    elif args.mode == "provinces":
        provs = PROVINCES[: args.limit_provinces] if args.limit_provinces else PROVINCES
        variants = QUERY_VARIANTS[: max(1, min(args.variants, 3))]
        print(f"[run] Seed su {len(provs)} province × {len(variants)} variants = {len(provs)*len(variants)} query")
        t0 = time.time()
        seeded = seed_provinces(api_key, provs, variants)
        print(f"[run] Seed completo: {len(seeded)} dealer univoci in {time.time()-t0:.0f}s")
        # SALVA CACHE SUBITO (prima di qualsiasi operazione DB che può fallire)
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        cache_path.write_text(json.dumps(seeded, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[cache] Seed salvato in {cache_path} ({len(seeded)} record, usabile con --skip-seed)")
    else:
        cities = [c.strip() for c in (args.cities or "Milano").split(",") if c.strip()]
        seeded = seed_cities(api_key, cities)
        print(f"[run] Seed city mode: {len(seeded)} dealer")

    if args.count > 0 and len(seeded) > args.count:
        seeded = seeded[: args.count]

    if not seeded:
        print("[run] Nessun dealer. Abort."); sys.exit(2)

    # prepare_threshold=None: disabilita prepared statements per compatibilità
    # con il Supabase pooler (porta 6543, transaction mode).
    conn = psycopg.connect(db_url, prepare_threshold=None, autocommit=False) if not args.no_db else None

    if conn:
        n_up = upsert_watchlist(conn, seeded)
        print(f"[db] Upsert watchlist: {n_up} record")

    if args.skip_audit:
        print("[run] --skip-audit: fine.")
        if conn:
            print_summary(conn); conn.close()
        return

    # AUDIT
    print(f"[run] Audit {len(seeded)} domini × {args.workers} workers...")
    t1 = time.time()
    ok, err = run_audits_with_progress(seeded, conn, max_workers=args.workers)
    print(f"[run] Audit completo in {time.time()-t1:.0f}s — ok={ok} err={err}")

    if conn:
        print_summary(conn)
        conn.close()


if __name__ == "__main__":
    main()
