"""
platform_fingerprint_discovery.py — Identifica fingerprint UNICI per piattaforma
analizzando 10 siti dealer ground-truth etichettati manualmente.

Input: mapping dominio → piattaforma conosciuta.
Output: per ogni piattaforma, lista di pattern unici (script src, CSS class
prefix, meta generator, header distintivi) che non appaiono nelle altre.
"""
from __future__ import annotations

import json
import re
import sys
from collections import Counter
from pathlib import Path

import httpx

GROUND_TRUTH = {
    "maininiauto.it":          "MotorK WebSpark",
    "carecar.it":              "GestionaleAuto",
    "nextcarmilano.com":       "Carmove",
    "matareseautomobili.it":   "DealerMAX v2",
    "newcarshop.it":           "Labycar",
    "automilano.it":           "One AM (custom)",
    "soccol.it":               "PHP/Slim custom",
    "mcmotors.it":             "WordPress+WPML+WPBakery",
    "scuderia76.it":           "DealerMAX v1",
    "rossocorsa.it":           "Buzzlab PHP custom",
}

USER_AGENT = "DealerMAXPlatformDiscovery/1.0 (+https://dealermax.app)"


def fetch(domain: str) -> dict | None:
    url = f"https://www.{domain}" if not domain.startswith("www.") else f"https://{domain}"
    try:
        with httpx.Client(follow_redirects=True, timeout=15.0,
                          headers={"User-Agent": USER_AGENT}, verify=True, http2=True) as c:
            r = c.get(url)
            try:
                r2 = c.get(f"https://{domain}")
            except Exception:
                r2 = None
    except Exception as e:
        print(f"[{domain}] fetch error: {e}")
        # Try plain domain
        try:
            with httpx.Client(follow_redirects=True, timeout=15.0,
                              headers={"User-Agent": USER_AGENT}, verify=False, http2=True) as c:
                r = c.get(f"https://{domain}")
        except Exception as e2:
            print(f"[{domain}] retry error: {e2}")
            return None
    return {"domain": domain, "html": r.text, "headers": dict(r.headers), "url": str(r.url)}


def extract_fingerprints(html: str, headers: dict) -> dict:
    fp = {
        "script_srcs": [],
        "link_hrefs": [],
        "class_prefixes": [],
        "id_prefixes": [],
        "data_attrs": [],
        "meta_generator": None,
        "cookies": [],
        "custom_headers": {},
        "html_comments": [],
        "body_class_tokens": [],
        "inline_script_snippets": [],
    }

    # meta generator
    m = re.search(r'<meta\s+name=["\']generator["\']\s+content=["\']([^"\']+)', html, re.IGNORECASE)
    if m:
        fp["meta_generator"] = m.group(1)

    # script srcs — third-party CDN unici
    for s in re.findall(r'<script[^>]+src=["\']([^"\']+)["\']', html, re.IGNORECASE):
        if s.startswith("//") or s.startswith("http"):
            # domain estratto
            m = re.search(r'(?://|https?://)([^/]+)', s)
            if m:
                fp["script_srcs"].append(m.group(1))
        else:
            # relative: può avere nomi file distintivi
            fp["script_srcs"].append(s)

    # link hrefs (CSS/preload)
    for h in re.findall(r'<link[^>]+href=["\']([^"\']+)["\']', html, re.IGNORECASE):
        if h.startswith("//") or h.startswith("http"):
            m = re.search(r'(?://|https?://)([^/]+)', h)
            if m:
                fp["link_hrefs"].append(m.group(1))

    # class prefixes (primi 3-4 char di ogni class)
    for c_attr in re.findall(r'class=["\']([^"\']+)["\']', html[:80000], re.IGNORECASE):
        for tok in c_attr.split():
            # Escludi token comuni (non distintivi)
            if len(tok) >= 4 and not tok.startswith(("col-", "row", "btn", "container", "mb-", "mt-", "px-", "py-", "d-", "text-", "bg-", "flex", "grid", "w-", "h-")):
                fp["class_prefixes"].append(tok[:20])

    # id prefixes
    for i_attr in re.findall(r'\sid=["\']([^"\']+)["\']', html[:80000], re.IGNORECASE):
        if len(i_attr) >= 4:
            fp["id_prefixes"].append(i_attr[:20])

    # data-* attributes
    for d in re.findall(r'\sdata-([a-zA-Z0-9-]+)\s*=', html[:80000]):
        fp["data_attrs"].append(d)

    # body class
    m = re.search(r'<body[^>]*class=["\']([^"\']+)', html, re.IGNORECASE)
    if m:
        fp["body_class_tokens"] = m.group(1).split()

    # HTML comments
    for c in re.findall(r'<!--([^-]{5,80})-->', html[:50000]):
        s = c.strip()
        if s and not s.lower().startswith(('[if', 'end', 'ie')):
            fp["html_comments"].append(s[:80])

    # Headers custom (non standard)
    STD = {"server", "date", "content-type", "content-length", "connection",
           "cache-control", "expires", "last-modified", "etag", "vary",
           "content-encoding", "accept-ranges", "alt-svc", "strict-transport-security",
           "x-frame-options", "x-content-type-options", "x-xss-protection",
           "referrer-policy", "content-security-policy", "set-cookie",
           "nel", "report-to", "cf-ray", "cf-cache-status", "age",
           "x-cache", "x-served-by", "x-timer", "via", "x-backend-server",
           "fly-request-id", "location", "transfer-encoding", "pragma",
           "permissions-policy", "feature-policy", "cross-origin-opener-policy",
           "cross-origin-embedder-policy", "cross-origin-resource-policy",
           "x-download-options", "x-permitted-cross-domain-policies",
           "x-dns-prefetch-control", "x-pingback", "x-aspnet-version",
           "link", "server-timing", "x-robots-tag", "content-digest",
           "access-control-allow-origin"}
    for k, v in headers.items():
        if k.lower() not in STD:
            fp["custom_headers"][k] = v[:120]
    # Anche server + x-powered-by sono indicativi
    if headers.get("server"):
        fp["custom_headers"]["Server"] = headers["server"]
    if headers.get("x-powered-by"):
        fp["custom_headers"]["X-Powered-By"] = headers["x-powered-by"]
    # Cookies names
    sc = headers.get("set-cookie") or ""
    for c in sc.split(","):
        m = re.match(r'\s*([a-zA-Z0-9_.-]+)=', c)
        if m:
            fp["cookies"].append(m.group(1))

    # Inline script snippets (primi 50 char di ogni script inline)
    for s in re.findall(r'<script[^>]*>([^<]{30,120})</script>', html[:40000], re.IGNORECASE):
        snippet = s.strip()[:60]
        if snippet and not snippet.startswith(("var ", "window.", "(function")):
            fp["inline_script_snippets"].append(snippet)

    return fp


def find_unique_patterns(all_fps: dict[str, dict]) -> dict[str, dict[str, list]]:
    """Per ogni piattaforma, identifica valori che appaiono SOLO in quel sito."""
    uniqueness: dict[str, dict[str, list]] = {}

    # Raccolgo tutti i valori per categoria, contando in quanti siti appaiono
    categories = ["script_srcs", "link_hrefs", "class_prefixes", "id_prefixes",
                  "data_attrs", "cookies", "body_class_tokens", "html_comments",
                  "inline_script_snippets"]

    for cat in categories:
        # conto occorrenze inter-sito: per ogni valore, in quanti siti appare
        value_in_sites: dict[str, set[str]] = {}
        for domain, fp in all_fps.items():
            for v in set(fp.get(cat, [])):
                value_in_sites.setdefault(v, set()).add(domain)
        # valori unici a un solo sito
        for domain, fp in all_fps.items():
            uniqueness.setdefault(domain, {}).setdefault(cat, [])
            for v in set(fp.get(cat, [])):
                if value_in_sites.get(v, set()) == {domain}:
                    uniqueness[domain][cat].append(v)

    # meta_generator e custom_headers vanno mostrati anche se non unici
    for domain, fp in all_fps.items():
        uniqueness.setdefault(domain, {})["meta_generator"] = [fp.get("meta_generator")] if fp.get("meta_generator") else []
        uniqueness[domain]["custom_headers"] = fp.get("custom_headers", {})

    return uniqueness


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

    all_fps: dict[str, dict] = {}
    print("Fetching all 10 sites...\n")
    for dom, platform in GROUND_TRUTH.items():
        print(f"  fetching {dom} ({platform})...", end=" ", flush=True)
        r = fetch(dom)
        if not r:
            print("FAILED")
            continue
        fp = extract_fingerprints(r["html"], r["headers"])
        all_fps[dom] = fp
        print(f"OK ({len(r['html'])/1024:.0f}KB)")

    unique = find_unique_patterns(all_fps)

    # Report per piattaforma
    print("\n" + "=" * 90)
    print("FINGERPRINT UNICI PER PIATTAFORMA")
    print("=" * 90)
    for domain, platform in GROUND_TRUTH.items():
        if domain not in unique:
            continue
        print(f"\n▸ {platform}  ({domain})")
        print("─" * 90)
        u = unique[domain]

        if u.get("meta_generator"):
            print(f"  META GENERATOR: {u['meta_generator'][0]}")

        if u.get("custom_headers"):
            print(f"  HEADERS:")
            for k, v in u["custom_headers"].items():
                print(f"    {k}: {v[:100]}")

        # Script srcs unici (massimo 10)
        ss = u.get("script_srcs", [])
        if ss:
            print(f"  SCRIPT SRC unici ({len(ss)}): " + ", ".join(sorted(ss)[:8]))

        # Link hrefs unici
        lh = u.get("link_hrefs", [])
        if lh:
            print(f"  LINK HREF unici ({len(lh)}): " + ", ".join(sorted(lh)[:8]))

        # Cookies unici
        ck = u.get("cookies", [])
        if ck:
            print(f"  COOKIES unici: " + ", ".join(sorted(ck)[:10]))

        # Body class tokens unici
        bc = u.get("body_class_tokens", [])
        if bc:
            print(f"  BODY CLASS unici: " + ", ".join(sorted(bc)[:10]))

        # Class prefixes più frequenti (anche non unici)
        cp = u.get("class_prefixes", [])
        if cp:
            # aggregati per prefix
            pref_counter = Counter(c[:8] for c in cp)
            top = [p for p, c in pref_counter.most_common(10) if c >= 2]
            if top:
                print(f"  CLASS PREFIX unici (rilevanti): " + ", ".join(top))

        # Data attr unici
        da = u.get("data_attrs", [])
        if da:
            print(f"  DATA-* unici: " + ", ".join(sorted(set(da))[:10]))

        # HTML comments unici
        hc = u.get("html_comments", [])
        if hc:
            print(f"  HTML COMMENTS: ")
            for c in hc[:3]:
                print(f"    \"{c[:80]}\"")

    # Salva raw fingerprints
    out = Path(__file__).parent / "platform_fingerprints.json"
    out.write_text(json.dumps({d: all_fps[d] for d in all_fps}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\n[saved] {out}")


if __name__ == "__main__":
    main()
