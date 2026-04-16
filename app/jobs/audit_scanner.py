"""
audit_scanner.py — Audit tecnico/SEO/AI-readiness di un sito dealer.

Tutti i check sono DETERMINISTICI (HTTP + parser). Zero AI nell'audit stesso.
La platform detection fuzzy (opzionale) usa Gemini 2.5 Flash, solo come
fallback quando i fingerprint statici non bastano.

Output: dict JSON serializzabile con tre assi scorecard (tech/seo/ai) + evidenze.

CLI standalone:
    python -m app.jobs.audit_scanner https://www.matareseautomobili.it
    python -m app.jobs.audit_scanner https://www.maininiauto.it --pretty
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import asdict, dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

import httpx

# ─────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────
USER_AGENT = "DealerMAXAuditBot/1.0 (+https://dealermax.app/audit-bot)"
REQUEST_TIMEOUT = 12.0
MAX_HTML_BYTES = 2_500_000  # 2.5 MB cap per HTML grandi
AI_UA_LIST = [
    "GPTBot", "OAI-SearchBot", "ChatGPT-User", "ClaudeBot", "Claude-Web",
    "anthropic-ai", "PerplexityBot", "Perplexity-User", "Google-Extended",
    "CCBot", "Applebot-Extended", "Meta-ExternalAgent", "MistralAI-User",
    "Bytespider", "Amazonbot", "cohere-ai",
]

# ─────────────────────────────────────────────────────────────
# DATA CLASSES
# ─────────────────────────────────────────────────────────────
@dataclass
class CheckResult:
    id: str
    label: str
    axis: str               # 'tech'|'seo'|'ai'
    status: str             # 'pass'|'warn'|'fail'|'skip'
    score: float            # 0.0..1.0
    weight: float = 1.0
    evidence: Any = None

    def weighted(self) -> float:
        return self.score * self.weight


@dataclass
class AuditResult:
    domain: str
    final_url: str
    scanned_at: float
    scores: dict[str, float] = field(default_factory=dict)  # tech, seo, ai, total
    checks: list[CheckResult] = field(default_factory=list)
    platform: dict[str, Any] = field(default_factory=dict)
    http: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "domain": self.domain,
            "final_url": self.final_url,
            "scanned_at": self.scanned_at,
            "scores": self.scores,
            "platform": self.platform,
            "http": self.http,
            "errors": self.errors,
            "checks": [asdict(c) for c in self.checks],
        }


# ─────────────────────────────────────────────────────────────
# HTTP HELPERS
# ─────────────────────────────────────────────────────────────
def _normalize_url(raw: str) -> str:
    raw = raw.strip()
    if not raw.startswith(("http://", "https://")):
        raw = "https://" + raw
    return raw


def _fetch(client: httpx.Client, url: str, **kw) -> httpx.Response | None:
    try:
        r = client.get(url, timeout=REQUEST_TIMEOUT, **kw)
        return r
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────
# TECH AXIS CHECKS
# ─────────────────────────────────────────────────────────────
def check_tech_ttfb(resp: httpx.Response | None, timing: float | None) -> CheckResult:
    if timing is None:
        return CheckResult("tech.ttfb", "TTFB homepage", "tech", "skip", 0.0, 1.5)
    if timing < 0.3:
        return CheckResult("tech.ttfb", "TTFB homepage", "tech", "pass", 1.0, 1.5, f"{timing*1000:.0f} ms")
    if timing < 0.5:
        return CheckResult("tech.ttfb", "TTFB homepage", "tech", "pass", 0.9, 1.5, f"{timing*1000:.0f} ms")
    if timing < 0.8:
        return CheckResult("tech.ttfb", "TTFB homepage", "tech", "warn", 0.6, 1.5, f"{timing*1000:.0f} ms")
    if timing < 1.5:
        return CheckResult("tech.ttfb", "TTFB homepage", "tech", "warn", 0.3, 1.5, f"{timing*1000:.0f} ms")
    return CheckResult("tech.ttfb", "TTFB homepage", "tech", "fail", 0.0, 1.5, f"{timing*1000:.0f} ms (>1.5s)")


def check_tech_http_version(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("tech.http_version", "HTTP/2 o HTTP/3", "tech", "skip", 0.0, 1.0)
    v = str(resp.http_version or "").upper()
    if "3" in v:
        return CheckResult("tech.http_version", "HTTP/2 o HTTP/3", "tech", "pass", 1.0, 1.0, v)
    if "2" in v:
        return CheckResult("tech.http_version", "HTTP/2 o HTTP/3", "tech", "pass", 0.9, 1.0, v)
    return CheckResult("tech.http_version", "HTTP/2 o HTTP/3", "tech", "fail", 0.0, 1.0, v or "HTTP/1.1")


def check_tech_compression(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("tech.compression", "Compressione Brotli/gzip", "tech", "skip", 0.0, 0.8)
    enc = (resp.headers.get("content-encoding") or "").lower()
    if "br" in enc:
        return CheckResult("tech.compression", "Compressione Brotli/gzip", "tech", "pass", 1.0, 0.8, "br")
    if "gzip" in enc or "deflate" in enc:
        return CheckResult("tech.compression", "Compressione Brotli/gzip", "tech", "pass", 0.7, 0.8, enc)
    if "zstd" in enc:
        return CheckResult("tech.compression", "Compressione Brotli/gzip", "tech", "pass", 1.0, 0.8, "zstd")
    return CheckResult("tech.compression", "Compressione Brotli/gzip", "tech", "fail", 0.0, 0.8, enc or "none")


def check_tech_cdn(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("tech.cdn", "CDN attivo", "tech", "skip", 0.0, 0.8)
    h = {k.lower(): v for k, v in resp.headers.items()}
    cdn = None
    if "cf-ray" in h or "cloudflare" in (h.get("server", "").lower()):
        cdn = "Cloudflare"
    elif "x-amz-cf-id" in h or "cloudfront" in (h.get("server", "").lower()):
        cdn = "CloudFront"
    elif "x-served-by" in h and "fastly" in h.get("x-served-by", "").lower():
        cdn = "Fastly"
    elif "x-vercel-id" in h:
        cdn = "Vercel Edge"
    elif "x-railway-edge" in h:
        cdn = "Railway Edge"
    elif "x-akamai" in h or "akamai" in h.get("server", "").lower():
        cdn = "Akamai"
    if cdn:
        return CheckResult("tech.cdn", "CDN attivo", "tech", "pass", 1.0, 0.8, cdn)
    return CheckResult("tech.cdn", "CDN attivo", "tech", "warn", 0.3, 0.8, "nessuno rilevato")


def check_tech_cache_control(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("tech.cache_control", "Cache-Control su homepage", "tech", "skip", 0.0, 0.6)
    cc = (resp.headers.get("cache-control") or "").lower()
    if not cc:
        return CheckResult("tech.cache_control", "Cache-Control su homepage", "tech", "fail", 0.0, 0.6, "missing")
    if "no-store" in cc:
        return CheckResult("tech.cache_control", "Cache-Control su homepage", "tech", "fail", 0.1, 0.6, cc)
    if "max-age=0" in cc and "public" not in cc:
        return CheckResult("tech.cache_control", "Cache-Control su homepage", "tech", "warn", 0.4, 0.6, cc)
    return CheckResult("tech.cache_control", "Cache-Control su homepage", "tech", "pass", 1.0, 0.6, cc)


def check_tech_security_headers(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("tech.security", "Security headers", "tech", "skip", 0.0, 1.0)
    h = {k.lower(): v for k, v in resp.headers.items()}
    wanted = {
        "strict-transport-security": 1,
        "x-content-type-options": 1,
        "x-frame-options": 1,
        "referrer-policy": 1,
        "content-security-policy": 2,  # bonus
    }
    found = {k: h.get(k) for k in wanted if h.get(k)}
    base_count = sum(1 for k in ["strict-transport-security", "x-content-type-options", "x-frame-options", "referrer-policy"] if k in found)
    score = base_count / 4.0
    if "content-security-policy" in found:
        score = min(1.0, score + 0.1)
    status = "pass" if score >= 0.9 else ("warn" if score >= 0.5 else "fail")
    return CheckResult("tech.security", "Security headers", "tech", status, score, 1.0, list(found.keys()))


def check_tech_redirect_chain(redirects: int) -> CheckResult:
    if redirects == 0:
        return CheckResult("tech.redirects", "Redirect chain homepage", "tech", "pass", 1.0, 0.5, "0 hop")
    if redirects == 1:
        return CheckResult("tech.redirects", "Redirect chain homepage", "tech", "pass", 0.9, 0.5, "1 hop")
    if redirects == 2:
        return CheckResult("tech.redirects", "Redirect chain homepage", "tech", "warn", 0.5, 0.5, "2 hop")
    return CheckResult("tech.redirects", "Redirect chain homepage", "tech", "fail", 0.1, 0.5, f"{redirects} hop")


def check_tech_ssr(html: str) -> CheckResult:
    # SSR heuristic: presenza di H1 + almeno un prezzo/listing nel DOM statico
    has_h1 = bool(re.search(r"<h1[\s>]", html, re.IGNORECASE))
    has_price = bool(re.search(r"€\s?\d|EUR\s?\d|\bprezzo\b", html, re.IGNORECASE))
    has_body_text = len(re.sub(r"<[^>]+>", " ", html)) > 3000
    score = sum([has_h1, has_price, has_body_text]) / 3.0
    status = "pass" if score >= 0.66 else ("warn" if score >= 0.33 else "fail")
    ev = {"has_h1": has_h1, "has_price_in_html": has_price, "text_chars": len(re.sub(r"<[^>]+>", " ", html))}
    return CheckResult("tech.ssr", "SSR (contenuto nel DOM statico)", "tech", status, score, 1.5, ev)


def check_tech_blocking_scripts(html: str) -> CheckResult:
    # Conta <script src=...> senza defer/async nel <head>
    head_match = re.search(r"<head[^>]*>(.*?)</head>", html, re.IGNORECASE | re.DOTALL)
    head = head_match.group(1) if head_match else html[:20000]
    scripts = re.findall(r"<script\b[^>]*\bsrc\s*=\s*['\"][^'\"]+['\"][^>]*>", head, re.IGNORECASE)
    blocking = [s for s in scripts if not re.search(r"\b(defer|async|type\s*=\s*['\"]module['\"])\b", s, re.IGNORECASE)]
    n = len(blocking)
    if n == 0:
        return CheckResult("tech.blocking_scripts", "Script render-blocking (head)", "tech", "pass", 1.0, 1.0, 0)
    if n <= 3:
        return CheckResult("tech.blocking_scripts", "Script render-blocking (head)", "tech", "pass", 0.8, 1.0, n)
    if n <= 8:
        return CheckResult("tech.blocking_scripts", "Script render-blocking (head)", "tech", "warn", 0.5, 1.0, n)
    return CheckResult("tech.blocking_scripts", "Script render-blocking (head)", "tech", "fail", 0.1, 1.0, f"{n} script")


def check_tech_server_leak(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("tech.server_leak", "Nessuna info versione server leakata", "tech", "skip", 0.0, 0.4)
    leaks = []
    x_pow = resp.headers.get("x-powered-by")
    server = resp.headers.get("server")
    if x_pow:
        leaks.append(f"x-powered-by: {x_pow}")
    if server and re.search(r"\d", server):  # versione numerica
        leaks.append(f"server: {server}")
    if not leaks:
        return CheckResult("tech.server_leak", "Nessuna info versione server leakata", "tech", "pass", 1.0, 0.4, None)
    return CheckResult("tech.server_leak", "Nessuna info versione server leakata", "tech", "warn", 0.4, 0.4, leaks)


# ─────────────────────────────────────────────────────────────
# SEO AXIS CHECKS
# ─────────────────────────────────────────────────────────────
def check_seo_title(html: str) -> CheckResult:
    m = re.search(r"<title[^>]*>(.*?)</title>", html, re.IGNORECASE | re.DOTALL)
    if not m:
        return CheckResult("seo.title", "Title tag", "seo", "fail", 0.0, 1.0, "missing")
    t = m.group(1).strip()
    n = len(t)
    if 50 <= n <= 60:
        return CheckResult("seo.title", "Title tag", "seo", "pass", 1.0, 1.0, f"{n} chars")
    if 30 <= n <= 70:
        return CheckResult("seo.title", "Title tag", "seo", "pass", 0.8, 1.0, f"{n} chars")
    if n < 30:
        return CheckResult("seo.title", "Title tag", "seo", "warn", 0.4, 1.0, f"{n} chars (corto)")
    return CheckResult("seo.title", "Title tag", "seo", "warn", 0.4, 1.0, f"{n} chars (lungo)")


def check_seo_meta_description(html: str) -> CheckResult:
    m = re.search(r'<meta\s+name=["\']description["\']\s+content=["\']([^"\']+)', html, re.IGNORECASE)
    if not m:
        return CheckResult("seo.meta_desc", "Meta description", "seo", "fail", 0.0, 0.8, "missing")
    d = m.group(1).strip()
    n = len(d)
    if 130 <= n <= 160:
        return CheckResult("seo.meta_desc", "Meta description", "seo", "pass", 1.0, 0.8, f"{n} chars")
    if 100 <= n <= 180:
        return CheckResult("seo.meta_desc", "Meta description", "seo", "pass", 0.8, 0.8, f"{n} chars")
    return CheckResult("seo.meta_desc", "Meta description", "seo", "warn", 0.4, 0.8, f"{n} chars")


def check_seo_h1(html: str) -> CheckResult:
    matches = re.findall(r"<h1[^>]*>(.*?)</h1>", html, re.IGNORECASE | re.DOTALL)
    count = len(matches)
    if count == 0:
        return CheckResult("seo.h1", "H1 presente", "seo", "fail", 0.0, 1.0, 0)
    if count == 1:
        return CheckResult("seo.h1", "H1 presente", "seo", "pass", 1.0, 1.0, 1)
    return CheckResult("seo.h1", "H1 presente", "seo", "warn", 0.5, 1.0, f"{count} H1 (dovrebbe essere 1)")


def check_seo_canonical(html: str, final_url: str) -> CheckResult:
    m = re.search(r'<link\s+rel=["\']canonical["\']\s+href=["\']([^"\']+)', html, re.IGNORECASE)
    if not m:
        return CheckResult("seo.canonical", "Canonical tag", "seo", "fail", 0.0, 1.0, "missing")
    href = m.group(1).strip()
    if not href.startswith("https://"):
        return CheckResult("seo.canonical", "Canonical tag", "seo", "fail", 0.2, 1.0, f"non-HTTPS: {href}")
    return CheckResult("seo.canonical", "Canonical tag", "seo", "pass", 1.0, 1.0, href)


def check_seo_robots_meta(html: str) -> CheckResult:
    m = re.search(r'<meta\s+name=["\']robots["\']\s+content=["\']([^"\']+)', html, re.IGNORECASE)
    if not m:
        return CheckResult("seo.robots_meta", "Robots meta avanzato", "seo", "warn", 0.3, 0.5, "missing")
    content = m.group(1).lower()
    if "noindex" in content:
        return CheckResult("seo.robots_meta", "Robots meta avanzato", "seo", "fail", 0.0, 0.5, content)
    advanced = any(k in content for k in ["max-snippet", "max-image-preview", "max-video-preview"])
    if advanced:
        return CheckResult("seo.robots_meta", "Robots meta avanzato", "seo", "pass", 1.0, 0.5, content)
    return CheckResult("seo.robots_meta", "Robots meta avanzato", "seo", "warn", 0.5, 0.5, content)


def check_seo_og(html: str) -> CheckResult:
    props = ["og:title", "og:description", "og:image", "og:url", "og:type"]
    found = [p for p in props if re.search(rf'<meta\s+property=["\']{re.escape(p)}["\']', html, re.IGNORECASE)]
    twitter = bool(re.search(r'<meta\s+name=["\']twitter:card["\']', html, re.IGNORECASE))
    score = (len(found) + (1 if twitter else 0)) / 6.0
    status = "pass" if score >= 0.8 else ("warn" if score >= 0.5 else "fail")
    return CheckResult("seo.og", "OG + Twitter card", "seo", status, score, 0.7, {"og": found, "twitter": twitter})


# ─────────────────────────────────────────────────────────────
# AI-READINESS AXIS CHECKS
# ─────────────────────────────────────────────────────────────
def check_ai_llms_txt(client: httpx.Client, base: str) -> CheckResult:
    r = _fetch(client, urljoin(base, "/llms.txt"))
    if not r or r.status_code != 200:
        return CheckResult("ainative.llms_txt", "/llms.txt presente", "ainative", "fail", 0.0, 0.5, "missing")
    body = r.text
    size_kb = len(body.encode()) / 1024
    has_h1 = body.lstrip().startswith("#")
    has_blockquote = ">" in body[:500]
    has_h2 = "\n## " in body
    has_optional = "## Optional" in body
    score = sum([has_h1, has_blockquote, has_h2, has_optional]) / 4.0
    if size_kb > 100:
        score *= 0.7  # penalizza file enormi (indice deve essere snello)
    status = "pass" if score >= 0.75 else ("warn" if score >= 0.4 else "fail")
    return CheckResult("ainative.llms_txt", "/llms.txt conforme llmstxt.org", "ainative", status, score, 2.0,
                       {"size_kb": round(size_kb, 1), "has_h1": has_h1, "has_blockquote": has_blockquote,
                        "has_h2_sections": has_h2, "has_optional": has_optional})


def check_ai_llms_full(client: httpx.Client, base: str) -> CheckResult:
    r = _fetch(client, urljoin(base, "/llms-full.txt"))
    if not r or r.status_code != 200:
        return CheckResult("ainative.llms_full", "/llms-full.txt presente", "ainative", "fail", 0.0, 0.3, "missing")
    body = r.text
    size_kb = len(body.encode()) / 1024
    has_front_matter = body.lstrip().startswith("---")
    has_anchors = bool(re.search(r"\{#[\w-]+\}", body))
    has_yaml_meta = "```yaml" in body
    score = sum([has_front_matter, has_anchors, has_yaml_meta]) / 3.0
    status = "pass" if score >= 0.66 else ("warn" if score >= 0.33 else "fail")
    return CheckResult("ainative.llms_full", "/llms-full.txt corpus citabile", "ainative", status, score, 1.5,
                       {"size_kb": round(size_kb, 1), "has_front_matter": has_front_matter,
                        "has_anchors": has_anchors, "has_yaml_blocks": has_yaml_meta})


def check_ai_txt(client: httpx.Client, base: str) -> CheckResult:
    r = _fetch(client, urljoin(base, "/ai.txt"))
    if not r or r.status_code != 200:
        return CheckResult("ainative.ai_txt", "/ai.txt AI policy", "ainative", "fail", 0.0, 0.3, "missing")
    body = r.text.lower()
    signals = ["allow-citation", "require-attribution", "license", "llms-txt", "ai-plugin"]
    found = [s for s in signals if s in body]
    score = len(found) / len(signals)
    status = "pass" if score >= 0.8 else ("warn" if score >= 0.4 else "fail")
    return CheckResult("ainative.ai_txt", "/ai.txt policy", "ainative", status, score, 1.2, found)


def check_ai_plugin_manifest(client: httpx.Client, base: str) -> CheckResult:
    r = _fetch(client, urljoin(base, "/.well-known/ai-plugin.json"))
    if not r or r.status_code != 200:
        return CheckResult("ainative.plugin_manifest", "/.well-known/ai-plugin.json", "ainative", "fail", 0.0, 0.3, "missing")
    try:
        data = r.json()
    except Exception:
        return CheckResult("ainative.plugin_manifest", "/.well-known/ai-plugin.json", "ainative", "warn", 0.3, 0.3, "invalid JSON")
    required = ["name_for_human", "description_for_model", "api", "contact_email"]
    found = [k for k in required if k in data]
    score = len(found) / len(required)
    status = "pass" if score >= 0.75 else "warn"
    return CheckResult("ainative.plugin_manifest", "/.well-known/ai-plugin.json", "ainative", status, score, 1.2,
                       {"keys_found": found, "has_extensions": "extensions" in data})


def check_ai_sitemap(client: httpx.Client, base: str) -> CheckResult:
    r = _fetch(client, urljoin(base, "/ai-sitemap.xml"))
    if not r or r.status_code != 200:
        return CheckResult("ainative.ai_sitemap", "/ai-sitemap.xml", "ainative", "fail", 0.0, 0.2, "missing")
    has_llms = "llms" in r.text.lower()
    score = 1.0 if has_llms else 0.5
    return CheckResult("ainative.ai_sitemap", "/ai-sitemap.xml", "ainative", "pass" if has_llms else "warn",
                       score, 0.8, {"size_kb": round(len(r.content)/1024, 1), "includes_llms": has_llms})


def check_ai_robots_allowlist(client: httpx.Client, base: str) -> CheckResult:
    r = _fetch(client, urljoin(base, "/robots.txt"))
    if not r or r.status_code != 200:
        return CheckResult("ainative.robots_ai_ua", "robots.txt UA AI allowlist", "ainative", "fail", 0.0, 0.8, "robots.txt missing")
    body = r.text.lower()
    allowed = []
    for ua in AI_UA_LIST:
        # pattern "User-agent: UA" seguito in qualche punto da "Allow: /" senza "Disallow: /"
        block_re = re.compile(rf"user-agent:\s*{re.escape(ua.lower())}\s*\n(.*?)(?=\nuser-agent:|\Z)",
                              re.IGNORECASE | re.DOTALL)
        m = block_re.search(body)
        if m:
            block = m.group(1)
            if "disallow: /" in block and "disallow: /api" not in block.replace("disallow: /\n", ""):
                continue
            if "allow: /" in block or "disallow:" in block:  # se c'è disallow vuoto = allow
                allowed.append(ua)
    count = len(allowed)
    if count >= 10:
        status, score = "pass", 1.0
    elif count >= 5:
        status, score = "warn", 0.6
    elif count >= 1:
        status, score = "warn", 0.3
    else:
        status, score = "fail", 0.0
    return CheckResult("ainative.robots_ai_ua", "robots.txt UA AI esplicitamente allowlisted", "ainative",
                       status, score, 1.5, {"count": count, "allowed": allowed})


def check_ai_jsonld(html: str) -> CheckResult:
    scripts = re.findall(r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
                         html, re.IGNORECASE | re.DOTALL)
    types_found: set[str] = set()
    for s in scripts:
        try:
            data = json.loads(s.strip())
        except Exception:
            continue
        items = data if isinstance(data, list) else [data]
        # Gestisci anche @graph
        flat = []
        for it in items:
            if isinstance(it, dict) and "@graph" in it and isinstance(it["@graph"], list):
                flat.extend(it["@graph"])
            else:
                flat.append(it)
        for it in flat:
            if isinstance(it, dict):
                t = it.get("@type")
                if isinstance(t, str):
                    types_found.add(t)
                elif isinstance(t, list):
                    types_found.update([x for x in t if isinstance(x, str)])
    if not types_found:
        return CheckResult("machine.jsonld", "Schema JSON-LD presenti", "machine", "fail", 0.0, 2.0, [])
    # Punteggio su presenza di tipi di alto valore
    high_value = {
        "AutoDealer", "Organization", "LocalBusiness", "Vehicle", "Car", "Offer",
        "BreadcrumbList", "FAQPage", "Dataset", "WebPage",
    }
    matches = types_found & high_value
    score = min(1.0, len(matches) / 6.0)
    status = "pass" if score >= 0.7 else ("warn" if score >= 0.3 else "fail")
    return CheckResult("machine.jsonld", "Schema JSON-LD presenti", "machine", status, score, 1.5,
                       {"types_found": sorted(types_found), "high_value": sorted(matches)})


def check_ai_dataset(html: str) -> CheckResult:
    if '"Dataset"' not in html:
        return CheckResult("ainative.dataset", "JSON-LD Dataset → llms-full.txt", "ainative", "fail", 0.0, 0.3, "missing")
    # cerca link a llms-full.txt nel Dataset
    has_llms_ref = bool(re.search(r"llms-full\.txt", html, re.IGNORECASE))
    if has_llms_ref:
        return CheckResult("ainative.dataset", "JSON-LD Dataset → llms-full.txt", "ainative", "pass", 1.0, 0.3, "Dataset + llms-full link")
    return CheckResult("ainative.dataset", "JSON-LD Dataset → llms-full.txt", "ainative", "warn", 0.5, 0.3, "Dataset senza llms-full link")


def check_ai_speakable(html: str) -> CheckResult:
    if "SpeakableSpecification" in html:
        return CheckResult("ainative.speakable", "SpeakableSpecification", "ainative", "pass", 1.0, 0.7, "found")
    return CheckResult("ainative.speakable", "SpeakableSpecification", "ainative", "fail", 0.0, 0.7, "missing")


def check_ai_offer_valid_until(html: str) -> CheckResult:
    # Check rilevante solo per pagine veicolo (Vehicle/Car). In homepage ci sono
    # spesso Offer sui Service, non sui veicoli — quelli non necessitano priceValidUntil.
    has_vehicle = bool(re.search(r'"@type":\s*"(Vehicle|Car)"', html))
    if not has_vehicle:
        return CheckResult("machine.offer_valid_until", "Offer.priceValidUntil (detail veicolo)", "machine", "skip", 0.0, 0.6, "no Vehicle/Car schema (skip su homepage)")
    has_valid = "priceValidUntil" in html
    if has_valid:
        return CheckResult("machine.offer_valid_until", "Offer.priceValidUntil (detail veicolo)", "machine", "pass", 1.0, 0.6, "present")
    return CheckResult("machine.offer_valid_until", "Offer.priceValidUntil (detail veicolo)", "machine", "warn", 0.3, 0.6, "missing")


def check_ai_link_rel_llms(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("ainative.link_rel_llms", 'HTTP Link rel="llms"', "ai", "skip", 0.0, 0.2)
    link = resp.headers.get("link", "")
    has_llms = 'rel="llms"' in link or "rel=llms" in link
    has_full = 'rel="llms-full"' in link or "rel=llms-full" in link
    score = sum([has_llms, has_full]) / 2.0
    status = "pass" if score == 1.0 else ("warn" if score > 0 else "fail")
    return CheckResult("ainative.link_rel_llms", 'HTTP Link rel="llms"', "ai", status, score, 0.2,
                       {"rel_llms": has_llms, "rel_llms_full": has_full})


def check_ai_content_digest(client: httpx.Client, base: str) -> CheckResult:
    r = _fetch(client, urljoin(base, "/llms.txt"))
    if not r:
        return CheckResult("ainative.content_digest", "Content-Digest sha-256 su llms.txt", "ainative", "skip", 0.0, 0.2)
    cd = r.headers.get("content-digest") or r.headers.get("Content-Digest")
    if cd and "sha-256" in cd.lower():
        return CheckResult("ainative.content_digest", "Content-Digest sha-256 su llms.txt", "ainative", "pass", 1.0, 0.2, cd[:80])
    return CheckResult("ainative.content_digest", "Content-Digest sha-256 su llms.txt", "ainative", "fail", 0.0, 0.2, "missing")


def check_ai_x_robots_tag(resp: httpx.Response | None) -> CheckResult:
    if not resp:
        return CheckResult("machine.x_robots_tag", "X-Robots-Tag avanzato", "machine", "skip", 0.0, 0.8)
    tag = (resp.headers.get("x-robots-tag") or "").lower()
    if not tag:
        return CheckResult("machine.x_robots_tag", "X-Robots-Tag avanzato", "machine", "warn", 0.3, 0.8, "missing")
    has_all = "all" in tag or "index" in tag
    has_advanced = "max-snippet" in tag or "max-image-preview" in tag
    score = sum([has_all, has_advanced]) / 2.0
    return CheckResult("machine.x_robots_tag", "X-Robots-Tag avanzato", "machine",
                       "pass" if score >= 0.5 else "warn", score, 0.4, tag)


# ─────────────────────────────────────────────────────────────
# PLATFORM DETECTION (static fingerprint first, Gemini fallback)
# ─────────────────────────────────────────────────────────────
def detect_platform_static(html: str, headers: dict) -> dict[str, Any]:
    """Detect dealer platform da fingerprint MULTIPLI verificati su ground-truth.

    Principio: ogni piattaforma richiede ≥2 signals distinti per essere taggata
    con alta confidence (>=0.9). Un solo signal → confidence media (0.6-0.7).
    Le regole sono ordinate per specificità: prima quelle univoche, poi i fallback.
    """
    h_lower = {k.lower(): v for k, v in headers.items()}
    html_head = html[:80000]  # performance
    signals: list[str] = []

    # ═══ helpers ═══
    def has_class(token: str) -> bool:
        return bool(re.search(rf'class=["\'][^"\']*\b{re.escape(token)}[^"\']*["\']', html_head, re.IGNORECASE))

    def has_body_class(token: str) -> bool:
        m = re.search(r'<body[^>]*class=["\']([^"\']+)', html_head, re.IGNORECASE)
        return bool(m and token in m.group(1))

    def has_script_src(pattern: str) -> bool:
        return bool(re.search(rf'<script[^>]+src=["\'][^"\']*{re.escape(pattern)}', html_head, re.IGNORECASE))

    def has_link_href(pattern: str) -> bool:
        return bool(re.search(rf'<link[^>]+href=["\'][^"\']*{re.escape(pattern)}', html_head, re.IGNORECASE))

    def cookie_name(name: str) -> bool:
        sc = headers.get("set-cookie") or headers.get("Set-Cookie") or ""
        return bool(re.search(rf'\b{re.escape(name)}=', sc))

    # ═══ Fingerprint tables (ground-truth verified) ═══
    # Ogni voce: (platform_name, [list of signal-checks], min_signals_required)
    # signal-checks: funzione lambda che ritorna (bool, descrizione)

    # --- DealerMAX v2 (Matarese) ---
    dm_v2 = []
    if "cdn.azcore.it" in html: dm_v2.append("cdn.azcore.it storage ref")
    if "apimax.azcore.it" in html: dm_v2.append("apimax.azcore.it API ref")
    if "__NUXT__" in html and re.search(r'dealermax', html, re.IGNORECASE):
        dm_v2.append("Nuxt + DealerMAX brand")
    if re.search(r'<meta\s+name=["\']generator["\']\s+content=["\']DealerMAX', html, re.IGNORECASE):
        dm_v2.append("generator meta = DealerMAX")
    if re.search(r'rel=["\']llms["\']', html, re.IGNORECASE):
        dm_v2.append("link rel=llms (DealerMAX 2026 stack)")

    # --- DealerMAX v1 (Scuderia76) — legacy AZURELease ---
    dm_v1 = []
    if "/AZURELease/dealer/" in html: dm_v1.append("/AZURELease/dealer/ path")
    if "x-railway-cdn-edge" in h_lower and "dealer/assets/js" in html:
        dm_v1.append("Railway edge + legacy asset path")

    # --- MotorK WebSpark ---
    motork = []
    if "webspark-boilerplate-theme" in html: motork.append("webspark-boilerplate-theme body class")
    if has_body_class("wp-child-theme-webspark"): motork.append("wp-child-theme-webspark-* body class")
    if "dealerk.cloud" in html or "motork.cloud" in html: motork.append("motork.cloud CDN")

    # --- GestionaleAuto (variant WebSpark) ---
    gest = []
    if "webspark-theme-car-and-car" in html: gest.append("webspark-theme-car-and-car body class")
    if has_body_class("ws-site"): gest.append("ws-site body class")

    # --- Carmove ---
    carmove = []
    if cookie_name("sitebuilder_session"): carmove.append("sitebuilder_session cookie")
    if has_body_class("app-bg-light") and has_body_class("app-button-style-rounded"):
        carmove.append("Carmove body class signature (app-bg-light + app-button-style-rounded)")
    if has_body_class("app-card-style-with-border"): carmove.append("app-card-style-with-border body class")

    # --- Labycar (ASP.NET Web Forms) ---
    labycar = []
    if "/ScriptResource.axd" in html: labycar.append("ScriptResource.axd (ASP.NET)")
    if cookie_name("ASP.NET_SessionId"): labycar.append("ASP.NET_SessionId cookie")
    if has_class("boxcar-body") or has_class("boxcar-template"): labycar.append("boxcar-* class (Labycar theme)")

    # --- One AM (custom Milan) ---
    oneam = []
    if re.search(r'<script[^>]+src=["\']/build/frontend/\d+\.[a-f0-9]+\.js', html):
        oneam.append("/build/frontend/*.js bundle pattern")
    if "automilano" in html.lower() and oneam: oneam.append("automilano brand reference")

    # --- Buzzlab PHP custom (Rossocorsa) ---
    buzzlab = []
    if "/frontend/assets/include/rs-plugin/" in html: buzzlab.append("/frontend/assets/include/rs-plugin/")
    if has_body_class("side-push-panel") and "litespeed" in (h_lower.get("server") or "").lower():
        buzzlab.append("side-push-panel + LiteSpeed (Buzzlab signature)")

    # --- WordPress + WPBakery + WPML (generic commercial) ---
    wp = []
    if 'content="WordPress' in html: wp.append("WordPress generator")
    if "/wp-content/" in html or "/wp-includes/" in html: wp.append("wp-content/wp-includes path")
    # Yoast
    yoast = "yoast-schema-graph" in html or "Yoast SEO" in html
    # WPBakery (vc_ class prefix)
    wpbakery = bool(re.search(r'class=["\'][^"\']*\bvc_(row|column|grid|btn|icon|custom)', html_head))
    # WPML
    wpml = bool(re.search(r'class=["\'][^"\']*\bwpml-ls-', html_head))

    # --- PHP/Slim custom (soccol) ---
    phpslim = []
    xpb = (h_lower.get("x-powered-by") or "").lower()
    if "plesklin" in xpb or "plesk" in xpb: phpslim.append(f"x-powered-by plesk: {xpb[:60]}")
    if "php" in xpb: phpslim.append(f"x-powered-by PHP: {xpb[:60]}")

    # ═══ Decision tree (specificità decrescente) ═══

    # DealerMAX v2: 2+ signals → definitivo
    if len(dm_v2) >= 2:
        signals.extend(dm_v2)
        return {"name": "DealerMAX v2", "confidence": 0.97, "signals": signals}
    if len(dm_v2) == 1 and "__NUXT__" in html:
        signals.extend(dm_v2)
        return {"name": "Nuxt (possibly DealerMAX v2)", "confidence": 0.6, "signals": signals}

    # DealerMAX v1: 1+ signal forte
    if len(dm_v1) >= 1:
        signals.extend(dm_v1)
        return {"name": "DealerMAX v1 (AZURELease)", "confidence": 0.95, "signals": signals}

    # GestionaleAuto (variante WebSpark — se pattern car-and-car): PRIMA di MotorK
    # perché entrambi sono WebSpark, ma GestionaleAuto ha footprint specifica.
    if len(gest) >= 1:
        signals.extend(gest)
        return {"name": "GestionaleAuto (WebSpark)", "confidence": 0.92, "signals": signals}

    # MotorK/WebSpark: fallback dopo GestionaleAuto (entrambi su stack WebSpark)
    if len(motork) >= 1:
        signals.extend(motork)
        return {"name": "MotorK WebSpark", "confidence": 0.95, "signals": signals}

    # Carmove: 2+ signals
    if len(carmove) >= 2:
        signals.extend(carmove)
        return {"name": "Carmove", "confidence": 0.95, "signals": signals}
    if len(carmove) == 1:
        signals.extend(carmove)
        return {"name": "Carmove (partial)", "confidence": 0.7, "signals": signals}

    # Labycar: 2+ signals (ASP.NET da solo non basta — molti siti usano .NET)
    if len(labycar) >= 2:
        signals.extend(labycar)
        return {"name": "Labycar", "confidence": 0.95, "signals": signals}
    if len(labycar) == 1:
        signals.extend(labycar)
        return {"name": "ASP.NET (possibly Labycar)", "confidence": 0.5, "signals": signals}

    # One AM: 1+ signal
    if len(oneam) >= 1:
        signals.extend(oneam)
        return {"name": "One AM (custom)", "confidence": 0.85, "signals": signals}

    # Buzzlab: 1+ signal
    if len(buzzlab) >= 1:
        signals.extend(buzzlab)
        return {"name": "Buzzlab (custom PHP)", "confidence": 0.8, "signals": signals}

    # WordPress — con plugin chiari
    if wp:
        signals.extend(wp)
        extra = []
        if yoast: extra.append("Yoast SEO")
        if wpbakery: extra.append("WPBakery")
        if wpml: extra.append("WPML")
        label = "WordPress"
        if extra:
            label = f"WordPress + {' + '.join(extra)}"
            signals.extend(extra)
        return {"name": label, "confidence": 0.9, "signals": signals}

    # PHP generico
    if phpslim:
        signals.extend(phpslim)
        return {"name": "PHP custom", "confidence": 0.7, "signals": signals}

    # Nuxt senza branding DealerMAX
    if "__NUXT__" in html:
        signals.append("Nuxt __NUXT__ hydration")
        return {"name": "Nuxt (generic)", "confidence": 0.7, "signals": signals}

    # Generator meta fallback
    gen = re.search(r'<meta\s+name=["\']generator["\']\s+content=["\']([^"\']+)', html, re.IGNORECASE)
    if gen:
        signals.append(f"generator: {gen.group(1)}")
        return {"name": gen.group(1)[:40], "confidence": 0.6, "signals": signals}

    return {"name": "unknown", "confidence": 0.0, "signals": signals}


def detect_platform_gemini(html: str, headers: dict) -> dict[str, Any] | None:
    """Fallback AI-powered platform detection via Gemini 2.5 Flash.
    Skippata se name già noto con confidence >= 0.8 o se GEMINI_API_KEY assente."""
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        return None
    try:
        import google.generativeai as genai  # type: ignore
    except ImportError:
        return None
    genai.configure(api_key=api_key)
    # Prendi un campione dell'HTML compatto
    head_match = re.search(r"<head[^>]*>(.*?)</head>", html, re.IGNORECASE | re.DOTALL)
    head = (head_match.group(1) if head_match else html)[:4000]
    prompt = (
        "Analizza il seguente HTML head e header HTTP. Identifica la piattaforma/sitebuilder "
        "usata per il sito del dealer auto. Possibili: MotorK/DealerK, Carmove, DealerMAX v1, "
        "DealerMAX v2, Labycar, One AM, Buzzlab, WordPress (+ plugin), sviluppo custom PHP, altro. "
        "Rispondi in JSON: {\"name\": \"...\", \"confidence\": 0-1, \"signals\": [\"...\"]}.\n\n"
        f"HEADERS: {json.dumps({k: v for k, v in list(headers.items())[:15]})}\n\n"
        f"HEAD: {head}"
    )
    try:
        model = genai.GenerativeModel(
            "gemini-2.5-flash",
            generation_config={"response_mime_type": "application/json"},
        )
        resp = model.generate_content(prompt)
        return json.loads(resp.text)
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────────────────────────
# MAIN AUDIT
# ─────────────────────────────────────────────────────────────
def audit_domain(domain: str, enable_platform_ai: bool = False) -> AuditResult:
    url = _normalize_url(domain)
    parsed = urlparse(url)
    base = f"{parsed.scheme}://{parsed.netloc}"
    result = AuditResult(domain=parsed.netloc, final_url=url, scanned_at=time.time())

    with httpx.Client(
        follow_redirects=True,
        max_redirects=5,
        headers={"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml,*/*"},
        http2=True,
        verify=True,
    ) as client:
        # Fetch homepage con timing + fallback www.
        # Molti cert SSL sono validi SOLO su www.domain (SAN limitato).
        # Se il primo tentativo fallisce e la URL non aveva già "www.", retry con www.
        t0 = time.time()
        resp = None
        fetch_err = None
        try:
            resp = client.get(url, timeout=REQUEST_TIMEOUT)
        except Exception as e:
            fetch_err = e

        if resp is None and not parsed.netloc.startswith("www."):
            www_url = f"{parsed.scheme}://www.{parsed.netloc}{parsed.path or '/'}"
            try:
                resp = client.get(www_url, timeout=REQUEST_TIMEOUT)
                url = www_url  # aggiorna URL di lavoro
                # Flag: il dominio nudo è rotto, solo www funziona → signal debole
                # (HSTS / SSL SAN incompleto / redirect 301 mancante da apex)
                result.http["www_fallback_used"] = True
                result.http["apex_error"] = str(fetch_err)[:200]
            except Exception as e2:
                # Preserva il primo errore se entrambi falliscono (più informativo)
                result.errors.append(f"homepage fetch failed: {fetch_err} | www retry: {e2}")
                return result

        if resp is None:
            result.errors.append(f"homepage fetch failed: {fetch_err}")
            return result
        ttfb = time.time() - t0
        html = resp.text[:MAX_HTML_BYTES]
        result.final_url = str(resp.url)
        result.http = {
            "status": resp.status_code,
            "http_version": resp.http_version,
            "content_type": resp.headers.get("content-type"),
            "server": resp.headers.get("server"),
            "cdn_hint": resp.headers.get("cf-ray") or resp.headers.get("x-railway-edge"),
            "ttfb_ms": round(ttfb * 1000),
            "html_bytes": len(resp.content),
            "redirects": len(resp.history),
        }

        # TECH CHECKS
        result.checks.append(check_tech_ttfb(resp, ttfb))
        result.checks.append(check_tech_http_version(resp))
        result.checks.append(check_tech_compression(resp))
        result.checks.append(check_tech_cdn(resp))
        result.checks.append(check_tech_cache_control(resp))
        result.checks.append(check_tech_security_headers(resp))
        result.checks.append(check_tech_redirect_chain(len(resp.history)))
        result.checks.append(check_tech_ssr(html))
        result.checks.append(check_tech_blocking_scripts(html))
        result.checks.append(check_tech_server_leak(resp))

        # SEO CHECKS
        result.checks.append(check_seo_title(html))
        result.checks.append(check_seo_meta_description(html))
        result.checks.append(check_seo_h1(html))
        result.checks.append(check_seo_canonical(html, result.final_url))
        result.checks.append(check_seo_robots_meta(html))
        result.checks.append(check_seo_og(html))

        # AI CHECKS
        result.checks.append(check_ai_llms_txt(client, base))
        result.checks.append(check_ai_llms_full(client, base))
        result.checks.append(check_ai_txt(client, base))
        result.checks.append(check_ai_plugin_manifest(client, base))
        result.checks.append(check_ai_sitemap(client, base))
        result.checks.append(check_ai_robots_allowlist(client, base))
        result.checks.append(check_ai_jsonld(html))
        result.checks.append(check_ai_dataset(html))
        result.checks.append(check_ai_speakable(html))
        result.checks.append(check_ai_offer_valid_until(html))
        result.checks.append(check_ai_link_rel_llms(resp))
        result.checks.append(check_ai_content_digest(client, base))
        result.checks.append(check_ai_x_robots_tag(resp))

        # PLATFORM DETECTION
        static_det = detect_platform_static(html, dict(resp.headers))
        result.platform = static_det
        if enable_platform_ai and static_det["confidence"] < 0.8:
            ai_det = detect_platform_gemini(html, dict(resp.headers))
            if ai_det:
                result.platform["ai_refinement"] = ai_det

    # ══════════════════════════════════════════════════════════════════
    # SCORE PER ASSE — 4 ASSI, 2 GRUPPI
    # ──────────────────────────────────────────────────────────────────
    # GRUPPO "CONSOLIDATO" (entra nel Totale /10):
    #   tech     → infrastruttura (HTTP/2, HTTPS, TTFB, cache, security)
    #   seo      → SEO on-page standard Google (title, H1, canonical, OG)
    #   machine  → leggibilità macchina standard (schema.org, robots.txt
    #              con UA ufficiali, X-Robots-Tag Google)
    # GRUPPO "SPERIMENTALE" (mostrato ma NON entra nel Totale):
    #   ainative → segnali emergenti non ancora standardizzati
    #              (llms.txt, ai-plugin, Content-Digest, link rel=llms)
    # Totale = media aritmetica di (tech + seo + machine).
    # ══════════════════════════════════════════════════════════════════
    for axis in ("tech", "seo", "machine", "ainative"):
        axis_checks = [c for c in result.checks if c.axis == axis and c.status != "skip"]
        total_w = sum(c.weight for c in axis_checks)
        weighted_sum = sum(c.weighted() for c in axis_checks)
        result.scores[axis] = round((weighted_sum / total_w) * 10, 2) if total_w > 0 else 0.0
    # Totale SOLO sul consolidato (no bias vendor-specifico)
    result.scores["total"] = round(
        (result.scores["tech"] + result.scores["seo"] + result.scores["machine"]) / 3, 2
    )
    return result


# ─────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────
STATUS_ICONS = {"pass": "[OK]", "warn": "[!!]", "fail": "[XX]", "skip": "[--]"}


def _print_report(res: AuditResult) -> None:
    print("=" * 72)
    print(f"DOMAIN: {res.domain}")
    print(f"Final URL: {res.final_url}")
    print(f"HTTP: {res.http.get('status')} {res.http.get('http_version')} "
          f"| TTFB {res.http.get('ttfb_ms')}ms | {res.http.get('html_bytes', 0)/1024:.1f} KB")
    print(f"CDN hint: {res.http.get('cdn_hint') or '-'} | Server: {res.http.get('server') or '-'}")
    print(f"Platform: {res.platform.get('name')} (conf {res.platform.get('confidence'):.2f})")
    for s in res.platform.get("signals", [])[:8]:
        print(f"   · {s}")
    print("-" * 72)
    print(f"SCORES   TECH {res.scores.get('tech'):5.2f}/10"
          f"   SEO {res.scores.get('seo'):5.2f}/10"
          f"   AI  {res.scores.get('ai'):5.2f}/10"
          f"   TOTAL {res.scores.get('total'):5.2f}/10")
    print("-" * 72)
    for axis_label, axis_key in [("TECH", "tech"), ("SEO", "seo"), ("AI-READINESS", "ai")]:
        print(f"\n[{axis_label}]")
        for c in res.checks:
            if c.axis != axis_key:
                continue
            ev = c.evidence
            if isinstance(ev, (dict, list)):
                ev = json.dumps(ev, ensure_ascii=False)[:80]
            elif ev is None:
                ev = ""
            print(f"  {STATUS_ICONS.get(c.status, '[??]')}  {c.score:.2f}  {c.label:<45} {ev}")
    if res.errors:
        print("\n[ERRORS]")
        for e in res.errors:
            print(f"  - {e}")
    print("=" * 72)


def main():
    # Windows: forza stdout UTF-8 per caratteri speciali (→, ·, €)
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

    p = argparse.ArgumentParser(description="Audit tecnico/SEO/AI di un sito dealer")
    p.add_argument("domain", help="es. matareseautomobili.it oppure https://www.example.com")
    p.add_argument("--json", action="store_true", help="Output JSON invece di report testuale")
    p.add_argument("--pretty", action="store_true", help="JSON indentato")
    p.add_argument("--platform-ai", action="store_true",
                   help="Abilita Gemini 2.5 Flash per platform detection (richiede GEMINI_API_KEY)")
    args = p.parse_args()

    res = audit_domain(args.domain, enable_platform_ai=args.platform_ai)

    if args.json:
        print(json.dumps(res.to_dict(), ensure_ascii=False, indent=2 if args.pretty else None))
    else:
        _print_report(res)


if __name__ == "__main__":
    main()
