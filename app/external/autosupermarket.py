"""AutoSuperMarket (ASM) — REST API client."""

import logging
import requests

logger = logging.getLogger(__name__)

ASM_BASE_URL = "https://api.autosupermarket.it"


class AsmClientError(Exception):
    pass


def _headers(token: str) -> dict:
    return {
        "X-Auth-Token": token,
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


# ============================================================
# POST /listings — Crea annuncio
# ============================================================

def create_listing(token: str, payload: dict) -> int:
    url = f"{ASM_BASE_URL}/listings"
    logger.info("[ASM_HTTP] POST %s", url)

    resp = requests.post(url, headers=_headers(token), json=payload, timeout=30)

    if resp.status_code not in (200, 201):
        raise AsmClientError(
            f"POST /listings failed | HTTP {resp.status_code} | body={resp.text}"
        )

    data = resp.json()
    listing_id = data.get("id")
    if not listing_id:
        raise AsmClientError(f"POST /listings: id mancante nella response | body={resp.text}")

    logger.info("[ASM_HTTP] Listing creato: id=%s", listing_id)
    return int(listing_id)


# ============================================================
# PATCH /listings/:id — Aggiorna annuncio
# ============================================================

def update_listing(token: str, listing_id: int, payload: dict) -> None:
    url = f"{ASM_BASE_URL}/listings/{listing_id}"
    logger.info("[ASM_HTTP] PATCH %s", url)

    resp = requests.patch(url, headers=_headers(token), json=payload, timeout=30)

    if resp.status_code not in (200, 204):
        raise AsmClientError(
            f"PATCH /listings/{listing_id} failed | HTTP {resp.status_code} | body={resp.text}"
        )

    logger.info("[ASM_HTTP] Listing aggiornato: id=%s", listing_id)


# ============================================================
# DELETE /listings/:id — Elimina annuncio
# ============================================================

def delete_listing(token: str, listing_id: int) -> None:
    url = f"{ASM_BASE_URL}/listings/{listing_id}"
    logger.info("[ASM_HTTP] DELETE %s", url)

    resp = requests.delete(url, headers=_headers(token), timeout=30)

    if resp.status_code not in (200, 204):
        raise AsmClientError(
            f"DELETE /listings/{listing_id} failed | HTTP {resp.status_code} | body={resp.text}"
        )

    logger.info("[ASM_HTTP] Listing eliminato: id=%s", listing_id)


# ============================================================
# GET /listings/:id — Dettaglio annuncio
# ============================================================

def get_listing(token: str, listing_id: int) -> dict:
    url = f"{ASM_BASE_URL}/listings/{listing_id}"
    logger.info("[ASM_HTTP] GET %s", url)

    resp = requests.get(url, headers=_headers(token), timeout=30)

    if resp.status_code != 200:
        raise AsmClientError(
            f"GET /listings/{listing_id} failed | HTTP {resp.status_code} | body={resp.text}"
        )

    return resp.json()


# ============================================================
# GET /dealer/headquarters/:id/listings — Lista annunci sede
# ============================================================

def list_listings(token: str, headquarters_id: str) -> list:
    url = f"{ASM_BASE_URL}/dealer/headquarters/{headquarters_id}/listings"
    logger.info("[ASM_HTTP] GET %s", url)

    resp = requests.get(url, headers=_headers(token), timeout=60)

    if resp.status_code != 200:
        raise AsmClientError(
            f"GET headquarters/{headquarters_id}/listings failed | HTTP {resp.status_code} | body={resp.text}"
        )

    return resp.json()
