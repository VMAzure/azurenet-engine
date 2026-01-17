import os
import logging
import requests
from requests.auth import HTTPBasicAuth

logger = logging.getLogger(__name__)


AUTOSCOUT_BASE_URL = os.environ["AUTOSCOUT_BASE_URL"].rstrip("/")
AUTOSCOUT_USER = os.environ["AUTOSCOUT_USER"]
AUTOSCOUT_PASSWORD = os.environ["AUTOSCOUT_PASSWORD"]


class AutoScoutClientError(Exception):
    pass


# ============================================================
# GET /customers  → resolve customerId from sellId
# ============================================================

def resolve_customer_id(sell_id: str) -> str:
    """
    Ritorna customers.id partendo dal sellId noto al dealer.
    """

    url = f"{AUTOSCOUT_BASE_URL}/customers"

    headers = {
        "Accept": "application/json",
    }

    logger.info("[AUTOSCOUT_HTTP] GET %s", url)

    resp = requests.get(
        url,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code != 200:
        raise AutoScoutClientError(
            f"GET /customers failed | HTTP {resp.status_code} | body={resp.text}"
        )

    data = resp.json()
    customers = data.get("customers", [])

    matches = [c for c in customers if str(c.get("sellId")) == str(sell_id)]

    if not matches:
        raise AutoScoutClientError(
            f"Nessun customer trovato per sellId={sell_id}"
        )

    if len(matches) > 1:
        raise AutoScoutClientError(
            f"Più customer trovati per sellId={sell_id}"
        )

    customer_id = matches[0].get("id")

    if not customer_id:
        raise AutoScoutClientError(
            f"Customer trovato ma senza id | sellId={sell_id}"
        )

    logger.info(
        "[AUTOSCOUT_HTTP] Resolved sellId=%s → customerId=%s",
        sell_id,
        customer_id,
    )

    return customer_id


# ============================================================
# POST /customers/{customerId}/listings
# ============================================================

def create_listing(customer_id: str, payload: dict, test_mode: bool = True) -> str:
    """
    Crea un listing AutoScout24.
    Ritorna listing_id.
    """

    url = f"{AUTOSCOUT_BASE_URL}/customers/{customer_id}/listings"

    headers = {
        "Content-Type": "application/json",
        "X-Testmode": "true" if test_mode else "false",
    }

    logger.info(
        "[AUTOSCOUT_HTTP] POST %s (test_mode=%s)",
        url,
        test_mode,
    )

    resp = requests.post(
        url,
        json=payload,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code not in (200, 201):
        raise AutoScoutClientError(
            f"POST /listings failed | HTTP {resp.status_code} | body={resp.text}"
        )

    data = resp.json()

    listing_id = data.get("listingId") or data.get("id")

    if not listing_id:
        raise AutoScoutClientError(
            f"Listing creato ma id mancante | body={data}"
        )

    return listing_id

def get_makes() -> dict:
    """
    Ritorna il catalogo ufficiale AutoScout24 (marche + modelli).
    """
    url = f"{AUTOSCOUT_BASE_URL}/makes"

    headers = {
        "Accept": "application/json",
    }

    logger.info("[AUTOSCOUT_HTTP] GET %s", url)

    resp = requests.get(
        url,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code != 200:
        raise AutoScoutClientError(
            f"GET /makes failed | HTTP {resp.status_code} | body={resp.text}"
        )

    return resp.json()


def update_listing_publication_status(
    customer_id: str,
    listing_id: str,
    status: str,
    test_mode: bool = True,
):
    """
    Aggiorna lo stato di pubblicazione del listing AS24.
    status: "Active" | "Inactive"
    """
    url = f"{AUTOSCOUT_BASE_URL}/customers/{customer_id}/listings/{listing_id}"

    payload = {
        "publication": {
            "status": status
        }
    }

    headers = {
        "Content-Type": "application/json",
        "X-Testmode": "true" if test_mode else "false",

    }

    logger.info(
        "[AUTOSCOUT_HTTP] PUT %s (publication.status=%s)",
        url,
        status,
    )

    resp = requests.put(
        url,
        json=payload,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code not in (200, 204):
        raise AutoScoutClientError(
            f"PUT publication failed | HTTP {resp.status_code} | body={resp.text}"
        )


def delete_listing(
    customer_id: str,
    listing_id: str,
    test_mode: bool = True,
):
    """
    Elimina definitivamente un listing AutoScout24.
    """

    url = f"{AUTOSCOUT_BASE_URL}/customers/{customer_id}/listings/{listing_id}"

    headers = {
        "Accept": "application/json",
        "X-Testmode": "true" if test_mode else "false",
    }

    logger.info(
        "[AUTOSCOUT_HTTP] DELETE %s (test_mode=%s)",
        url,
        test_mode,
    )

    resp = requests.delete(
        url,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code not in (200, 204):
        raise AutoScoutClientError(
            f"DELETE listing failed | HTTP {resp.status_code} | body={resp.text}"
        )


# ============================================================
# POST /customers/{customerId}/images  (pre-upload image)
# ============================================================

def upload_image(
    customer_id: str,
    image_bytes: bytes,
    content_type: str = "image/jpeg",
    test_mode: bool = True,
) -> str:
    """
    Pre-upload di una immagine AutoScout24.
    Ritorna imageId.
    """

    url = f"{AUTOSCOUT_BASE_URL}/customers/{customer_id}/images"

    headers = {
        "Content-Type": content_type,
        "X-Testmode": "true" if test_mode else "false",
    }

    logger.info(
        "[AUTOSCOUT_HTTP] POST %s (pre-upload image, test_mode=%s)",
        url,
        test_mode,
    )

    resp = requests.post(
        url,
        data=image_bytes,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code not in (200, 201):
        raise AutoScoutClientError(
            f"POST /customers/{{id}}/images failed | HTTP {resp.status_code} | body={resp.text}"
        )

    data = resp.json()
    image_id = data.get("id")

    if not image_id:
        raise AutoScoutClientError(
            f"Image uploaded but id missing | body={data}"
        )

    return image_id

# ============================================================
# PUT /customers/{customerId}/listings/{listingId} (attach images)
# ============================================================

def update_listing_images(customer_id: str, listing_id: str, image_ids: list):
    """
    Aggancia le immagini (pre-uploaded) a un listing AS24.
    Sostituisce completamente la lista immagini.
    """

    url = f"{AUTOSCOUT_BASE_URL}/customers/{customer_id}/listings/{listing_id}"

    payload = {
        "images": [{"id": img_id} for img_id in image_ids]
    }

    headers = {
        "Content-Type": "application/json",
    }

    logger.info(
        "[AUTOSCOUT_HTTP] PUT %s (attach %d images)",
        url,
        len(image_ids),
    )

    resp = requests.put(
        url,
        json=payload,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code not in (200, 204):
        raise AutoScoutClientError(
            f"PUT listing images failed | HTTP {resp.status_code} | body={resp.text}"
        )

def update_listing(
    customer_id: str,
    listing_id: str,
    payload: dict,
    test_mode: bool = True,
):
    url = f"{AUTOSCOUT_BASE_URL}/customers/{customer_id}/listings/{listing_id}"

    headers = {
        "Content-Type": "application/json",
        "X-Testmode": "true" if test_mode else "false",
    }

    logger.info("[AUTOSCOUT_HTTP] PUT %s", url)

    resp = requests.put(
        url,
        json=payload,
        headers=headers,
        auth=HTTPBasicAuth(AUTOSCOUT_USER, AUTOSCOUT_PASSWORD),
        timeout=30,
    )

    if resp.status_code not in (200, 204):
        raise AutoScoutClientError(
            f"PUT /listings failed | HTTP {resp.status_code} | body={resp.text}"
        )
