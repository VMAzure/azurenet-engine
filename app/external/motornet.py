import os
import time
import logging
from typing import Any, Dict, Optional

import httpx
from asyncio import Lock

# ============================================================
# CONFIG
# ============================================================

AUTH_URL = (
    "https://webservice.motornet.it/auth/realms/webservices/"
    "protocol/openid-connect/token"
)

TIMEOUT = 30

CLIENT_ID = "webservice"
USERNAME = os.getenv("MOTORN_CLIENT_ID")
PASSWORD = os.getenv("MOTORN_CLIENT_SECRET")

if not USERNAME or not PASSWORD:
    raise RuntimeError("Credenziali Motornet mancanti")

# ============================================================
# TOKEN CACHE (process-level)
# ============================================================

_access_token: Optional[str] = None
_refresh_token: Optional[str] = None
_access_expiry: float = 0
_refresh_expiry: float = 0

_token_lock = Lock()

# ============================================================
# AUTH CALLS
# ============================================================

async def _login() -> None:
    global _access_token, _refresh_token, _access_expiry, _refresh_expiry

    logging.info("[MOTORN] LOGIN (password)")

    payload = {
        "grant_type": "password",
        "client_id": CLIENT_ID,
        "username": USERNAME,
        "password": PASSWORD,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(AUTH_URL, data=payload)

    if resp.status_code != 200:
        raise RuntimeError(f"Motornet login failed: {resp.text}")

    data = resp.json()
    now = time.time()

    _access_token = data["access_token"]
    _refresh_token = data["refresh_token"]

    _access_expiry = now + data.get("expires_in", 300) - 20
    _refresh_expiry = now + data.get("refresh_expires_in", 1800) - 20


async def _refresh() -> bool:
    global _access_token, _access_expiry

    if not _refresh_token or time.time() >= _refresh_expiry:
        return False

    logging.info("[MOTORN] REFRESH token")

    payload = {
        "grant_type": "refresh_token",
        "client_id": CLIENT_ID,
        "refresh_token": _refresh_token,
    }

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.post(AUTH_URL, data=payload)

    if resp.status_code != 200:
        logging.warning("[MOTORN] refresh failed")
        return False

    data = resp.json()
    now = time.time()

    _access_token = data["access_token"]
    _access_expiry = now + data.get("expires_in", 300) - 20

    return True


# ============================================================
# TOKEN ACCESS (SAFE)
# ============================================================

async def get_access_token() -> str:
    async with _token_lock:
        now = time.time()

        if _access_token and now < _access_expiry:
            return _access_token

        if await _refresh():
            return _access_token

        await _login()
        return _access_token


# ============================================================
# REQUEST WRAPPER
# ============================================================

async def motornet_get(url: str) -> Dict[str, Any]:
    token = await get_access_token()
    headers = {"Authorization": f"Bearer {token}"}

    async with httpx.AsyncClient(timeout=TIMEOUT) as client:
        resp = await client.get(url, headers=headers)

    if resp.status_code == 401:
        logging.warning("[MOTORN] 401 → retry with refreshed token")

        async with _token_lock:
            if not await _refresh():
                await _login()

            headers["Authorization"] = f"Bearer {_access_token}"

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            resp = await client.get(url, headers=headers)

    if resp.status_code != 200:
        raise RuntimeError(
            f"Motornet GET failed [{resp.status_code}]: {resp.text}"
        )

    return resp.json()
