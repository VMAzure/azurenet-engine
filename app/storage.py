import os
import httpx

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL:
    raise RuntimeError("SUPABASE_URL mancante")

if not SUPABASE_KEY:
    raise RuntimeError("SUPABASE_KEY mancante")


def upload_bytes_and_get_public_url(
    bucket: str,
    path: str,
    content: bytes,
    content_type: str = "image/png",
) -> str:
    """
    Upload diretto su Supabase Storage via REST API.
    Ritorna URL pubblico HTTPS.
    """
    upload_url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": content_type,
        "x-upsert": "true",
    }

    with httpx.Client(timeout=60) as client:
        r = client.put(
            upload_url,
            headers=headers,
            content=content,
        )
        r.raise_for_status()

    return f"{SUPABASE_URL}/storage/v1/object/public/{bucket}/{path}"

def download_bytes(
    bucket: str,
    path: str,
) -> bytes:
    """
    Download diretto da Supabase Storage via REST API.
    Ritorna i bytes del file.
    """
    download_url = f"{SUPABASE_URL}/storage/v1/object/{bucket}/{path}"

    headers = {
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    with httpx.Client(timeout=60) as client:
        r = client.get(download_url, headers=headers)
        r.raise_for_status()
        return r.content
