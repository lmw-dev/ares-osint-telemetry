import os
import ssl
import urllib.request
import hashlib
from datetime import datetime, timezone
from typing import Dict, Any, Optional

def get_utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def sha256_bytes(b: bytes) -> str:
    return hashlib.sha256(b).hexdigest()

def fetch_url(
    url: str,
    save_dir: Optional[str] = None,
    save_filename: Optional[str] = None,
    timeout: int = 12
) -> Dict[str, Any]:
    """
    Fetches URL using standard verified TLS context.
    Strictly verifies TLS certificates (no unverified context).
    Records requested_url, final_url, fetch_utc, http_status, bytes_len, sha256.
    Saves raw bytes into save_dir/save_filename if specified.
    """
    ctx = ssl.create_default_context() # Mandatory TLS Certificate Verification
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-GB,en;q=0.9'
    }
    fetch_utc = get_utc_now()
    try:
        req = urllib.request.Request(url, headers=headers)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            content_bytes = resp.read()
            http_status = resp.status
            final_url = resp.geturl()
            sha = sha256_bytes(content_bytes)

            saved_abspath = None
            saved_relpath = None
            if save_dir and save_filename:
                os.makedirs(save_dir, exist_ok=True)
                saved_abspath = os.path.abspath(os.path.join(save_dir, save_filename))
                saved_relpath = os.path.relpath(saved_abspath, os.getcwd())
                with open(saved_abspath, "wb") as f:
                    f.write(content_bytes)

            return {
                "requested_url": url,
                "final_url": final_url,
                "fetch_utc": fetch_utc,
                "http_status": http_status,
                "bytes_len": len(content_bytes),
                "sha256": sha,
                "saved_relpath": saved_relpath,
                "saved_abspath": saved_abspath,
                "content_bytes": content_bytes,
                "content_str": content_bytes.decode('utf-8', errors='replace'),
                "error": None
            }
    except Exception as e:
        status_code = getattr(e, 'code', 'NETWORK_OR_TIMEOUT_ERROR')
        return {
            "requested_url": url,
            "final_url": url,
            "fetch_utc": fetch_utc,
            "http_status": status_code,
            "bytes_len": 0,
            "sha256": None,
            "saved_relpath": None,
            "saved_abspath": None,
            "content_bytes": b"",
            "content_str": "",
            "error": str(e)
        }
