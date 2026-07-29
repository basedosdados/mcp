import json
import os
import time
from pathlib import Path
from typing import Any

import requests

from ._app import URLS

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------

_cache: dict[str, Any] = {
    "token": None,
    "expires_at": 0.0,
    "env": None,
    "ids": {},   # {cache_key: (result, fetched_at)}
}
_IDS_TTL = 30  # seconds


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _get_credentials(env: str) -> tuple:
    """Return a tagged credential tuple for the given environment.

    Returns either:
      ("token", "bdtoken_...")           ← use as Authorization: Token <value>
      ("password", email, password)      ← exchange for JWT via tokenAuth mutation

    Lookup order:
      1. Env var BACKEND_TOKEN
      2. Env vars EMAIL + PASSWORD
      3. ~/.basedosdados/credentials.json, env key, "token" field
      4. ~/.basedosdados/credentials.json, env key, "email"+"password" fields
    """
    backend_token = os.environ.get("BACKEND_TOKEN")
    if backend_token:
        return ("token", backend_token)

    email = os.environ.get("EMAIL")
    password = os.environ.get("PASSWORD")
    if email and password:
        return ("password", email, password)

    bd_dir = Path.home() / ".basedosdados"
    for fname in ("credentials.json", "backend_credentials.json"):
        creds_path = bd_dir / fname
        if creds_path.exists():
            data = json.loads(creds_path.read_text())
            env_data = data.get(env, data)  # fall back to flat structure
            if isinstance(env_data, dict):
                if "token" in env_data:
                    return ("token", env_data["token"])
                if "email" in env_data and "password" in env_data:
                    return ("password", env_data["email"], env_data["password"])

    raise RuntimeError(
        f"No credentials found for env='{env}'. "
        "Set BACKEND_TOKEN env var, or EMAIL+PASSWORD, or create "
        "~/.basedosdados/credentials.json with "
        '{"dev": {"token": "bdtoken_..."}, "prod": {...}}'
    )


def _get_token(env: str | None = None) -> tuple[str, str]:
    """Return (auth_header_value, base_url).

    For backend tokens: returns immediately with no HTTP call.
    For password auth: exchanges email+password for a JWT, cached 24 hours.
    """
    env = env or os.environ.get("ENV", "dev")
    if env not in URLS:
        raise ValueError(f"env must be 'local', 'dev', 'staging', or 'prod', got: {env!r}")

    base_url = URLS[env]
    creds = _get_credentials(env)

    if creds[0] == "token":
        return f"Token {creds[1]}", base_url

    # Password path — use JWT with 24-hour cache.
    _, email, password = creds
    now = time.time()
    if _cache["token"] and _cache["expires_at"] > now and _cache["env"] == env:
        return f"Bearer {_cache['token']}", base_url

    r = requests.post(
        f"{base_url}/graphql",
        json={
            "query": (
                f'mutation {{ tokenAuth(email: "{email}", password: "{password}") '
                f"{{ token }} }}"
            )
        },
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if "errors" in data:
        raise RuntimeError(f"Auth error: {data['errors']}")

    jwt = data["data"]["tokenAuth"]["token"]
    _cache.update(token=jwt, expires_at=now + 86400, env=env, ids={})
    return f"Bearer {jwt}", base_url
