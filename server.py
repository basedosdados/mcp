"""
databasis-mcp: MCP server wrapping the Data Basis GraphQL backend.

## Consumo de dados (sem autenticação de backend)

Ferramentas de leitura de metadados e consulta ao BigQuery não requerem token de backend.
Para consultas ao BigQuery, é necessário:
  1. Conta GCP autenticada via ADC: gcloud auth application-default login
  2. Projeto de faturamento GCP (billing project), definido via:
       a. Parâmetro billing_project na ferramenta
       b. Variável de ambiente GCP_PROJECT_ID
       c. Campo "gcp_project" em ~/.basedosdados/credentials.json

## Cadastro de dados (requer autenticação de backend)

Credenciais em ordem de prioridade:
  1. Env var: BACKEND_TOKEN  (bdtoken_...)
  2. Env vars: EMAIL e PASSWORD  (legado)
  3. ~/.basedosdados/credentials.json:
     {"dev": {"token": "bdtoken_..."}, "prod": {...}}          ← preferido
     {"dev": {"email": ..., "password": ...}, "prod": {...}}   ← legado

Ambiente:
  ENV=dev (padrão), ENV=local, ou ENV=prod

Tokens JWT (via senha) são cacheados em memória por 24 horas.
Tokens de backend são usados diretamente sem cache.
"""

import json
import os
import time
from pathlib import Path
from typing import Any

import requests
from fastmcp import FastMCP

mcp = FastMCP(
    "databasis-mcp",
    instructions=(
        "Tools for interacting with the Data Basis backend API. "
        "Read tools (search_datasets, list_datasets, get_dataset, lookup_id, etc.) "
        "require no authentication. "
        "BigQuery tools (query_bigquery, preview_table) require GCP ADC credentials and a billing project. "
        "Write tools are idempotent: pass an existing id to update, omit it to create. "
        "Write tools require backend credentials — call auth first or rely on auto-auth."
    ),
)

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

URLS = {
    "local": "http://localhost:8080",
    "dev": "https://development.backend.basedosdados.org",
    "staging": "https://staging.backend.basedosdados.org",
    "prod": "https://backend.basedosdados.org",
}

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

    creds_path = Path.home() / ".basedosdados" / "credentials.json"
    if creds_path.exists():
        data = json.loads(creds_path.read_text())
        env_data = data.get(env, data)  # fall back to flat structure
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


# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------


def _gql(query: str, variables: dict | None = None, env: str | None = None, auth: bool = True) -> dict:
    env = env or os.environ.get("ENV", "dev")
    if env not in URLS:
        raise ValueError(f"env must be 'local', 'dev', 'staging', or 'prod', got: {env!r}")
    base_url = URLS[env]
    headers: dict[str, str] = {}
    if auth:
        auth_header, _ = _get_token(env)
        headers["Authorization"] = auth_header
    r = requests.post(
        f"{base_url}/graphql",
        json={"query": query, "variables": variables or {}},
        headers=headers,
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}:\n{r.text}")
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]


def _mut(
    mutation_name: str,
    input_fields: dict,
    result_fields: str,
    env: str | None = None,
) -> dict:
    q = f"""
    mutation($input: {mutation_name}Input!) {{
        {mutation_name}(input: $input) {{
            errors {{ field messages }}
            {result_fields}
        }}
    }}
    """
    result = _gql(q, {"input": input_fields}, env=env)
    payload = result[mutation_name]
    if payload.get("errors"):
        raise RuntimeError(f"{mutation_name} errors: {payload['errors']}")
    return payload


def _strip_id(node_id: str) -> str:
    s = str(node_id)
    return s.split(":", 1)[1] if ":" in s else s


def _lookup_directory_column(directory_column_str: str, env: str) -> str | None:
    """
    Given an architecture-sheet directory_column string like
    "br_bd_diretorios_data_tempo.ano:ano", look up and return the
    backend Column node ID for that column, or None if not found.

    Format: "<dataset_slug>.<table_slug>:<column_name>"
    """
    if not directory_column_str or "." not in directory_column_str or ":" not in directory_column_str:
        return None
    dot_pos = directory_column_str.rfind(".")
    colon_pos = directory_column_str.find(":", dot_pos)
    if colon_pos == -1:
        return None
    dataset_slug = directory_column_str[:dot_pos]
    table_slug = directory_column_str[dot_pos + 1:colon_pos]
    column_name = directory_column_str[colon_pos + 1:]

    gql = """
    query($slug: String!) {
        allDataset(slug: $slug) {
            edges { node {
                tables(first: 100) { edges { node {
                    slug
                    columns(first: 200) { edges { node { id name } } }
                } } }
            } }
        }
    }
    """

    def _search(slug: str) -> str | None:
        data = _gql(gql, {"slug": slug}, env=env)
        edges = data["allDataset"]["edges"]
        if not edges:
            return None
        for te in edges[0]["node"]["tables"]["edges"]:
            t = te["node"]
            if t["slug"] == table_slug:
                for ce in t["columns"]["edges"]:
                    col = ce["node"]
                    if col["name"] == column_name:
                        return _strip_id(col["id"])
        return None

    # Try the slug as written in the architecture sheet first
    result = _search(dataset_slug)
    if result:
        return result

    # Retry without common BD prefixes (e.g. "br_bd_" → "") used in dev
    for prefix in ("br_bd_", "br_"):
        if dataset_slug.startswith(prefix):
            result = _search(dataset_slug[len(prefix):])
            if result:
                return result

    return None


def _fetch_all(token_env: str, query_name: str, fields: str, auth: bool = True) -> list[dict]:
    nodes: list[dict] = []
    cursor: str | None = None
    while True:
        after = f', after: "{cursor}"' if cursor else ""
        q = f"""
        query {{
            {query_name}(first: 500{after}) {{
                pageInfo {{ hasNextPage endCursor }}
                edges {{ node {{ {fields} }} }}
            }}
        }}
        """
        data = _gql(q, env=token_env, auth=auth)
        page = data[query_name]
        nodes.extend(e["node"] for e in page["edges"])
        if not page["pageInfo"]["hasNextPage"]:
            break
        cursor = page["pageInfo"]["endCursor"]
    return nodes


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


@mcp.tool()
def auth(env: str = "dev") -> dict:
    """
    Authenticate to the Data Basis backend.

    Reads credentials from EMAIL/PASSWORD env vars or
    ~/.basedosdados/credentials.json (keyed by env). Token is cached for 24 hours.

    Args:
        env: "dev" or "prod" (default: "dev", overridden by ENV env var)

    Returns:
        {"authenticated": True, "env": env, "base_url": url}
    """
    token, base_url = _get_token(env)
    return {"authenticated": True, "env": env, "base_url": base_url, "token_cached": True}


@mcp.tool()
def discover_ids(
    env: str = "dev",
    keys: list[str] | None = None,
) -> dict:
    """
    Fetch and return reference IDs needed for metadata creation.

    Fetches entire reference lists. Use lookup_id() instead when you only need
    one or a few slugs — discover_ids is expensive for large categories like
    organization and tag.

    By default fetches: status, bigquery_type, entity, license, availability,
    organization, theme, tag, entity_category, language, measurement_unit_category.
    The "area" category is excluded — use lookup_id(category="area", slug=...) instead.

    Args:
        env: "dev" or "prod"
        keys: list of categories to fetch, e.g. ["status", "entity"].
              Valid keys: status, bigquery_type, entity, license, availability,
                          organization, theme, tag, entity_category, language,
                          measurement_unit_category.
              Defaults to all except "area".

    Returns a dict mapping category → {slug: id}.
    """
    _DEFAULT_KEYS = [
        "status", "bigquery_type", "entity", "license", "availability",
        "organization", "theme", "tag", "entity_category", "language",
        "measurement_unit_category",
    ]
    requested = set(keys) if keys else set(_DEFAULT_KEYS)

    cache_key = f"ids_{env}_{'_'.join(sorted(requested))}"
    cached = _cache.get("ids", {}).get(cache_key)
    if cached is not None:
        result_cached, fetched_at = cached
        if time.time() - fetched_at < _IDS_TTL:
            return result_cached

    result: dict[str, dict] = {}

    if "status" in requested:
        nodes = _fetch_all(env, "allStatus", "id slug", auth=False)
        result["status"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "bigquery_type" in requested:
        for qname in ["allBigquerytype", "allBigQueryType"]:
            try:
                nodes = _fetch_all(env, qname, "id name", auth=False)
                result["bigquery_type"] = {n["name"]: _strip_id(n["id"]) for n in nodes}
                break
            except Exception:
                continue
        if "bigquery_type" not in result:
            result["bigquery_type"] = {}

    if "entity" in requested:
        nodes = _fetch_all(env, "allEntity", "id slug namePt", auth=False)
        result["entity"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "license" in requested:
        nodes = _fetch_all(env, "allLicense", "id slug namePt", auth=False)
        result["license"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "availability" in requested:
        nodes = _fetch_all(env, "allAvailability", "id slug namePt", auth=False)
        result["availability"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "organization" in requested:
        nodes = _fetch_all(env, "allOrganization", "id slug namePt", auth=False)
        result["organization"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "theme" in requested:
        nodes = _fetch_all(env, "allTheme", "id slug namePt", auth=False)
        result["theme"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "tag" in requested:
        nodes = _fetch_all(env, "allTag", "id slug name", auth=False)
        result["tag"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "entity_category" in requested:
        nodes = _fetch_all(env, "allEntityCategory", "id slug name", auth=False)
        result["entity_category"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "language" in requested:
        nodes = _fetch_all(env, "allLanguage", "id slug name", auth=False)
        result["language"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "measurement_unit_category" in requested:
        nodes = _fetch_all(env, "allMeasurementUnitCategory", "id slug name", auth=False)
        result["measurement_unit_category"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    if "ids" not in _cache:
        _cache["ids"] = {}
    _cache["ids"][cache_key] = (result, time.time())
    return result


_CATEGORY_QUERY_MAP = {
    "organization": ("allOrganization", "id slug namePt"),
    "theme": ("allTheme", "id slug namePt"),
    "tag": ("allTag", "id slug name"),
    "entity": ("allEntity", "id slug namePt"),
    "entity_category": ("allEntityCategory", "id slug name"),
    "language": ("allLanguage", "id slug name"),
    "measurement_unit_category": ("allMeasurementUnitCategory", "id slug name"),
    "license": ("allLicense", "id slug namePt"),
    "availability": ("allAvailability", "id slug namePt"),
    "status": ("allStatus", "id slug"),
    "area": ("allArea", "id slug"),
}


@mcp.tool()
def lookup_id(category: str, slug: str, env: str = "dev") -> dict:
    """
    Look up a single reference object by category and slug.

    Use this instead of discover_ids when you only need one or a few IDs —
    discover_ids fetches entire lists which can be very large for orgs/tags.

    Args:
        category: one of organization, theme, tag, entity, entity_category,
                  language, measurement_unit_category, license, availability, status, area
        slug: the slug to look up, e.g. "mma", "environment", "conservacao", "br"
        env: "dev" or "prod"

    Returns: {"slug": str, "id": str, "name": str}
    """
    if category not in _CATEGORY_QUERY_MAP:
        raise ValueError(f"Unknown category {category!r}. Valid: {list(_CATEGORY_QUERY_MAP)}")
    query_name, fields = _CATEGORY_QUERY_MAP[category]
    q = f'query($slug: String!) {{ {query_name}(slug: $slug, first: 1) {{ edges {{ node {{ {fields} }} }} }} }}'
    data = _gql(q, {"slug": slug}, env=env, auth=False)
    edges = data[query_name]["edges"]
    if not edges:
        raise RuntimeError(f"{category} not found: {slug!r}")
    node = edges[0]["node"]
    name = node.get("namePt") or node.get("name") or node.get("slug")
    return {"slug": node["slug"], "id": _strip_id(node["id"]), "name": name}



@mcp.tool()
def list_datasets(
    organization_slug: str | None = None,
    env: str = "dev",
) -> dict:
    """
    List datasets, optionally filtered by organization slug.

    Returns total count and a list of {id, slug, name_pt, description} for each dataset.

    Args:
        organization_slug: if provided, return only datasets for that organization
        env: "dev", "local", or "prod"

    Returns:
        {"total": int, "datasets": [{"id": str, "slug": str, "name_pt": str, "description": str}]}
    """
    if organization_slug:
        # allDataset supports organizations_Id, not organizations_Slug, so
        # resolve the org slug to its id first.
        org_q = "query($slug: String!) { allOrganization(slug: $slug, first: 1) { edges { node { id } } } }"
        org_edges = _gql(org_q, {"slug": organization_slug}, env=env, auth=False)["allOrganization"]["edges"]
        if not org_edges:
            return {"total": 0, "datasets": []}
        org_id = _strip_id(org_edges[0]["node"]["id"])
        q = """
        query($org: ID) {
            allDataset(organizations_Id: $org) {
                totalCount
                edges { node { id slug namePt description } }
            }
        }
        """
        data = _gql(q, {"org": org_id}, env=env, auth=False)
    else:
        q = """
        {
            allDataset {
                totalCount
                edges { node { id slug namePt description } }
            }
        }
        """
        data = _gql(q, env=env, auth=False)

    result = data["allDataset"]
    datasets = [
        {
            "id": _strip_id(e["node"]["id"]),
            "slug": e["node"]["slug"],
            "name_pt": e["node"]["namePt"],
            "description": e["node"].get("description") or "",
        }
        for e in result["edges"]
    ]
    return {"total": result["totalCount"], "datasets": datasets}


@mcp.tool()
def get_dataset(slug: str, env: str = "dev") -> dict:
    """
    Fetch a dataset by slug and return its full metadata.

    Returns:
      {
        "found": bool,
        "id": str | None,
        "slug": str,
        "name_pt/en/es": str,
        "description_pt/en/es": str,
        "organizations": [{"id", "slug"}],
        "themes": [{"id", "slug"}],
        "tags": [{"id", "slug"}],
        "tables": {
          "<table_slug>": {
            "id": str,
            "columns": [{"id", "name"}],
            "observation_levels": [{"id", "entity_id"}],
            "cloud_tables": [{"id"}],
            "coverages": [{"id", "area_id", "area_slug", "datetime_ranges": [...]}],
            "updates": [{"id", "entity_id"}],
            "published_by": [{"id", "email"}],
            "data_cleaned_by": [{"id", "email"}],
          }
        }
      }

    Args:
        slug: dataset slug (e.g. "siconfi")
        env: "dev" or "prod"
    """
    q = """
    query($slug: String!) {
        allDataset(slug: $slug) {
            edges {
                node {
                    id slug
                    namePt nameEn nameEs
                    descriptionPt descriptionEn descriptionEs
                    organizations(first: 10) { edges { node { id slug } } }
                    themes(first: 10) { edges { node { id slug } } }
                    tags(first: 20) { edges { node { id slug } } }
                    tables(first: 200) {
                        edges {
                            node {
                                id slug
                                columns(first: 200) { edges { node { id name } } }
                                observationLevels(first: 20) {
                                    edges { node { id entity { id slug } } }
                                }
                                cloudTables(first: 10) { edges { node { id gcpProjectId gcpDatasetId gcpTableId } } }
                                coverages(first: 10) {
                                    edges {
                                        node {
                                            id
                                            area { id slug }
                                            datetimeRanges(first: 10) {
                                                edges {
                                                    node { id startYear endYear interval }
                                                }
                                            }
                                        }
                                    }
                                }
                                updates(first: 10) {
                                    edges { node { id entity { id slug } } }
                                }
                                __GATED_FIELDS__
                            }
                        }
                    }
                }
            }
        }
    }
    """
    # publishedBy/dataCleanedBy require an authenticated request on some
    # backends (staging/prod). Query them only when credentials work;
    # otherwise fall back to the public query without those fields.
    gated = """publishedBy(first: 10) { edges { node { id email } } }
                                dataCleanedBy(first: 10) { edges { node { id email } } }"""
    try:
        data = _gql(q.replace("__GATED_FIELDS__", gated), {"slug": slug}, env=env)
    except (RuntimeError, requests.RequestException):
        data = _gql(q.replace("__GATED_FIELDS__", ""), {"slug": slug}, env=env, auth=False)
    edges = data["allDataset"]["edges"]
    if not edges:
        return {"found": False, "id": None, "slug": slug, "tables": {}}

    ds = edges[0]["node"]
    tables = {}
    for te in ds["tables"]["edges"]:
        t = te["node"]
        tables[t["slug"]] = {
            "id": _strip_id(t["id"]),
            "columns": [
                {"id": _strip_id(c["node"]["id"]), "name": c["node"]["name"]}
                for c in t["columns"]["edges"]
            ],
            "observation_levels": [
                {
                    "id": _strip_id(ol["node"]["id"]),
                    "entity_id": _strip_id(ol["node"]["entity"]["id"]) if ol["node"].get("entity") else None,
                    "entity_slug": ol["node"]["entity"]["slug"] if ol["node"].get("entity") else None,
                }
                for ol in t["observationLevels"]["edges"]
            ],
            "cloud_tables": [
                {
                    "id": _strip_id(ct["node"]["id"]),
                    "gcp_project_id": ct["node"].get("gcpProjectId"),
                    "gcp_dataset_id": ct["node"].get("gcpDatasetId"),
                    "gcp_table_id": ct["node"].get("gcpTableId"),
                }
                for ct in t["cloudTables"]["edges"]
            ],
            "coverages": [
                {
                    "id": _strip_id(cov["node"]["id"]),
                    "area_id": _strip_id(cov["node"]["area"]["id"]) if cov["node"].get("area") else None,
                    "area_slug": cov["node"]["area"]["slug"] if cov["node"].get("area") else None,
                    "datetime_ranges": [
                        {
                            "id": _strip_id(dtr["node"]["id"]),
                            "start_year": dtr["node"]["startYear"],
                            "end_year": dtr["node"]["endYear"],
                            "interval": dtr["node"]["interval"],
                        }
                        for dtr in cov["node"]["datetimeRanges"]["edges"]
                    ],
                }
                for cov in t["coverages"]["edges"]
            ],
            "updates": [
                {
                    "id": _strip_id(upd["node"]["id"]),
                    "entity_id": _strip_id(upd["node"]["entity"]["id"]) if upd["node"].get("entity") else None,
                    "entity_slug": upd["node"]["entity"]["slug"] if upd["node"].get("entity") else None,
                }
                for upd in t["updates"]["edges"]
            ],
            "published_by": [
                {"id": _strip_id(u["node"]["id"]), "email": u["node"]["email"]}
                for u in t.get("publishedBy", {"edges": []})["edges"]
            ],
            "data_cleaned_by": [
                {"id": _strip_id(u["node"]["id"]), "email": u["node"]["email"]}
                for u in t.get("dataCleanedBy", {"edges": []})["edges"]
            ],
        }

    return {
        "found": True,
        "id": _strip_id(ds["id"]),
        "slug": ds["slug"],
        "name_pt": ds.get("namePt"),
        "name_en": ds.get("nameEn"),
        "name_es": ds.get("nameEs"),
        "description_pt": ds.get("descriptionPt"),
        "description_en": ds.get("descriptionEn"),
        "description_es": ds.get("descriptionEs"),
        "organizations": [{"id": _strip_id(o["node"]["id"]), "slug": o["node"]["slug"]} for o in ds["organizations"]["edges"]],
        "themes": [{"id": _strip_id(t["node"]["id"]), "slug": t["node"]["slug"]} for t in ds["themes"]["edges"]],
        "tags": [{"id": _strip_id(t["node"]["id"]), "slug": t["node"]["slug"]} for t in ds["tags"]["edges"]],
        "tables": tables,
    }


@mcp.tool()
def reorder_tables(
    dataset_slug: str,
    table_slugs: list[str],
    env: str = "dev",
) -> dict:
    """
    Set the display order of tables within a dataset.

    Args:
        dataset_slug: dataset slug (e.g. "siconfi")
        table_slugs: ordered list of table slugs — first slug gets order 0
        env: "dev" or "prod"

    Returns: {"reordered": int, "order": [{"slug": str, "id": str}]}
    """
    data = _gql(
        """
        query($slug: String!) {
            allDataset(slug: $slug) {
                edges { node {
                    tables { edges { node { _id slug } } }
                } }
            }
        }
        """,
        {"slug": dataset_slug},
        env=env,
        auth=False,
    )
    edges = data["allDataset"]["edges"]
    if not edges:
        raise RuntimeError(f"Dataset not found: {dataset_slug}")
    slug_to_id = {
        t["node"]["slug"]: _strip_id(t["node"]["_id"])
        for t in edges[0]["node"]["tables"]["edges"]
    }

    missing = [s for s in table_slugs if s not in slug_to_id]
    if missing:
        raise RuntimeError(f"Table slugs not found in dataset: {missing}")

    ordered_ids = [slug_to_id[s] for s in table_slugs]

    result = _gql(
        """
        mutation($ids: [UUID]!) {
            reorderTables(ids: $ids) { ok errors }
        }
        """,
        {"ids": ordered_ids},
        env=env,
    )
    payload = result["reorderTables"]
    if not payload["ok"]:
        raise RuntimeError(f"reorderTables failed: {payload['errors']}")

    return {
        "reordered": len(ordered_ids),
        "order": [{"slug": s, "id": slug_to_id[s]} for s in table_slugs],
    }


@mcp.tool()
def reorder_observation_levels(
    table_id: str,
    ol_ids: list[str],
    env: str = "dev",
) -> dict:
    """
    Set the display order of observation levels on a table.

    Args:
        table_id: bare table ID
        ol_ids: ordered list of bare OL IDs — first ID gets order 0
        env: "dev" or "prod"

    Returns: {"reordered": int}
    """
    result = _gql(
        """
        mutation($ids: [UUID]!) {
            reorderObservationLevels(ids: $ids) { ok errors }
        }
        """,
        {"ids": ol_ids},
        env=env,
    )
    payload = result["reorderObservationLevels"]
    if not payload["ok"]:
        raise RuntimeError(f"reorderObservationLevels failed: {payload['errors']}")
    return {"reordered": len(ol_ids)}


@mcp.tool()
def reorder_columns(
    table_id: str,
    column_names: list[str],
    env: str = "dev",
) -> dict:
    """
    Set the display order of columns within a table.

    Args:
        table_id: bare table ID
        column_names: ordered list of column names — first name gets order 0
        env: "dev" or "prod"

    Returns: {"reordered": int, "order": [{"name": str, "id": str}]}
    """
    ds_data = _gql(
        """
        query($id: ID!) {
            allColumn(table_Id: $id) {
                edges { node { _id name } }
            }
        }
        """,
        {"id": table_id},
        env=env,
        auth=False,
    )
    name_to_id = {
        edge["node"]["name"]: edge["node"]["_id"]
        for edge in ds_data["allColumn"]["edges"]
    }

    missing = [n for n in column_names if n not in name_to_id]
    if missing:
        raise RuntimeError(f"Column names not found in table: {missing}")

    ordered_ids = [name_to_id[n] for n in column_names]

    result = _gql(
        """
        mutation($ids: [UUID]!) {
            reorderColumns(ids: $ids) { ok errors }
        }
        """,
        {"ids": ordered_ids},
        env=env,
    )
    payload = result["reorderColumns"]
    if not payload["ok"]:
        raise RuntimeError(f"reorderColumns failed: {payload['errors']}")

    return {
        "reordered": len(ordered_ids),
        "order": [{"name": n, "id": name_to_id[n]} for n in column_names],
    }


@mcp.tool()
def create_update_dataset(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    description_pt: str,
    description_en: str,
    description_es: str,
    organization_ids: list[str],
    theme_ids: list[str],
    status_id: str,
    tag_ids: list[str] | None = None,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a dataset record.

    Pass id to update an existing record; omit to create new.

    organizations, themes, and tags are ManyToMany fields — pass lists of IDs from discover_ids/lookup_id.

    Returns: {"id": str, "slug": str}
    """
    fields: dict[str, Any] = {
        "slug": slug,
        "name": name_pt,  # API requires a single 'name' field
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
        "descriptionPt": description_pt,
        "descriptionEn": description_en,
        "descriptionEs": description_es,
        "organizations": organization_ids,
        "themes": theme_ids,
        "tags": tag_ids or [],
        "status": status_id,
    }
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateDataset", fields, "dataset { id slug }", env=env)
    ds = payload["dataset"]
    return {"id": _strip_id(ds["id"]), "slug": ds["slug"]}


@mcp.tool()
def create_update_table(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    dataset_id: str,
    status_id: str,
    published_by_ids: list[str],
    data_cleaned_by_ids: list[str],
    description_pt: str = "",
    description_en: str = "",
    description_es: str = "",
    raw_data_source_ids: list[str] | None = None,
    is_directory: bool = False,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a table record.

    Args:
        is_directory: set True for a directory table (a table whose primary-key
            column other datasets reference via directory_column). Required for
            the table's columns to be selectable as a directory_primary_key
            target. Only sent when True, so it never accidentally clears the
            flag on a normal update.

    Returns: {"id": str, "slug": str}
    """
    fields: dict[str, Any] = {
        "slug": slug,
        "name": name_pt,  # API requires a single 'name' field
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
        "dataset": dataset_id,
        "status": status_id,
        "publishedBy": published_by_ids,
        "dataCleanedBy": data_cleaned_by_ids,
    }
    if description_pt:
        fields["descriptionPt"] = description_pt
    if description_en:
        fields["descriptionEn"] = description_en
    if description_es:
        fields["descriptionEs"] = description_es
    if raw_data_source_ids:
        fields["rawDataSource"] = raw_data_source_ids
    if is_directory:
        fields["isDirectory"] = is_directory
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateTable", fields, "table { id slug namePt }", env=env)
    t = payload["table"]
    return {"id": _strip_id(t["id"]), "slug": t["slug"]}


@mcp.tool()
def upload_columns(
    table_id: str,
    dataset_id: str,
    architecture_url: str,
    env: str = "dev",
) -> dict:
    """
    Upload columns from an architecture Google Sheets URL to a table.

    Uses the /upload_columns/ REST endpoint. Requires a valid CSRF token.

    NOTE: This REST endpoint currently returns 500. Use upload_columns_from_sheet instead.

    Args:
        table_id: bare table ID
        dataset_id: bare dataset ID
        architecture_url: Google Sheets URL with the architecture table
        env: "dev" or "prod"

    Returns: {"success": bool, "status_code": int}
    """
    token, base_url = _get_token(env)

    session = requests.Session()
    session.get(f"{base_url}/admin/login/", timeout=30)
    csrf_token = session.cookies.get("csrftoken", "")

    resp = session.post(
        f"{base_url}/upload_columns/",
        data={
            "token": token,
            "table_id": table_id,
            "dataset_id": dataset_id,
            "link": architecture_url,
            "csrfmiddlewaretoken": csrf_token,
        },
        headers={
            "Referer": f"{base_url}/admin/",
            "X-CSRFToken": csrf_token,
        },
        timeout=120,
    )
    return {"success": resp.ok, "status_code": resp.status_code, "text": resp.text[:500]}


@mcp.tool()
def upload_columns_from_sheet(
    table_id: str,
    architecture_url: str,
    env: str = "dev",
    observation_levels: str = "",
) -> dict:
    """
    Read columns from a public Google Sheet and create them on a table via GraphQL.

    Bypasses the broken /upload_columns/ REST endpoint (500 error) by downloading
    the sheet as CSV, parsing column definitions, and calling CreateUpdateColumn
    mutations directly.

    The sheet must be shared as "Anyone with link can view". Expected columns:
      name, bigquery_type, description, temporal_coverage, covered_by_dictionary,
      directory_column, measurement_unit, has_sensitive_data

    Args:
        table_id: bare table ID
        architecture_url: Google Sheets URL
        env: "dev" or "prod"
        observation_levels: JSON dict mapping column name → bare OL ID,
            e.g. '{"ano": "ol-id-1", "sigla_uf": "ol-id-2"}'.
            Columns present in the dict get their observationLevel linked on creation.

    Returns: {"created": int, "columns": [{"name": str, "id": str}], "errors": [...]}
    """
    import csv
    import io
    import re

    match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", architecture_url)
    if not match:
        raise ValueError(f"Cannot extract sheet ID from URL: {architecture_url}")
    sheet_id = match.group(1)

    csv_url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    resp = requests.get(csv_url, timeout=30, allow_redirects=True)
    if not resp.ok:
        raise RuntimeError(f"Failed to download sheet CSV: HTTP {resp.status_code}")

    rows = list(csv.DictReader(io.StringIO(resp.content.decode('utf-8'))))

    ol_map: dict[str, str] = json.loads(observation_levels) if observation_levels.strip() else {}

    ids = discover_ids(env=env, keys=["bigquery_type", "status"])
    bq_type_ids: dict[str, str] = ids.get("bigquery_type", {})
    published_status_id: str = ids.get("status", {}).get("published", "")

    # Pre-fetch existing columns so re-runs are idempotent: CreateUpdateColumn
    # has no id here and would otherwise create a DUPLICATE for every existing
    # name. Skip names that already exist (this tool is additive — update
    # existing columns via update_column instead).
    existing_names: set[str] = set()
    try:
        ec = _gql(
            "query($id: ID!) { allColumn(table_Id: $id) { edges { node { name } } } }",
            {"id": table_id},
            env=env,
            auth=False,
        )
        existing_names = {e["node"]["name"] for e in ec["allColumn"]["edges"]}
    except Exception:
        existing_names = set()

    # Build one input dict per row
    column_inputs = []
    skipped = []
    for row in rows:
        name = row.get("name", "").strip()
        if not name:
            continue
        if name in existing_names:
            skipped.append(name)
            continue

        bq_type_name = row.get("bigquery_type", "STRING").strip()
        bq_type_id = bq_type_ids.get(bq_type_name)

        fields: dict[str, Any] = {
            "name": name,
            "table": table_id,
        }
        if published_status_id:
            fields["status"] = published_status_id
        if bq_type_id:
            fields["bigqueryType"] = bq_type_id

        desc = row.get("description", "").strip()
        if desc:
            fields["descriptionPt"] = desc

        cbd = row.get("covered_by_dictionary", "no").strip().lower()
        fields["coveredByDictionary"] = cbd in ("yes", "true", "1")

        mu = row.get("measurement_unit", "").strip()
        if mu:
            fields["measurementUnit"] = mu

        hsd = row.get("has_sensitive_data", "no").strip().lower()
        fields["containsSensitiveData"] = hsd in ("yes", "true", "1")

        if name in ol_map:
            fields["observationLevel"] = ol_map[name]

        dir_col = row.get("directory_column", "").strip()
        if dir_col:
            col_node_id = _lookup_directory_column(dir_col, env)
            if col_node_id:
                fields["directoryPrimaryKey"] = col_node_id

        column_inputs.append(fields)

    if not column_inputs:
        return {"created": 0, "columns": [], "errors": [], "skipped": skipped}

    # Batch all columns into a single GraphQL mutation request using aliases
    auth_header, base_url = _get_token(env)
    variables = {f"input{i}": inp for i, inp in enumerate(column_inputs)}
    aliases = "\n".join(
        f'  col{i}: CreateUpdateColumn(input: $input{i}) {{ errors {{ field messages }} column {{ id name }} }}'
        for i in range(len(column_inputs))
    )
    var_defs = ", ".join(
        f"$input{i}: CreateUpdateColumnInput!" for i in range(len(column_inputs))
    )
    query = f"mutation({var_defs}) {{\n{aliases}\n}}"

    r = requests.post(
        f"{base_url}/graphql",
        json={"query": query, "variables": variables},
        headers={"Authorization": auth_header},
        timeout=120,
    )
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}:\n{r.text}")
    data = r.json()

    created = []
    errors = []
    gql_errors = data.get("errors")
    if gql_errors:
        raise RuntimeError(json.dumps(gql_errors, indent=2))

    retry_inputs = []
    for i, inp in enumerate(column_inputs):
        name = inp["name"]
        payload = data.get("data", {}).get(f"col{i}", {})
        if payload.get("errors"):
            # A directoryPrimaryKey the backend rejects (e.g. the target isn't a
            # recognized directory, or this is a directory dataset's own column)
            # fails the WHOLE column. Retry once without the FK so the column is
            # still created — matching the "silently skip the FK" contract.
            if "directoryPrimaryKey" in inp:
                retry_inputs.append({k: v for k, v in inp.items() if k != "directoryPrimaryKey"})
            else:
                errors.append({"name": name, "error": payload["errors"]})
        elif payload.get("column"):
            created.append({"name": name, "id": _strip_id(payload["column"]["id"])})
        else:
            errors.append({"name": name, "error": "no column returned"})

    for inp in retry_inputs:
        try:
            rr = _gql(
                "mutation($i: CreateUpdateColumnInput!) { CreateUpdateColumn(input: $i) "
                "{ errors { field messages } column { id name } } }",
                {"i": inp},
                env=env,
            )
            pay = rr.get("CreateUpdateColumn", {}) or {}
            if pay.get("errors"):
                errors.append({"name": inp["name"], "error": pay["errors"]})
            elif pay.get("column"):
                created.append({
                    "name": inp["name"],
                    "id": _strip_id(pay["column"]["id"]),
                    "note": "created without directoryPrimaryKey (FK rejected)",
                })
            else:
                errors.append({"name": inp["name"], "error": "no column returned on retry"})
        except Exception as e:
            errors.append({"name": inp["name"], "error": f"retry without directoryPrimaryKey failed: {e}"})

    return {"created": len(created), "columns": created, "errors": errors, "skipped": skipped}


def _fetch_table_columns(table_id: str, env: str) -> list[dict]:
    """
    All columns ({id, name}) for a table, via the top-level allColumn query.

    Unlike the nested tables{columns(first: 200)} path used by get_dataset —
    which caps at 200 columns — allColumn(table_Id:) returns every column, so
    this is safe for wide tables (400+ columns).
    """
    data = _gql(
        "query($id: ID!) { allColumn(table_Id: $id) { edges { node { id name } } } }",
        {"id": table_id},
        env=env,
        auth=False,
    )
    return [e["node"] for e in data["allColumn"]["edges"]]


@mcp.tool()
def bulk_upsert_columns(
    table_id: str,
    architecture_url: str = "",
    columns_json: str = "",
    env: str = "dev",
    update_only: bool = False,
    dry_run: bool = False,
    batch_size: int = 50,
) -> dict:
    """
    Bulk create-or-update many columns on a table in one call, matched by NAME.

    This is the bulk counterpart to update_column. The server resolves each
    column's id by name internally, so NO column UUID is ever passed by the
    caller — eliminating the id-transcription errors that make per-column
    updates unreliable at scale — and tables with more than 200 columns work
    (ids are read with the uncapped allColumn query, not get_dataset's 200-cap).

    Provide EXACTLY ONE source:
      - architecture_url: a public Google Sheet (same format as
        upload_columns_from_sheet) plus OPTIONAL `description_pt`,
        `description_en`, `description_es` columns. A bare `description` column
        is used as Portuguese when `description_pt` is absent.
      - columns_json: a JSON list of column dicts, e.g.
        '[{"name": "age", "description_pt": "Idade", "description_en": "Age",
           "description_es": "Edad", "covered_by_dictionary": true,
           "directory_column": "br_bd_diretorios_mundo.pais:sigla_iso3",
           "measurement_unit": "year", "has_sensitive_data": false,
           "bigquery_type": "INT64"}]'

    Only fields present (non-empty) for a row are written; omitted fields are
    left untouched — no accidental blanking, and partition/primary-key flags are
    never clobbered. Rows whose name already exists are UPDATED; new names are
    CREATED unless update_only=True (then reported under skipped_not_on_table).
    Idempotent: safe to re-run.

    Args:
        table_id: bare table ID
        architecture_url: Google Sheet URL (mutually exclusive with columns_json)
        columns_json: JSON list of column dicts (mutually exclusive with architecture_url)
        env: "local" | "dev" | "staging" | "prod"
        update_only: when True, do not create columns for names absent from the table
        dry_run: when True, return the planned actions without writing anything
        batch_size: columns per GraphQL request (aliased batch), 1-100

    Returns (compact — never dumps every column):
      {"updated": int, "created": int, "skipped_not_on_table": [str],
       "unchanged_no_fields": [str], "errors": [{"name", "error"}],
       "source_rows": int, "table_columns": int, "dry_run": bool,
       "planned_writes": int, "plan": [...]  (last two only when dry_run)}
    """
    import csv
    import io
    import re

    if bool(architecture_url.strip()) == bool(columns_json.strip()):
        raise ValueError("Provide exactly one of architecture_url or columns_json.")

    # --- load source rows -------------------------------------------------
    if architecture_url.strip():
        match = re.search(r"/spreadsheets/d/([a-zA-Z0-9_-]+)", architecture_url)
        if not match:
            raise ValueError(f"Cannot extract sheet ID from URL: {architecture_url}")
        csv_url = f"https://docs.google.com/spreadsheets/d/{match.group(1)}/export?format=csv"
        resp = requests.get(csv_url, timeout=30, allow_redirects=True)
        if not resp.ok:
            raise RuntimeError(f"Failed to download sheet CSV: HTTP {resp.status_code}")
        rows: list[dict] = list(csv.DictReader(io.StringIO(resp.content.decode("utf-8"))))
    else:
        rows = json.loads(columns_json)
        if not isinstance(rows, list):
            raise ValueError("columns_json must be a JSON list of column dicts.")

    def _get(row: dict, *keys: str) -> str:
        for k in keys:
            v = row.get(k)
            if v is None:
                continue
            v = str(v).strip()
            if v:
                return v
        return ""

    def _truthy(row: dict, key: str) -> bool | None:
        v = row.get(key)
        if v is None or (isinstance(v, str) and not v.strip()):
            return None
        if isinstance(v, bool):
            return v
        return str(v).strip().lower() in ("yes", "true", "1")

    # --- resolve existing columns by name (uncapped) ----------------------
    existing = _fetch_table_columns(table_id, env)
    name_to_id = {c["name"]: _strip_id(c["id"]) for c in existing}

    named_rows = [r for r in rows if _get(r, "name")]
    creates_needed = not update_only and any(
        _get(r, "name") not in name_to_id for r in named_rows
    )
    bq_type_ids: dict[str, str] = {}
    published_status_id = ""
    if creates_needed:
        ids = discover_ids(env=env, keys=["bigquery_type", "status"])
        bq_type_ids = ids.get("bigquery_type", {})
        published_status_id = ids.get("status", {}).get("published", "")

    # --- build one CreateUpdateColumn input per row -----------------------
    inputs: list[dict] = []
    actions: list[dict] = []
    skipped_not_on_table: list[str] = []
    unchanged: list[str] = []

    for row in named_rows:
        name = _get(row, "name")
        col_id = name_to_id.get(name)
        is_update = col_id is not None
        if not is_update and update_only:
            skipped_not_on_table.append(name)
            continue

        fields: dict[str, Any] = {"name": name, "table": table_id}
        if is_update:
            fields["id"] = col_id
        set_fields: list[str] = []

        pt = _get(row, "description_pt", "description")
        if pt:
            fields["descriptionPt"] = pt
            set_fields.append("descriptionPt")
        en = _get(row, "description_en")
        if en:
            fields["descriptionEn"] = en
            set_fields.append("descriptionEn")
        es = _get(row, "description_es")
        if es:
            fields["descriptionEs"] = es
            set_fields.append("descriptionEs")

        cbd = _truthy(row, "covered_by_dictionary")
        if cbd is not None:
            fields["coveredByDictionary"] = cbd
            set_fields.append("coveredByDictionary")

        mu = _get(row, "measurement_unit")
        if mu:
            fields["measurementUnit"] = mu
            set_fields.append("measurementUnit")

        hsd = _truthy(row, "has_sensitive_data")
        if hsd is not None:
            fields["containsSensitiveData"] = hsd
            set_fields.append("containsSensitiveData")

        dir_col = _get(row, "directory_column")
        if dir_col:
            fk = _lookup_directory_column(dir_col, env)
            if fk:
                fields["directoryPrimaryKey"] = fk
                set_fields.append("directoryPrimaryKey")

        if not is_update:
            bq_type_name = _get(row, "bigquery_type") or "STRING"
            if bq_type_ids.get(bq_type_name):
                fields["bigqueryType"] = bq_type_ids[bq_type_name]
            if published_status_id:
                fields["status"] = published_status_id

        # An existing column with no fields to set would be pure churn — skip it.
        if is_update and not set_fields:
            unchanged.append(name)
            continue

        inputs.append(fields)
        actions.append(
            {"name": name, "action": "update" if is_update else "create", "sets": set_fields}
        )

    result: dict[str, Any] = {
        "source_rows": len(named_rows),
        "table_columns": len(existing),
        "updated": 0,
        "created": 0,
        "skipped_not_on_table": skipped_not_on_table,
        "unchanged_no_fields": unchanged,
        "errors": [],
        "dry_run": dry_run,
    }

    if dry_run:
        result["planned_writes"] = len(inputs)
        result["plan"] = actions[:200]
        return result

    if not inputs:
        return result

    # --- execute in aliased batches (per-alias errors are isolated) -------
    auth_header, base_url = _get_token(env)
    updated = 0
    created = 0
    errors: list[dict] = []

    def _run_batch(batch: list[dict]) -> None:
        nonlocal updated, created
        variables = {f"input{i}": inp for i, inp in enumerate(batch)}
        aliases = "\n".join(
            f"  col{i}: CreateUpdateColumn(input: $input{i}) "
            f"{{ errors {{ field messages }} column {{ id name }} }}"
            for i in range(len(batch))
        )
        var_defs = ", ".join(f"$input{i}: CreateUpdateColumnInput!" for i in range(len(batch)))
        query = f"mutation({var_defs}) {{\n{aliases}\n}}"
        r = requests.post(
            f"{base_url}/graphql",
            json={"query": query, "variables": variables},
            headers={"Authorization": auth_header},
            timeout=120,
        )
        if not r.ok:
            raise RuntimeError(f"HTTP {r.status_code}:\n{r.text}")
        data = r.json()
        if data.get("errors"):
            raise RuntimeError(json.dumps(data["errors"], indent=2))
        for i, inp in enumerate(batch):
            payload = data.get("data", {}).get(f"col{i}", {}) or {}
            if payload.get("errors"):
                # A rejected directoryPrimaryKey fails the whole column; retry
                # once without the FK (mirrors upload_columns_from_sheet).
                if "directoryPrimaryKey" in inp:
                    retry = {k: v for k, v in inp.items() if k != "directoryPrimaryKey"}
                    try:
                        rr = _mut("CreateUpdateColumn", retry, "column { id name }", env=env)
                        if rr.get("column"):
                            if "id" in inp:
                                updated += 1
                            else:
                                created += 1
                            continue
                    except Exception as e:
                        errors.append({"name": inp["name"], "error": f"retry w/o FK failed: {e}"})
                        continue
                errors.append({"name": inp["name"], "error": payload["errors"]})
            elif payload.get("column"):
                if "id" in inp:
                    updated += 1
                else:
                    created += 1
            else:
                errors.append({"name": inp["name"], "error": "no column returned"})

    bs = max(1, min(int(batch_size), 100))
    for start in range(0, len(inputs), bs):
        _run_batch(inputs[start:start + bs])

    result["updated"] = updated
    result["created"] = created
    result["errors"] = errors
    return result


@mcp.tool()
def update_column(
    column_id: str,
    column_name: str,
    table_id: str,
    observation_level_id: str | None = None,
    clear_observation_level: bool = False,
    is_partition: bool = False,
    is_primary_key: bool = False,
    description_pt: str = "",
    description_en: str = "",
    description_es: str = "",
    measurement_unit: str = "",
    has_sensitive_data: bool = False,
    covered_by_dictionary: bool = False,
    directory_column_name: str = "",
    env: str = "dev",
) -> dict:
    """
    Update a single column record.

    Args:
        column_id: bare column ID
        column_name: column name (required by CreateUpdateColumn)
        table_id: bare table ID
        observation_level_id: bare OL ID to link (optional)
        clear_observation_level: when True, explicitly sets observationLevel to None (clears the FK)
        is_partition: whether this column is a BQ partition key
        is_primary_key: whether this is a primary key column
        description_pt/en/es: descriptions in each language
        measurement_unit: unit string
        has_sensitive_data: sensitive data flag
        covered_by_dictionary: whether covered by the dataset dictionary
        directory_column_name: BD directories FK (e.g. "br_bd_diretorios_brasil.municipio:id_municipio")
        env: "dev" or "prod"

    Returns: {"id": str, "name": str}
    """
    fields: dict[str, Any] = {
        "id": column_id,
        "name": column_name,
        "table": table_id,
        "isPartition": is_partition,
        "isPrimaryKey": is_primary_key,
    }
    if clear_observation_level:
        fields["observationLevel"] = None
    elif observation_level_id:
        fields["observationLevel"] = observation_level_id
    if description_pt:
        fields["descriptionPt"] = description_pt
    if description_en:
        fields["descriptionEn"] = description_en
    if description_es:
        fields["descriptionEs"] = description_es
    if measurement_unit:
        fields["measurementUnit"] = measurement_unit
    if has_sensitive_data:
        fields["containsSensitiveData"] = has_sensitive_data
    if covered_by_dictionary:
        fields["coveredByDictionary"] = covered_by_dictionary
    # Resolve the BD directories FK (e.g. "br_bd_diretorios_us.state:id_state")
    # to the target column node id and set it. The backend only accepts a target
    # whose column is is_primary_key=True and whose table is is_directory=True
    # (limit_choices_to); if the lookup misses, the FK is silently skipped.
    if directory_column_name:
        directory_pk_id = _lookup_directory_column(directory_column_name, env)
        if directory_pk_id:
            fields["directoryPrimaryKey"] = directory_pk_id

    payload = _mut("CreateUpdateColumn", fields, "column { id name }", env=env)
    col = payload["column"]
    return {"id": _strip_id(col["id"]), "name": col["name"]}


@mcp.tool()
def delete_column(
    column_id: str,
    env: str = "dev",
) -> dict:
    """
    Delete a column record from a table.

    Args:
        column_id: bare column ID (UUID)
        env: "dev" or "prod"

    Returns: {"deleted": True, "id": str}
    """
    q = """
    mutation($id: UUID!) {
        DeleteColumn(id: $id) {
            errors
        }
    }
    """
    result = _gql(q, {"id": column_id}, env=env)
    payload = result["DeleteColumn"]
    if payload and payload.get("errors"):
        raise RuntimeError(f"DeleteColumn errors: {payload['errors']}")
    return {"deleted": True, "id": column_id}


@mcp.tool()
def delete_table(
    table_id: str,
    env: str = "dev",
) -> dict:
    """
    Delete a table record from the backend.

    Args:
        table_id: bare table ID (UUID)
        env: "dev" or "prod"

    Returns: {"deleted": True, "id": str}
    """
    q = """
    mutation($id: UUID!) {
        DeleteTable(id: $id) {
            errors
        }
    }
    """
    result = _gql(q, {"id": table_id}, env=env)
    payload = result["DeleteTable"]
    if payload and payload.get("errors"):
        raise RuntimeError(f"DeleteTable errors: {payload['errors']}")
    return {"deleted": True, "id": table_id}


@mcp.tool()
def create_update_observation_level(
    table_id: str,
    entity_id: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update an observation level on a table.

    Args:
        table_id: bare table ID
        entity_id: bare entity ID (e.g. for "year", "municipality", etc.)
        id: bare OL ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    fields: dict[str, Any] = {"table": table_id, "entity": entity_id}
    if id:
        fields["id"] = id

    payload = _mut(
        "CreateUpdateObservationLevel",
        fields,
        "observationlevel { id }",
        env=env,
    )
    return {"id": _strip_id(payload["observationlevel"]["id"])}


@mcp.tool()
def create_update_cloud_table(
    table_id: str,
    gcp_project_id: str,
    gcp_dataset_id: str,
    gcp_table_id: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a cloud table (BigQuery table reference) on a table.

    Args:
        table_id: bare table ID
        gcp_project_id: e.g. "basedosdados" or "basedosdados-dev"
        gcp_dataset_id: e.g. "br_me_siconfi"
        gcp_table_id: e.g. "brasil_despesas_orcamentarias"
        id: bare cloud table ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    fields: dict[str, Any] = {
        "table": table_id,
        "gcpProjectId": gcp_project_id,
        "gcpDatasetId": gcp_dataset_id,
        "gcpTableId": gcp_table_id,
    }
    if id:
        fields["id"] = id

    payload = _mut(
        "CreateUpdateCloudTable",
        fields,
        "cloudtable { id }",
        env=env,
    )
    return {"id": _strip_id(payload["cloudtable"]["id"])}


@mcp.tool()
def create_update_coverage(
    table_id: str,
    area_id: str,
    is_closed: bool | None = None,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a coverage record on a table.

    Args:
        table_id: bare table ID
        area_id: bare area ID (e.g. the ID for area slug "br")
        is_closed: False (default on create) = open/free data; True = BD Pro
            data. A table paywalling a rolling window needs two coverages: the
            free one (is_closed=False) and the pro one (is_closed=True), which
            is what `Table.contains_closed_data` keys the Pro badge off, and
            what the pipelines' `PartBdpro` coverage spec requires to exist
            before it will run. Omit to leave the current value untouched, so a
            routine update cannot silently un-paywall data.
        id: bare coverage ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    fields: dict[str, Any] = {"table": table_id, "area": area_id}
    if is_closed is not None:
        fields["isClosed"] = is_closed
    if id:
        fields["id"] = id

    payload = _mut(
        "CreateUpdateCoverage",
        fields,
        "coverage { id }",
        env=env,
    )
    return {"id": _strip_id(payload["coverage"]["id"])}


@mcp.tool()
def create_update_datetime_range(
    coverage_id: str,
    start_year: int,
    end_year: int,
    interval: int = 1,
    is_closed: bool = False,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a datetime range on a coverage.

    Args:
        coverage_id: bare coverage ID
        start_year: e.g. 2013
        end_year: e.g. 2025
        interval: years between observations (1 = annual)
        is_closed: True if the series has ended
        id: bare datetime range ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    fields: dict[str, Any] = {
        "coverage": coverage_id,
        "startYear": start_year,
        "endYear": end_year,
        "interval": interval,
        "isClosed": is_closed,
    }
    if id:
        fields["id"] = id

    payload = _mut(
        "CreateUpdateDateTimeRange",
        fields,
        "datetimerange { id }",
        env=env,
    )
    return {"id": _strip_id(payload["datetimerange"]["id"])}


@mcp.tool()
def create_update_update(
    table_id: str,
    entity_id: str,
    frequency: int,
    lag: int,
    latest: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update an update record (publishing cadence) on a table.

    Args:
        table_id: bare table ID
        entity_id: bare entity ID for the update frequency unit (usually "year")
        frequency: how many units between updates (e.g. 1 for annual)
        lag: expected lag in the same units (e.g. 1 year)
        latest: ISO datetime string of the latest update, e.g. "2025-03-28T14:30:00"
        id: bare update ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    fields: dict[str, Any] = {
        "table": table_id,
        "entity": entity_id,
        "frequency": frequency,
        "lag": lag,
        "latest": latest,
    }
    if id:
        fields["id"] = id

    payload = _mut(
        "CreateUpdateUpdate",
        fields,
        "update { id }",
        env=env,
    )
    return {"id": _strip_id(payload["update"]["id"])}


@mcp.tool()
def get_raw_data_sources(dataset_slug: str, env: str = "dev") -> list[dict]:
    """
    Return raw data sources associated with a dataset.

    Queries via dataset.rawDataSources (not allRawdatasource, which has auth/visibility issues).

    Args:
        dataset_slug: e.g. "siconfi"
        env: "dev" or "prod"

    Returns: [{"id": str, "name": str, "url": str}]
    """
    data = _gql(
        """
        query($slug: String!) {
            allDataset(slug: $slug) {
                edges { node {
                    rawDataSources(first: 50) {
                        edges { node { id name url } }
                    }
                } }
            }
        }
        """,
        {"slug": dataset_slug},
        env=env,
        auth=False,
    )
    edges = data["allDataset"]["edges"]
    if not edges:
        return []
    results = []
    for e in edges[0]["node"]["rawDataSources"]["edges"]:
        n = e["node"]
        results.append({
            "id": _strip_id(n["id"]),
            "name": n.get("name", ""),
            "url": n.get("url", ""),
        })
    return results


@mcp.tool()
def create_update_raw_data_source(
    dataset_id: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    url: str,
    license_id: str,
    availability_id: str,
    description_pt: str = "",
    description_en: str = "",
    description_es: str = "",
    has_structured_data: bool = True,
    is_free: bool | None = None,
    contains_api: bool | None = None,
    requires_registration: bool | None = None,
    language_ids: list[str] | None = None,
    status_id: str | None = None,
    version: int | None = None,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a raw data source record on a dataset.

    Pass id to update an existing record; omit to create new.

    Fields (booleans and status/version are only written when provided, so
    partial updates never blank an existing value):
        has_structured_data: source provides structured (tabular) data.
        is_free: source is freely available at no cost.
        contains_api: source is accessible via an API.
        requires_registration: accessing the source requires registration/login.
        language_ids: list of Language IDs (discover_ids/lookup_id category
            "language") the source is published in, e.g. ["<en-id>"] for English.
        status_id: Status ID (discover_ids category "status").
        version: integer version of the source.

    Note: the RawDataSource model has no sensitive-data field (sensitivity is a
    column-level attribute), so no has_sensitive_data argument is accepted.

    Returns: {"id": str}
    """
    fields: dict[str, Any] = {
        "dataset": dataset_id,
        "name": name_pt,
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
        "url": url,
        "license": license_id,
        "availability": availability_id,
        "containsStructuredData": has_structured_data,
    }
    if description_pt:
        fields["descriptionPt"] = description_pt
    if description_en:
        fields["descriptionEn"] = description_en
    if description_es:
        fields["descriptionEs"] = description_es
    if is_free is not None:
        fields["isFree"] = is_free
    if contains_api is not None:
        fields["containsApi"] = contains_api
    if requires_registration is not None:
        fields["requiresRegistration"] = requires_registration
    if language_ids:
        fields["languages"] = language_ids
    if status_id:
        fields["status"] = status_id
    if version is not None:
        fields["version"] = version
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateRawDataSource", fields, "rawdatasource { id }", env=env)
    return {"id": _strip_id(payload["rawdatasource"]["id"])}


@mcp.tool()
def create_update_tag(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a tag record.

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    fields: dict[str, Any] = {
        "slug": slug,
        "name": name_pt,
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
    }
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateTag", fields, "tag { id slug }", env=env)
    t = payload["tag"]
    return {"id": _strip_id(t["id"]), "slug": t["slug"]}


@mcp.tool()
def create_update_theme(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a theme record.

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    fields: dict[str, Any] = {
        "slug": slug,
        "name": name_pt,
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
    }
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateTheme", fields, "theme { id slug }", env=env)
    t = payload["theme"]
    return {"id": _strip_id(t["id"]), "slug": t["slug"]}


@mcp.tool()
def create_update_organization(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    description_pt: str = "",
    description_en: str = "",
    description_es: str = "",
    area_id: str | None = None,
    website: str = "",
    twitter: str = "",
    facebook: str = "",
    linkedin: str = "",
    instagram: str = "",
    env: str = "dev",
) -> dict:
    """
    Create or update an organization record.

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    fields: dict[str, Any] = {
        "slug": slug,
        "name": name_pt,
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
    }
    if description_pt:
        fields["descriptionPt"] = description_pt
    if description_en:
        fields["descriptionEn"] = description_en
    if description_es:
        fields["descriptionEs"] = description_es
    if area_id:
        fields["area"] = area_id
    if website:
        fields["website"] = website
    if twitter:
        fields["twitter"] = twitter
    if facebook:
        fields["facebook"] = facebook
    if linkedin:
        fields["linkedin"] = linkedin
    if instagram:
        fields["instagram"] = instagram
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateOrganization", fields, "organization { id slug }", env=env)
    o = payload["organization"]
    return {"id": _strip_id(o["id"]), "slug": o["slug"]}


def _create_update_ref(
    mutation_name: str,
    result_field: str,
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    extra: dict | None = None,
    env: str = "dev",
) -> dict:
    """Shared create/update helper for simple reference tables that carry
    slug + name (pt/en/es). `extra` adds model-specific fields (only truthy
    values are sent). `result_field` is the camelCase model field on the
    mutation payload (e.g. "license", "entityCategory")."""
    fields: dict[str, Any] = {
        "slug": slug,
        "name": name_pt,
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
    }
    if extra:
        fields.update({k: v for k, v in extra.items() if v not in (None, "")})
    if id:
        fields["id"] = id
    payload = _mut(mutation_name, fields, f"{result_field} {{ id slug }}", env=env)
    o = payload[result_field]
    return {"id": _strip_id(o["id"]), "slug": o["slug"]}


@mcp.tool()
def create_update_license(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    url: str = "",
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a license record (e.g. cc_by_sa, cc_by_nc_sa, cc0).

    Pass id to update an existing record; omit to create new.

    Args:
        slug: license slug, e.g. "cc_by_sa"
        name_pt/en/es: display name in each language, e.g.
            "Creative Commons Attribution-ShareAlike 4.0 (CC BY-SA 4.0)"
        url: canonical license URL, e.g.
            "https://creativecommons.org/licenses/by-sa/4.0/"
        id: pass to update an existing record

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateLicense", "license", slug, name_pt, name_en, name_es,
        id=id, extra={"url": url}, env=env,
    )


@mcp.tool()
def create_update_availability(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update an availability record (e.g. online, physical, in_person).

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateAvailability", "availability", slug, name_pt, name_en, name_es,
        id=id, env=env,
    )


@mcp.tool()
def create_update_language(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a language record (e.g. en, pt, es).

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateLanguage", "language", slug, name_pt, name_en, name_es,
        id=id, env=env,
    )


@mcp.tool()
def create_update_status(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a status record (e.g. published, under_review, processing).

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateStatus", "status", slug, name_pt, name_en, name_es,
        id=id, env=env,
    )


@mcp.tool()
def create_update_entity_category(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update an entity-category record (groups observation-level
    entities, e.g. "datetime", "spatial", "person").

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateEntityCategory", "entitycategory", slug, name_pt, name_en, name_es,
        id=id, env=env,
    )


@mcp.tool()
def create_update_entity(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    category_id: str | None = None,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update an observation-level entity record (e.g. person, country,
    year, municipality).

    Pass id to update an existing record; omit to create new.

    Args:
        category_id: bare EntityCategory ID this entity belongs to (optional).

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateEntity", "entity", slug, name_pt, name_en, name_es,
        id=id, extra={"category": category_id}, env=env,
    )


@mcp.tool()
def create_update_measurement_unit_category(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a measurement-unit-category record (groups measurement
    units, e.g. "currency", "length", "time").

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateMeasurementUnitCategory", "measurementunitcategory",
        slug, name_pt, name_en, name_es, id=id, env=env,
    )


@mcp.tool()
def create_update_area(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    administrative_level: str = "",
    entity_id: str | None = None,
    parent_id: str | None = None,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a spatial-coverage area record (e.g. "world", "eu", "br",
    a continent or country).

    Pass id to update an existing record; omit to create new.

    Args:
        administrative_level: optional administrative level string.
        entity_id: bare Entity ID for the area's spatial entity (optional).
        parent_id: bare parent Area ID (optional).

    Returns: {"id": str, "slug": str}
    """
    return _create_update_ref(
        "CreateUpdateArea", "area", slug, name_pt, name_en, name_es, id=id,
        extra={"administrativeLevel": administrative_level, "entity": entity_id,
               "parent": parent_id},
        env=env,
    )


@mcp.tool()
def get_authenticated_account(env: str = "dev") -> dict:
    """
    Return the ID and email of the currently authenticated account.

    Returns: {"id": str, "email": str}
    """
    creds = _get_credentials(env)
    if creds[0] == "password":
        email = creds[1]
    else:
        creds_path = Path.home() / ".basedosdados" / "credentials.json"
        env_data = json.loads(creds_path.read_text()).get(env, {}) if creds_path.exists() else {}
        email = os.environ.get("EMAIL") or env_data.get("email")
        if not email:
            raise RuntimeError(
                f"Token-based credentials for env='{env}' carry no email. "
                'Add an "email" field next to the token in ~/.basedosdados/credentials.json.'
            )
    data = _gql(
        'query($email: String!) { allAccount(first: 1, email: $email) { edges { node { id email } } } }',
        {"email": email},
        env=env,
    )
    edges = data["allAccount"]["edges"]
    if edges:
        n = edges[0]["node"]
        return {"id": _strip_id(n["id"]), "email": n["email"]}
    raise RuntimeError(f"Account not found for email: {email}")


# ---------------------------------------------------------------------------
# BigQuery helpers
# ---------------------------------------------------------------------------


def _get_bq_client(billing_project: str | None = None):
    """Return a BigQuery client, resolving billing project from arg → env var → credentials file."""
    from google.cloud import bigquery  # deferred import: only needed for BQ tools

    project = billing_project or os.environ.get("GCP_PROJECT_ID")
    if not project:
        creds_path = Path.home() / ".basedosdados" / "credentials.json"
        if creds_path.exists():
            data = json.loads(creds_path.read_text())
            project = data.get("gcp_project")
    if not project:
        raise RuntimeError(
            "Projeto GCP de faturamento não encontrado. Forneça o parâmetro billing_project, "
            "defina a variável de ambiente GCP_PROJECT_ID, ou adicione 'gcp_project' em "
            "~/.basedosdados/credentials.json"
        )
    return bigquery.Client(project=project)


def _bq_row_to_dict(row) -> dict:
    """Convert a BigQuery Row to a JSON-serializable dict."""
    from datetime import date, datetime
    from decimal import Decimal

    result = {}
    for key, value in row.items():
        if isinstance(value, (datetime, date)):
            result[key] = value.isoformat()
        elif isinstance(value, Decimal):
            result[key] = float(value)
        else:
            result[key] = value
    return result


# ---------------------------------------------------------------------------
# BigQuery tools
# ---------------------------------------------------------------------------


@mcp.tool()
def search_datasets(
    query: str,
    limit: int = 10,
    env: str = "prod",
) -> dict:
    """
    Busca datasets na Base dos Dados por nome (em português).

    Não requer autenticação.

    Args:
        query: termo de busca (ex: "educação", "saúde", "clima")
        limit: número máximo de resultados (padrão 10, máximo 50)
        env: "dev" ou "prod" (padrão: "prod")

    Returns:
        {"total": int, "datasets": [{"slug", "name_pt", "description_pt", "organizations", "themes"}]}
    """
    limit = min(limit, 50)
    q = """
    query($search: String!, $limit: Int!) {
        allDataset(namePt_Icontains: $search, first: $limit) {
            totalCount
            edges { node {
                slug namePt descriptionPt
                organizations(first: 5) { edges { node { slug namePt } } }
                themes(first: 5) { edges { node { slug namePt } } }
            } }
        }
    }
    """
    data = _gql(q, {"search": query, "limit": limit}, env=env, auth=False)
    result = data["allDataset"]
    datasets = [
        {
            "slug": e["node"]["slug"],
            "name_pt": e["node"].get("namePt"),
            "description_pt": e["node"].get("descriptionPt"),
            "organizations": [o["node"]["slug"] for o in e["node"]["organizations"]["edges"]],
            "themes": [t["node"]["slug"] for t in e["node"]["themes"]["edges"]],
        }
        for e in result["edges"]
    ]
    return {"total": result["totalCount"], "datasets": datasets}


@mcp.tool()
def preview_table(
    dataset_slug: str,
    table_slug: str,
    billing_project: str | None = None,
    limit: int = 10,
) -> dict:
    """
    Visualiza as primeiras linhas de uma tabela da Base dos Dados via BigQuery.

    Resolve automaticamente a referência BigQuery a partir dos metadados do backend.
    Não requer autenticação de backend, mas requer GCP autenticado via ADC:
      gcloud auth application-default login

    Args:
        dataset_slug: slug do dataset (ex: "br_ibge_censo_demografico")
        table_slug: slug da tabela (ex: "municipio")
        billing_project: projeto GCP para faturamento (opcional se GCP_PROJECT_ID definido)
        limit: número máximo de linhas (padrão 10, máximo 100)

    Returns:
        {"bq_table": str, "rows": list[dict], "row_count": int}
    """
    limit = min(limit, 100)

    q = """
    query($slug: String!) {
        allDataset(slug: $slug) {
            edges { node {
                tables(first: 200) { edges { node {
                    slug
                    cloudTables(first: 1) { edges { node {
                        gcpProjectId gcpDatasetId gcpTableId
                    } } }
                } } }
            } }
        }
    }
    """
    data = _gql(q, {"slug": dataset_slug}, auth=False)
    ds_edges = data["allDataset"]["edges"]
    if not ds_edges:
        raise RuntimeError(f"Dataset não encontrado: {dataset_slug!r}")

    table_node = None
    for te in ds_edges[0]["node"]["tables"]["edges"]:
        if te["node"]["slug"] == table_slug:
            table_node = te["node"]
            break
    if table_node is None:
        raise RuntimeError(f"Tabela {table_slug!r} não encontrada no dataset {dataset_slug!r}")

    ct_edges = table_node["cloudTables"]["edges"]
    if not ct_edges:
        raise RuntimeError(f"Tabela {table_slug!r} não possui referência BigQuery registrada")

    ct = ct_edges[0]["node"]
    bq_table = f"{ct['gcpProjectId']}.{ct['gcpDatasetId']}.{ct['gcpTableId']}"

    client = _get_bq_client(billing_project)
    sql = f"SELECT * FROM `{bq_table}` LIMIT {limit}"
    rows = list(client.query(sql).result())

    return {
        "bq_table": bq_table,
        "rows": [_bq_row_to_dict(row) for row in rows],
        "row_count": len(rows),
    }


@mcp.tool()
def query_bigquery(
    sql: str,
    billing_project: str | None = None,
) -> dict:
    """
    Executa uma consulta SQL em tabelas da Base dos Dados no BigQuery.

    As tabelas da BD estão no projeto `basedosdados`, no formato:
      `basedosdados.<gcp_dataset_id>.<gcp_table_id>`

    Use get_dataset() para obter os valores corretos de gcp_dataset_id e gcp_table_id
    (campo cloud_tables na resposta).

    Não requer autenticação de backend, mas requer GCP autenticado via ADC:
      gcloud auth application-default login

    Sempre inclua LIMIT na consulta para evitar leituras desnecessárias.

    Args:
        sql: consulta SQL referenciando tabelas em `basedosdados.*`
        billing_project: projeto GCP para faturamento (opcional se GCP_PROJECT_ID definido)

    Returns:
        {"rows": list[dict], "row_count": int, "bytes_processed": int | None}
    """
    if "basedosdados" not in sql.lower():
        raise ValueError(
            "A consulta deve referenciar tabelas do projeto `basedosdados`. "
            "Exemplo: SELECT * FROM `basedosdados.br_ibge_censo_demografico.municipio` LIMIT 10"
        )

    client = _get_bq_client(billing_project)
    job = client.query(sql)
    rows = list(job.result())

    return {
        "rows": [_bq_row_to_dict(row) for row in rows],
        "row_count": len(rows),
        "bytes_processed": job.total_bytes_processed,
    }


# ---------------------------------------------------------------------------
# Prefect helpers
# ---------------------------------------------------------------------------

PREFECT_URL = "https://prefect.basedosdados.org/api"


def _prefect_key() -> str:
    creds_path = Path.home() / ".basedosdados" / "credentials.json"
    if creds_path.exists():
        data = json.loads(creds_path.read_text())
        key = data.get("prod", {}).get("prefect")
        if key:
            return key
    raise RuntimeError(
        "No Prefect API key found. Add 'prefect' key under 'prod' in "
        "~/.basedosdados/credentials.json"
    )


def _prefect_gql(query: str, variables: dict | None = None) -> dict:
    r = requests.post(
        PREFECT_URL,
        json={"query": query, "variables": variables or {}},
        headers={"Authorization": f"Bearer {_prefect_key()}"},
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}:\n{r.text}")
    data = r.json()
    if "errors" in data:
        raise RuntimeError(json.dumps(data["errors"], indent=2))
    return data["data"]


# ---------------------------------------------------------------------------
# Prefect tools
# ---------------------------------------------------------------------------


@mcp.tool()
def list_flow_runs(
    state: str | None = None,
    flow_name: str | None = None,
    limit: int = 20,
) -> list[dict]:
    """List recent Prefect flow runs.

    Args:
        state: Filter by state, e.g. 'Failed', 'Success', 'Running'. None = all.
        flow_name: Filter by flow name substring (case-sensitive). None = all.
        limit: Max number of runs to return (default 20, max 100).
    """
    limit = min(limit, 100)

    where_parts = []
    if state:
        where_parts.append(f'state: {{_eq: "{state}"}}')
    if flow_name:
        where_parts.append(f'flow: {{name: {{_like: "%{flow_name}%"}}}}')
    where_clause = "{" + ", ".join(where_parts) + "}" if where_parts else "{}"

    q = f"""
    {{
        flow_run(
            where: {where_clause},
            order_by: {{end_time: desc_nulls_last}},
            limit: {limit}
        ) {{
            id
            name
            state
            state_message
            start_time
            end_time
            flow {{ name }}
        }}
    }}
    """
    runs = _prefect_gql(q)["flow_run"]
    return [
        {
            "id": r["id"],
            "name": r["name"],
            "flow_name": r["flow"]["name"],
            "state": r["state"],
            "state_message": r["state_message"],
            "start_time": r["start_time"],
            "end_time": r["end_time"],
        }
        for r in runs
    ]


@mcp.tool()
def get_flow_run_logs(
    flow_run_id: str,
    min_level: str | None = None,
    limit: int = 200,
) -> list[dict]:
    """Get logs for a specific Prefect flow run.

    Args:
        flow_run_id: The UUID of the flow run.
        min_level: Minimum log level to return: 'DEBUG', 'INFO', 'WARNING', 'ERROR',
                   'CRITICAL'. None = all levels.
        limit: Max number of log entries to return (default 200, max 500).
    """
    limit = min(limit, 500)

    level_order = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    level_filter = ""
    if min_level:
        upper = min_level.upper()
        if upper not in level_order:
            raise ValueError(f"min_level must be one of {level_order}; got {min_level!r}")
        included = level_order[level_order.index(upper):]
        levels_gql = "[" + ", ".join(f'"{l}"' for l in included) + "]"
        level_filter = f", level: {{_in: {levels_gql}}}"

    q = f"""
    {{
        log(
            where: {{flow_run_id: {{_eq: "{flow_run_id}"}}{level_filter}}},
            order_by: {{timestamp: asc}},
            limit: {limit}
        ) {{
            timestamp
            level
            name
            message
        }}
    }}
    """
    return _prefect_gql(q)["log"]


@mcp.tool()
def get_failed_flow_runs(
    flow_name: str | None = None,
    runs_limit: int = 5,
    logs_per_run: int = 100,
    min_log_level: str = "ERROR",
) -> list[dict]:
    """Get recent failed Prefect flow runs together with their logs.

    Args:
        flow_name: Filter by flow name substring. None = all flows.
        runs_limit: Max number of failed runs to return (default 5, max 20).
        logs_per_run: Max log entries per run (default 100, max 200).
        min_log_level: Minimum log level to include: 'DEBUG', 'INFO', 'WARNING',
                       'ERROR', 'CRITICAL' (default 'ERROR').
    """
    runs_limit = min(runs_limit, 20)
    logs_per_run = min(logs_per_run, 200)

    runs = list_flow_runs(state="Failed", flow_name=flow_name, limit=runs_limit)
    result = []
    for run in runs:
        logs = get_flow_run_logs(
            flow_run_id=run["id"],
            min_level=min_log_level,
            limit=logs_per_run,
        )
        result.append({**run, "logs": logs})
    return result


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
