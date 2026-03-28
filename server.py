"""
bd-mcp: MCP server wrapping the basedosdados GraphQL backend.

Credentials (in priority order):
  1. Env vars: BD_EMAIL and BD_PASSWORD
  2. ~/.basedosdados/bd_credentials.json: {"dev": {"email": ..., "password": ...}, "prod": {...}}

Environment:
  BD_ENV=dev (default) or BD_ENV=prod

Token is cached in memory for 24 hours.
"""

import json
import os
import time
from pathlib import Path
from typing import Any

import requests
from fastmcp import FastMCP

mcp = FastMCP(
    "bd-mcp",
    instructions=(
        "Tools for interacting with the basedosdados backend API. "
        "All write tools are idempotent: pass an existing id to update, "
        "omit it to create. Always call db_auth first or rely on auto-auth."
    ),
)

# ---------------------------------------------------------------------------
# URLs
# ---------------------------------------------------------------------------

URLS = {
    "dev": "https://development.backend.basedosdados.org",
    "prod": "https://backend.basedosdados.org",
}

# ---------------------------------------------------------------------------
# In-memory cache
# ---------------------------------------------------------------------------

_cache: dict[str, Any] = {
    "token": None,
    "expires_at": 0.0,
    "env": None,
    "ids": {},   # {env: {category: {slug: id}}}
}


# ---------------------------------------------------------------------------
# Auth helpers
# ---------------------------------------------------------------------------


def _get_credentials(env: str) -> tuple[str, str]:
    """
    Return (email, password) for the given environment.

    Lookup order:
      1. Env vars BD_EMAIL / BD_PASSWORD (environment-agnostic override)
      2. ~/.basedosdados/bd_credentials.json under key "dev" or "prod"
         Falls back to flat {"email", "password"} structure for compatibility.
    """
    email = os.environ.get("BD_EMAIL")
    password = os.environ.get("BD_PASSWORD")
    if email and password:
        return email, password

    creds_path = Path.home() / ".basedosdados" / "bd_credentials.json"
    if creds_path.exists():
        data = json.loads(creds_path.read_text())
        if env in data:
            return data[env]["email"], data[env]["password"]
        if "email" in data:  # flat fallback
            return data["email"], data["password"]

    raise RuntimeError(
        f"No credentials found for env='{env}'. "
        "Set BD_EMAIL / BD_PASSWORD env vars or create "
        "~/.basedosdados/bd_credentials.json with "
        '{"dev": {"email": "...", "password": "..."}, "prod": {...}}'
    )


def _get_token(env: str | None = None) -> tuple[str, str]:
    """Return (token, base_url), refreshing if expired."""
    env = env or os.environ.get("BD_ENV", "dev")
    if env not in URLS:
        raise ValueError(f"env must be 'dev' or 'prod', got: {env!r}")

    now = time.time()
    if _cache["token"] and _cache["expires_at"] > now and _cache["env"] == env:
        return _cache["token"], URLS[env]

    email, password = _get_credentials(env)
    base_url = URLS[env]
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

    token = data["data"]["tokenAuth"]["token"]
    _cache.update(
        token=token,
        expires_at=now + 86400,
        env=env,
        ids={},
    )
    return token, base_url


# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------


def _gql(query: str, variables: dict | None = None, env: str | None = None) -> dict:
    token, base_url = _get_token(env)
    r = requests.post(
        f"{base_url}/graphql",
        json={"query": query, "variables": variables or {}},
        headers={"Authorization": f"Bearer {token}"},
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


def _fetch_all(token_env: str, query_name: str, fields: str) -> list[dict]:
    q = f"""
    query {{
        {query_name}(first: 500) {{
            edges {{ node {{ {fields} }} }}
        }}
    }}
    """
    data = _gql(q, env=token_env)
    return [e["node"] for e in data[query_name]["edges"]]


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------


@mcp.tool()
def db_auth(env: str = "dev") -> dict:
    """
    Authenticate to the basedosdados backend.

    Reads credentials from BD_EMAIL/BD_PASSWORD env vars or
    ~/.basedosdados/bd_credentials.json (keyed by env). Token is cached for 24 hours.

    Args:
        env: "dev" or "prod" (default: "dev", overridden by BD_ENV env var)

    Returns:
        {"authenticated": True, "env": env, "base_url": url}
    """
    token, base_url = _get_token(env)
    return {"authenticated": True, "env": env, "base_url": base_url, "token_cached": True}


@mcp.tool()
def db_discover_ids(env: str = "dev") -> dict:
    """
    Fetch and return all reference IDs needed for metadata creation.

    Returns a dict with keys:
      status, bigquery_type, entity, area, license, availability, organization

    Each value is a dict mapping slug/name → bare ID string.

    Args:
        env: "dev" or "prod"
    """
    cache_key = f"ids_{env}"
    if cache_key in _cache.get("ids", {}):
        return _cache["ids"][cache_key]

    result: dict[str, dict] = {}

    # Status
    nodes = _fetch_all(env, "allStatus", "id slug")
    result["status"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    # BigQueryType
    for qname in ["allBigquerytype", "allBigQueryType"]:
        try:
            nodes = _fetch_all(env, qname, "id name")
            result["bigquery_type"] = {n["name"]: _strip_id(n["id"]) for n in nodes}
            break
        except Exception:
            continue
    if "bigquery_type" not in result:
        result["bigquery_type"] = {}

    # Entity
    nodes = _fetch_all(env, "allEntity", "id slug namePt")
    result["entity"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    # Area (server-side filter to avoid pagination issues)
    data = _gql(
        '{ allArea(first: 500) { edges { node { id slug } } } }',
        env=env,
    )
    result["area"] = {
        e["node"]["slug"]: _strip_id(e["node"]["id"])
        for e in data["allArea"]["edges"]
    }

    # License
    nodes = _fetch_all(env, "allLicense", "id slug namePt")
    result["license"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    # Availability
    nodes = _fetch_all(env, "allAvailability", "id slug namePt")
    result["availability"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    # Organization
    nodes = _fetch_all(env, "allOrganization", "id slug namePt")
    result["organization"] = {n["slug"]: _strip_id(n["id"]) for n in nodes}

    # Cache result
    if "ids" not in _cache:
        _cache["ids"] = {}
    _cache["ids"][cache_key] = result
    return result


@mcp.tool()
def db_get_dataset(slug: str, env: str = "dev") -> dict:
    """
    Fetch a dataset by slug and return its full metadata.

    Returns:
      {
        "found": bool,
        "id": str | None,
        "slug": str,
        "tables": {
          "<table_slug>": {
            "id": str,
            "columns": [{"id", "name"}],
            "observation_levels": [{"id", "entity_id"}],
            "cloud_tables": [{"id"}],
            "coverages": [{"id", "area_id", "area_slug", "datetime_ranges": [...]}],
            "updates": [{"id", "entity_id"}],
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
                    tables(first: 200) {
                        edges {
                            node {
                                id slug
                                columns(first: 200) { edges { node { id name } } }
                                observationLevels(first: 20) {
                                    edges { node { id entity { id slug } } }
                                }
                                cloudTables(first: 10) { edges { node { id } } }
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
                            }
                        }
                    }
                }
            }
        }
    }
    """
    data = _gql(q, {"slug": slug}, env=env)
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
                    "entity_id": _strip_id(ol["node"]["entity"]["id"]),
                    "entity_slug": ol["node"]["entity"]["slug"],
                }
                for ol in t["observationLevels"]["edges"]
            ],
            "cloud_tables": [
                {"id": _strip_id(ct["node"]["id"])}
                for ct in t["cloudTables"]["edges"]
            ],
            "coverages": [
                {
                    "id": _strip_id(cov["node"]["id"]),
                    "area_id": _strip_id(cov["node"]["area"]["id"]),
                    "area_slug": cov["node"]["area"]["slug"],
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
                    "entity_id": _strip_id(upd["node"]["entity"]["id"]),
                    "entity_slug": upd["node"]["entity"]["slug"],
                }
                for upd in t["updates"]["edges"]
            ],
        }

    return {
        "found": True,
        "id": _strip_id(ds["id"]),
        "slug": ds["slug"],
        "tables": tables,
    }


@mcp.tool()
def db_create_update_dataset(
    slug: str,
    name_pt: str,
    name_en: str,
    name_es: str,
    description_pt: str,
    description_en: str,
    description_es: str,
    organization_id: str,
    theme_ids: list[str],
    license_id: str,
    availability_id: str,
    status_id: str,
    is_closed: bool = False,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a dataset record.

    Pass id to update an existing record; omit to create new.

    Returns: {"id": str, "slug": str}
    """
    fields: dict[str, Any] = {
        "slug": slug,
        "namePt": name_pt,
        "nameEn": name_en,
        "nameEs": name_es,
        "descriptionPt": description_pt,
        "descriptionEn": description_en,
        "descriptionEs": description_es,
        "organization": organization_id,
        "themes": theme_ids,
        "license": license_id,
        "availability": availability_id,
        "status": status_id,
        "isClosed": is_closed,
    }
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateDataset", fields, "dataset { id slug }", env=env)
    ds = payload["dataset"]
    return {"id": _strip_id(ds["id"]), "slug": ds["slug"]}


@mcp.tool()
def db_create_update_table(
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
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a table record.

    Returns: {"id": str, "slug": str}
    """
    fields: dict[str, Any] = {
        "slug": slug,
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
        fields["rawDataSources"] = raw_data_source_ids
    if id:
        fields["id"] = id

    payload = _mut("CreateUpdateTable", fields, "table { id slug namePt }", env=env)
    t = payload["table"]
    return {"id": _strip_id(t["id"]), "slug": t["slug"]}


@mcp.tool()
def db_upload_columns(
    table_id: str,
    dataset_id: str,
    architecture_url: str,
    env: str = "dev",
) -> dict:
    """
    Upload columns from an architecture Google Sheets URL to a table.

    Uses the /upload_columns/ REST endpoint. Requires a valid CSRF token.

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
def db_update_column(
    column_id: str,
    column_name: str,
    table_id: str,
    observation_level_id: str | None = None,
    is_partition: bool = False,
    is_primary_key: bool = False,
    description_pt: str = "",
    description_en: str = "",
    description_es: str = "",
    measurement_unit: str = "",
    has_sensitive_data: bool = False,
    covered_by_dictionary: bool = False,
    directory_column_name: str = "",
    temporal_coverage: str = "",
    env: str = "dev",
) -> dict:
    """
    Update a single column record.

    Args:
        column_id: bare column ID
        column_name: column name (required by CreateUpdateColumn)
        table_id: bare table ID
        observation_level_id: bare OL ID to link (optional)
        is_partition: whether this column is a BQ partition key
        is_primary_key: whether this is a primary key column
        description_pt/en/es: descriptions in each language
        measurement_unit: unit string
        has_sensitive_data: sensitive data flag
        covered_by_dictionary: whether covered by the dataset dictionary
        directory_column_name: BD directories FK (e.g. "br_bd_diretorios_brasil.municipio:id_municipio")
        temporal_coverage: per-column temporal coverage if different from table
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
    if observation_level_id:
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
        fields["hasSensitiveData"] = has_sensitive_data
    if covered_by_dictionary:
        fields["coveredByDictionary"] = covered_by_dictionary
    if directory_column_name:
        fields["directoryColumn"] = directory_column_name
    if temporal_coverage:
        fields["temporalCoverage"] = temporal_coverage

    payload = _mut("CreateUpdateColumn", fields, "column { id name }", env=env)
    col = payload["column"]
    return {"id": _strip_id(col["id"]), "name": col["name"]}


@mcp.tool()
def db_create_update_observation_level(
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
def db_create_update_cloud_table(
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
def db_create_update_coverage(
    table_id: str,
    area_id: str,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a coverage record on a table.

    Args:
        table_id: bare table ID
        area_id: bare area ID (e.g. the ID for area slug "br")
        id: bare coverage ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    fields: dict[str, Any] = {"table": table_id, "area": area_id}
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
def db_create_update_datetime_range(
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
def db_create_update_update(
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
def db_get_raw_data_sources(dataset_slug: str, env: str = "dev") -> list[dict]:
    """
    Return raw data sources associated with a dataset.

    Args:
        dataset_slug: e.g. "siconfi"
        env: "dev" or "prod"

    Returns: [{"id": str, "name": str, "url": str}]
    """
    data = _gql(
        """
        { allRawdatasource(first: 200) {
            edges { node { id name url dataset { slug } } }
        } }
        """,
        env=env,
    )
    results = []
    for e in data["allRawdatasource"]["edges"]:
        n = e["node"]
        ds_slug = (n.get("dataset") or {}).get("slug", "")
        if ds_slug.lower() == dataset_slug.lower() or dataset_slug.lower() in (n.get("name") or "").lower():
            results.append({
                "id": _strip_id(n["id"]),
                "name": n.get("name", ""),
                "url": n.get("url", ""),
            })
    return results


@mcp.tool()
def db_get_authenticated_account(env: str = "dev") -> dict:
    """
    Return the ID and email of the currently authenticated account.

    Returns: {"id": str, "email": str}
    """
    email, _ = _get_credentials(env)
    data = _gql(
        '{ allAccount(first: 500) { edges { node { id email } } } }',
        env=env,
    )
    for e in data["allAccount"]["edges"]:
        n = e["node"]
        if n.get("email", "").lower() == email.lower():
            return {"id": _strip_id(n["id"]), "email": n["email"]}
    raise RuntimeError(f"Account not found for email: {email}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    mcp.run()
