import json
import os
import time
from pathlib import Path

import requests

from .._app import mcp
from ..auth import _cache, _IDS_TTL, _get_credentials, _get_token
from ..gql import _gql, _fetch_all, _strip_id


# ---------------------------------------------------------------------------
# MCP tools — metadata (read-only)
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
                                columns(first: 200) { edges { node { id name isPartition } } }
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
                {
                    "id": _strip_id(c["node"]["id"]),
                    "name": c["node"]["name"],
                    "is_partition": c["node"].get("isPartition") or False,
                }
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
