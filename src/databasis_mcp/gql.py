import json
import os

import requests

from ._app import URLS
from .auth import _get_token


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
