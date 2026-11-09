import json
import os

import requests

from ._app import URLS
from .auth import _get_token

# ---------------------------------------------------------------------------
# GraphQL helpers
# ---------------------------------------------------------------------------


def _gql(
    query: str,
    variables: dict | None = None,
    env: str | None = None,
    auth: bool = True,
) -> dict:
    env = env or os.environ.get("ENV", "dev")
    if env not in URLS:
        raise ValueError(
            f"env must be 'local', 'dev', 'staging', or 'prod', got: {env!r}"
        )
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


def resolve_directory_column(
    directory_column_str: str, env: str
) -> tuple[str | None, str | None]:
    """
    Resolve an architecture-sheet directory_column string to a backend Column id.

    Format: "<dataset_slug>.<table_slug>:<column_name>", e.g.
    "br_bd_diretorios_data_tempo.ano:ano".

    Returns `(column_id, error)`. Exactly one is non-None. The error is a
    caller-facing sentence explaining *why* the target was rejected, because the
    backend's own message for an ineligible target is the useless
    "Faça uma escolha válida. Sua escolha não é uma das disponíveis."

    `Column.directory_primary_key` carries
    `limit_choices_to={"is_primary_key": True, "table__is_directory": True}`, so
    a target is eligible only when it is flagged as a primary key of a table
    flagged as a directory. That is deliberate: the FK must identify one row.
    `br_bd_diretorios_brasil.empresa.cnpj_basico`, for instance, is not eligible
    and must not be made so — `empresa` has 72.8M rows with `cnpj` unique but
    only 69.5M distinct `cnpj_basico`, so the FK would be ambiguous. Enforce the
    integrity of such a column with a dbt `relationships` test instead.
    """
    if (
        not directory_column_str
        or "." not in directory_column_str
        or ":" not in directory_column_str
    ):
        return None, (
            f"directory_column {directory_column_str!r} is malformed; expected "
            '"<dataset_slug>.<table_slug>:<column_name>", e.g. '
            '"br_bd_diretorios_data_tempo.ano:ano".'
        )
    dot_pos = directory_column_str.rfind(".")
    colon_pos = directory_column_str.find(":", dot_pos)
    if colon_pos == -1:
        return None, (
            f"directory_column {directory_column_str!r} is malformed; the "
            "column name must follow a colon."
        )
    dataset_slug = directory_column_str[:dot_pos]
    table_slug = directory_column_str[dot_pos + 1 : colon_pos]
    column_name = directory_column_str[colon_pos + 1 :]

    gql = """
    query($slug: String!) {
        allDataset(slug: $slug) {
            edges { node {
                tables(first: 100) { edges { node {
                    slug
                    isDirectory
                    columns(first: 500) { edges { node { id name isPrimaryKey } } }
                } } }
            } }
        }
    }
    """

    # What the search found, for a precise message when nothing is eligible.
    seen_dataset = False
    seen_table = False
    seen_column: dict | None = None
    table_is_directory = False
    eligible_keys: list[str] = []

    def _search(slug: str) -> str | None:
        nonlocal \
            seen_dataset, \
            seen_table, \
            seen_column, \
            table_is_directory, \
            eligible_keys
        data = _gql(gql, {"slug": slug}, env=env)
        edges = data["allDataset"]["edges"]
        if not edges:
            return None
        seen_dataset = True
        for te in edges[0]["node"]["tables"]["edges"]:
            t = te["node"]
            if t["slug"] != table_slug:
                continue
            seen_table = True
            table_is_directory = bool(t.get("isDirectory"))
            eligible_keys = [
                c["node"]["name"]
                for c in t["columns"]["edges"]
                if c["node"].get("isPrimaryKey")
            ]
            for ce in t["columns"]["edges"]:
                col = ce["node"]
                if col["name"] == column_name:
                    seen_column = col
                    if col.get("isPrimaryKey") and table_is_directory:
                        return _strip_id(col["id"])
        return None

    # Try the slug as written in the architecture sheet first, then without the
    # BD prefixes that the sheets carry but the backend slugs do not.
    candidates = [dataset_slug]
    for prefix in ("br_bd_", "br_"):
        if dataset_slug.startswith(prefix):
            candidates.append(dataset_slug[len(prefix) :])
    for slug in candidates:
        result = _search(slug)
        if result:
            return result, None

    if not seen_dataset:
        return None, (
            f"directory dataset {dataset_slug!r} not found in env {env!r} "
            f"(tried {', '.join(repr(c) for c in candidates)})."
        )
    if not seen_table:
        return (
            None,
            f"table {table_slug!r} not found in directory dataset {dataset_slug!r}.",
        )
    if seen_column is None:
        return None, (
            f"column {column_name!r} not found in {dataset_slug}.{table_slug}."
        )
    if not table_is_directory:
        return None, (
            f"{dataset_slug}.{table_slug} is not flagged as a directory table, so "
            f"{column_name!r} cannot be a directory FK target."
        )
    return None, (
        f"{dataset_slug}.{table_slug}:{column_name} is not flagged as a primary key "
        f"of that directory, so the backend rejects it as a directory FK target. "
        f"Eligible keys there: {', '.join(eligible_keys) or '(none)'}. "
        f"If {column_name!r} is not unique in the directory it should stay "
        f"ineligible — enforce the relationship with a dbt `relationships` test."
    )


def _fetch_all(
    token_env: str, query_name: str, fields: str, auth: bool = True
) -> list[dict]:
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
