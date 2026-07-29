import json
from typing import Any

import requests

from .._app import mcp
from ..auth import _get_token
from ..gql import _gql, _mut, _strip_id, _lookup_directory_column
from .metadata import discover_ids


# ---------------------------------------------------------------------------
# MCP tools — write (create/update/delete)
# ---------------------------------------------------------------------------


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
      directory_column, measurement_unit, has_sensitive_data, observations

    NOTE: the sheet's bare `description` column is written to descriptionPt.
    For an English-source sheet that leaves descriptionEn NULL — use
    bulk_upsert_columns, which understands description_pt/en/es, instead.

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

        obs = row.get("observations", "").strip()
        if obs:
            fields["observations"] = obs

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
           "observations": "Top-coded at 90 from 2011 on.",
           "bigquery_type": "INT64"}]'

    `observations` is the column's free-text notes field (source quirks,
    caveats) — a single field, not per-language.

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

        obs = _get(row, "observations")
        if obs:
            fields["observations"] = obs
            set_fields.append("observations")

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
    observations: str = "",
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
        observations: free-text notes on the column (the architecture sheet's
            `observations` column) — caveats, source quirks, coverage notes.
            Single field, not per-language. Empty leaves the stored value alone.
        env: "dev" or "prod"

    Returns: {"id": str, "name": str}

    WARNING: the boolean args default to False, so a partial call CLOBBERS
    is_partition / is_primary_key. Prefer bulk_upsert_columns for patches — it
    writes only the fields you pass.
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
    if observations:
        fields["observations"] = observations
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
    start_month: int | None = None,
    end_month: int | None = None,
    start_day: int | None = None,
    end_day: int | None = None,
    interval: int = 1,
    is_closed: bool = False,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update a datetime range on a coverage.

    Match the range's granularity to the table's: a monthly table needs
    start_month/end_month, a daily table also needs start_day/end_day. Giving a
    month-granular table a year-only range (e.g. 1913..2026 for data that really
    spans 1913-01..2026-06) understates the coverage and renders wrong on the
    site. Year-only is correct only for genuinely annual tables.

    A day requires a month, and a month requires a year — on each side
    independently.

    Args:
        coverage_id: bare coverage ID
        start_year: e.g. 2013
        end_year: e.g. 2025
        start_month: 1-12; required for monthly/daily tables
        end_month: 1-12; required for monthly/daily tables
        start_day: 1-31; required for daily tables
        end_day: 1-31; required for daily tables
        interval: years between observations (1 = annual)
        is_closed: True if the series has ended
        id: bare datetime range ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    for side, month, day in (
        ("start", start_month, start_day),
        ("end", end_month, end_day),
    ):
        if day is not None and month is None:
            raise ValueError(f"{side}_day requires {side}_month")

    fields: dict[str, Any] = {
        "coverage": coverage_id,
        "startYear": start_year,
        "endYear": end_year,
        "interval": interval,
        "isClosed": is_closed,
    }
    for key, val in (
        ("startMonth", start_month),
        ("endMonth", end_month),
        ("startDay", start_day),
        ("endDay", end_day),
    ):
        if val is not None:
            fields[key] = val
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
    entity_id: str,
    frequency: int,
    latest: str,
    table_id: str | None = None,
    raw_data_source_id: str | None = None,
    lag: int | None = None,
    id: str | None = None,
    env: str = "dev",
) -> dict:
    """
    Create or update an update record (publishing cadence).

    An Update hangs off EITHER a table or a raw data source, and the two mean
    different things — a recurring dataset needs both:

    - `table_id`: when WE last refreshed the table, and how often we do.
      `latest` is a wall-clock timestamp of the last materialization.
    - `raw_data_source_id`: what the SOURCE has published, and how often it
      publishes. `latest` is the source's max coverage date (e.g.
      "2026-06-01T00:00:00" for June data), NOT the time you looked.

    Pass exactly one of the two.

    Args:
        entity_id: bare entity ID for the frequency unit ("month", "year", …)
        frequency: how many units between updates (e.g. 1 for monthly/annual)
        latest: ISO datetime string; see the two meanings above
        table_id: bare table ID — anchors the Update to a table
        raw_data_source_id: bare raw data source ID — anchors it to a source
        lag: expected lag in the same units (e.g. 1 = data for month M lands in
            M+1). Omit when unknown; source-anchored Updates usually leave it
            unset.
        id: bare update ID if updating
        env: "dev" or "prod"

    Returns: {"id": str}
    """
    if (table_id is None) == (raw_data_source_id is None):
        raise ValueError(
            "pass exactly one of table_id or raw_data_source_id"
        )

    fields: dict[str, Any] = {
        "entity": entity_id,
        "frequency": frequency,
        "latest": latest,
    }
    if table_id:
        fields["table"] = table_id
    if raw_data_source_id:
        fields["rawDataSource"] = raw_data_source_id
    if lag is not None:
        fields["lag"] = lag
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
