import json
import os
import re
from pathlib import Path
from typing import Any

from .._app import mcp
from ..gql import _gql, _strip_id
from .write import update_column


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
# Partition / cluster metadata audit
# ---------------------------------------------------------------------------

def _parse_sql_partitions(pipelines_path: str) -> dict[str, list[str]]:
    """
    Walk dbt SQL models and extract partition + cluster columns per table.

    Returns a dict mapping '{gcp_dataset_id}.{gcp_table_id}' to a sorted list
    of column names that should have isPartition=True (partition field union
    cluster columns).
    """
    models_dir = os.path.join(pipelines_path, "models")
    result: dict[str, list[str]] = {}

    for root, _dirs, files in os.walk(models_dir):
        for fname in files:
            if not fname.endswith(".sql"):
                continue

            fpath = os.path.join(root, fname)
            try:
                content = Path(fpath).read_text(encoding="utf-8", errors="ignore")
            except OSError:
                continue

            # Extract the config(...) block (may span multiple lines)
            config_match = re.search(r"config\s*\((.+?)\)\s*\}", content, re.DOTALL)
            if not config_match:
                config_match = re.search(r"config\s*\((.+?)\)", content, re.DOTALL)
            if not config_match:
                continue
            cfg = config_match.group(0)

            # schema and alias — prefer explicit config values, fall back to filename
            schema_m = re.search(r'schema\s*=\s*"([^"]+)"', cfg)
            alias_m = re.search(r'alias\s*=\s*"([^"]+)"', cfg)
            if schema_m and alias_m:
                schema = schema_m.group(1)
                alias = alias_m.group(1)
            else:
                name = fname[:-4]
                if "__" not in name:
                    continue
                schema, alias = name.split("__", 1)

            partition_cols: set[str] = set()

            # partition_by={"field": "col_name", ...}
            pf_m = re.search(r'"field"\s*:\s*"([^"]+)"', cfg)
            if pf_m:
                partition_cols.add(pf_m.group(1))

            # cluster_by="col" or cluster_by=["col1", "col2"]
            cb_m = re.search(r'cluster_by\s*=\s*(\[.*?\]|"[^"]*")', cfg, re.DOTALL)
            if cb_m:
                val = cb_m.group(1).strip()
                if val.startswith("["):
                    partition_cols.update(re.findall(r'"([^"]+)"', val))
                else:
                    partition_cols.add(val.strip('"'))

            if partition_cols:
                result[f"{schema}.{alias}"] = sorted(partition_cols)

    return result


def _get_tables_for_audit(gcp_dataset_id: str, env: str) -> dict:
    """Fetch tables for a GCP dataset ID via cloudTables — no slug dependency."""
    q = """
    query($gcpDatasetId: String!) {
        allTable(cloudTables_GcpDatasetId: $gcpDatasetId, first: 200) {
            edges { node {
                id slug
                columns(first: 200) { edges { node { id name isPartition } } }
                cloudTables(first: 1) { edges { node { gcpDatasetId gcpTableId } } }
            } }
        }
    }
    """
    data = _gql(q, {"gcpDatasetId": gcp_dataset_id}, env=env, auth=False)
    edges = data["allTable"]["edges"]
    if not edges:
        return {}
    tables = {}
    for te in edges:
        t = te["node"]
        ct_edges = t["cloudTables"]["edges"]
        if not ct_edges:
            continue
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
            "cloud_tables": [
                {
                    "gcp_dataset_id": ct["node"].get("gcpDatasetId"),
                    "gcp_table_id": ct["node"].get("gcpTableId"),
                }
                for ct in ct_edges
            ],
        }
    return tables


def _get_all_tables_for_audit(env: str) -> list[tuple[str, str, dict]]:
    """Fetch ALL tables that have cloudTables entries — used for full audit scan."""
    q = """
    query($after: String) {
        allTable(cloudTables_GcpDatasetId_Isnull: false, first: 100, after: $after) {
            pageInfo { hasNextPage endCursor }
            edges { node {
                id slug
                columns(first: 200) { edges { node { id name isPartition } } }
                cloudTables(first: 1) { edges { node { gcpDatasetId gcpTableId } } }
            } }
        }
    }
    """
    results: list[tuple[str, str, dict]] = []
    after = None
    while True:
        data = _gql(q, {"after": after}, env=env, auth=False)
        page = data["allTable"]
        for te in page["edges"]:
            t = te["node"]
            ct_edges = t["cloudTables"]["edges"]
            if not ct_edges:
                continue
            gcp_ds_id = ct_edges[0]["node"].get("gcpDatasetId")
            if not gcp_ds_id:
                continue
            tdata = {
                "id": _strip_id(t["id"]),
                "columns": [
                    {
                        "id": _strip_id(c["node"]["id"]),
                        "name": c["node"]["name"],
                        "is_partition": c["node"].get("isPartition") or False,
                    }
                    for c in t["columns"]["edges"]
                ],
                "cloud_tables": [
                    {
                        "gcp_dataset_id": ct["node"].get("gcpDatasetId"),
                        "gcp_table_id": ct["node"].get("gcpTableId"),
                    }
                    for ct in ct_edges
                ],
            }
            results.append((gcp_ds_id, t["slug"], tdata))
        if not page["pageInfo"]["hasNextPage"]:
            break
        after = page["pageInfo"]["endCursor"]
    return results


def _write_partition_report(
    report_path: str,
    changes: list[dict],
    stats: dict,
    fix: bool,
    env: str,
    bq_validation: dict | None = None,
) -> None:
    """Write a markdown report of partition audit changes grouped by dataset/table."""
    from datetime import date

    lines: list[str] = []
    mode = "Aplicadas" if fix else "Encontradas (dry-run)"
    lines.append(f"# Audit de isPartition — {date.today()} ({env})")
    lines.append(f"\n**Modo:** {mode}  ")
    lines.append(
        f"**Tabelas verificadas:** {stats['tables_checked']}  \n"
        f"**Colunas verificadas:** {stats['columns_checked']}  \n"
        f"**Alterações:** {stats['fixed'] if fix else len(changes)}  \n"
        f"**Erros:** {stats['errors']}"
    )

    # Group by dataset → table
    grouped: dict[str, dict[str, list[dict]]] = {}
    for c in changes:
        grouped.setdefault(c["dataset"], {}).setdefault(c["table"], []).append(c)

    for ds_slug, tables in sorted(grouped.items()):
        lines.append(f"\n## {ds_slug}")
        for tslug, cols in sorted(tables.items()):
            gcp_key = cols[0].get("gcp_key", "")
            header = f"### {tslug}"
            if gcp_key:
                header += f" (`{gcp_key}`)"
            lines.append(f"\n{header}")
            lines.append("\n| Coluna | Antes | Depois | Motivo |")
            lines.append("|--------|-------|--------|--------|")
            for c in cols:
                before = "True" if c["before"] else "False"
                after  = "True" if c["after"] else "False"
                error  = f" ⚠️ {c['error']}" if c.get("error") else ""
                lines.append(f"| {c['column_name']} | {before} | {after} | {c['reason']}{error} |")

    if bq_validation:
        lines.append("\n---\n")
        lines.append("# Validação BigQuery (4 fontes: SQL · BQ dev · BQ prod · API)")

        sql_dev  = bq_validation.get("sql_dev_mismatches", [])
        sql_prod = bq_validation.get("sql_prod_mismatches", [])
        dev_prod = bq_validation.get("dev_prod_mismatches", [])
        bq_api   = bq_validation.get("bq_api_mismatches", [])
        dev_inacc  = bq_validation.get("bq_dev_inaccessible", [])
        prod_inacc = bq_validation.get("bq_prod_inaccessible", [])

        ok_tables = bq_validation.get("tables_ok", 0)
        lines.append(
            f"\n**Tabelas OK (4 fontes alinhadas):** {ok_tables}  \n"
            f"**Divergências SQL ↔ BQ dev:** {len(sql_dev)}  \n"
            f"**Divergências SQL ↔ BQ prod:** {len(sql_prod)}  \n"
            f"**Divergências BQ dev ↔ BQ prod:** {len(dev_prod)}  \n"
            f"**Divergências BQ prod ↔ API:** {len(bq_api)}  \n"
            f"**Datasets inacessíveis BQ dev:** {len(dev_inacc)}  \n"
            f"**Datasets inacessíveis BQ prod:** {len(prod_inacc)}"
        )

        if dev_inacc:
            lines.append("\n## Datasets inacessíveis no BQ dev")
            for ds in sorted(dev_inacc):
                lines.append(f"- `{ds}`")

        if prod_inacc:
            lines.append("\n## Datasets inacessíveis no BQ prod")
            for ds in sorted(prod_inacc):
                lines.append(f"- `{ds}`")

        if sql_dev:
            lines.append("\n## Divergências SQL ↔ BQ dev")
            lines.append("| Dataset.Tabela | Coluna | No SQL | No BQ dev | Nota |")
            lines.append("|---|---|:---:|:---:|---|")
            for m in sorted(sql_dev, key=lambda x: (x["gcp_key"], x["column"])):
                lines.append(
                    f"| `{m['gcp_key']}` | {m['column']} "
                    f"| {'✓' if m['in_sql'] else '✗'} "
                    f"| {'✓' if m['in_bq_dev'] else '✗'} "
                    f"| {m['note']} |"
                )

        if sql_prod:
            lines.append("\n## Divergências SQL ↔ BQ prod")
            lines.append("| Dataset.Tabela | Coluna | No SQL | No BQ prod | Nota |")
            lines.append("|---|---|:---:|:---:|---|")
            for m in sorted(sql_prod, key=lambda x: (x["gcp_key"], x["column"])):
                lines.append(
                    f"| `{m['gcp_key']}` | {m['column']} "
                    f"| {'✓' if m['in_sql'] else '✗'} "
                    f"| {'✓' if m['in_bq_prod'] else '✗'} "
                    f"| {m['note']} |"
                )

        if dev_prod:
            lines.append("\n## Divergências BQ dev ↔ BQ prod")
            lines.append("| Dataset.Tabela | Coluna | No BQ dev | No BQ prod | Nota |")
            lines.append("|---|---|:---:|:---:|---|")
            for m in sorted(dev_prod, key=lambda x: (x["gcp_key"], x["column"])):
                lines.append(
                    f"| `{m['gcp_key']}` | {m['column']} "
                    f"| {'✓' if m['in_bq_dev'] else '✗'} "
                    f"| {'✓' if m['in_bq_prod'] else '✗'} "
                    f"| {m['note']} |"
                )

        if bq_api:
            lines.append("\n## Divergências BQ prod ↔ API")
            lines.append("| Dataset.Tabela | Coluna | No BQ prod | Na API | Nota |")
            lines.append("|---|---|:---:|:---:|---|")
            for m in sorted(bq_api, key=lambda x: (x["gcp_key"], x["column"])):
                lines.append(
                    f"| `{m['gcp_key']}` | {m['column']} "
                    f"| {'✓' if m['in_bq_prod'] else '✗'} "
                    f"| {'✓' if m['in_api'] else '✗'} "
                    f"| {m['note']} |"
                )

    Path(report_path).write_text("\n".join(lines) + "\n", encoding="utf-8")


def _bq_get_partition_map(
    gcp_dataset_ids: list[str],
    billing_project: str | None,
    data_project: str = "basedosdados",
) -> tuple[dict[str, dict], list[str]]:
    """
    Query INFORMATION_SCHEMA.COLUMNS for each gcp_dataset_id.

    Uses billing_project for job billing and data_project for INFORMATION_SCHEMA queries.

    Returns:
        bq_map: {"{dataset}.{table}": {"partition_col": str|None, "cluster_cols": set[str], "all_cols": set[str]}}
        inaccessible: list of dataset IDs that couldn't be queried (403 or other errors)
    """
    client = _get_bq_client(billing_project)
    bq_map: dict[str, dict] = {}
    inaccessible: list[str] = []

    for ds_id in gcp_dataset_ids:
        sql = (
            f"SELECT table_name,"
            f" MAX(IF(is_partitioning_column='YES',column_name,NULL)) AS partition_col,"
            f" STRING_AGG(IF(clustering_ordinal_position IS NOT NULL,column_name,NULL),"
            f" ', ' ORDER BY clustering_ordinal_position) AS cluster_cols"
            f" FROM `{data_project}`.`{ds_id}`.INFORMATION_SCHEMA.COLUMNS"
            f" GROUP BY table_name"
        )
        try:
            rows = list(client.query(sql).result())
            for row in rows:
                key = f"{ds_id}.{row['table_name']}"
                p = row["partition_col"]
                c_str = row["cluster_cols"] or ""
                c_set = {c.strip() for c in c_str.split(",") if c.strip()} if c_str else set()
                bq_map[key] = {
                    "partition_col": p,
                    "cluster_cols": c_set,
                    "all_cols": ({p} if p else set()) | c_set,
                }
        except Exception:
            inaccessible.append(ds_id)

    return bq_map, inaccessible


@mcp.tool()
def audit_partition_metadata(
    pipelines_path: str,
    gcp_dataset_id: str | list[str] | None = None,
    fix: bool = False,
    env: str = "prod",
    report_path: str | None = None,
    validate_bq: bool = False,
    billing_project: str | None = None,
) -> dict:
    """
    Audit (and optionally fix) isPartition flags on columns by comparing the
    API against dbt SQL models in the pipelines repository.

    Columns listed as partition_by.field or cluster_by in a model's config()
    are all treated as isPartition=True.

    Steps:
      1. Parse SQL models in pipelines_path to build a partition map.
      2. Fetch tables from the API via cloudTables.gcpDatasetId (no slug dependency).
      3. Match tables via gcpDatasetId + gcpTableId.
      4. Report (and fix when fix=True) two types of discrepancies:
         - false_positives: API isPartition=True but not in SQL → set False
         - missing: SQL says partition/cluster but API isPartition=False → set True
      5. If validate_bq=True, also query BigQuery INFORMATION_SCHEMA.COLUMNS for BOTH
         dev (basedosdados-dev) and prod (basedosdados) and perform a 4-way comparison
         (SQL · BQ dev · BQ prod · API), adding the results to the report.
      6. If report_path is given, write a single markdown report for all datasets.

    Args:
        pipelines_path: local path to the basedosdados/pipelines repo
        gcp_dataset_id: GCP dataset ID (e.g. "br_denatran_frota"), a list, or None for all
        fix: when True, apply corrections via update_column (requires auth)
        env: "dev" or "prod"
        report_path: optional path to write a markdown report (e.g. "/tmp/audit.md")
        validate_bq: when True, query BigQuery INFORMATION_SCHEMA for dev and prod and do a 4-way comparison
        billing_project: GCP billing project for BigQuery (required when validate_bq=True)

    Returns:
        {
          "false_positives": [{"dataset", "table", "column_id", "column_name"}],
          "missing":         [{"dataset", "table", "column_id", "column_name"}],
          "no_sql_file":     ["gcp_dataset.gcp_table", ...],
          "stats": {"tables_checked", "columns_checked", "fixed", "errors"},
          "report_path": str | None,
          "bq_validation": {   # only when validate_bq=True
            "sql_dev_mismatches":  [{"gcp_key", "column", "in_sql", "in_bq_dev", "note"}],
            "sql_prod_mismatches": [{"gcp_key", "column", "in_sql", "in_bq_prod", "note"}],
            "dev_prod_mismatches": [{"gcp_key", "column", "in_bq_dev", "in_bq_prod", "note"}],
            "bq_api_mismatches":   [{"gcp_key", "column", "in_bq_prod", "in_api", "note"}],
            "bq_dev_inaccessible":  ["gcp_dataset_id", ...],
            "bq_prod_inaccessible": ["gcp_dataset_id", ...],
            "tables_ok": int,
          } | None
        }
    """
    sql_map = _parse_sql_partitions(pipelines_path)

    # Build flat list of (ds_id, tslug, tdata) tuples to iterate
    full_scan = gcp_dataset_id is None
    if full_scan:
        # Query ALL tables with cloudTables in one paginated call — avoids slug mismatch
        table_triples = _get_all_tables_for_audit(env=env)
    else:
        gcp_dataset_ids = (
            gcp_dataset_id if isinstance(gcp_dataset_id, list) else [gcp_dataset_id]
        )
        table_triples = []
        for ds_id in gcp_dataset_ids:
            for tslug, tdata in _get_tables_for_audit(ds_id, env=env).items():
                table_triples.append((ds_id, tslug, tdata))

    false_positives: list[dict] = []
    missing: list[dict] = []
    no_sql_file: list[str] = []
    all_changes: list[dict] = []
    # Track table data for BQ validation
    _checked_tables: list[dict] = []
    tables_checked = 0
    columns_checked = 0
    fixed = 0
    errors = 0

    for ds_id, tslug, tdata in table_triples:
        cloud_tables = tdata.get("cloud_tables", [])
        if not cloud_tables:
            continue

        ct = cloud_tables[0]
        gcp_key = f"{ct['gcp_dataset_id']}.{ct['gcp_table_id']}"

        if gcp_key not in sql_map:
            no_sql_file.append(gcp_key)
            continue

        tables_checked += 1
        partition_cols = set(sql_map[gcp_key])

        if validate_bq:
            _checked_tables.append({
                "gcp_key": gcp_key,
                "gcp_dataset_id": ct["gcp_dataset_id"],
                "api_cols": {col["name"]: col["is_partition"] for col in tdata["columns"]},
                "sql_cols": partition_cols,
            })

        for col in tdata["columns"]:
            columns_checked += 1
            col_in_sql = col["name"] in partition_cols
            col_in_api = col["is_partition"]

            if col_in_api and not col_in_sql:
                entry = {
                    "dataset": ds_id,
                    "table": tslug,
                    "column_id": col["id"],
                    "column_name": col["name"],
                }
                false_positives.append(entry)
                change = {
                    "dataset": ds_id, "table": tslug, "gcp_key": gcp_key,
                    "column_id": col["id"], "column_name": col["name"],
                    "before": True, "after": False, "reason": "não está no SQL",
                }
                if fix:
                    try:
                        update_column(
                            column_id=col["id"],
                            column_name=col["name"],
                            table_id=tdata["id"],
                            is_partition=False,
                            env=env,
                        )
                        fixed += 1
                    except Exception as exc:
                        change["error"] = str(exc)
                        entry["error"] = str(exc)
                        errors += 1
                all_changes.append(change)

            elif not col_in_api and col_in_sql:
                entry = {
                    "dataset": ds_id,
                    "table": tslug,
                    "column_id": col["id"],
                    "column_name": col["name"],
                }
                missing.append(entry)
                change = {
                    "dataset": ds_id, "table": tslug, "gcp_key": gcp_key,
                    "column_id": col["id"], "column_name": col["name"],
                    "before": False, "after": True, "reason": "partition/cluster no SQL",
                }
                if fix:
                    try:
                        update_column(
                            column_id=col["id"],
                            column_name=col["name"],
                            table_id=tdata["id"],
                            is_partition=True,
                            env=env,
                        )
                        fixed += 1
                    except Exception as exc:
                        change["error"] = str(exc)
                        entry["error"] = str(exc)
                        errors += 1
                all_changes.append(change)

    stats = {
        "tables_checked": tables_checked,
        "columns_checked": columns_checked,
        "fixed": fixed,
        "errors": errors,
    }

    # BQ 4-way validation (SQL · BQ dev · BQ prod · API)
    bq_validation: dict | None = None
    if validate_bq and _checked_tables:
        unique_ds = list({t["gcp_dataset_id"] for t in _checked_tables})
        bq_dev_map, dev_inaccessible = _bq_get_partition_map(
            unique_ds, billing_project, data_project="basedosdados-dev"
        )
        bq_prod_map, prod_inaccessible = _bq_get_partition_map(
            unique_ds, billing_project, data_project="basedosdados"
        )

        sql_dev_mismatches: list[dict] = []
        sql_prod_mismatches: list[dict] = []
        dev_prod_mismatches: list[dict] = []
        bq_api_mismatches: list[dict] = []
        tables_ok = 0

        for tinfo in _checked_tables:
            gcp_key = tinfo["gcp_key"]
            sql_cols = tinfo["sql_cols"]
            api_cols = tinfo["api_cols"]
            dev_cols = bq_dev_map.get(gcp_key, {}).get("all_cols", set())
            prod_cols = bq_prod_map.get(gcp_key, {}).get("all_cols", set())

            table_ok = True

            # SQL ↔ BQ dev
            for col in sql_cols - dev_cols:
                sql_dev_mismatches.append({
                    "gcp_key": gcp_key, "column": col,
                    "in_sql": True, "in_bq_dev": False,
                    "note": "SQL define mas BQ dev não tem",
                })
                table_ok = False
            for col in dev_cols - sql_cols:
                sql_dev_mismatches.append({
                    "gcp_key": gcp_key, "column": col,
                    "in_sql": False, "in_bq_dev": True,
                    "note": "BQ dev tem mas SQL não define",
                })
                table_ok = False

            # SQL ↔ BQ prod
            for col in sql_cols - prod_cols:
                sql_prod_mismatches.append({
                    "gcp_key": gcp_key, "column": col,
                    "in_sql": True, "in_bq_prod": False,
                    "note": "SQL define mas BQ prod não tem",
                })
                table_ok = False
            for col in prod_cols - sql_cols:
                sql_prod_mismatches.append({
                    "gcp_key": gcp_key, "column": col,
                    "in_sql": False, "in_bq_prod": True,
                    "note": "BQ prod tem mas SQL não define",
                })
                table_ok = False

            # BQ dev ↔ BQ prod
            for col in dev_cols - prod_cols:
                dev_prod_mismatches.append({
                    "gcp_key": gcp_key, "column": col,
                    "in_bq_dev": True, "in_bq_prod": False,
                    "note": "BQ dev tem mas BQ prod não tem",
                })
                table_ok = False
            for col in prod_cols - dev_cols:
                dev_prod_mismatches.append({
                    "gcp_key": gcp_key, "column": col,
                    "in_bq_dev": False, "in_bq_prod": True,
                    "note": "BQ prod tem mas BQ dev não tem",
                })
                table_ok = False

            # BQ prod ↔ API
            for col_name, is_partition_api in api_cols.items():
                in_bq_prod = col_name in prod_cols
                if is_partition_api != in_bq_prod:
                    bq_api_mismatches.append({
                        "gcp_key": gcp_key, "column": col_name,
                        "in_bq_prod": in_bq_prod, "in_api": is_partition_api,
                        "note": (
                            "BQ prod tem partition/cluster mas API diz False"
                            if in_bq_prod else
                            "API diz True mas BQ prod não tem partition/cluster"
                        ),
                    })
                    table_ok = False

            if table_ok:
                tables_ok += 1

        bq_validation = {
            "sql_dev_mismatches": sql_dev_mismatches,
            "sql_prod_mismatches": sql_prod_mismatches,
            "dev_prod_mismatches": dev_prod_mismatches,
            "bq_api_mismatches": bq_api_mismatches,
            "bq_dev_inaccessible": dev_inaccessible,
            "bq_prod_inaccessible": prod_inaccessible,
            "tables_ok": tables_ok,
        }

    if report_path and (all_changes or bq_validation):
        _write_partition_report(report_path, all_changes, stats, fix, env, bq_validation)

    return {
        "false_positives": false_positives,
        "missing": missing,
        "no_sql_file": no_sql_file,
        "stats": stats,
        "report_path": report_path if (report_path and (all_changes or bq_validation)) else None,
        "bq_validation": bq_validation,
    }
