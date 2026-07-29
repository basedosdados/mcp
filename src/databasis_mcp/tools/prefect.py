import json
from pathlib import Path
from typing import Any

import requests

from .._app import mcp


# ---------------------------------------------------------------------------
# Prefect helpers
# ---------------------------------------------------------------------------

# Prefect 3 exposes a REST API (no GraphQL). Filter endpoints take a JSON body
# and return a list; flow runs carry only `flow_id`, so flow names are resolved
# separately.
PREFECT_URL = "https://prefect3.basedosdados.org/api"

_LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARNING": 30, "ERROR": 40, "CRITICAL": 50}


def _prefect_key() -> str:
    """Backend Token authorized for the prefect3 domain.

    Despite the name, this is not a Prefect key: prefect3.basedosdados.org sits
    behind an nginx ingress whose auth-url is backend.basedosdados.org/auth/,
    and that view resolves a Domain from the request and requires the token to
    be scoped to it. So the value is a backend Token (uuid4), and a token issued
    for prefect.basedosdados.org — the Prefect 2 host, stored under the older
    "prefect" key — is rejected here with a redirect to the login page.
    """
    bd_dir = Path.home() / ".basedosdados"
    for fname in ("backend_credentials.json", "credentials.json"):
        creds_path = bd_dir / fname
        if creds_path.exists():
            data = json.loads(creds_path.read_text())
            key = data.get("prod", {}).get("prefect3")
            if key:
                return key
    raise RuntimeError(
        "No prefect3 token found. Add a 'prefect3' key under 'prod' in "
        "~/.basedosdados/credentials.json — a backend Token issued for the "
        "prefect3.basedosdados.org domain (Django admin > Account Auth > "
        "Tokens). The older 'prefect' key is scoped to the Prefect 2 host and "
        "will not authenticate here."
    )


def _prefect_post(path: str, body: dict) -> Any:
    """POST to a Prefect 3 REST endpoint (e.g. '/flow_runs/filter')."""
    r = requests.post(
        f"{PREFECT_URL}{path}",
        json=body,
        headers={"Authorization": f"Bearer {_prefect_key()}"},
        timeout=60,
    )
    if not r.ok:
        raise RuntimeError(f"HTTP {r.status_code}:\n{r.text}")
    return r.json()


_PREFECT_PAGE_MAX = 200


def _prefect_post_paged(path: str, body: dict, limit: int) -> list:
    """POST to a filter endpoint, paging around Prefect's per-request row cap.

    Prefect 3 rejects `limit > 200` outright ("Invalid limit: must be less than
    or equal to 200"), so anything larger has to be walked with `offset`.
    Clamping to 200 instead would silently return a truncated slice — the worse
    failure, since a flow run's interesting log line is usually not in the first
    200.
    """
    out: list = []
    while len(out) < limit:
        page = _prefect_post(
            path,
            {
                **body,
                "limit": min(_PREFECT_PAGE_MAX, limit - len(out)),
                "offset": len(out),
            },
        )
        out.extend(page)
        if len(page) < _PREFECT_PAGE_MAX:
            break  # exhausted
    return out


def _flow_names(flow_ids: list[str]) -> dict:
    """Map flow_id -> flow name (flow runs only carry flow_id in Prefect 3)."""
    ids = [i for i in dict.fromkeys(flow_ids) if i]
    if not ids:
        return {}
    flows = _prefect_post(
        "/flows/filter", {"flows": {"id": {"any_": ids}}, "limit": len(ids)}
    )
    return {f["id"]: f["name"] for f in flows}


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
        state: Filter by state name, e.g. 'Failed', 'Completed', 'Running',
               'Crashed', 'Cancelled'. None = all.
        flow_name: Filter by flow name substring (case-insensitive). None = all.
        limit: Max number of runs to return (default 20, max 100).
    """
    limit = min(limit, 100)

    body: dict = {"limit": limit, "sort": "END_TIME_DESC"}
    if state:
        body["flow_runs"] = {"state": {"name": {"any_": [state]}}}
    if flow_name:
        body["flows"] = {"name": {"like_": flow_name}}

    runs = _prefect_post("/flow_runs/filter", body)
    names = _flow_names([r.get("flow_id") for r in runs])
    return [
        {
            "id": r["id"],
            "name": r.get("name"),
            "flow_name": names.get(r.get("flow_id")),
            "state": (r.get("state") or {}).get("name"),
            "state_message": (r.get("state") or {}).get("message"),
            "start_time": r.get("start_time"),
            "end_time": r.get("end_time"),
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
        limit: Max number of log entries to return (default 200, max 2000).
               Values above Prefect's 200-per-request cap are paged internally.
    """
    limit = min(limit, 2000)

    logs_filter: dict = {"flow_run_id": {"any_": [flow_run_id]}}
    if min_level:
        upper = min_level.upper()
        if upper not in _LOG_LEVELS:
            raise ValueError(
                f"min_level must be one of {list(_LOG_LEVELS)}; got {min_level!r}"
            )
        logs_filter["level"] = {"ge_": _LOG_LEVELS[upper]}

    logs = _prefect_post_paged(
        "/logs/filter",
        {"logs": logs_filter, "sort": "TIMESTAMP_ASC"},
        limit,
    )
    return [
        {
            "timestamp": lg.get("timestamp"),
            "level": lg.get("level"),
            "name": lg.get("name"),
            "message": lg.get("message"),
        }
        for lg in logs
    ]


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
# Prefect trigger
# ---------------------------------------------------------------------------

DBT_FLOW_NAME = "BD template: Executa DBT model"
DBT_FLOW_PROJECT = "main"


def _prefect_get_flow(flow_name: str, project: str) -> dict:
    q = """
    query($name: String!, $project: String!) {
        flow(
            where: {name: {_eq: $name}, project: {name: {_eq: $project}}},
            order_by: {created: desc},
            limit: 1
        ) {
            id
            run_config
        }
    }
    """
    flows = _prefect_gql(q, {"name": flow_name, "project": project})["flow"]
    if not flows:
        raise RuntimeError(f"Flow {flow_name!r} not found in project {project!r}")
    return flows[0]


@mcp.tool()
def trigger_dbt_model(
    dataset_id: str,
    target: str,
    table_id: str | None = None,
    dbt_command: str = "run",
    flags: str | None = None,
    dbt_alias: bool = True,
    download_csv_file: bool = False,
    image: str | None = None,
) -> dict:
    """
    Trigger a Prefect flow run for 'BD template: Executa DBT model' (project: main).

    Args:
        dataset_id: GCP dataset ID, e.g. "br_ibge_censo_demografico"
        target: dbt target — "dev" or "prod" (required)
        table_id: GCP table ID, e.g. "microdados_domicilio_2010". None runs all tables in the dataset.
        dbt_command: dbt command to run (default: "run"). Use "run --full-refresh" to force rebuild.
        flags: extra dbt flags string, e.g. "--full-refresh"
        dbt_alias: whether dbt uses alias (default True)
        download_csv_file: whether to download CSV after run (default False)
        image: optional Docker image override, e.g. "ghcr.io/basedosdados/prefect-flows:sha"

    Returns:
        {"flow_run_id": str, "dataset_id": str, "table_id": str | None, "prefect_url": str}
    """
    if target not in ("dev", "prod"):
        raise ValueError(f"target must be 'dev' or 'prod', got {target!r}")

    flow = _prefect_get_flow(DBT_FLOW_NAME, DBT_FLOW_PROJECT)
    flow_id = flow["id"]

    # Use the flow's run_config (preserving existing fields), override labels and optionally image
    label = "basedosdados" if target == "prod" else "basedosdados-dev"
    run_config = {**(flow["run_config"] or {}), "labels": [label]}
    if image is not None:
        run_config["image"] = image

    parameters: dict = {
        "dataset_id": dataset_id,
        "dbt_command": dbt_command,
        "dbt_alias": dbt_alias,
        "download_csv_file": download_csv_file,
        "target": target,
        "_vars": None,
        "flags": flags,
    }
    if table_id is not None:
        parameters["table_id"] = table_id

    mutation = """
    mutation($input: create_flow_run_input!) {
        create_flow_run(input: $input) {
            id
        }
    }
    """
    result = _prefect_gql(mutation, {"input": {"flow_id": flow_id, "parameters": parameters, "run_config": run_config}})
    flow_run_id = result["create_flow_run"]["id"]
    return {
        "flow_run_id": flow_run_id,
        "dataset_id": dataset_id,
        "table_id": table_id,
        "prefect_url": f"https://prefect.basedosdados.org/flow-run/{flow_run_id}",
    }
