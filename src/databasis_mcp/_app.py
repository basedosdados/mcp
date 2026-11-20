from fastmcp import FastMCP

mcp = FastMCP(
    "databasis-mcp",
    instructions=(
        "Tools for interacting with the Data Basis backend API. "
        "Read tools (search_datasets, list_datasets, get_dataset, lookup_id, etc.) "
        "require no authentication. "
        "BigQuery tools (query_bigquery, preview_table) require GCP ADC credentials and a billing project. "
        "Write tools are idempotent: pass an existing id to update, omit it to create. "
        "Write tools require backend credentials — call auth first or rely on auto-auth. "
        "Temporal tables need coverage_entity on their date columns (update_column or "
        "bulk_upsert_columns); check the result with get_coverage_spec."
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
