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

import argparse
import os

from . import __version__
from ._app import URLS, mcp

# Import tool modules to register all @mcp.tool() decorators
from .tools import (
    bigquery,  # noqa: F401
    metadata,  # noqa: F401
    prefect,  # noqa: F401
    write,  # noqa: F401
)

__all__ = ["main", "mcp"]

TRANSPORTS = ["stdio", "http", "sse", "streamable-http"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="databasis-mcp",
        description=(
            "MCP server exposing the Base dos Dados API — metadata search, "
            "BigQuery queries, and metadata registration."
        ),
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"%(prog)s {__version__}",
    )
    parser.add_argument(
        "-t",
        "--transport",
        choices=TRANSPORTS,
        default="stdio",
        help="MCP transport (default: stdio)",
    )
    parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="host to bind for HTTP transports (default: 127.0.0.1)",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=8000,
        help="port to bind for HTTP transports (default: 8000)",
    )
    parser.add_argument(
        "-e",
        "--env",
        choices=list(URLS),
        help="default backend environment; overrides the ENV variable",
    )
    parser.add_argument(
        "--gcp-project",
        help="GCP billing project; overrides the GCP_PROJECT_ID variable",
    )
    parser.add_argument(
        "--no-banner",
        action="store_true",
        help="do not print the FastMCP startup banner",
    )
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)

    # Tools read these at call time, so setting them before run() is enough.
    if args.env:
        os.environ["ENV"] = args.env
    if args.gcp_project:
        os.environ["GCP_PROJECT_ID"] = args.gcp_project

    show_banner = False if args.no_banner else None
    if args.transport == "stdio":
        mcp.run(transport="stdio", show_banner=show_banner)
    else:
        mcp.run(
            transport=args.transport,
            show_banner=show_banner,
            host=args.host,
            port=args.port,
        )


if __name__ == "__main__":
    main()
