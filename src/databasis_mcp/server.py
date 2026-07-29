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

from ._app import mcp

# Import tool modules to register all @mcp.tool() decorators
from .tools import metadata  # noqa: F401
from .tools import write     # noqa: F401
from .tools import bigquery  # noqa: F401
from .tools import prefect   # noqa: F401

if __name__ == "__main__":
    mcp.run()
