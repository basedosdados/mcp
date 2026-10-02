# databasis-mcp

Servidor MCP (_Model Context Protocol_) que expõe a API da
[Base dos Dados](https://basedosdados.org) como ferramentas para agentes de IA.

Oferece três conjuntos de ferramentas:

- **Consumo de metadados** — busca e consulta de metadados públicos. Não requer
  conta.
- **Consumo de dados** — execução de queries SQL nas tabelas públicas via
  BigQuery. Requer conta GCP.
- **Cadastro de dados** — criação e atualização de metadados no backend. Requer
  conta BD.

---

## Consumo de metadados

Permite buscar conjuntos e consultar metadados sem qualquer autenticação.

### Ferramentas disponíveis

| Ferramenta             | Descrição                                                                  |
| ---------------------- | -------------------------------------------------------------------------- |
| `search_datasets`      | Busca conjuntos por nome                                                   |
| `list_datasets`        | Lista conjuntos, opcionalmente filtrado por organização                    |
| `get_dataset`          | Retorna metadados completos de um conjunto, incluindo referências BigQuery |
| `lookup_id`            | Busca ID de uma entidade de referência por slug                            |
| `discover_ids`         | Resolve slugs → IDs de referência (áreas, entidades, etc.)                 |
| `get_raw_data_sources` | Lista fontes brutas de um conjunto                                         |

### Exemplo de uso

```
1. search_datasets("educação") → encontra conjuntos relevantes
2. get_dataset("br_inep_censo_escolar") → vê tabelas disponíveis e referências BigQuery
```

---

## Consumo de dados

Permite executar queries SQL nas tabelas públicas da Base dos Dados via
BigQuery.

### Requisitos

- Python 3.11+
- Conta no Google Cloud Platform (GCP) com um projeto criado
- `gcloud` CLI instalado e autenticado:

```bash
gcloud auth application-default login
```

### Ferramentas disponíveis

| Ferramenta       | Descrição                                                |
| ---------------- | -------------------------------------------------------- |
| `preview_table`  | Visualiza as primeiras linhas de uma tabela via BigQuery |
| `query_bigquery` | Executa SQL nas tabelas `basedosdados.*` no BigQuery     |

### Projeto de faturamento GCP

As queries são cobradas ao seu projeto GCP. Forneça o projeto de faturamento em
ordem de prioridade:

1. Parâmetro `billing_project` na chamada da ferramenta
2. Variável de ambiente `GCP_PROJECT_ID`
3. Campo `gcp_project` em `~/.basedosdados/credentials.json`:

```json
{
  "gcp_project": "meu-projeto-gcp"
}
```

### Exemplo de uso

```
1. get_dataset("br_inep_censo_escolar") → vê referências BigQuery (gcp_dataset_id, gcp_table_id)
2. preview_table("br_inep_censo_escolar", "escola") → visualiza as primeiras linhas
3. query_bigquery("SELECT uf, COUNT(*) as n FROM `basedosdados.br_inep_censo_escolar.escola` GROUP BY uf LIMIT 10")
```

---

## Cadastro de dados

Permite criar e atualizar metadados de conjuntos, tabelas, colunas e demais
entidades no backend da Base dos Dados.

### Requisitos

- Python 3.11+
- Conta no backend da BD com credenciais configuradas

### Ferramentas disponíveis

| Ferramenta                                | Descrição                                              |
| ----------------------------------------- | ------------------------------------------------------ |
| `auth`                                    | Autentica e armazena token em memória (24h)            |
| `get_authenticated_account`               | Retorna conta autenticada no momento                   |
| `create_update_dataset`                   | Cria ou atualiza conjunto                              |
| `create_update_table`                     | Cria ou atualiza tabela                                |
| `upload_columns_from_sheet`               | Registra colunas a partir de planilha do Google Sheets |
| `update_column`                           | Atualiza coluna individual                             |
| `delete_column`                           | Remove coluna                                          |
| `delete_table`                            | Remove tabela                                          |
| `create_update_observation_level`         | Cria ou atualiza nível de observação                   |
| `create_update_cloud_table`               | Cria ou atualiza referência BigQuery de uma tabela     |
| `create_update_coverage`                  | Cria ou atualiza cobertura geográfica                  |
| `create_update_datetime_range`            | Cria ou atualiza intervalo temporal                    |
| `create_update_update`                    | Cria ou atualiza metadados de atualização              |
| `create_update_raw_data_source`           | Cria ou atualiza fonte de dados bruta                  |
| `create_update_tag`                       | Cria ou atualiza tag                                   |
| `create_update_theme`                     | Cria ou atualiza tema                                  |
| `create_update_organization`              | Cria ou atualiza organização                           |
| `create_update_license`                   | Cria ou atualiza licença (ex.: `cc_by_sa`, `cc0`)      |
| `create_update_availability`              | Cria ou atualiza disponibilidade                       |
| `create_update_language`                  | Cria ou atualiza idioma                                |
| `create_update_status`                    | Cria ou atualiza status                                |
| `create_update_entity`                    | Cria ou atualiza entidade (nível de observação)        |
| `create_update_entity_category`           | Cria ou atualiza categoria de entidade                 |
| `create_update_measurement_unit_category` | Cria ou atualiza categoria de unidade de medida        |
| `create_update_area`                      | Cria ou atualiza área de cobertura espacial            |
| `reorder_tables`                          | Define a ordem de exibição das tabelas em um conjunto  |
| `reorder_observation_levels`              | Define a ordem de exibição dos níveis de observação    |
| `reorder_columns`                         | Define a ordem de exibição das colunas                 |

Todas as ferramentas de escrita são **idempotentes**: passe um `id` existente
para atualizar, omita para criar.

### Ferramentas Prefect

Consultam a instância Prefect da Base dos Dados. Requerem chave de API em
`~/.basedosdados/credentials.json` sob `prod.prefect`.

| Ferramenta             | Descrição                                          |
| ---------------------- | -------------------------------------------------- |
| `list_flow_runs`       | Lista execuções recentes; filtra por estado e nome |
| `get_flow_run_logs`    | Retorna logs de uma execução por ID                |
| `get_failed_flow_runs` | Retorna execuções com falha com logs embutidos     |

### Credenciais de backend

O servidor lê credenciais na seguinte ordem de prioridade:

1. **Variável de ambiente:** `BACKEND_TOKEN` (`bdtoken_...`)
2. **Variáveis de ambiente:** `EMAIL` e `PASSWORD` (legado)
3. **Arquivo local:** `~/.basedosdados/credentials.json`

Formato do arquivo:

```json
{
  "gcp_project": "<billing-project-id>",
  "local": { "token": "bdtoken_..." },
  "dev": { "email": "...", "password": "..." },
  "staging": { "email": "...", "password": "..." },
  "prod": { "email": "...", "password": "...", "prefect": "<prefect-api-key>" }
}
```

O campo `gcp_project` é usado pelas ferramentas BigQuery. O campo `prefect` sob
`prod` é necessário para as ferramentas Prefect.

### Ambientes

Cada ferramenta de cadastro aceita um parâmetro `env`:

| Valor                | URL                                            |
| -------------------- | ---------------------------------------------- |
| `local`              | `http://localhost:8080`                        |
| `dev`                | `https://development.backend.basedosdados.org` |
| `staging` _(padrão)_ | `https://staging.backend.basedosdados.org`     |
| `prod`               | `https://backend.basedosdados.org`             |

---

## Instalação

O projeto usa [uv](https://docs.astral.sh/uv/) e é distribuído como o pacote
Python `databasis-mcp`, que fornece o módulo `databasis_mcp` e a CLI
`databasis-mcp`.

### Executar sem instalar

```bash
uvx --from git+https://github.com/basedosdados/mcp databasis-mcp
```

### Desenvolvimento a partir do repositório

```bash
git clone https://github.com/basedosdados/mcp.git
cd mcp
uv sync                 # cria o .venv e instala as dependências
uv run databasis-mcp    # executa o servidor (stdio)
uv run python -m databasis_mcp  # equivalente, via módulo
```

### CLI

```
databasis-mcp [-t {stdio,http,sse,streamable-http}] [--host HOST] [-p PORT]
              [-e {local,dev,staging,prod}] [--gcp-project PROJETO]
              [--no-banner] [--version]
```

| Opção               | Descrição                                                    |
| ------------------- | ------------------------------------------------------------ |
| `-t`, `--transport` | Transporte MCP (padrão: `stdio`)                             |
| `--host`            | Host para transportes HTTP (padrão: `127.0.0.1`)             |
| `-p`, `--port`      | Porta para transportes HTTP (padrão: `8000`)                 |
| `-e`, `--env`       | Ambiente padrão do backend; sobrescreve a variável `ENV`     |
| `--gcp-project`     | Projeto de faturamento GCP; sobrescreve `GCP_PROJECT_ID`     |
| `--no-banner`       | Não exibe o banner de inicialização do FastMCP               |

Exemplo servindo via HTTP:

```bash
uvx --from git+https://github.com/basedosdados/mcp databasis-mcp -t http -p 8000
# endpoint em http://127.0.0.1:8000/mcp
```

### Uso como biblioteca

```python
from databasis_mcp import mcp  # instância FastMCP com todas as ferramentas

mcp.run()
```

---

## Integração com clientes MCP

As credenciais são lidas automaticamente de `~/.basedosdados/credentials.json` —
não é necessário passá-las como variáveis de ambiente.

### Claude Code (CLI)

```bash
claude mcp add databasis -- uvx --from git+https://github.com/basedosdados/mcp databasis-mcp
```

Ou adicione manualmente ao `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "databasis": {
      "type": "stdio",
      "command": "uvx",
      "args": ["--from", "git+https://github.com/basedosdados/mcp", "databasis-mcp"]
    }
  }
}
```

Reconecte com `/mcp` após salvar.

### Claude Desktop

Adicione ao `claude_desktop_config.json`:

- **macOS:** `~/Library/Application Support/Claude/claude_desktop_config.json`
- **Windows:** `%APPDATA%\Claude\claude_desktop_config.json`

```json
{
  "mcpServers": {
    "databasis": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/basedosdados/mcp", "databasis-mcp"]
    }
  }
}
```

### VS Code

Adicione ao `.vscode/mcp.json` no seu workspace (ou nas configurações globais do
usuário):

```json
{
  "servers": {
    "databasis": {
      "type": "stdio",
      "command": "uvx",
      "args": ["--from", "git+https://github.com/basedosdados/mcp", "databasis-mcp"]
    }
  }
}
```

### Cursor / Windsurf / Continue

Qualquer cliente compatível com MCP via stdio pode usar o mesmo padrão:

```json
{
  "mcpServers": {
    "databasis": {
      "command": "uvx",
      "args": ["--from", "git+https://github.com/basedosdados/mcp", "databasis-mcp"]
    }
  }
}
```

Consulte a documentação do seu cliente para o local exato do arquivo de
configuração.

Para usar uma cópia local do repositório em vez da versão do GitHub, troque o
comando por `uv` com os argumentos
`["run", "--directory", "path/to/mcp", "databasis-mcp"]`.
