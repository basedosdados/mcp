# databasis-mcp

Servidor MCP (*Model Context Protocol*) que expõe a API da [Base dos Dados](https://basedosdados.org) como ferramentas para agentes de IA.

Oferece dois conjuntos de ferramentas:

- **Consumo de dados** — busca e consulta de dados públicos via metadados e BigQuery. Não requer conta na BD.
- **Cadastro de dados** — criação e atualização de metadados no backend. Requer conta e credenciais BD.

---

## Consumo de dados

Permite buscar datasets, consultar metadados e executar queries SQL nas tabelas públicas da Base dos Dados via BigQuery.

### Requisitos

- Python 3.11+
- Conta no Google Cloud Platform (GCP) com um projeto criado
- `gcloud` CLI instalado e autenticado:

```bash
gcloud auth application-default login
```

### Ferramentas disponíveis

| Ferramenta | Descrição |
|---|---|
| `search_datasets` | Busca datasets por nome (sem autenticação) |
| `list_datasets` | Lista datasets, opcionalmente filtrado por organização |
| `get_dataset` | Retorna metadados completos de um dataset, incluindo referências BigQuery |
| `lookup_id` | Busca ID de uma entidade de referência por slug |
| `discover_ids` | Resolve slugs → IDs de referência (áreas, entidades, etc.) |
| `get_raw_data_sources` | Lista fontes brutas de um dataset |
| `preview_table` | Visualiza as primeiras linhas de uma tabela via BigQuery |
| `query_bigquery` | Executa SQL nas tabelas `basedosdados.*` no BigQuery |

Todas as ferramentas de leitura de metadados não requerem autenticação de backend.

### Projeto de faturamento GCP

As ferramentas `preview_table` e `query_bigquery` executam queries no BigQuery cobradas ao seu projeto GCP. Forneça o projeto de faturamento em ordem de prioridade:

1. Parâmetro `billing_project` na chamada da ferramenta
2. Variável de ambiente `GCP_PROJECT_ID`
3. Campo `gcp_project` em `~/.basedosdados/backend_credentials.json`:

```json
{
  "gcp_project": "meu-projeto-gcp"
}
```

### Exemplo de uso

```
1. search_datasets("educação") → encontra datasets relevantes
2. get_dataset("br_inep_censo_escolar") → vê tabelas disponíveis e referências BigQuery
3. preview_table("br_inep_censo_escolar", "escola") → visualiza as primeiras linhas
4. query_bigquery("SELECT uf, COUNT(*) as n FROM `basedosdados.br_inep_censo_escolar.escola` GROUP BY uf LIMIT 10")
```

---

## Cadastro de dados

Permite criar e atualizar metadados de datasets, tabelas, colunas e demais entidades no backend da Base dos Dados.

### Requisitos

- Python 3.11+
- Conta no backend da BD com credenciais configuradas

### Ferramentas disponíveis

| Ferramenta | Descrição |
|---|---|
| `auth` | Autentica e armazena token em memória (24h) |
| `get_authenticated_account` | Retorna conta autenticada no momento |
| `create_update_dataset` | Cria ou atualiza dataset |
| `create_update_table` | Cria ou atualiza tabela |
| `upload_columns_from_sheet` | Registra colunas a partir de planilha do Google Sheets |
| `update_column` | Atualiza coluna individual |
| `delete_column` | Remove coluna |
| `delete_table` | Remove tabela |
| `create_update_observation_level` | Cria ou atualiza nível de observação |
| `create_update_cloud_table` | Cria ou atualiza referência BigQuery de uma tabela |
| `create_update_coverage` | Cria ou atualiza cobertura geográfica |
| `create_update_datetime_range` | Cria ou atualiza intervalo temporal |
| `create_update_update` | Cria ou atualiza metadados de atualização |
| `create_update_raw_data_source` | Cria ou atualiza fonte de dados bruta |
| `create_update_tag` | Cria ou atualiza tag |
| `create_update_theme` | Cria ou atualiza tema |
| `create_update_organization` | Cria ou atualiza organização |
| `reorder_tables` | Define a ordem de exibição das tabelas em um dataset |
| `reorder_observation_levels` | Define a ordem de exibição dos níveis de observação |
| `reorder_columns` | Define a ordem de exibição das colunas |

Todas as ferramentas de escrita são **idempotentes**: passe um `id` existente para atualizar, omita para criar.

### Ferramentas Prefect

Consultam a instância Prefect da Base dos Dados. Requerem chave de API em `~/.basedosdados/backend_credentials.json` sob `prod.prefect`.

| Ferramenta | Descrição |
|---|---|
| `list_flow_runs` | Lista execuções recentes; filtra por estado e nome |
| `get_flow_run_logs` | Retorna logs de uma execução por ID |
| `get_failed_flow_runs` | Retorna execuções com falha com logs embutidos |

### Credenciais de backend

O servidor lê credenciais na seguinte ordem de prioridade:

1. **Variável de ambiente:** `BACKEND_TOKEN` (`bdtoken_...`)
2. **Variáveis de ambiente:** `EMAIL` e `PASSWORD` (legado)
3. **Arquivo local:** `~/.basedosdados/backend_credentials.json`

Formato do arquivo:

```json
{
  "gcp_project": "meu-projeto-gcp",
  "local": { "token": "bdtoken_..." },
  "dev":   { "email": "...", "password": "..." },
  "prod":  { "email": "...", "password": "...", "prefect": "<prefect-api-key>" }
}
```

O campo `gcp_project` é usado pelas ferramentas BigQuery. O campo `prefect` sob `prod` é necessário para as ferramentas Prefect.

### Ambientes

Cada ferramenta de cadastro aceita um parâmetro `env`:

| Valor | URL |
|---|---|
| `local` | `http://localhost:8080` |
| `dev` *(padrão)* | `https://development.backend.basedosdados.org` |
| `prod` | `https://backend.basedosdados.org` |

---

## Instalação

```bash
pip install -r requirements.txt
```

---

## Integração com clientes MCP

As credenciais são lidas automaticamente de `~/.basedosdados/backend_credentials.json` — não é necessário passá-las como variáveis de ambiente.

Substitua `/caminho/para/python` e `/caminho/para/mcp/server.py` pelos caminhos corretos na sua máquina.

### Claude Code (CLI)

```bash
claude mcp add databasis -- /caminho/para/python /caminho/para/mcp/server.py
```

Ou adicione manualmente ao `~/.claude/settings.json`:

```json
{
  "mcpServers": {
    "databasis": {
      "type": "stdio",
      "command": "/caminho/para/python",
      "args": ["/caminho/para/mcp/server.py"]
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
      "command": "/caminho/para/python",
      "args": ["/caminho/para/mcp/server.py"]
    }
  }
}
```

### VS Code

Adicione ao `.vscode/mcp.json` no seu workspace (ou nas configurações globais do usuário):

```json
{
  "servers": {
    "databasis": {
      "type": "stdio",
      "command": "/caminho/para/python",
      "args": ["/caminho/para/mcp/server.py"]
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
      "command": "/caminho/para/python",
      "args": ["/caminho/para/mcp/server.py"]
    }
  }
}
```

Consulte a documentação do seu cliente para o local exato do arquivo de configuração.
