# databasis-mcp

Servidor MCP (*Model Context Protocol*) que expõe a API GraphQL do backend da [Base dos Dados](https://basedosdados.org) como ferramentas para agentes de IA.

## O que faz

Permite que LLMs (Claude, GPT, etc.) interajam diretamente com o backend da Base dos Dados para consultar e registrar metadados de datasets, tabelas, colunas, níveis de observação, coberturas e tabelas na nuvem.

### Ferramentas disponíveis

| Ferramenta | Descrição |
|---|---|
| `auth` | Autentica e armazena token em memória (24h) |
| `discover_ids` | Resolve slugs → IDs de referência (áreas, entidades, etc.) |
| `lookup_area` | Busca ID de uma área geográfica por slug |
| `get_dataset` | Retorna metadados de um dataset |
| `create_update_dataset` | Cria ou atualiza dataset |
| `create_update_table` | Cria ou atualiza tabela |
| `upload_columns` | Registra colunas em lote a partir de lista |
| `upload_columns_from_sheet` | Registra colunas a partir de planilha do Google Sheets |
| `update_column` | Atualiza coluna individual (incluindo limpar OL via `clear_observation_level`) |
| `delete_column` | Remove coluna |
| `delete_table` | Remove tabela |
| `create_update_observation_level` | Cria ou atualiza nível de observação |
| `create_update_cloud_table` | Cria ou atualiza tabela na nuvem (BigQuery) |
| `create_update_coverage` | Cria ou atualiza cobertura |
| `create_update_datetime_range` | Cria ou atualiza intervalo temporal |
| `create_update_update` | Cria ou atualiza metadados de atualização |
| `reorder_tables` | Define a ordem de exibição das tabelas em um dataset |
| `reorder_observation_levels` | Define a ordem de exibição dos níveis de observação em uma tabela |
| `reorder_columns` | Define a ordem de exibição das colunas em uma tabela |
| `get_raw_data_sources` | Lista fontes brutas de um dataset |
| `get_authenticated_account` | Retorna conta autenticada no momento |

Todas as ferramentas de escrita são **idempotentes**: passe um `id` existente para atualizar, omita para criar.

### Ferramentas Prefect

Consultam a instância Prefect 0.15 da Base dos Dados (`prefect.basedosdados.org`) via GraphQL. Requerem chave de API em `~/.basedosdados/backend_credentials.json` sob `prod.prefect`.

| Ferramenta | Descrição |
|---|---|
| `list_flow_runs` | Lista execuções recentes; filtra por `state` (ex: `"Failed"`) e `flow_name` (substring) |
| `get_flow_run_logs` | Retorna logs de uma execução por ID; filtra por `min_level` (`"DEBUG"`, `"INFO"`, `"WARNING"`, `"ERROR"`, `"CRITICAL"`) |
| `get_failed_flow_runs` | Atalho: retorna as N execuções com falha mais recentes já com os logs embutidos |

---

## Requisitos

- Python 3.11+
- [`fastmcp`](https://github.com/jlowin/fastmcp) >= 2.0
- `requests` >= 2.31

```bash
pip install -r requirements.txt
```

---

## Credenciais

O servidor lê credenciais na seguinte ordem de prioridade:

1. **Variáveis de ambiente:** `EMAIL` e `PASSWORD`
2. **Arquivo local:** `~/.basedosdados/backend_credentials.json`

Formato do arquivo de credenciais:

```json
{
  "local": { "token": "bdtoken_..." },
  "dev":   { "email": "...", "password": "..." },
  "prod":  { "email": "...", "password": "...", "prefect": "<prefect-api-key>" }
}
```

O campo `prefect` sob `prod` é necessário para as ferramentas Prefect. Tokens de backend (`bdtoken_...`) têm prioridade sobre email/senha quando presentes.

---

## Ambientes

Cada ferramenta aceita um parâmetro `env` com os valores:

| Valor | URL |
|---|---|
| `local` | `http://localhost:8080` |
| `dev` *(padrão)* | `https://development.backend.basedosdados.org` |
| `prod` | `https://backend.basedosdados.org` |

---

## Rodando localmente

```bash
python server.py
```

O servidor inicia no modo stdio (padrão do MCP).

---

## Integração com Claude Code

Adicione ao arquivo `~/.claude/settings.json` (ou `settings.local.json`):

```json
{
  "mcpServers": {
    "databasis": {
      "type": "stdio",
      "command": "/caminho/para/python3.11",
      "args": ["/caminho/para/mcp/server.py"],
      "env": {}
    }
  }
}
```

As credenciais são lidas automaticamente de `~/.basedosdados/backend_credentials.json` — não é necessário passá-las como variáveis de ambiente.

Após salvar, reconecte com `/mcp` no Claude Code.

## Integração com outros clientes MCP

Qualquer cliente compatível com MCP (Cursor, Windsurf, Continue, etc.) pode usar o servidor via stdio com o mesmo comando acima. Consulte a documentação do seu cliente para o formato exato de configuração.
