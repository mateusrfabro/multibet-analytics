# CRM Report — Documentacao da API

> **Para:** Gusta (Squad Infra) — acoplamento no Super Nova Front
> **De:** Mateus F. (Squad Intelligence Engine)
> **Data:** 30/03/2026

## Visao Geral

Dashboard de Performance de Campanhas CRM com backend Flask + API REST.
Os dados sao alimentados pela pipeline `pipelines/crm_report_daily_v3_agent.py`
e persistidos no Super Nova DB (schema `multibet`).

O front atual (`dashboards/crm_report/templates/dashboard.html`) e um demo
funcional que consome a API via `fetch()`. O Gusta pode usar os mesmos
endpoints JSON no Super Nova Front sem precisar do Flask.

---

## Como rodar (dev local)

```bash
cd MultiBet
pip install -r dashboards/crm_report/requirements.txt
python dashboards/crm_report/app.py
# Acessa em http://localhost:5051
# Login: multibet / Multibet2026!
```

---

## Endpoints da API

### `GET /api/data` — Dados completos do dashboard

Retorna TODOS os dados em uma unica chamada (KPIs, campanhas paginadas,
funil, graficos, VIP, recuperacao, dispatch).

**Query params:**

| Param | Tipo | Default | Descricao |
|---|---|---|---|
| `date_from` | `YYYY-MM-DD` | 30 dias atras | Data inicio |
| `date_to` | `YYYY-MM-DD` | D-1 | Data fim |
| `campaign_type` | string | `all` | Filtro por tipo: RETEM, DailyFS, Cashback, Torneio, Freebet, etc |
| `channel` | string | `all` | Filtro por canal: popup, SMS, WhatsApp, push |
| `page` | int | `1` | Pagina da tabela de campanhas |
| `page_size` | int | `25` | Itens por pagina (max 100) |
| `sort_by` | string | `total_ggr` | Coluna para ordenar |
| `sort_dir` | string | `DESC` | Direcao: ASC ou DESC |

**Response JSON:**

```json
{
  "kpis": {
    "campanhas_ativas": 1092,
    "usuarios_impactados": 28177,
    "taxa_conversao": 88.1,
    "ggr_total": 34215.00,
    "roi_medio": 2.1,
    "turnover_total": 1820000.00,
    "depositos_total": 11090000.00,
    "custo_bonus_total": 5603.00,
    "custo_disparo_total": 8208.00,
    "net_deposit_total": 5581723.00
  },
  "campaigns": {
    "data": [
      {
        "report_date": "2026-03-27",
        "rule_id": 12345,
        "rule_name": "[RETEM] Sweet Bonanza - Recarregue e Ganhe",
        "campaign_type": "RETEM",
        "channel": "popup",
        "segment_name": "Retencao 30d",
        "is_active": true,
        "enviados": 388,
        "entregues": 388,
        "abertos": 388,
        "clicados": 380,
        "convertidos": 271,
        "cumpriram_condicao": 388,
        "custo_bonus_brl": 0.0,
        "coorte_users": 388,
        "casino_ggr": 41242.00,
        "sportsbook_ggr": 0.0,
        "total_ggr": 41242.00,
        "total_deposit": 52000.00,
        "total_withdrawal": 10758.00,
        "net_deposit": 41242.00,
        "casino_turnover": 520000.00,
        "sportsbook_turnover": 0.0,
        "custo_disparo_brl": 0.0,
        "roi": 5486.0
      }
    ],
    "total": 1092,
    "page": 1,
    "page_size": 25,
    "total_pages": 44
  },
  "funnel": {
    "segmentados": 28177,
    "entregues": 28177,
    "abertos": 28156,
    "clicados": 28013,
    "convertidos": 24835,
    "completaram": 28187,
    "pct_entregues": 100.0,
    "pct_abertos": 99.9,
    "pct_clicados": 99.4,
    "pct_convertidos": 88.1,
    "pct_completaram": 100.0
  },
  "funnel_by_type": [
    {"campaign_type": "RETEM", "segmentados": 112801, "convertidos": 15912, "completaram": 11066}
  ],
  "daily_volume": [
    {"date": "2026-03-01", "segmentados": 4200, "convertidos": 780, "completaram": 420}
  ],
  "top_games": [
    {"game_name": "Fortune Ox", "game_id": "123", "users": 3456, "turnover_brl": 2345670.0, "ggr_brl": 189450.0, "rtp_pct": 91.9}
  ],
  "vip_analysis": [
    {"vip_tier": "Elite", "users": 45, "ngr_total": 892340.0, "ngr_medio": 19830.0, "apd": 5.2}
  ],
  "dispatch_budget": [
    {"channel": "Push", "provider": "PushFY", "custo_unitario": 0.06, "total_sent": 93937, "custo_total_brl": 5636.0}
  ],
  "roi_by_type": [
    {"campaign_type": "RETEM", "roi": 2.3, "custo_total": 98240.0, "ggr_total": 285430.0}
  ],
  "recovery": [
    {"channel": "SMS", "inativos_impactados": 8234, "reengajados": 1456, "depositaram": 892, "tempo_medio": 4.2, "churn_d7_pct": 62.0}
  ],
  "filters": {
    "date_from": "2026-03-01",
    "date_to": "2026-03-27",
    "campaign_type": "all",
    "channel": "all"
  },
  "updated_at": "30/03/2026 10:30:00"
}
```

---

### `GET /api/export/csv` — Export CSV

Retorna arquivo CSV (separador `;`, BOM UTF-8) com TODAS as campanhas
do periodo filtrado (sem paginacao). Download direto no browser.

**Query params:** mesmos filtros de `/api/data` (sem page/sort).

---

### `POST /api/refresh` — Limpar cache

Limpa o cache em memoria e forca reconsulta ao Super Nova DB.

---

### `GET /health` — Health check (sem auth)

```json
{"status": "ok", "service": "crm-report-dashboard", "timestamp": "2026-03-30T10:30:00"}
```

---

## Tabelas no Super Nova DB (schema `multibet`)

Todas as tabelas sao criadas pela DDL em `pipelines/ddl_crm_report.py`
e alimentadas pela pipeline `pipelines/crm_report_daily_v3_agent.py`.

| Tabela | Descricao | Chave unica |
|---|---|---|
| `crm_campaign_daily` | 1 linha por campanha x dia (principal) | `(report_date, rule_id)` |
| `crm_campaign_segment_daily` | Quebra por segmento | `(report_date, rule_id, segment_id)` |
| `crm_campaign_game_daily` | Top jogos por campanha | `(report_date, rule_id, game_id)` |
| `crm_campaign_comparison` | Antes/durante/depois | `(rule_id, period)` |
| `crm_dispatch_budget` | Orcamento de disparos | `(report_date, channel, provider)` |
| `crm_vip_group_daily` | Metricas por faixa VIP | `(report_date, rule_id, vip_tier)` |
| `crm_recovery_daily` | Recuperacao de inativos | `(report_date, rule_id)` |
| `crm_player_vip_tier` | Classificacao VIP por player | `(ecr_id, periodo_inicio)` |

---

## Para o Gusta — Acoplamento no Super Nova Front

**Opcao 1 (recomendada):** Consumir diretamente as tabelas do Super Nova DB
via queries SQL. Todas as queries estao documentadas em
`dashboards/crm_report/queries.py` — basta replicar no backend do Super Nova.

**Opcao 2:** Usar a API Flask como microservico — o Super Nova Front faz
`fetch()` para `http://crm-report:5051/api/data?...` e renderiza os dados.

**Opcao 3:** Copiar o template `dashboard.html` e adaptar ao design system
do Super Nova Front (React/Vue/etc), mantendo as mesmas chamadas fetch.

---

## Arquitetura de arquivos

```
dashboards/crm_report/
  app.py              — Flask principal (rotas + API)
  config.py           — Configuracoes (porta, auth, cache, tipos)
  queries.py          — Queries ao Super Nova DB (11 funcoes)
  requirements.txt    — Dependencias Python
  API_DOCS.md         — Este documento
  templates/
    login.html        — Tela de login
    dashboard.html    — Dashboard dinamico (consome API via fetch)
  static/             — Assets estaticos (vazio por ora)

pipelines/
  crm_report_daily_v3_agent.py  — Pipeline oficial (alimenta as tabelas)
  crm_report_daily.py           — Pipeline v1 (DEPRECADA)
  ddl_crm_report.py             — DDL das tabelas destino
```
