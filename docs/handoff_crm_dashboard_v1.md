# Handoff: Dashboard CRM Report v1

**De:** Mateus F. (Squad Intelligence Engine)
**Para:** Gusta (Squad Infra) + time de dados
**Data:** 01/04/2026
**Status:** Preview funcional em localhost:5051, dados de Marco/2026

---

## 1. Visao Geral

Dashboard de Performance de Campanhas CRM da MultiBet.
Mostra KPIs, campanhas agrupadas, financeiro, custos de disparo, top jogos e analise VIP.

**Stack atual:** Flask + HTML/JS + Chart.js | Dados via CSVs extraidos de BigQuery + Athena
**Stack destino:** Super Nova Front (db.supernovagaming.com.br) | Dados via tabelas no Super Nova DB

---

## 2. Fontes de Dados

### 2.1 BigQuery (Smartico CRM)
**Dataset:** `smartico-bq6.dwh_ext_24105`
**Credenciais:** `bigquery_credentials.json`

| Tabela | O que contem | Join key |
|---|---|---|
| `dm_automation_rule` | Metadados das campaigns (rule_id, rule_name, is_active, segment_id, activity_type_id) | rule_id |
| `dm_segment` | Nomes dos segmentos | segment_id |
| `j_automation_rule_progress` | Execucoes por user/dia — FONTE PRINCIPAL de users por campanha | automation_rule_id = rule_id |
| `j_communication` | Funil CRM (fact_type_id 1-5) + disparos por canal | user_ext_id + date |
| `j_bonuses` | Bonus creditados (pendente v2) | entity_id |

**IMPORTANTE:** `j_communication.resource_id` NAO mapeia para `dm_automation_rule.rule_id`. O link correto e `j_automation_rule_progress.automation_rule_id`.

### 2.2 Athena (Iceberg Data Lake — bronze)
**Conta:** 803633136520 | sa-east-1 | User: mb-prod-db-iceberg-ro (READ-ONLY)

| Tabela | O que contem | Valores |
|---|---|---|
| `bireports_ec2.tbl_ecr_wise_daily_bi_summary` | Financeiro por player/dia (GGR, deposits, saques) | Centavos (/100 para BRL) |
| `bireports_ec2.tbl_ecr` | Bridge ecr_id <-> external_id (user_ext_id do Smartico) | - |
| `ecr_ec2.tbl_ecr_flags` | Filtro test users (c_test_user = false) | Boolean |
| `fund_ec2.tbl_real_fund_txn` | Transacoes individuais (para top jogos com c_game_id) | Centavos |
| `bireports_ec2.tbl_vendor_games_mapping_data` | Catalogo de jogos (game_id -> game_name, category) | - |

### 2.3 Super Nova DB (PostgreSQL — destino)
**Host:** supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com:5432
**Schema:** multibet

---

## 3. Agrupamento de Campanhas

O Smartico tem ~152 automation rules. Agrupamos em **25 campanhas logicas**:

| Tipo | Regra de agrupamento | Campanhas |
|---|---|---|
| DailyFS | Por ticket (Low/Medium/High) — jogo e variante, nao campanha | 3 |
| RETEM | Cada faixa de deposito e individual (R$50, R$175, ..., R$2000) | 7 |
| KLC | Cada faixa de fidelidade e individual (R$20, R$25, ..., R$250) | 7 |
| Challenge | Por animal (Tiger, Rabbit, Ox, Dragon, Mouse, Snake) — quests sao etapas | 6 |
| Lifecycle | Individual | 1 |
| Cashback | Individual | 1 |

**Rules excluidas** (nao sao CRM): Limpar, Limpeza, Unmark, Marcador, Retirar, Teste Rollback, Reset.

**Logica de classificacao:** arquivo `scripts/extract_crm_report_csvs.py`, funcao `classify_campaign_group()`.

---

## 4. Queries de Extracao

### 4.1 Campanhas (BigQuery)
```sql
SELECT
    p.automation_rule_id AS rule_id,
    r.rule_name,
    CAST(r.is_active AS STRING) AS is_active,
    r.activity_type_id,
    s.segment_name,
    COUNT(DISTINCT p.user_ext_id) AS users,
    MIN(DATE(p.dt_executed)) AS first_exec,
    MAX(DATE(p.dt_executed)) AS last_exec,
    COUNT(DISTINCT DATE(p.dt_executed)) AS dias_ativa
FROM `smartico-bq6.dwh_ext_24105.j_automation_rule_progress` p
JOIN `smartico-bq6.dwh_ext_24105.dm_automation_rule` r
    ON p.automation_rule_id = r.rule_id
LEFT JOIN `smartico-bq6.dwh_ext_24105.dm_segment` s
    ON r.segment_id = s.segment_id
WHERE DATE(p.dt_executed) BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY 1, 2, 3, 4, 5
```

### 4.2 Users por campanha (BigQuery)
```sql
SELECT DISTINCT
    p.automation_rule_id AS rule_id,
    p.user_ext_id
FROM `smartico-bq6.dwh_ext_24105.j_automation_rule_progress` p
WHERE p.automation_rule_id IN (lista_de_rule_ids)
  AND DATE(p.dt_executed) BETWEEN '2026-03-01' AND '2026-03-31'
```

### 4.3 Financeiro por user (Athena bireports_ec2)
```sql
SELECT
    CAST(e.c_external_id AS VARCHAR) AS user_ext_id,
    SUM(b.c_casino_realcash_bet_amount) / 100.0 AS casino_turnover,
    SUM(b.c_sb_realcash_bet_amount) / 100.0 AS sportsbook_turnover,
    SUM(b.c_casino_realcash_bet_amount - b.c_casino_realcash_win_amount
        + b.c_sb_realcash_bet_amount - b.c_sb_realcash_win_amount) / 100.0 AS total_ggr,
    SUM(b.c_deposit_success_amount) / 100.0 AS total_deposit,
    SUM(b.c_co_success_amount) / 100.0 AS total_withdrawal,
    SUM(b.c_deposit_success_amount - b.c_co_success_amount) / 100.0 AS net_deposit,
    SUM(b.c_bonus_issued_amount) / 100.0 AS bonus_cost,
    COUNT(DISTINCT b.c_created_date) AS play_days,
    SUM(b.c_login_count) AS login_count
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary b
JOIN bireports_ec2.tbl_ecr e ON b.c_ecr_id = e.c_ecr_id
LEFT JOIN ecr_ec2.tbl_ecr_flags f ON b.c_ecr_id = f.c_ecr_id
WHERE CAST(e.c_external_id AS VARCHAR) IN (lista_user_ext_ids)
  AND b.c_created_date >= DATE '2026-03-01'
  AND b.c_created_date <= DATE '2026-03-31'
  AND (f.c_test_user = false OR f.c_test_user IS NULL)
GROUP BY CAST(e.c_external_id AS VARCHAR)
```

### 4.4 Funil CRM (BigQuery)
```sql
SELECT
    DATE(fact_date) AS report_date,
    COUNTIF(fact_type_id = 1) AS enviados,
    COUNTIF(fact_type_id = 2) AS entregues,
    COUNTIF(fact_type_id = 3) AS abertos,
    COUNTIF(fact_type_id = 4) AS clicados,
    COUNTIF(fact_type_id = 5) AS convertidos,
    COUNT(DISTINCT CASE WHEN fact_type_id = 1 THEN user_ext_id END) AS users_enviados,
    COUNT(DISTINCT CASE WHEN fact_type_id = 5 THEN user_ext_id END) AS users_convertidos
FROM `smartico-bq6.dwh_ext_24105.j_communication`
WHERE DATE(fact_date) BETWEEN '2026-03-01' AND '2026-03-31'
GROUP BY 1
```

### 4.5 Custos de disparo (BigQuery)
```sql
SELECT
    activity_type_id,
    label_provider_id,
    COUNT(*) AS total_sent,
    COUNT(DISTINCT user_ext_id) AS users
FROM `smartico-bq6.dwh_ext_24105.j_communication`
WHERE DATE(fact_date) BETWEEN '2026-03-01' AND '2026-03-31'
  AND fact_type_id = 1
GROUP BY 1, 2
```

**Mapa de canais:** 50=Popup, 60=SMS, 64=WhatsApp, 30=Push, 40=Push, 31=Inbox
**Mapa de provedores:** 1536=DisparoPro, 1545=PushFY, 1268=Comtele

**Custos confirmados CRM (31/03/2026):**
- SMS Ligue Lead (DisparoPro): R$ 0,047
- SMS PushFY: R$ 0,060
- SMS Comtele: R$ 0,063
- WhatsApp Loyalty: R$ 0,160
- Outros: R$ 0 (desconsiderar)

### 4.6 Top jogos (Athena fund_ec2)
```sql
WITH crm_users AS (
    SELECT e.c_ecr_id
    FROM bireports_ec2.tbl_ecr e
    WHERE CAST(e.c_external_id AS VARCHAR) IN (lista_user_ext_ids)
)
SELECT
    t.c_game_id AS game_id,
    v.c_game_desc AS game_name,
    COUNT(DISTINCT t.c_ecr_id) AS users,
    SUM(COALESCE(r.c_amount_in_ecr_ccy, 0)) / 100.0 AS turnover_brl,
    SUM(CASE
        WHEN m.c_op_type = 'DB' AND m.c_is_cancel_txn = false THEN COALESCE(r.c_amount_in_ecr_ccy, 0)
        WHEN m.c_op_type = 'CR' AND m.c_is_cancel_txn = false THEN -COALESCE(r.c_amount_in_ecr_ccy, 0)
        ELSE 0
    END) / 100.0 AS ggr_brl
FROM fund_ec2.tbl_real_fund_txn t
JOIN crm_users cu ON t.c_ecr_id = cu.c_ecr_id
LEFT JOIN fund_ec2.tbl_realcash_sub_fund_txn r ON t.c_txn_id = r.c_fund_txn_id
JOIN fund_ec2.tbl_real_fund_txn_type_mst m ON t.c_txn_type = m.c_txn_type
JOIN bireports_ec2.tbl_vendor_games_mapping_data v
    ON t.c_sub_product_id = v.c_vendor_id AND t.c_game_id = v.c_game_id
WHERE t.c_product_id = 'CASINO'
  AND t.c_txn_status = 'SUCCESS'
  AND m.c_is_gaming_txn = 'Y'
GROUP BY t.c_game_id, v.c_game_desc
ORDER BY turnover_brl DESC
LIMIT 15
```

### 4.7 VIP (Athena bireports_ec2)
Mesma query 4.3 mas com classificacao:
- Elite: NGR >= R$ 10.000
- Key Account: NGR >= R$ 5.000 e < R$ 10.000
- High Value: NGR >= R$ 3.000 e < R$ 5.000
- Standard: NGR < R$ 3.000

NGR = GGR - bonus_cost

---

## 5. Tabelas a Criar no Super Nova DB

### 5.1 crm_campaign_summary (nova — substituir CSV)
```sql
CREATE TABLE IF NOT EXISTS multibet.crm_campaign_summary (
    id                  SERIAL PRIMARY KEY,
    periodo_ref         DATE NOT NULL,
    campaign_group      VARCHAR(100) NOT NULL,
    campaign_type       VARCHAR(50),
    segment_name        VARCHAR(500),
    is_active           BOOLEAN DEFAULT true,
    is_targeted         BOOLEAN DEFAULT true,
    users               INTEGER DEFAULT 0,
    rules_count         INTEGER DEFAULT 0,
    dias_ativa          INTEGER DEFAULT 0,
    first_exec          DATE,
    last_exec           DATE,
    canais_disparo      VARCHAR(200),
    -- Financeiro (bireports_ec2)
    fin_users           INTEGER DEFAULT 0,
    total_ggr           NUMERIC(14,2) DEFAULT 0,
    ngr                 NUMERIC(14,2) DEFAULT 0,
    casino_turnover     NUMERIC(14,2) DEFAULT 0,
    sportsbook_turnover NUMERIC(14,2) DEFAULT 0,
    total_deposit       NUMERIC(14,2) DEFAULT 0,
    total_withdrawal    NUMERIC(14,2) DEFAULT 0,
    net_deposit         NUMERIC(14,2) DEFAULT 0,
    bonus_cost          NUMERIC(14,2) DEFAULT 0,
    avg_play_days       NUMERIC(6,2) DEFAULT 0,
    roi                 NUMERIC(10,4),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_crm_camp_summary UNIQUE (periodo_ref, campaign_group)
);
```

### 5.2 crm_vip_player (nova — classificacao VIP)
```sql
CREATE TABLE IF NOT EXISTS multibet.crm_vip_player (
    id              SERIAL PRIMARY KEY,
    user_ext_id     VARCHAR(50) NOT NULL,
    vip_tier        VARCHAR(30) NOT NULL,
    ngr_brl         NUMERIC(14,2) DEFAULT 0,
    ggr_brl         NUMERIC(14,2) DEFAULT 0,
    play_days       INTEGER DEFAULT 0,
    deposits_brl    NUMERIC(14,2) DEFAULT 0,
    periodo_ref     DATE NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT uq_crm_vip_player UNIQUE (user_ext_id, periodo_ref)
);
```

### 5.3 Tabelas existentes que podem ser reaproveitadas
- `multibet.crm_dispatch_budget` — ja existe, manter
- `multibet.crm_campaign_game_daily` — ja existe, popular com query 4.6
- `multibet.crm_vip_group_daily` — ja existe, popular com agregado da 5.2

---

## 6. Fluxo de Automacao (ETL)

```
1. BigQuery: campanhas + users     (query 4.1 + 4.2)
2. Python: agrupar campanhas       (classify_campaign_group)
3. Athena: financeiro por user     (query 4.3)
4. Python: cruzar e agregar        (users x financeiro)
5. Athena: top jogos               (query 4.6)
6. Athena: VIP                     (query 4.7)
7. BigQuery: funil + dispatch      (queries 4.4 + 4.5)
8. Super Nova DB: INSERT/UPSERT    (tabelas 5.1, 5.2, dispatch, games)
```

**Tempo estimado:** ~5 min total (BigQuery ~30s, Athena ~3min, insert ~30s)
**Frequencia:** diario D-1 pela manha

---

## 7. Arquivos do Projeto

```
dashboards/crm_report/
  app.py              — Flask (rotas + API)
  config.py           — Config (porta, auth, tipos, custos)
  queries_csv.py      — Data layer (le CSVs, retorna JSON)
  templates/
    dashboard.html    — Frontend (Chart.js + fetch API)
    login.html        — Login

scripts/
  extract_crm_report_csvs.py  — Extracao BigQuery + Athena -> CSVs

data/crm_report/
  campaigns.csv               — 25 campanhas com financeiro
  campaign_financials.csv     — Financeiro detalhado
  dispatch_costs.csv          — Custos de disparo
  funnel_daily.csv            — Funil por dia
  vip_summary.csv             — VIP agregado
  top_games.csv               — Top 15 jogos
  top_games_meta.json         — Users unicos que jogaram
  rule_channels.csv           — Canais por automation rule
```

---

## 8. Pendencias v2

| Item | Descricao | Fonte |
|---|---|---|
| Apostaram no funil | Cross users convertidos x Athena turnover > 0 | BigQuery + Athena |
| Cumpriram condicao | j_bonuses bonus_status_id = 3 | BigQuery |
| Comparativo antes/durante/depois | Baseline mes anterior vs periodo campanha vs D+1 a D+3 | Athena |
| Recuperacao de inativos | Users inativos reengajados, deposito pos-acao, churn D+7 | BigQuery + Athena |
| Metas por campanha | Input manual do CRM (campo meta_conversao_pct) | Manual |
| Verba mensal disparos | Input manual do CRM (campo budget_monthly_brl) | Manual |
| Projecao consumo mensal | Calculo: custo_acumulado / dias_passados * dias_mes | Derivado |
