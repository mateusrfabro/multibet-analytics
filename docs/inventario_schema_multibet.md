# Inventario Schema `multibet` — Super Nova DB

**Data:** 2026-03-30
**Responsavel:** Mateus Fabro (Squad Intelligence Engine)
**Objetivo:** Mapear todos os objetos criados no schema `multibet` do Super Nova DB (PostgreSQL), suas fontes, finalidades e recorrencia de atualizacao.

---

## Resumo Quantitativo

| Camada | Tipo | Quantidade |
|--------|------|-----------|
| Bronze | Tabelas (dados brutos Athena) | 24 (6 com dados, 18 vazias) |
| Silver | Tabelas fact/dim/agg (tratadas) | ~30 |
| Gold | Views (agregacoes de leitura) | 8 |
| **Total** | | **~62 objetos** |

---

## 1. CAMADA BRONZE — Dados brutos do Athena, sem tratamento

Todas com prefixo `bronze_`. Replicam colunas selecionadas das tabelas Athena (Iceberg).

### 1.1 Tabelas COM dados

| Tabela | Linhas | Fonte Athena | Descricao |
|--------|--------|-------------|-----------|
| `bronze_ecr_flags` | 1.1M | bireports_ec2.tbl_ecr | Flags do jogador (test_user, referral_ban, etc.) |
| `bronze_bonus_sub_fund` | 4.2M | fund_ec2.tbl_bonus_sub_fund | Sub-fund de bonus por transacao |
| `bronze_ecr_banner` | 336K | ecr_ec2.tbl_ecr_banner | Trackers, affiliates e click IDs por jogador |
| `bronze_instrument` | 120K | cashier_ec2.tbl_instrument | Meios de pagamento (PIX, cartao, etc.) |
| `bronze_games_mapping_data` | 2.7K | bireports_ec2.tbl_vendor_games_mapping_data | Catalogo de jogos (game_id, vendor, nome) |
| `bronze_fund_txn_type_mst` | 157 | fund_ec2.tbl_fund_txn_type_mst | Tipos de transacao (27=Bet, 45=Win, 72=Rollback) |

### 1.2 Tabelas VAZIAS (DDL criada, ETL de carga pendente)

| Tabela | Fonte Athena | Descricao |
|--------|-------------|-----------|
| `bronze_ecr` | ecr_ec2.tbl_ecr | Cadastro de jogadores |
| `bronze_cashier_deposit` | cashier_ec2.tbl_cashier_deposit | Depositos |
| `bronze_cashier_cashout` | cashier_ec2.tbl_cashier_cashout | Saques |
| `bronze_real_fund_txn` | fund_ec2.tbl_real_fund_txn | Transacoes de gaming (bets, wins, rollbacks) |
| `bronze_realcash_sub_fund` | fund_ec2.tbl_realcash_sub_fund | Sub-fund realcash |
| `bronze_daily_payment_summary` | cashier_ec2.tbl_daily_payment_summary | Resumo pagamentos por player/dia |
| `bronze_gaming_sessions` | bireports_ec2.tbl_ecr_gaming_sessions | Sessoes de jogo |
| `bronze_sports_bets` | vendor_ec2.tbl_sports_book_bets_info | Apostas esportivas |
| `bronze_bonus_details` | bonus_ec2.tbl_ecr_bonus_details | Detalhes de bonus |
| `bronze_ccf_score` | risk_ec2.tbl_ecr_ccf_score | Score de risco CCF |
| `bronze_kyc_level` | csm_ec2.tbl_ecr_kyc_level | Nivel KYC do jogador |
| `bronze_games_catalog` | bireports_ec2.tbl_vendor_games_master | Catalogo completo de jogos |
| `bronze_fund_txn_casino` | fund_ec2.tbl_real_fund_txn (filtro CASINO) | Subconjunto filtrado de transacoes casino |
| `bronze_games_catalog_full` | — | Possivelmente duplicata de bronze_games_catalog |
| `bronze_big_wins` | — | Avaliacao pendente: talvez deva ser Silver |
| `bronze_crm_campaigns` | BigQuery (Smartico) | Fase 2 — ainda nao criada |
| `bronze_crm_communications` | BigQuery (Smartico) | Fase 2 — ainda nao criada |
| `bronze_crm_player_responses` | BigQuery (Smartico) | Fase 2 — ainda nao criada |

### 1.3 Tabelas que faltam criar (documento bronze_selects_kpis v2)

| Tabela | Fonte | Justificativa |
|--------|-------|---------------|
| `bronze_sports_bet_details` | vendor_ec2.tbl_sports_book_bet_details | Unica fonte de c_sport_type_name (nome do esporte) |
| `bronze_dim_game` | ps_bi.dim_game | Dimensao de jogos (RTP, volatilidade) |
| `bronze_dim_user` | ps_bi.dim_user | Dimensao completa do jogador |

**Script de diagnostico:** `scripts/diagnostico_bronze_ddl.py` — compara colunas esperadas (doc v2) vs existentes no banco.
**Documento de referencia:** `docs/diagnostico_bronze_v2.md`

---

## 2. CAMADA SILVER — Tabelas tratadas com regras de negocio

### 2.1 Produto & Performance

| Tabela | Grao | Fonte | Estrategia | Pipeline |
|--------|------|-------|-----------|----------|
| `fact_casino_rounds` | dia x jogo | ps_bi.fct_casino_activity_daily | TRUNCATE+INSERT | `pipelines/fact_casino_rounds.py` |
| `fact_sports_bets` | dia x esporte | vendor_ec2 (bets + details) | TRUNCATE+INSERT | `pipelines/fact_sports_bets.py` |
| `fact_sports_open_bets` | snapshot x esporte | vendor_ec2 | TRUNCATE+INSERT | `pipelines/fact_sports_bets.py` |
| `fact_live_casino` | dia x live game | ps_bi | TRUNCATE+INSERT | `pipelines/fact_live_casino.py` |
| `fact_jackpots` | mes x jogo | ps_bi | TRUNCATE+INSERT | `pipelines/fact_jackpots.py` |
| `fct_casino_activity` | dia | ps_bi | TRUNCATE+INSERT | `pipelines/fct_casino_activity.py` |
| `fct_sports_activity` | dia | vendor_ec2 | TRUNCATE+INSERT | `pipelines/fct_sports_activity.py` |

### 2.2 Player & Aquisicao

| Tabela | Grao | Fonte | Pipeline |
|--------|------|-------|----------|
| `fact_player_activity` | dia | ps_bi | `pipelines/fact_player_activity.py` |
| `fact_gaming_activity_daily` | dia x tracker | Athena multi | `pipelines/fact_gaming_activity_daily.py` |
| `fact_player_engagement_daily` | player (c_ecr_id) | Athena multi | `pipelines/fact_player_engagement_daily.py` |
| `fact_redeposits` | player (c_ecr_id) | cashier_ec2 | `pipelines/fact_redeposits.py` |
| `fact_registrations` | dia | ecr_ec2 | `pipelines/fact_registrations.py` |
| `fact_ftd_deposits` | dia x tracker | Athena + dim_user | `pipelines/fact_ftd_deposits.py` |
| `fact_attribution` | dia x tracker | Athena multi | `pipelines/fact_attribution.py` |

### 2.3 CRM

| Tabela | Grao | Fonte | Pipeline | Obs |
|--------|------|-------|----------|-----|
| `fact_crm_daily_performance` | campanha x periodo (BEFORE/DURING/AFTER) | BigQuery + Athena | `pipelines/crm_daily_performance.py` | Versao principal, usa JSONB |
| `dim_crm_friendly_names` | entity_id (campanha) | Manual/CRM | `pipelines/crm_daily_performance.py` | De-Para nomes campanhas |
| `crm_campaign_daily` | campanha x dia | BigQuery + Athena | `pipelines/ddl_crm_report.py` | v1 (8 tabelas abaixo) |
| `crm_campaign_segment_daily` | campanha x segmento x dia | BigQuery + Athena | `pipelines/ddl_crm_report.py` | v1 |
| `crm_campaign_game_daily` | campanha x jogo x dia | BigQuery + Athena | `pipelines/ddl_crm_report.py` | v1 |
| `crm_campaign_comparison` | campanha x periodo | BigQuery + Athena | `pipelines/ddl_crm_report.py` | v1 |
| `crm_dispatch_budget` | mes x canal x provedor | Custos fixos (SMS/WhatsApp) | `pipelines/crm_report_daily.py` | Custos de disparo |
| `crm_vip_group_daily` | campanha x VIP group x dia | BigQuery + Athena | `pipelines/ddl_crm_report.py` | v1 |
| `crm_recovery_daily` | campanha x canal x dia | BigQuery + Athena | `pipelines/ddl_crm_report.py` | v1 |
| `crm_player_vip_tier` | player x periodo | Athena (NGR) | `pipelines/ddl_crm_report.py` | Elite/Key Account/High Value |

### 2.4 Dimensoes & Mapeamento

| Tabela | Grao | Fonte | Pipeline |
|--------|------|-------|----------|
| `dim_games_catalog` | game_id (PK) | bireports_ec2 | `pipelines/dim_games_catalog.py` |
| `game_image_mapping` | game (SERIAL PK) | CDN provedores | `pipelines/game_image_mapper.py` |
| `dim_marketing_mapping` | tracker_id | Athena + inferencia forense (click IDs) | `pipelines/dim_marketing_mapping_canonical.py` |
| `dim_campaign_affiliate` | campaign x affiliate | Google Ads API | `pipelines/sync_google_ads_spend.py` |

### 2.5 Agregacoes

| Tabela | Grao | Fonte | Pipeline |
|--------|------|-------|----------|
| `agg_cohort_acquisition` | player x safra (mes FTD) | Athena + dim_marketing_mapping | `pipelines/agg_cohort_acquisition.py` |
| `agg_game_performance` | semana x jogo | ps_bi | `pipelines/agg_game_performance.py` |

### 2.6 Operacionais / ETL

| Tabela | Grao | Fonte | Pipeline | Recorrencia |
|--------|------|-------|----------|-------------|
| `grandes_ganhos` | evento (big win individual) | BigQuery Smartico (tr_casino_win) | `pipelines/grandes_ganhos.py` | Cron diario 00:30 BRT |
| `aquisicao_trafego_diario` | dia x canal x source | Athena + BigQuery | `pipelines/etl_aquisicao_trafego_diario.py` | Cron horario (60min) |
| `fact_google_ads_spend` | dia x campaign | Google Ads API | `pipelines/sync_google_ads_spend.py` | Manual |
| `etl_active_player_retention_weekly` | semana | cashier_ec2 | `pipelines/vw_active_player_retention_weekly.py` | Sugerido diario 06:00 |

### 2.7 Tabelas auxiliares da Matriz Financeiro

Estas tabelas sao base das views `matriz_financeiro_mensal` e `matriz_financeiro_semanal`:

| Tabela | Descricao |
|--------|-----------|
| `tab_dep_with` | Depositos e saques por dia |
| `tab_user_ftd` | FTD metricas por dia |
| `tab_cassino` | KPIs casino por dia |
| `tab_sports` | KPIs sports por dia |
| `tab_ativos` | Jogadores ativos (betting) por dia |

---

## 3. CAMADA GOLD — Views (agregacoes de leitura)

| View | Tabelas-fonte (no multibet) | Proposito | Criada por |
|------|----------------------------|----------|------------|
| `vw_active_player_retention_weekly` | etl_active_player_retention_weekly | Retencao semanal de depositantes | `scripts/create_vw_retention_weekly.py` |
| `vw_cohort_roi` | agg_cohort_acquisition | ROI por cohort/safra de FTD | `pipelines/agg_cohort_acquisition.py` |
| `vw_attribution_metrics` | fact_attribution | Metricas de atribuicao por modelo | `pipelines/fact_attribution.py` |
| `vw_acquisition_channel` | dim_marketing_mapping + facts | Canal de aquisicao consolidado | `pipelines/dim_marketing_mapping_canonical.py` |
| `vw_aquisicao_trafego` | aquisicao_trafego_diario | Trafego/aquisicao formatado | `pipelines/etl_aquisicao_trafego_diario.py` |
| `vw_google_ads_spend_daily` | fact_google_ads_spend | Google Ads spend diario | `pipelines/sync_google_ads_spend.py` |
| `matriz_financeiro_mensal` | tab_dep_with, tab_user_ftd, tab_cassino, tab_sports, tab_ativos | KPIs financeiros agregados por mes | `scripts/criar_views_matriz_financeiro.py` |
| `matriz_financeiro_semanal` | mesmas 5 tabelas acima | KPIs financeiros agregados por semana | `scripts/criar_views_matriz_financeiro.py` |

---

## 4. Pipelines em producao (EC2: 54.197.63.138)

| Pipeline | Cron | Tabela destino | Status |
|----------|------|---------------|--------|
| `grandes_ganhos.py` | `30 3 * * *` (00:30 BRT) | multibet.grandes_ganhos | Ativo |
| `game_image_mapper.py` | Pre-req do grandes_ganhos | multibet.game_image_mapping | Ativo |
| `etl_aquisicao_trafego_diario.py` | `10 * * * *` (a cada 60min) | multibet.aquisicao_trafego_diario | Ativo |
| `anti_abuse_multiverso.py` | Loop a cada 5min (systemd) | — (monitoramento CRM, nao persiste) | Ativo |

---

## 5. Dependencias entre objetos

```
Views Gold dependem de:
  matriz_financeiro_mensal  -->  tab_dep_with, tab_user_ftd, tab_cassino, tab_sports, tab_ativos
  matriz_financeiro_semanal -->  (mesmas 5 tabelas)
  vw_active_player_retention_weekly --> etl_active_player_retention_weekly
  vw_cohort_roi             -->  agg_cohort_acquisition
  vw_attribution_metrics    -->  fact_attribution
  vw_acquisition_channel    -->  dim_marketing_mapping + fact tables
  vw_aquisicao_trafego      -->  aquisicao_trafego_diario
  vw_google_ads_spend_daily -->  fact_google_ads_spend

Pipelines dependem de:
  grandes_ganhos.py         -->  game_image_mapping (pre-requisito)
  agg_cohort_acquisition.py -->  dim_marketing_mapping (lookup source)
```

---

## 6. Notas tecnicas

- **Upsert:** maioria das tabelas usa `INSERT...ON CONFLICT DO UPDATE` para idempotencia
- **TRUNCATE+INSERT:** tabelas fact de produto (casino_rounds, sports_bets, etc.) fazem full reload
- **JSONB:** fact_crm_daily_performance usa colunas JSONB (funil, financeiro, comparativo) para dados hierarquicos
- **LGPD:** grandes_ganhos hasheia nomes de jogadores (ex: "Ri***s")
- **Backfill:** tabelas fact tem dados desde 2025-10-01
- **Fontes externas:** Athena (Iceberg), BigQuery (Smartico CRM), Google Ads API
