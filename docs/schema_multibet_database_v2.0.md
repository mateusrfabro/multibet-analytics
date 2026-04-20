# Schema do Banco de Dados MultiBet — v2.0

**Schema:** `multibet`
**Banco:** Super Nova DB (PostgreSQL — AWS RDS)
**Host:** `supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com:5432`
**Versao:** 2.0 (refresh automatico)
**Data:** 2026-04-19 (substitui v1.1 de 19/03/2026)
**Responsavel:** Mateus Fabro — Squad Intelligence Engine
**Fonte:** gerado por `scripts/generate_schema_doc_v2.py` a partir do JSON em `reports/schema_columns_multibet_20260419.json`

---

## Resumo

| Categoria | Quantidade |
|-----------|-----------|
| Tabelas BASE (incluindo `mv_*` legadas) | 74 |
| Views                                    | 43 |
| Materialized views (`mv_*` reais)        | 3  |
| **Total** | **120 objetos no schema multibet** |

**Mudancas vs v1.1:**
- Camada **Bronze descontinuada** (24 tabelas `bronze_*` removidas). Pipelines leem Athena direto.
- Novas tabelas: `risk_tags`, `risk_tags_pgs`, `pcr_ratings`, `segment_tags`, `silver_*` (5), `tab_user_daily`, `tab_dep_user`, `tab_hour_*` (5), `tab_user_affiliate`, `tab_with_user`, `tab_affiliate`, `tab_btr`, `fact_ad_spend`, `fact_sports_odds_performance`, `fact_affiliate_revenue`, `fct_player_performance_by_period`, `fct_active_players_by_period`, `dim_affiliate_source`, `game_image_mapping`, `etl_control`, `migrations`.
- 3 novas matviews: `mv_aquisicao`, `mv_cohort_aquisicao`, `mv_cohort_retencao_ftd`.
- +35 views: `vw_front_*` (6), `matriz_*` (6), `vw_odds_performance_*` (2), `vw_ad_spend_*`, `vw_roi_*`, `vw_ltv_cac_ratio`, `vw_segmentacao_hibrida`, `pcr_atual`, `pcr_resumo`, varias de live ops.

**Arquivos complementares:**
- [docs/inventario_schema_multibet.md](inventario_schema_multibet.md) — inventario resumido (tabelas x camadas, pipelines, dependencias)
- [docs/schema_play4_supernova.md](schema_play4_supernova.md) — foreign tables do schema `play4`
- [docs/supernova_bet_guide.md](supernova_bet_guide.md) — Super Nova Bet Paquistao (outro DB)
- [docs/_migration/schema_bronze_multibet_v1.0.md](_migration/schema_bronze_multibet_v1.0.md) — bronze arquivado (referencia historica)

---

## Regras gerais

- **Somente destino.** Super Nova DB nao e fonte para entregas ao negocio — usar apenas Athena como fonte. Regra oficial desde 2026-03.
- **UTC → BRT:** timestamps sao armazenados em UTC. Converter usando `AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'` nas views/relatorios.
- **Upsert:** maioria das tabelas fact usa `INSERT ... ON CONFLICT DO UPDATE`.
- **Full reload:** algumas fact (casino/sports) fazem TRUNCATE+INSERT.
- **JSONB:** `fact_crm_daily_performance` usa colunas JSONB (funil, financeiro, comparativo).
- **LGPD:** `grandes_ganhos` hasheia nomes (ex: "Ri***s").


## Fact / Fct (produto, player, aquisicao)

### `fact_ad_spend`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 2.605 &nbsp;&nbsp; **Tamanho:** 1000.0 KB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `ad_source` | character varying(50) | PK | N | — |
| 3 | `campaign_id` | character varying(100) | PK | N | — |
| 4 | `campaign_name` | character varying(500) |  | Y | — |
| 5 | `channel_type` | character varying(50) |  | Y | — |
| 6 | `cost_brl` | numeric(18,2) |  | Y | 0 |
| 7 | `impressions` | integer |  | Y | 0 |
| 8 | `clicks` | integer |  | Y | 0 |
| 9 | `conversions` | numeric(18,2) |  | Y | 0 |
| 10 | `affiliate_id` | character varying(50) |  | Y | — |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_affiliate_revenue`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 14

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `affiliate_id` | character varying(50) | PK | N | — |
| 3 | `registrations` | integer |  | Y | 0 |
| 4 | `ftds` | integer |  | Y | 0 |
| 5 | `ftd_amount_brl` | numeric(18,2) |  | Y | 0 |
| 6 | `dep_amount_brl` | numeric(18,2) |  | Y | 0 |
| 7 | `withdrawal_brl` | numeric(18,2) |  | Y | 0 |
| 8 | `net_deposit_brl` | numeric(18,2) |  | Y | 0 |
| 9 | `ggr_casino_brl` | numeric(18,2) |  | Y | 0 |
| 10 | `ggr_sport_brl` | numeric(18,2) |  | Y | 0 |
| 11 | `ggr_total_brl` | numeric(18,2) |  | Y | 0 |
| 12 | `bonus_cost_brl` | numeric(18,2) |  | Y | 0 |
| 13 | `ngr_brl` | numeric(18,2) |  | Y | 0 |
| 14 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_attribution`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 154.684 &nbsp;&nbsp; **Tamanho:** 66.7 MB &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `c_tracker_id` | character varying(255) | PK | N | — |
| 3 | `qty_registrations` | integer |  | Y | 0 |
| 4 | `qty_ftds` | integer |  | Y | 0 |
| 5 | `ggr` | numeric(18,2) |  | Y | 0 |
| 6 | `marketing_spend` | numeric(18,2) |  | Y | 0 |
| 7 | `refreshed_at` | timestamp with time zone |  | Y | now() |
| 8 | `source` | character varying(100) |  | Y | — |

### `fact_casino_rounds`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 122.910 &nbsp;&nbsp; **Tamanho:** 35.7 MB &nbsp;&nbsp; **Colunas:** 25

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `game_id` | character varying(50) | PK | N | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `vendor_id` | character varying(50) |  | Y | — |
| 5 | `sub_vendor_id` | character varying(50) |  | Y | — |
| 6 | `game_category` | character varying(100) |  | Y | — |
| 7 | `qty_players` | integer |  | Y | 0 |
| 8 | `total_rounds` | integer |  | Y | 0 |
| 9 | `rounds_per_player` | numeric(10,2) |  | Y | 0 |
| 10 | `turnover_real` | numeric(18,2) |  | Y | 0 |
| 11 | `wins_real` | numeric(18,2) |  | Y | 0 |
| 12 | `ggr_real` | numeric(18,2) |  | Y | 0 |
| 13 | `turnover_bonus` | numeric(18,2) |  | Y | 0 |
| 14 | `wins_bonus` | numeric(18,2) |  | Y | 0 |
| 15 | `ggr_bonus` | numeric(18,2) |  | Y | 0 |
| 16 | `turnover_total` | numeric(18,2) |  | Y | 0 |
| 17 | `wins_total` | numeric(18,2) |  | Y | 0 |
| 18 | `ggr_total` | numeric(18,2) |  | Y | 0 |
| 19 | `hold_rate_pct` | numeric(10,4) |  | Y | 0 |
| 20 | `rtp_pct` | numeric(10,4) |  | Y | 0 |
| 21 | `jackpot_win` | numeric(18,2) |  | Y | 0 |
| 22 | `jackpot_contribution` | numeric(18,2) |  | Y | 0 |
| 23 | `free_spins_bet` | numeric(18,2) |  | Y | 0 |
| 24 | `free_spins_win` | numeric(18,2) |  | Y | 0 |
| 25 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_crm_daily_performance`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 2.179 &nbsp;&nbsp; **Tamanho:** 2.8 MB &nbsp;&nbsp; **Colunas:** 13

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.fact_crm_daily_performance_id_seq'::regclass) |
| 2 | `campanha_id` | character varying(100) |  | N | — |
| 3 | `campanha_name` | character varying(255) |  | Y | — |
| 4 | `campanha_start` | date |  | N | — |
| 5 | `campanha_end` | date |  | N | — |
| 6 | `period` | character varying(10) |  | N | — |
| 7 | `period_start` | date |  | N | — |
| 8 | `period_end` | date |  | N | — |
| 9 | `funil` | jsonb |  | Y | '{}'::jsonb |
| 10 | `financeiro` | jsonb |  | Y | '{}'::jsonb |
| 11 | `comparativo` | jsonb |  | Y | '{}'::jsonb |
| 12 | `created_at` | timestamp with time zone |  | Y | now() |
| 13 | `updated_at` | timestamp with time zone |  | Y | now() |

### `fact_ftd_deposits`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 29.833 &nbsp;&nbsp; **Tamanho:** 11.5 MB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `c_tracker_id` | character varying(255) | PK | N | — |
| 3 | `qty_ftds` | integer |  | N | 0 |
| 4 | `total_ftd_amount` | numeric(18,2) |  | Y | 0 |
| 5 | `avg_ticket_ftd` | numeric(18,2) |  | Y | — |
| 6 | `min_ticket_ftd` | numeric(18,2) |  | Y | — |
| 7 | `max_ticket_ftd` | numeric(18,2) |  | Y | — |
| 8 | `qty_ftds_below_50` | integer |  | Y | 0 |
| 9 | `qty_ftds_50_to_500` | integer |  | Y | 0 |
| 10 | `qty_ftds_above_500` | integer |  | Y | 0 |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_gaming_activity_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 116.739 &nbsp;&nbsp; **Tamanho:** 53.7 MB &nbsp;&nbsp; **Colunas:** 15

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `c_tracker_id` | character varying(255) | PK | N | — |
| 3 | `qty_players` | integer |  | Y | 0 |
| 4 | `total_bets` | numeric(18,2) |  | Y | 0 |
| 5 | `total_wins` | numeric(18,2) |  | Y | 0 |
| 6 | `ggr` | numeric(18,2) |  | Y | 0 |
| 7 | `bonus_cost` | numeric(18,2) |  | Y | 0 |
| 8 | `ngr` | numeric(18,2) |  | Y | 0 |
| 9 | `margin_pct` | numeric(10,4) |  | Y | 0 |
| 10 | `ggr_casino` | numeric(18,2) |  | Y | 0 |
| 11 | `ggr_sports` | numeric(18,2) |  | Y | 0 |
| 12 | `max_single_win_val` | numeric(18,2) |  | Y | — |
| 13 | `rollback_count` | integer |  | Y | 0 |
| 14 | `rollback_total` | numeric(18,2) |  | Y | 0 |
| 15 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_jackpots`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 56.0 KB &nbsp;&nbsp; **Colunas:** 12

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `month_start` | date | PK | N | — |
| 2 | `game_id` | character varying(50) | PK | N | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `vendor_id` | character varying(50) |  | Y | — |
| 5 | `jackpots_count` | integer |  | Y | 0 |
| 6 | `jackpot_total_paid` | numeric(18,2) |  | Y | 0 |
| 7 | `avg_jackpot_value` | numeric(18,2) |  | Y | 0 |
| 8 | `max_jackpot_value` | numeric(18,2) |  | Y | 0 |
| 9 | `contribution_total` | numeric(18,2) |  | Y | 0 |
| 10 | `ggr_total` | numeric(18,2) |  | Y | 0 |
| 11 | `jackpot_impact_pct` | numeric(10,4) |  | Y | 0 |
| 12 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_live_casino`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 11.674 &nbsp;&nbsp; **Tamanho:** 2.9 MB &nbsp;&nbsp; **Colunas:** 17

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `game_id` | character varying(50) | PK | N | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `vendor_id` | character varying(50) |  | Y | — |
| 5 | `game_category_desc` | character varying(100) |  | Y | — |
| 6 | `qty_players` | integer |  | Y | 0 |
| 7 | `total_rounds` | integer |  | Y | 0 |
| 8 | `turnover_total` | numeric(18,2) |  | Y | 0 |
| 9 | `wins_total` | numeric(18,2) |  | Y | 0 |
| 10 | `ggr_total` | numeric(18,2) |  | Y | 0 |
| 11 | `hold_rate_pct` | numeric(10,4) |  | Y | 0 |
| 12 | `rtp_pct` | numeric(10,4) |  | Y | 0 |
| 13 | `qty_sessions` | integer |  | Y | 0 |
| 14 | `avg_session_duration_sec` | numeric(10,2) |  | Y | 0 |
| 15 | `avg_rounds_per_session` | numeric(10,2) |  | Y | 0 |
| 16 | `max_concurrent_players` | integer |  | Y | 0 |
| 17 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_player_activity`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 142 &nbsp;&nbsp; **Tamanho:** 80.0 KB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `dau` | integer |  | Y | 0 |
| 3 | `wau` | integer |  | Y | 0 |
| 4 | `mau` | integer |  | Y | 0 |
| 5 | `stickiness_pct` | numeric(10,4) |  | Y | 0 |
| 6 | `total_bets` | integer |  | Y | 0 |
| 7 | `avg_bets_per_player` | numeric(10,2) |  | Y | 0 |
| 8 | `total_ggr` | numeric(18,2) |  | Y | 0 |
| 9 | `ggr_per_dau` | numeric(18,2) |  | Y | 0 |
| 10 | `refreshed_at` | timestamp with time zone |  | Y | now() |
| 11 | `avg_session_min` | numeric(10,2) |  | Y | — |

### `fact_player_engagement_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 154.651 &nbsp;&nbsp; **Tamanho:** 23.8 MB &nbsp;&nbsp; **Colunas:** 14

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `c_ecr_id` | bigint | PK | N | — |
| 2 | `c_tracker_id` | character varying(255) |  | Y | — |
| 3 | `source` | character varying(100) |  | Y | — |
| 4 | `ftd_date` | date |  | Y | — |
| 5 | `first_active_date` | date |  | Y | — |
| 6 | `last_active_date` | date |  | Y | — |
| 7 | `days_active_since_ftd` | integer |  | Y | 0 |
| 8 | `total_active_days` | integer |  | Y | 0 |
| 9 | `total_bets_count` | integer |  | Y | 0 |
| 10 | `avg_bets_per_day` | numeric(10,2) |  | Y | 0 |
| 11 | `total_ggr` | numeric(18,2) |  | Y | 0 |
| 12 | `days_since_last_active` | integer |  | Y | — |
| 13 | `is_churned` | smallint |  | Y | 0 |
| 14 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_redeposits`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 154.980 &nbsp;&nbsp; **Tamanho:** 22.2 MB &nbsp;&nbsp; **Colunas:** 14

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `c_ecr_id` | bigint | PK | N | — |
| 2 | `c_tracker_id` | character varying(255) |  | Y | — |
| 3 | `ftd_date` | date |  | Y | — |
| 4 | `ftd_amount` | numeric(18,2) |  | Y | — |
| 5 | `total_deposits` | integer |  | Y | 0 |
| 6 | `redeposit_count` | integer |  | Y | 0 |
| 7 | `is_redepositor_d7` | smallint |  | Y | 0 |
| 8 | `second_deposit_date` | date |  | Y | — |
| 9 | `days_to_second_deposit` | integer |  | Y | — |
| 10 | `avg_redeposit_amount` | numeric(18,2) |  | Y | — |
| 11 | `total_redeposit_amount` | numeric(18,2) |  | Y | 0 |
| 12 | `avg_days_between_deposits` | numeric(10,2) |  | Y | — |
| 13 | `deposits_per_month` | numeric(10,2) |  | Y | — |
| 14 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_registrations`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 171 &nbsp;&nbsp; **Tamanho:** 56.0 KB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `qty_registrations` | integer |  | N | — |
| 3 | `qty_ftds` | integer |  | N | 0 |
| 4 | `ftd_rate` | numeric(10,4) |  | Y | 0 |
| 5 | `avg_time_to_ftd_h` | numeric(10,2) |  | Y | — |
| 6 | `kyc_pass_rate` | numeric(10,4) |  | Y | 0 |
| 7 | `device_mobile` | integer |  | Y | 0 |
| 8 | `device_desktop` | integer |  | Y | 0 |
| 9 | `device_tablet` | integer |  | Y | 0 |
| 10 | `device_nao_informado` | integer |  | Y | 0 |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_sports_bets`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 172 &nbsp;&nbsp; **Tamanho:** 80.0 KB &nbsp;&nbsp; **Colunas:** 17

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `sport_name` | character varying(255) | PK | N | — |
| 3 | `qty_bets` | integer |  | Y | 0 |
| 4 | `qty_players` | integer |  | Y | 0 |
| 5 | `turnover` | numeric(18,2) |  | Y | 0 |
| 6 | `total_return` | numeric(18,2) |  | Y | 0 |
| 7 | `ggr` | numeric(18,2) |  | Y | 0 |
| 8 | `margin_pct` | numeric(10,4) |  | Y | 0 |
| 9 | `avg_ticket` | numeric(18,2) |  | Y | 0 |
| 10 | `avg_odds` | numeric(18,4) |  | Y | 0 |
| 11 | `qty_pre_match` | integer |  | Y | 0 |
| 12 | `qty_live` | integer |  | Y | 0 |
| 13 | `turnover_pre_match` | numeric(18,2) |  | Y | 0 |
| 14 | `turnover_live` | numeric(18,2) |  | Y | 0 |
| 15 | `pct_pre_match` | numeric(10,4) |  | Y | 0 |
| 16 | `pct_live` | numeric(10,4) |  | Y | 0 |
| 17 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_sports_bets_by_sport`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 1.961 &nbsp;&nbsp; **Tamanho:** 632.0 KB &nbsp;&nbsp; **Colunas:** 17

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `sport_name` | character varying(255) | PK | N | — |
| 3 | `qty_bets` | integer |  | Y | 0 |
| 4 | `qty_players` | integer |  | Y | 0 |
| 5 | `turnover` | numeric(18,2) |  | Y | 0 |
| 6 | `total_return` | numeric(18,2) |  | Y | 0 |
| 7 | `ggr` | numeric(18,2) |  | Y | 0 |
| 8 | `margin_pct` | numeric(10,4) |  | Y | 0 |
| 9 | `avg_ticket` | numeric(18,2) |  | Y | 0 |
| 10 | `avg_odds` | double precision |  | Y | 0 |
| 11 | `qty_pre_match` | integer |  | Y | 0 |
| 12 | `qty_live` | integer |  | Y | 0 |
| 13 | `turnover_pre_match` | numeric(18,2) |  | Y | 0 |
| 14 | `turnover_live` | numeric(18,2) |  | Y | 0 |
| 15 | `pct_pre_match` | numeric(10,4) |  | Y | 0 |
| 16 | `pct_live` | numeric(10,4) |  | Y | 0 |
| 17 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_sports_odds_performance`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 879 &nbsp;&nbsp; **Tamanho:** 296.0 KB &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `odds_range` | character varying(20) | PK | N | — |
| 3 | `odds_order` | smallint |  | N | — |
| 4 | `bet_mode` | character varying(10) | PK | N | — |
| 5 | `total_bets` | integer |  | Y | 0 |
| 6 | `unique_players` | integer |  | Y | 0 |
| 7 | `bets_casa_ganha` | integer |  | Y | 0 |
| 8 | `bets_casa_perde` | integer |  | Y | 0 |
| 9 | `pct_casa_ganha` | numeric(10,4) |  | Y | 0 |
| 10 | `total_stake` | numeric(18,2) |  | Y | 0 |
| 11 | `total_payout` | numeric(18,2) |  | Y | 0 |
| 12 | `ggr` | numeric(18,2) |  | Y | 0 |
| 13 | `hold_rate_pct` | numeric(10,4) |  | Y | 0 |
| 14 | `avg_odds` | double precision |  | Y | 0 |
| 15 | `avg_ticket` | numeric(18,2) |  | Y | 0 |
| 16 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fact_sports_open_bets`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `snapshot_dt` | date | PK | N | — |
| 2 | `sport_name` | character varying(255) | PK | N | — |
| 3 | `qty_open_bets` | integer |  | Y | 0 |
| 4 | `total_stake_open` | double precision |  | Y | 0 |
| 5 | `avg_odds_open` | double precision |  | Y | 0 |
| 6 | `projected_liability` | double precision |  | Y | 0 |
| 7 | `projected_ggr` | double precision |  | Y | 0 |
| 8 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fct_active_players_by_period`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 18 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 7

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `period` | character varying(15) | PK | N | — |
| 2 | `period_label` | character varying(40) |  | Y | — |
| 3 | `period_start` | date |  | Y | — |
| 4 | `period_end` | date |  | Y | — |
| 5 | `product` | character varying(15) | PK | N | — |
| 6 | `unique_players` | integer |  | Y | 0 |
| 7 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fct_casino_activity`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 174 &nbsp;&nbsp; **Tamanho:** 64.0 KB &nbsp;&nbsp; **Colunas:** 12

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `qty_players` | integer |  | Y | 0 |
| 3 | `casino_real_bet` | numeric(18,2) |  | Y | 0 |
| 4 | `casino_real_win` | numeric(18,2) |  | Y | 0 |
| 5 | `casino_real_ggr` | numeric(18,2) |  | Y | 0 |
| 6 | `casino_bonus_bet` | numeric(18,2) |  | Y | 0 |
| 7 | `casino_bonus_win` | numeric(18,2) |  | Y | 0 |
| 8 | `casino_bonus_ggr` | numeric(18,2) |  | Y | 0 |
| 9 | `casino_total_bet` | numeric(18,2) |  | Y | 0 |
| 10 | `casino_total_win` | numeric(18,2) |  | Y | 0 |
| 11 | `casino_total_ggr` | numeric(18,2) |  | Y | 0 |
| 12 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fct_player_performance_by_period`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 693.429 &nbsp;&nbsp; **Tamanho:** 208.0 MB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `user_id` | bigint | PK | N | — |
| 2 | `period` | character varying(15) | PK | N | — |
| 3 | `period_label` | character varying(40) |  | Y | — |
| 4 | `period_start` | date |  | Y | — |
| 5 | `period_end` | date |  | Y | — |
| 6 | `vertical` | character varying(15) | PK | N | — |
| 7 | `player_result` | numeric(18,2) |  | Y | 0 |
| 8 | `turnover` | numeric(18,2) |  | Y | 0 |
| 9 | `deposit_total` | numeric(18,2) |  | Y | 0 |
| 10 | `qty_sessions` | integer |  | Y | 0 |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `fct_sports_activity`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 173 &nbsp;&nbsp; **Tamanho:** 64.0 KB &nbsp;&nbsp; **Colunas:** 13

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `qty_players` | integer |  | Y | 0 |
| 3 | `sports_real_bet` | numeric(18,2) |  | Y | 0 |
| 4 | `sports_real_win` | numeric(18,2) |  | Y | 0 |
| 5 | `sports_real_ggr` | numeric(18,2) |  | Y | 0 |
| 6 | `sports_bonus_bet` | numeric(18,2) |  | Y | 0 |
| 7 | `sports_bonus_win` | numeric(18,2) |  | Y | 0 |
| 8 | `sports_bonus_ggr` | numeric(18,2) |  | Y | 0 |
| 9 | `sports_total_bet` | numeric(18,2) |  | Y | 0 |
| 10 | `sports_total_win` | numeric(18,2) |  | Y | 0 |
| 11 | `sports_total_ggr` | numeric(18,2) |  | Y | 0 |
| 12 | `refreshed_at` | timestamp with time zone |  | Y | now() |
| 13 | `qty_bets` | integer |  | Y | 0 |


## Agregacoes

### `agg_btr_by_utm_campaign`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 51 &nbsp;&nbsp; **Tamanho:** 32.0 KB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `utm_campaign` | character varying(500) | PK | N | — |
| 2 | `total_users` | integer |  | Y | — |
| 3 | `total_btr_events` | integer |  | Y | — |
| 4 | `total_btr_brl` | numeric(14,2) |  | Y | — |
| 5 | `avg_btr_per_user` | numeric(14,2) |  | Y | — |
| 6 | `median_btr` | numeric(14,2) |  | Y | — |
| 7 | `min_btr` | numeric(14,2) |  | Y | — |
| 8 | `max_btr` | numeric(14,2) |  | Y | — |
| 9 | `first_btr` | timestamp without time zone |  | Y | — |
| 10 | `last_btr` | timestamp without time zone |  | Y | — |
| 11 | `updated_at` | timestamp without time zone |  | Y | now() |

### `agg_cohort_acquisition`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 203.413 &nbsp;&nbsp; **Tamanho:** 35.3 MB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `c_ecr_id` | bigint | PK | N | — |
| 2 | `month_of_ftd` | character varying(7) |  | N | — |
| 3 | `source` | character varying(100) |  | Y | 'unmapped_orphans'::character varying |
| 4 | `c_tracker_id` | character varying(255) |  | Y | — |
| 5 | `ftd_date` | date |  | Y | — |
| 6 | `ftd_amount` | numeric(18,2) |  | Y | — |
| 7 | `ggr_d0` | numeric(18,2) |  | Y | 0 |
| 8 | `ggr_d7` | numeric(18,2) |  | Y | 0 |
| 9 | `ggr_d30` | numeric(18,2) |  | Y | 0 |
| 10 | `is_2nd_depositor` | smallint |  | Y | 0 |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `agg_game_performance`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 27.622 &nbsp;&nbsp; **Tamanho:** 5.6 MB &nbsp;&nbsp; **Colunas:** 17

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `week_start` | date | PK | N | — |
| 2 | `game_id` | character varying(50) | PK | N | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `vendor_id` | character varying(50) |  | Y | — |
| 5 | `game_category` | character varying(100) |  | Y | — |
| 6 | `qty_active_days` | integer |  | Y | 0 |
| 7 | `dau_avg` | numeric(10,2) |  | Y | 0 |
| 8 | `total_players` | integer |  | Y | 0 |
| 9 | `total_rounds` | integer |  | Y | 0 |
| 10 | `turnover` | numeric(18,2) |  | Y | 0 |
| 11 | `ggr` | numeric(18,2) |  | Y | 0 |
| 12 | `hold_rate_pct` | numeric(10,4) |  | Y | 0 |
| 13 | `ggr_rank` | integer |  | Y | — |
| 14 | `concentration_pct` | numeric(10,4) |  | Y | 0 |
| 15 | `first_activity_date` | date |  | Y | — |
| 16 | `is_new_game` | boolean |  | Y | false |
| 17 | `refreshed_at` | timestamp with time zone |  | Y | now() |


## Dimensoes & mapeamento

### `dim_affiliate_source`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 4.579 &nbsp;&nbsp; **Tamanho:** 1.1 MB &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `affiliate_id` | character varying(50) | PK | N | — |
| 2 | `affiliate_name` | character varying(200) |  | Y | — |
| 3 | `source_id` | character varying(200) |  | Y | — |
| 4 | `fonte_trafego` | character varying(50) |  | N | 'Direct/Organic'::character varying |
| 5 | `utm_source` | character varying(200) |  | Y | — |
| 6 | `utm_medium` | character varying(200) |  | Y | — |
| 7 | `utm_campaign` | character varying(200) |  | Y | — |
| 8 | `updated_at` | timestamp without time zone |  | N | now() |

### `dim_campaign_affiliate`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 32.0 KB &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `ad_source` | character varying(50) | PK | N | — |
| 2 | `campaign_id` | character varying(100) | PK | N | — |
| 3 | `campaign_name` | character varying(500) |  | Y | — |
| 4 | `affiliate_id` | character varying(50) |  | Y | — |
| 5 | `notes` | character varying(500) |  | Y | — |
| 6 | `updated_at` | timestamp with time zone |  | Y | now() |

### `dim_crm_friendly_names`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 1.443 &nbsp;&nbsp; **Tamanho:** 328.0 KB &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `entity_id` | character varying(100) | PK | N | — |
| 2 | `friendly_name` | character varying(255) |  | N | — |
| 3 | `categoria` | character varying(100) |  | Y | — |
| 4 | `responsavel` | character varying(100) |  | Y | — |
| 5 | `created_at` | timestamp with time zone |  | Y | now() |
| 6 | `updated_at` | timestamp with time zone |  | Y | now() |

### `dim_games_catalog`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 381 &nbsp;&nbsp; **Tamanho:** 152.0 KB &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `game_id` | character varying(50) | PK | N | — |
| 2 | `game_name` | character varying(255) |  | Y | — |
| 3 | `vendor_id` | character varying(50) |  | Y | — |
| 4 | `sub_vendor_id` | character varying(50) |  | Y | — |
| 5 | `product_id` | character varying(30) |  | Y | — |
| 6 | `game_category` | character varying(100) |  | Y | — |
| 7 | `game_category_desc` | character varying(100) |  | Y | — |
| 8 | `game_type_id` | integer |  | Y | — |
| 9 | `game_type_desc` | character varying(255) |  | Y | — |
| 10 | `status` | character varying(30) |  | Y | — |
| 11 | `game_technology` | character varying(30) |  | Y | — |
| 12 | `has_jackpot` | boolean |  | Y | false |
| 13 | `free_spin_game` | boolean |  | Y | false |
| 14 | `feature_trigger_game` | boolean |  | Y | false |
| 15 | `snapshot_dt` | date |  | Y | CURRENT_DATE |
| 16 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `dim_marketing_mapping`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 3.241 &nbsp;&nbsp; **Tamanho:** 1.9 MB &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `affiliate_id` | character varying(50) | PK | N | — |
| 2 | `tracker_id` | character varying(255) | PK | N | — |
| 3 | `source_name` | character varying(100) |  | N | — |
| 4 | `partner_name` | character varying(200) |  | Y | — |
| 5 | `is_validated` | boolean |  | N | false |
| 6 | `evidence` | text |  | Y | — |
| 7 | `source` | character varying(100) |  | Y | — |
| 8 | `created_at` | timestamp with time zone |  | N | now() |
| 9 | `updated_at` | timestamp with time zone |  | N | now() |

### `dim_marketing_mapping_bkp_20260319`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 16.0 KB &nbsp;&nbsp; **Colunas:** 5

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `tracker_id` | character varying(255) |  | Y | — |
| 2 | `campaign_name` | character varying(255) |  | Y | — |
| 3 | `source` | character varying(100) |  | Y | — |
| 4 | `confidence` | character varying(50) |  | Y | — |
| 5 | `mapping_logic` | character varying(255) |  | Y | — |

### `game_image_mapping`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 2.715 &nbsp;&nbsp; **Tamanho:** 2.3 MB &nbsp;&nbsp; **Colunas:** 23

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.game_image_mapping_id_seq'::regclass) |
| 2 | `game_name` | character varying(255) |  | N | — |
| 3 | `game_name_upper` | character varying(255) |  | N | — |
| 4 | `provider_game_id` | character varying(50) |  | Y | — |
| 5 | `vendor_id` | character varying(100) |  | Y | — |
| 6 | `game_image_url` | character varying(500) |  | Y | — |
| 7 | `game_slug` | character varying(200) |  | Y | — |
| 8 | `source` | character varying(50) |  | Y | 'scraper'::character varying |
| 9 | `updated_at` | timestamp with time zone |  | Y | now() |
| 10 | `product_id` | character varying(20) |  | Y | — |
| 11 | `sub_vendor_id` | character varying(50) |  | Y | — |
| 12 | `game_category` | character varying(30) |  | Y | — |
| 13 | `game_category_desc` | character varying(50) |  | Y | — |
| 14 | `game_type_desc` | character varying(100) |  | Y | — |
| 15 | `live_subtype` | character varying(30) |  | Y | — |
| 16 | `has_jackpot` | boolean |  | Y | false |
| 17 | `is_active` | boolean |  | Y | true |
| 18 | `rounds_24h` | bigint |  | Y | 0 |
| 19 | `players_24h` | integer |  | Y | 0 |
| 20 | `popularity_rank_24h` | integer |  | Y | — |
| 21 | `popularity_window_end` | timestamp with time zone |  | Y | — |
| 22 | `total_bet_24h` | numeric(18,2) |  | Y | 0 |
| 23 | `total_wins_24h` | numeric(18,2) |  | Y | 0 |


## Silver / staging

### `silver_game_15min`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 110 &nbsp;&nbsp; **Tamanho:** 48.0 KB &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `janela_inicio` | timestamp without time zone |  | Y | — |
| 2 | `janela_fim` | timestamp without time zone |  | Y | — |
| 3 | `game_id` | character varying(255) |  | Y | — |
| 4 | `game_name` | character varying(255) |  | Y | — |
| 5 | `game_category` | character varying(255) |  | Y | — |
| 6 | `total_bets` | integer |  | Y | — |
| 7 | `unique_players` | integer |  | Y | — |
| 8 | `total_bet` | double precision |  | Y | — |
| 9 | `total_win` | double precision |  | Y | — |

### `silver_game_activity`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 332.550 &nbsp;&nbsp; **Tamanho:** 40.7 MB &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `user_id` | character varying(255) |  | Y | — |
| 2 | `game_id` | character varying(255) |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `provider_id` | character varying(255) |  | Y | — |
| 5 | `last_played_at` | timestamp without time zone |  | Y | — |
| 6 | `qty_rounds` | integer |  | Y | — |
| 7 | `total_bet` | double precision |  | Y | — |
| 8 | `total_win` | double precision |  | Y | — |
| 9 | `days_active` | integer |  | Y | — |

### `silver_jogadores_ganhos`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 2.105.849 &nbsp;&nbsp; **Tamanho:** 259.5 MB &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `game_id` | text |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `game_category` | character varying(255) |  | Y | — |
| 5 | `name_complete` | character varying(255) |  | Y | — |
| 6 | `total_bet_amount` | double precision |  | Y | — |
| 7 | `total_win_amount` | double precision |  | Y | — |
| 8 | `ggr` | double precision |  | Y | — |

### `silver_jogos_jogadores_ativos`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 7.266 &nbsp;&nbsp; **Tamanho:** 640.0 KB &nbsp;&nbsp; **Colunas:** 5

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `game_id` | text |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `game_category` | character varying(255) |  | Y | — |
| 5 | `qtd_jogadores` | integer |  | Y | — |

### `silver_tab_user_ftd`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 217.298 &nbsp;&nbsp; **Tamanho:** 25.6 MB &nbsp;&nbsp; **Colunas:** 3

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `ftd_data` | date |  | Y | — |
| 2 | `c_ecr_id` | character varying(50) | PK | N | — |
| 3 | `affiliate_id` | character varying(255) |  | Y | — |


## Tabelas auxiliares (matrizes financeiras)

### `tab_affiliate`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 3.233 &nbsp;&nbsp; **Tamanho:** 464.0 KB &nbsp;&nbsp; **Colunas:** 2

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `affiliate_id` | character varying(100) |  | Y | — |
| 2 | `nome` | character varying(100) |  | Y | — |

### `tab_ativos`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 176 &nbsp;&nbsp; **Tamanho:** 96.0 KB &nbsp;&nbsp; **Colunas:** 2

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `active_players_betting` | integer |  | Y | — |

### `tab_atualizacao`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 8.0 KB &nbsp;&nbsp; **Colunas:** 3

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `schema` | character varying(50) |  | Y | — |
| 2 | `tabela` | character varying(100) |  | Y | — |
| 3 | `update_time` | timestamp without time zone |  | Y | — |

### `tab_btr`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 173 &nbsp;&nbsp; **Tamanho:** 112.0 KB &nbsp;&nbsp; **Colunas:** 2

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `btr_amount_inhouse` | integer |  | Y | — |

### `tab_cassino`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 176 &nbsp;&nbsp; **Tamanho:** 208.0 KB &nbsp;&nbsp; **Colunas:** 5

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `casino_real_bet_amount_inhouse` | double precision |  | Y | — |
| 3 | `casino_bonus_bet_amount_inhouse` | double precision |  | Y | — |
| 4 | `casino_total_bet_amount_inhouse` | double precision |  | Y | — |
| 5 | `casino_total_win_amount_inhouse` | double precision |  | Y | — |

### `tab_dep_user`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 1.679.098 &nbsp;&nbsp; **Tamanho:** 1.1 GB &nbsp;&nbsp; **Colunas:** 4

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `c_ecr_id` | character varying(50) |  | Y | — |
| 2 | `qtd_dep` | character varying(50) |  | Y | — |
| 3 | `dep_amount` | integer |  | Y | — |
| 4 | `ftd_amount` | integer |  | Y | — |

### `tab_dep_with`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 474 &nbsp;&nbsp; **Tamanho:** 128.0 KB &nbsp;&nbsp; **Colunas:** 10

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `users_count` | integer |  | Y | — |
| 3 | `dep_count` | integer |  | Y | — |
| 4 | `dep_amount` | double precision |  | Y | — |
| 5 | `avg_per_user` | double precision |  | Y | — |
| 6 | `avg_dep` | double precision |  | Y | — |
| 7 | `withdrawal_count` | integer |  | Y | — |
| 8 | `withdrawal_amount` | double precision |  | Y | — |
| 9 | `avg_withdrawal` | double precision |  | Y | — |
| 10 | `net_deposit` | double precision |  | Y | — |

### `tab_hour_ativos`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 4.144 &nbsp;&nbsp; **Tamanho:** 1.2 MB &nbsp;&nbsp; **Colunas:** 3

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `hour` | integer | PK | N | — |
| 3 | `active_players_betting` | integer |  | Y | — |

### `tab_hour_cassino`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 4.156 &nbsp;&nbsp; **Tamanho:** 1.7 MB &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `hour` | integer | PK | N | — |
| 3 | `casino_real_bet_amount_inhouse` | double precision |  | Y | — |
| 4 | `casino_bonus_bet_amount_inhouse` | double precision |  | Y | — |
| 5 | `casino_total_bet_amount_inhouse` | double precision |  | Y | — |
| 6 | `casino_total_win_amount_inhouse` | double precision |  | Y | — |

### `tab_hour_dep_with`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 11.329 &nbsp;&nbsp; **Tamanho:** 5.5 MB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `hour` | integer | PK | N | — |
| 3 | `users_count` | integer |  | Y | — |
| 4 | `dep_count` | integer |  | Y | — |
| 5 | `dep_amount` | double precision |  | Y | — |
| 6 | `avg_per_user` | double precision |  | Y | — |
| 7 | `avg_dep` | double precision |  | Y | — |
| 8 | `withdrawal_count` | integer |  | Y | — |
| 9 | `withdrawal_amount` | double precision |  | Y | — |
| 10 | `avg_withdrawal` | double precision |  | Y | — |
| 11 | `net_deposit` | double precision |  | Y | — |

### `tab_hour_sports`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 4.132 &nbsp;&nbsp; **Tamanho:** 2.3 MB &nbsp;&nbsp; **Colunas:** 10

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `hour` | integer | PK | N | — |
| 3 | `sportsbook_real_bet` | double precision |  | Y | — |
| 4 | `sportsbook_real_win` | double precision |  | Y | — |
| 5 | `sportsbook_real_ggr` | double precision |  | Y | — |
| 6 | `sportsbook_bonus_bet` | double precision |  | Y | — |
| 7 | `sportsbook_bonus_win` | double precision |  | Y | — |
| 8 | `sportsbook_total_bet` | double precision |  | Y | — |
| 9 | `sportsbook_total_win` | double precision |  | Y | — |
| 10 | `sportsbook_total_ggr` | double precision |  | Y | — |

### `tab_hour_user_ftd`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 11.326 &nbsp;&nbsp; **Tamanho:** 4.0 MB &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `hour` | integer | PK | N | — |
| 3 | `users` | integer |  | Y | — |
| 4 | `ftd` | integer |  | Y | — |
| 5 | `conversion` | double precision |  | Y | — |
| 6 | `ftd_amount` | double precision |  | Y | — |
| 7 | `avg_ftd_amount` | double precision |  | Y | — |
| 8 | `base_acumulada` | integer |  | Y | — |

### `tab_sports`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 174 &nbsp;&nbsp; **Tamanho:** 80.0 KB &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `sportsbook_real_bet` | double precision |  | Y | — |
| 3 | `sportsbook_real_win` | double precision |  | Y | — |
| 4 | `sportsbook_real_ggr` | double precision |  | Y | — |
| 5 | `sportsbook_bonus_bet` | double precision |  | Y | — |
| 6 | `sportsbook_bonus_win` | double precision |  | Y | — |
| 7 | `sportsbook_total_bet` | double precision |  | Y | — |
| 8 | `sportsbook_total_win` | double precision |  | Y | — |
| 9 | `sportsbook_total_ggr` | double precision |  | Y | — |

### `tab_user_affiliate`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 1.100.181 &nbsp;&nbsp; **Tamanho:** 159.4 MB &nbsp;&nbsp; **Colunas:** 4

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data_registro` | date |  | Y | — |
| 2 | `c_ecr_id` | character varying(50) |  | Y | — |
| 3 | `c_external_id` | character varying(50) |  | Y | — |
| 4 | `affiliate_id` | character varying(255) |  | Y | — |

### `tab_user_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 4.940.444 &nbsp;&nbsp; **Tamanho:** 1.3 GB &nbsp;&nbsp; **Colunas:** 14

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `c_ecr_id` | character varying(50) | PK | N | — |
| 2 | `data` | date | PK | N | — |
| 3 | `qtd_dep` | integer |  | Y | 0 |
| 4 | `deposit` | numeric(18,2) |  | Y | 0 |
| 5 | `qtd_withdrawal` | integer |  | Y | 0 |
| 6 | `withdrawal` | numeric(18,2) |  | Y | 0 |
| 7 | `qtd_bet_casino` | integer |  | Y | 0 |
| 8 | `casino_turnover` | numeric(18,2) |  | Y | 0 |
| 9 | `casino_ggr` | numeric(18,2) |  | Y | 0 |
| 10 | `qtd_bet_sports` | integer |  | Y | 0 |
| 11 | `sports_turnover` | numeric(18,2) |  | Y | 0 |
| 12 | `sports_ggr` | numeric(18,2) |  | Y | 0 |
| 13 | `btr` | numeric(18,2) |  | Y | 0 |
| 14 | `ngr` | numeric(18,2) |  | Y | 0 |

### `tab_user_ftd`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 475 &nbsp;&nbsp; **Tamanho:** 232.0 KB &nbsp;&nbsp; **Colunas:** 7

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date | PK | N | — |
| 2 | `users` | integer |  | Y | — |
| 3 | `ftd` | integer |  | Y | — |
| 4 | `conversion` | double precision |  | Y | — |
| 5 | `ftd_amount` | double precision |  | Y | — |
| 6 | `avg_ftd_amount` | double precision |  | Y | — |
| 7 | `base_acumulada` | integer |  | Y | — |

### `tab_with_user`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 381.826 &nbsp;&nbsp; **Tamanho:** 183.5 MB &nbsp;&nbsp; **Colunas:** 2

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `c_ecr_id` | character varying(50) |  | Y | — |
| 2 | `withdrawl_amount` | integer |  | Y | — |


## CRM

### `crm_campaign_comparison`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 13

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_campaign_comparison_id_seq'::regclass) |
| 2 | `campaign_id` | character varying(100) |  | N | — |
| 3 | `period` | character varying(10) |  | N | — |
| 4 | `period_start` | date |  | Y | — |
| 5 | `period_end` | date |  | Y | — |
| 6 | `users` | integer |  | Y | 0 |
| 7 | `depositos_brl` | numeric(14,2) |  | Y | 0 |
| 8 | `ggr_brl` | numeric(14,2) |  | Y | 0 |
| 9 | `ngr_brl` | numeric(14,2) |  | Y | 0 |
| 10 | `sessoes` | integer |  | Y | 0 |
| 11 | `apd` | numeric(6,2) |  | Y | 0 |
| 12 | `created_at` | timestamp with time zone |  | Y | now() |
| 13 | `updated_at` | timestamp with time zone |  | Y | now() |

### `crm_campaign_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 3.037 &nbsp;&nbsp; **Tamanho:** 2.3 MB &nbsp;&nbsp; **Colunas:** 48

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_campaign_daily_id_seq'::regclass) |
| 2 | `report_date` | date |  | N | — |
| 3 | `campaign_id` | character varying(100) |  | N | — |
| 4 | `campaign_name` | character varying(255) |  | Y | — |
| 5 | `campaign_type` | character varying(50) |  | Y | — |
| 6 | `channel` | character varying(50) |  | Y | — |
| 7 | `segment_name` | character varying(255) |  | Y | — |
| 8 | `status` | character varying(20) |  | Y | 'ativa'::character varying |
| 9 | `campaign_start` | date |  | Y | — |
| 10 | `campaign_end` | date |  | Y | — |
| 11 | `segmentados` | integer |  | Y | 0 |
| 12 | `msg_entregues` | integer |  | Y | 0 |
| 13 | `msg_abertos` | integer |  | Y | 0 |
| 14 | `msg_clicados` | integer |  | Y | 0 |
| 15 | `convertidos` | integer |  | Y | 0 |
| 16 | `apostaram` | integer |  | Y | 0 |
| 17 | `cumpriram_condicao` | integer |  | Y | 0 |
| 18 | `tempo_medio_conversao_horas` | numeric(10,2) |  | Y | — |
| 19 | `optin_apostaram` | integer |  | Y | 0 |
| 20 | `optin_nao_apostaram` | integer |  | Y | 0 |
| 21 | `economia_optin_brl` | numeric(14,2) |  | Y | 0 |
| 22 | `turnover_total_brl` | numeric(14,2) |  | Y | 0 |
| 23 | `ggr_brl` | numeric(14,2) |  | Y | 0 |
| 24 | `ggr_pct` | numeric(6,2) |  | Y | 0 |
| 25 | `ngr_brl` | numeric(14,2) |  | Y | 0 |
| 26 | `ngr_pct` | numeric(6,2) |  | Y | 0 |
| 27 | `net_deposit_brl` | numeric(14,2) |  | Y | 0 |
| 28 | `depositos_brl` | numeric(14,2) |  | Y | 0 |
| 29 | `saques_brl` | numeric(14,2) |  | Y | 0 |
| 30 | `turnover_casino_brl` | numeric(14,2) |  | Y | 0 |
| 31 | `ggr_casino_brl` | numeric(14,2) |  | Y | 0 |
| 32 | `turnover_sports_brl` | numeric(14,2) |  | Y | 0 |
| 33 | `ggr_sports_brl` | numeric(14,2) |  | Y | 0 |
| 34 | `custo_bonus_brl` | numeric(14,2) |  | Y | 0 |
| 35 | `custo_disparos_brl` | numeric(14,2) |  | Y | 0 |
| 36 | `custo_total_brl` | numeric(14,2) |  | Y | 0 |
| 37 | `cpa_medio_brl` | numeric(10,2) |  | Y | 0 |
| 38 | `roi` | numeric(8,4) |  | Y | — |
| 39 | `disparos_sms` | integer |  | Y | 0 |
| 40 | `disparos_whatsapp` | integer |  | Y | 0 |
| 41 | `disparos_push` | integer |  | Y | 0 |
| 42 | `disparos_popup` | integer |  | Y | 0 |
| 43 | `disparos_email` | integer |  | Y | 0 |
| 44 | `disparos_inbox` | integer |  | Y | 0 |
| 45 | `meta_conversao_pct` | numeric(6,2) |  | Y | — |
| 46 | `meta_atingida` | boolean |  | Y | — |
| 47 | `created_at` | timestamp with time zone |  | Y | now() |
| 48 | `updated_at` | timestamp with time zone |  | Y | now() |

### `crm_campaign_game_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 20 &nbsp;&nbsp; **Tamanho:** 104.0 KB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_campaign_game_daily_id_seq'::regclass) |
| 2 | `report_date` | date |  | N | — |
| 3 | `campaign_id` | character varying(100) |  | N | — |
| 4 | `game_id` | character varying(50) |  | Y | — |
| 5 | `game_name` | character varying(255) |  | Y | — |
| 6 | `vendor_name` | character varying(100) |  | Y | — |
| 7 | `users` | integer |  | Y | 0 |
| 8 | `turnover_brl` | numeric(14,2) |  | Y | 0 |
| 9 | `ggr_brl` | numeric(14,2) |  | Y | 0 |
| 10 | `rtp_pct` | numeric(6,2) |  | Y | — |
| 11 | `created_at` | timestamp with time zone |  | Y | now() |

### `crm_campaign_segment_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 12

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_campaign_segment_daily_id_seq'::regclass) |
| 2 | `report_date` | date |  | N | — |
| 3 | `campaign_id` | character varying(100) |  | N | — |
| 4 | `segment_type` | character varying(50) |  | N | — |
| 5 | `product_preference` | character varying(50) |  | N | ''::character varying |
| 6 | `ticket_tier` | character varying(20) |  | N | ''::character varying |
| 7 | `users` | integer |  | Y | 0 |
| 8 | `apostaram` | integer |  | Y | 0 |
| 9 | `turnover_brl` | numeric(14,2) |  | Y | 0 |
| 10 | `ggr_brl` | numeric(14,2) |  | Y | 0 |
| 11 | `depositos_brl` | numeric(14,2) |  | Y | 0 |
| 12 | `created_at` | timestamp with time zone |  | Y | now() |

### `crm_dispatch_budget`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 56.0 KB &nbsp;&nbsp; **Colunas:** 12

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_dispatch_budget_id_seq'::regclass) |
| 2 | `month_ref` | date |  | N | — |
| 3 | `channel` | character varying(50) |  | N | — |
| 4 | `provider` | character varying(100) |  | N | ''::character varying |
| 5 | `cost_per_unit` | numeric(6,4) |  | N | — |
| 6 | `total_sent` | integer |  | Y | 0 |
| 7 | `total_cost_brl` | numeric(14,2) |  | Y | 0 |
| 8 | `budget_monthly_brl` | numeric(14,2) |  | Y | — |
| 9 | `budget_pct_used` | numeric(6,2) |  | Y | — |
| 10 | `projection_eom_brl` | numeric(14,2) |  | Y | — |
| 11 | `created_at` | timestamp with time zone |  | Y | now() |
| 12 | `updated_at` | timestamp with time zone |  | Y | now() |

### `crm_player_vip_tier`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 32.0 KB &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_player_vip_tier_id_seq'::regclass) |
| 2 | `ecr_id` | bigint |  | N | — |
| 3 | `external_id` | character varying(50) |  | Y | — |
| 4 | `vip_tier` | character varying(30) |  | N | — |
| 5 | `ngr_periodo_brl` | numeric(14,2) |  | Y | 0 |
| 6 | `periodo_inicio` | date |  | Y | — |
| 7 | `periodo_fim` | date |  | Y | — |
| 8 | `updated_at` | timestamp with time zone |  | Y | now() |

### `crm_recovery_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 56.0 KB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_recovery_daily_id_seq'::regclass) |
| 2 | `report_date` | date |  | N | — |
| 3 | `campaign_id` | character varying(100) |  | N | — |
| 4 | `channel` | character varying(50) |  | N | ''::character varying |
| 5 | `inativos_impactados` | integer |  | Y | 0 |
| 6 | `reengajados` | integer |  | Y | 0 |
| 7 | `depositaram` | integer |  | Y | 0 |
| 8 | `depositos_brl` | numeric(14,2) |  | Y | 0 |
| 9 | `tempo_medio_reengajamento_horas` | numeric(10,2) |  | Y | — |
| 10 | `churn_d7_pct` | numeric(6,2) |  | Y | — |
| 11 | `created_at` | timestamp with time zone |  | Y | now() |

### `crm_vip_group_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 56.0 KB &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.crm_vip_group_daily_id_seq'::regclass) |
| 2 | `report_date` | date |  | N | — |
| 3 | `campaign_id` | character varying(100) |  | N | — |
| 4 | `vip_group` | character varying(30) |  | N | — |
| 5 | `users` | integer |  | Y | 0 |
| 6 | `ngr_brl` | numeric(14,2) |  | Y | 0 |
| 7 | `apd` | numeric(6,2) |  | Y | 0 |
| 8 | `overlap_count` | integer |  | Y | 0 |
| 9 | `created_at` | timestamp with time zone |  | Y | now() |


## Risco / PCR / Segmentacao

### `pcr_ratings`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 151.572 &nbsp;&nbsp; **Tamanho:** 35.6 MB &nbsp;&nbsp; **Colunas:** 25

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `snapshot_date` | date | PK | N | — |
| 2 | `player_id` | bigint | PK | N | — |
| 3 | `external_id` | bigint |  | Y | — |
| 4 | `rating` | character varying(2) |  | N | — |
| 5 | `pvs` | numeric(8,2) |  | N | — |
| 6 | `ggr_total` | numeric(15,2) |  | Y | — |
| 7 | `ngr_total` | numeric(15,2) |  | Y | — |
| 8 | `total_deposits` | numeric(15,2) |  | Y | — |
| 9 | `total_cashouts` | numeric(15,2) |  | Y | — |
| 10 | `num_deposits` | integer |  | Y | — |
| 11 | `days_active` | integer |  | Y | — |
| 12 | `recency_days` | integer |  | Y | — |
| 13 | `product_type` | character varying(10) |  | Y | — |
| 14 | `casino_rounds` | bigint |  | Y | — |
| 15 | `sport_bets` | bigint |  | Y | — |
| 16 | `bonus_issued` | numeric(15,2) |  | Y | — |
| 17 | `bonus_ratio` | numeric(8,4) |  | Y | — |
| 18 | `wd_ratio` | numeric(8,4) |  | Y | — |
| 19 | `net_deposit` | numeric(15,2) |  | Y | — |
| 20 | `margem_ggr` | numeric(8,4) |  | Y | — |
| 21 | `ggr_por_dia` | numeric(15,2) |  | Y | — |
| 22 | `affiliate_id` | character varying(300) |  | Y | — |
| 23 | `c_category` | character varying(50) |  | Y | — |
| 24 | `registration_date` | date |  | Y | — |
| 25 | `created_at` | timestamp with time zone |  | Y | now() |

### `risk_tags`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 2.376.997 &nbsp;&nbsp; **Tamanho:** 756.7 MB &nbsp;&nbsp; **Colunas:** 30

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `label_id` | character varying(50) | PK | N | — |
| 2 | `user_id` | character varying(50) | PK | N | — |
| 3 | `user_ext_id` | character varying(100) |  | Y | — |
| 4 | `snapshot_date` | date | PK | N | — |
| 5 | `regular_depositor` | integer |  | Y | 0 |
| 6 | `promo_only` | integer |  | Y | 0 |
| 7 | `zero_risk_player` | integer |  | Y | 0 |
| 8 | `fast_cashout` | integer |  | Y | 0 |
| 9 | `sustained_player` | integer |  | Y | 0 |
| 10 | `non_bonus_depositor` | integer |  | Y | 0 |
| 11 | `promo_chainer` | integer |  | Y | 0 |
| 12 | `cashout_and_run` | integer |  | Y | 0 |
| 13 | `reinvest_player` | integer |  | Y | 0 |
| 14 | `non_promo_player` | integer |  | Y | 0 |
| 15 | `engaged_player` | integer |  | Y | 0 |
| 16 | `rg_alert_player` | integer |  | Y | 0 |
| 17 | `behav_risk_player` | integer |  | Y | 0 |
| 18 | `potencial_abuser` | integer |  | Y | 0 |
| 19 | `player_not_valid` | integer |  | Y | 0 |
| 20 | `player_reengaged` | integer |  | Y | 0 |
| 21 | `sleeper_low_player` | integer |  | Y | 0 |
| 22 | `vip_whale_player` | integer |  | Y | 0 |
| 23 | `winback_hi_val_player` | integer |  | Y | 0 |
| 24 | `behav_slotgamer` | integer |  | Y | 0 |
| 25 | `computed_at` | timestamp without time zone |  | Y | CURRENT_TIMESTAMP |
| 26 | `rollback_player` | integer |  | Y | 0 |
| 27 | `multi_game_player` | integer |  | Y | 0 |
| 28 | `score_bruto` | integer |  | Y | 0 |
| 29 | `score_norm` | numeric(5,1) |  | Y | 0 |
| 30 | `tier` | character varying(20) |  | Y | — |

### `risk_tags_pgs`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 398.570 &nbsp;&nbsp; **Tamanho:** 125.2 MB &nbsp;&nbsp; **Colunas:** 26

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `label_id` | character varying(50) |  | Y | — |
| 2 | `user_id` | character varying(255) |  | Y | — |
| 3 | `user_ext_id` | character varying(255) |  | Y | — |
| 4 | `snapshot_date` | date |  | Y | — |
| 5 | `regular_depositor` | integer |  | Y | — |
| 6 | `promo_only` | integer |  | Y | — |
| 7 | `zero_risk_player` | integer |  | Y | — |
| 8 | `fast_cashout` | integer |  | Y | — |
| 9 | `sustained_player` | integer |  | Y | — |
| 10 | `non_bonus_depositor` | integer |  | Y | — |
| 11 | `promo_chainer` | integer |  | Y | — |
| 12 | `cashout_and_run` | integer |  | Y | — |
| 13 | `reinvest_player` | integer |  | Y | — |
| 14 | `non_promo_player` | integer |  | Y | — |
| 15 | `engaged_player` | integer |  | Y | — |
| 16 | `rg_alert_player` | integer |  | Y | — |
| 17 | `behav_risk_player` | integer |  | Y | — |
| 18 | `potencial_abuser` | integer |  | Y | — |
| 19 | `player_reengaged` | integer |  | Y | — |
| 20 | `sleeper_low_player` | integer |  | Y | — |
| 21 | `vip_whale_player` | integer |  | Y | — |
| 22 | `winback_hi_val_player` | integer |  | Y | — |
| 23 | `behav_slotgamer` | integer |  | Y | — |
| 24 | `rollback_player` | integer |  | Y | — |
| 25 | `multi_game_player` | integer |  | Y | — |
| 26 | `computed_at` | timestamp without time zone |  | Y | — |

### `segment_tags`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 6.373 &nbsp;&nbsp; **Tamanho:** 1.6 MB &nbsp;&nbsp; **Colunas:** 26

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `user_id` | character varying(50) | PK | N | — |
| 2 | `signup_date` | timestamp without time zone |  | Y | — |
| 3 | `status` | character varying(50) |  | Y | — |
| 4 | `pvs_score` | numeric(5,1) |  | Y | — |
| 5 | `segmento_valor` | character varying(20) |  | Y | — |
| 6 | `ggr_total` | numeric(12,2) |  | Y | — |
| 7 | `deposit_lifetime` | numeric(12,2) |  | Y | — |
| 8 | `deposit_count` | integer |  | Y | — |
| 9 | `days_active` | integer |  | Y | — |
| 10 | `recency_days` | integer |  | Y | — |
| 11 | `is_mixed_player` | boolean |  | Y | false |
| 12 | `is_active_recent` | boolean |  | Y | false |
| 13 | `pts_ggr` | numeric(5,1) |  | Y | — |
| 14 | `pts_deposit` | numeric(5,1) |  | Y | — |
| 15 | `pts_recency` | numeric(5,1) |  | Y | — |
| 16 | `pts_margin` | numeric(5,1) |  | Y | — |
| 17 | `pts_dep_count` | numeric(5,1) |  | Y | — |
| 18 | `pts_days_active` | numeric(5,1) |  | Y | — |
| 19 | `pts_activity_rate` | numeric(5,1) |  | Y | — |
| 20 | `pts_bonus_penalty` | numeric(5,1) |  | Y | — |
| 21 | `pts_wd_penalty` | numeric(5,1) |  | Y | — |
| 22 | `activity_rate_pct` | numeric(5,2) |  | Y | — |
| 23 | `margin_ggr_turnover` | numeric(8,4) |  | Y | — |
| 24 | `bonus_sensitivity_pct` | numeric(5,2) |  | Y | — |
| 25 | `withdrawal_deposit_ratio` | numeric(8,4) |  | Y | — |
| 26 | `computed_at` | timestamp without time zone |  | Y | CURRENT_TIMESTAMP |


## Tabelas com prefixo mv_ (legado)

### `mv_sports_open_bets_by_odds`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `odds_range` | character varying(20) | PK | N | — |
| 2 | `odds_order` | smallint |  | N | — |
| 3 | `qty_open_bets` | bigint |  | Y | — |
| 4 | `total_stake` | numeric(20,2) |  | Y | — |
| 5 | `total_liability` | numeric(20,2) |  | Y | — |
| 6 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `mv_top_sports_events_daily`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 19.598 &nbsp;&nbsp; **Tamanho:** 8.3 MB &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `event_id` | character varying | PK | N | — |
| 3 | `event_name` | character varying |  | Y | — |
| 4 | `tournament_name` | character varying |  | Y | — |
| 5 | `sport_type_name` | character varying |  | Y | — |
| 6 | `qty_bets` | bigint |  | Y | — |
| 7 | `qty_players` | bigint |  | Y | — |
| 8 | `turnover` | numeric(14,2) |  | Y | — |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | now() |
| 12 | `qty_live_bets` | bigint |  | Y | — |
| 13 | `qty_prelive_bets` | bigint |  | Y | — |
| 14 | `qty_mixed_bets` | bigint |  | Y | — |
| 15 | `ts_realstart` | timestamp with time zone |  | Y | — |
| 16 | `ts_realend` | timestamp with time zone |  | Y | — |
| 17 | `qty_wins` | bigint |  | Y | — |
| 18 | `qty_losses` | bigint |  | Y | — |


## Operacionais / ETL / utilitarios

### `aquisicao_trafego_diario`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 111 &nbsp;&nbsp; **Tamanho:** 120.0 KB &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date | PK | N | — |
| 2 | `channel` | character varying(30) | PK | N | — |
| 3 | `registros` | integer |  | Y | 0 |
| 4 | `ftd_count` | integer |  | Y | 0 |
| 5 | `ftd_amount` | numeric(14,2) |  | Y | 0 |
| 6 | `conv_reg_ftd_pct` | numeric(5,1) |  | Y | 0 |
| 7 | `ftd_ticket_medio` | numeric(14,2) |  | Y | 0 |
| 8 | `depositos_amount` | numeric(14,2) |  | Y | 0 |
| 9 | `saques_amount` | numeric(14,2) |  | Y | 0 |
| 10 | `net_deposit` | numeric(14,2) |  | Y | 0 |
| 11 | `ggr_casino` | numeric(14,2) |  | Y | 0 |
| 12 | `ggr_sport` | numeric(14,2) |  | Y | 0 |
| 13 | `bonus_cost` | numeric(14,2) |  | Y | 0 |
| 14 | `ngr` | numeric(14,2) |  | Y | 0 |
| 15 | `players_ativos` | integer |  | Y | 0 |
| 16 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `etl_active_player_retention_weekly`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 26 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `semana` | date | PK | N | — |
| 2 | `semana_label` | character varying(15) |  | Y | — |
| 3 | `depositantes_semana_atual` | integer |  | Y | — |
| 4 | `depositantes_semana_anterior` | integer |  | Y | — |
| 5 | `retidos_da_semana_anterior` | integer |  | Y | — |
| 6 | `repeat_depositors` | integer |  | Y | — |
| 7 | `retention_pct` | numeric(5,1) |  | Y | — |
| 8 | `repeat_depositor_pct` | numeric(5,1) |  | Y | — |
| 9 | `refreshed_at` | timestamp with time zone |  | Y | now() |

### `etl_control`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 24.0 KB &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `pipeline_name` | character varying(100) | PK | N | — |
| 2 | `last_watermark` | timestamp with time zone |  | Y | — |
| 3 | `last_status` | character varying(20) |  | Y | — |
| 4 | `rows_processed` | integer |  | Y | 0 |
| 5 | `started_at` | timestamp with time zone |  | Y | — |
| 6 | `finished_at` | timestamp with time zone |  | Y | — |

### `grandes_ganhos`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 50 &nbsp;&nbsp; **Tamanho:** 80.0 KB &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.grandes_ganhos_id_seq'::regclass) |
| 2 | `game_name` | character varying(255) |  | Y | — |
| 3 | `provider_name` | character varying(100) |  | Y | — |
| 4 | `game_image_url` | character varying(500) |  | Y | — |
| 5 | `player_name_hashed` | character varying(50) |  | Y | — |
| 6 | `smr_user_id` | bigint |  | Y | — |
| 7 | `win_amount` | numeric(15,2) |  | Y | — |
| 8 | `event_time` | timestamp with time zone |  | Y | — |
| 9 | `refreshed_at` | timestamp with time zone |  | Y | — |
| 10 | `game_slug` | character varying(200) |  | Y | — |
| 12 | `ecr_id` | bigint |  | Y | — |

### `migrations`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** 32.0 KB &nbsp;&nbsp; **Colunas:** 3

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | integer | PK | N | nextval('multibet.migrations_id_seq'::regclass) |
| 2 | `timestamp` | bigint |  | N | — |
| 3 | `name` | character varying |  | N | — |

### `trackings`
**Tipo:** table &nbsp;&nbsp; **Linhas (est.):** 50.680 &nbsp;&nbsp; **Tamanho:** 10.5 MB &nbsp;&nbsp; **Colunas:** 7

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `id` | uuid | PK | N | — |
| 2 | `user_id` | character varying(255) |  | N | — |
| 3 | `utm_campaign` | character varying(255) |  | Y | — |
| 4 | `utm_content` | character varying(255) |  | Y | — |
| 5 | `utm_source` | character varying(255) |  | Y | — |
| 6 | `created_at` | timestamp without time zone |  | N | CURRENT_TIMESTAMP |
| 7 | `updated_at` | timestamp without time zone |  | N | CURRENT_TIMESTAMP |


## Views

### `active_users`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 4

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `game_id` | text |  | Y | — |
| 2 | `game_name` | character varying(255) |  | Y | — |
| 3 | `game_slug` | character varying(200) |  | Y | — |
| 4 | `active_users` | bigint |  | Y | — |

### `atualizacao`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 2

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `Tabela/ Dados` | character varying |  | Y | — |
| 2 | `update_time` | timestamp without time zone |  | Y | — |

### `cohort_aquisicao`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data_registro` | date |  | Y | — |
| 2 | `nome` | character varying(100) |  | Y | — |
| 3 | `data_dif` | integer |  | Y | — |
| 4 | `qtd_depositantes` | bigint |  | Y | — |
| 5 | `qtd_jogadores` | bigint |  | Y | — |
| 6 | `total_users` | bigint |  | Y | — |

### `cohort_retencao_ftd`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `ftd_data` | date |  | Y | — |
| 2 | `nome` | character varying(100) |  | Y | — |
| 3 | `data_dif` | integer |  | Y | — |
| 4 | `qtd_depositantes` | bigint |  | Y | — |
| 5 | `qtd_jogadores` | bigint |  | Y | — |
| 6 | `total_ftd` | bigint |  | Y | — |

### `game_paid_15min`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 3

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `date` | timestamp without time zone |  | Y | — |
| 2 | `game_id` | character varying(255) |  | Y | — |
| 3 | `total_win` | double precision |  | Y | — |

### `heatmap_hour`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `data_nome` | text |  | Y | — |
| 3 | `hour` | integer |  | Y | — |
| 4 | `dep_amount` | double precision |  | Y | — |
| 5 | `withdrawal_amount` | double precision |  | Y | — |
| 6 | `net_deposit` | double precision |  | Y | — |
| 7 | `ggr_cassino` | double precision |  | Y | — |
| 8 | `ggr_sport` | double precision |  | Y | — |
| 9 | `ngr` | double precision |  | Y | — |

### `jogo_total_pago_hoje`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 4

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `game_id` | text |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `total_paid` | double precision |  | Y | — |

### `matriz_aquisicao`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 20

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data_registro` | date |  | Y | — |
| 2 | `nome` | character varying(100) |  | Y | — |
| 3 | `users` | bigint |  | Y | — |
| 4 | `ftd` | bigint |  | Y | — |
| 5 | `ftd_amount` | numeric |  | Y | — |
| 6 | `std` | bigint |  | Y | — |
| 7 | `std_amount` | numeric |  | Y | — |
| 8 | `ttd` | bigint |  | Y | — |
| 9 | `ttd_amount` | numeric |  | Y | — |
| 10 | `qtd` | bigint |  | Y | — |
| 11 | `qtd_amount` | numeric |  | Y | — |
| 12 | `dep_amount` | numeric |  | Y | — |
| 13 | `withdrawal_amount` | numeric |  | Y | — |
| 14 | `net_deposit` | numeric |  | Y | — |
| 15 | `btr` | numeric |  | Y | — |
| 16 | `ngr` | numeric |  | Y | — |
| 17 | `cost` | numeric |  | Y | — |
| 18 | `cac` | numeric |  | Y | — |
| 19 | `cac_ftd` | numeric |  | Y | — |
| 20 | `retorno` | numeric |  | Y | — |

### `matriz_financeiro`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 26

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `deposit` | double precision |  | Y | — |
| 3 | `adpu` | double precision |  | Y | — |
| 4 | `avg_dep` | double precision |  | Y | — |
| 5 | `withdrawal` | double precision |  | Y | — |
| 6 | `net_deposit` | double precision |  | Y | — |
| 7 | `users` | integer |  | Y | — |
| 8 | `ftd` | integer |  | Y | — |
| 9 | `conversion` | double precision |  | Y | — |
| 10 | `ftd_amount` | double precision |  | Y | — |
| 11 | `avg_ftd_amount` | double precision |  | Y | — |
| 12 | `turnover_cassino` | double precision |  | Y | — |
| 13 | `win_cassino` | double precision |  | Y | — |
| 14 | `ggr_cassino` | double precision |  | Y | — |
| 15 | `turnover_sports` | double precision |  | Y | — |
| 16 | `win_sports` | double precision |  | Y | — |
| 17 | `ggr_sport` | double precision |  | Y | — |
| 18 | `ggr_total` | double precision |  | Y | — |
| 19 | `ngr` | double precision |  | Y | — |
| 20 | `retencao` | integer |  | Y | — |
| 21 | `arpu` | numeric |  | Y | — |
| 22 | `ativos` | integer |  | Y | — |
| 23 | `hold_cassino` | numeric |  | Y | — |
| 24 | `hold_sport` | numeric |  | Y | — |
| 25 | `btr_ggr` | numeric |  | Y | — |
| 26 | `hold_total` | numeric |  | Y | — |

### `matriz_financeiro_hora`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 22

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `hour` | integer |  | Y | — |
| 3 | `deposit` | double precision |  | Y | — |
| 4 | `adpu` | double precision |  | Y | — |
| 5 | `avg_dep` | double precision |  | Y | — |
| 6 | `withdrawal` | double precision |  | Y | — |
| 7 | `net_deposit` | double precision |  | Y | — |
| 8 | `users` | integer |  | Y | — |
| 9 | `ftd` | integer |  | Y | — |
| 10 | `conversion` | double precision |  | Y | — |
| 11 | `ftd_amount` | double precision |  | Y | — |
| 12 | `avg_ftd_amount` | double precision |  | Y | — |
| 13 | `turnover_cassino` | double precision |  | Y | — |
| 14 | `win_cassino` | double precision |  | Y | — |
| 15 | `ggr_cassino` | double precision |  | Y | — |
| 16 | `turnover_sports` | double precision |  | Y | — |
| 17 | `win_sports` | double precision |  | Y | — |
| 18 | `ggr_sports` | double precision |  | Y | — |
| 19 | `ggr_total` | double precision |  | Y | — |
| 20 | `ngr` | double precision |  | Y | — |
| 21 | `arpu` | numeric |  | Y | — |
| 22 | `ativos` | integer |  | Y | — |

### `matriz_financeiro_mensal`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 22

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `deposit` | numeric(15,2) |  | Y | — |
| 3 | `adpu` | numeric(15,2) |  | Y | — |
| 4 | `avg_dep` | numeric(15,2) |  | Y | — |
| 5 | `withdrawal` | numeric(15,2) |  | Y | — |
| 6 | `net_deposit` | numeric(15,2) |  | Y | — |
| 7 | `users` | bigint |  | Y | — |
| 8 | `ftd` | bigint |  | Y | — |
| 9 | `conversion` | numeric(10,2) |  | Y | — |
| 10 | `ftd_amount` | numeric(15,2) |  | Y | — |
| 11 | `avg_ftd_amount` | numeric(10,2) |  | Y | — |
| 12 | `turnover_cassino` | numeric(15,2) |  | Y | — |
| 13 | `win_cassino` | numeric(15,2) |  | Y | — |
| 14 | `ggr_cassino` | numeric(15,2) |  | Y | — |
| 15 | `turnover_sports` | numeric(15,2) |  | Y | — |
| 16 | `win_sports` | numeric(15,2) |  | Y | — |
| 17 | `ggr_sport` | numeric(15,2) |  | Y | — |
| 18 | `ggr_total` | numeric(15,2) |  | Y | — |
| 19 | `ngr` | numeric(15,2) |  | Y | — |
| 20 | `retencao` | numeric(15,2) |  | Y | — |
| 21 | `arpu` | numeric(10,2) |  | Y | — |
| 22 | `ativos` | integer |  | Y | — |

### `matriz_financeiro_semanal`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 22

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `deposit` | numeric(15,2) |  | Y | — |
| 3 | `adpu` | numeric(15,2) |  | Y | — |
| 4 | `avg_dep` | numeric(15,2) |  | Y | — |
| 5 | `withdrawal` | numeric(15,2) |  | Y | — |
| 6 | `net_deposit` | numeric(15,2) |  | Y | — |
| 7 | `users` | bigint |  | Y | — |
| 8 | `ftd` | bigint |  | Y | — |
| 9 | `conversion` | numeric(10,2) |  | Y | — |
| 10 | `ftd_amount` | numeric(15,2) |  | Y | — |
| 11 | `avg_ftd_amount` | numeric(10,2) |  | Y | — |
| 12 | `turnover_cassino` | numeric(15,2) |  | Y | — |
| 13 | `win_cassino` | numeric(15,2) |  | Y | — |
| 14 | `ggr_cassino` | numeric(15,2) |  | Y | — |
| 15 | `turnover_sports` | numeric(15,2) |  | Y | — |
| 16 | `win_sports` | numeric(15,2) |  | Y | — |
| 17 | `ggr_sport` | numeric(15,2) |  | Y | — |
| 18 | `ggr_total` | numeric(15,2) |  | Y | — |
| 19 | `ngr` | numeric(15,2) |  | Y | — |
| 20 | `retencao` | numeric(15,2) |  | Y | — |
| 21 | `arpu` | numeric(10,2) |  | Y | — |
| 22 | `ativos` | integer |  | Y | — |

### `matriz_risco`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `label_id` | character varying(50) |  | Y | — |
| 2 | `user_id` | character varying(50) |  | Y | — |
| 3 | `user_ext_id` | character varying(100) |  | Y | — |
| 4 | `snapshot_date` | date |  | Y | — |
| 5 | `score_bruto` | integer |  | Y | — |
| 6 | `score_norm` | numeric(5,1) |  | Y | — |
| 7 | `classificacao` | character varying(20) |  | Y | — |
| 8 | `computed_at` | timestamp without time zone |  | Y | — |

### `pcr_atual`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 25

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `snapshot_date` | date |  | Y | — |
| 2 | `player_id` | bigint |  | Y | — |
| 3 | `external_id` | bigint |  | Y | — |
| 4 | `rating` | character varying(2) |  | Y | — |
| 5 | `pvs` | numeric(8,2) |  | Y | — |
| 6 | `ggr_total` | numeric(15,2) |  | Y | — |
| 7 | `ngr_total` | numeric(15,2) |  | Y | — |
| 8 | `total_deposits` | numeric(15,2) |  | Y | — |
| 9 | `total_cashouts` | numeric(15,2) |  | Y | — |
| 10 | `num_deposits` | integer |  | Y | — |
| 11 | `days_active` | integer |  | Y | — |
| 12 | `recency_days` | integer |  | Y | — |
| 13 | `product_type` | character varying(10) |  | Y | — |
| 14 | `casino_rounds` | bigint |  | Y | — |
| 15 | `sport_bets` | bigint |  | Y | — |
| 16 | `bonus_issued` | numeric(15,2) |  | Y | — |
| 17 | `bonus_ratio` | numeric(8,4) |  | Y | — |
| 18 | `wd_ratio` | numeric(8,4) |  | Y | — |
| 19 | `net_deposit` | numeric(15,2) |  | Y | — |
| 20 | `margem_ggr` | numeric(8,4) |  | Y | — |
| 21 | `ggr_por_dia` | numeric(15,2) |  | Y | — |
| 22 | `affiliate_id` | character varying(300) |  | Y | — |
| 23 | `c_category` | character varying(50) |  | Y | — |
| 24 | `registration_date` | date |  | Y | — |
| 25 | `created_at` | timestamp with time zone |  | Y | — |

### `pcr_resumo`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `snapshot_date` | date |  | Y | — |
| 2 | `rating` | character varying(2) |  | Y | — |
| 3 | `jogadores` | bigint |  | Y | — |
| 4 | `pct_base` | numeric |  | Y | — |
| 5 | `ggr_total` | numeric |  | Y | — |
| 6 | `ggr_medio` | numeric |  | Y | — |
| 7 | `deposito_medio` | numeric |  | Y | — |
| 8 | `num_dep_medio` | numeric |  | Y | — |
| 9 | `dias_ativos_medio` | numeric |  | Y | — |
| 10 | `recencia_media` | numeric |  | Y | — |
| 11 | `pvs_medio` | numeric |  | Y | — |

### `top_jogadores_ganhos`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 7

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `game_id` | text |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `game_slug` | character varying(200) |  | Y | — |
| 5 | `name_complete` | character varying(255) |  | Y | — |
| 6 | `total_win_amount` | double precision |  | Y | — |
| 7 | `rank` | bigint |  | Y | — |

### `user_game_activity_30d`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `user_id` | character varying(255) |  | Y | — |
| 2 | `game_id` | character varying(255) |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `provider_id` | character varying(255) |  | Y | — |
| 5 | `last_played_at` | timestamp without time zone |  | Y | — |
| 6 | `qty_rounds` | integer |  | Y | — |
| 7 | `total_bet` | double precision |  | Y | — |
| 8 | `total_win` | double precision |  | Y | — |
| 9 | `days_active` | integer |  | Y | — |

### `vw_acquisition_channel`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `channel_tier` | text |  | Y | — |
| 3 | `source` | character varying |  | Y | — |
| 4 | `qty_registrations` | bigint |  | Y | — |
| 5 | `qty_ftds` | bigint |  | Y | — |
| 6 | `ggr` | numeric |  | Y | — |
| 7 | `marketing_spend` | numeric |  | Y | — |
| 8 | `ftd_rate` | numeric |  | Y | — |
| 9 | `roas` | numeric |  | Y | — |

### `vw_active_player_retention_weekly`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `semana` | date |  | Y | — |
| 2 | `semana_label` | character varying(15) |  | Y | — |
| 3 | `depositantes_semana_atual` | integer |  | Y | — |
| 4 | `depositantes_semana_anterior` | integer |  | Y | — |
| 5 | `retidos_da_semana_anterior` | integer |  | Y | — |
| 6 | `repeat_depositors` | integer |  | Y | — |
| 7 | `retention_pct` | numeric(5,1) |  | Y | — |
| 8 | `repeat_depositor_pct` | numeric(5,1) |  | Y | — |
| 9 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_active_players_period`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 7

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `period` | character varying(15) |  | Y | — |
| 2 | `period_label` | character varying(40) |  | Y | — |
| 3 | `period_start` | date |  | Y | — |
| 4 | `period_end` | date |  | Y | — |
| 5 | `product` | character varying(15) |  | Y | — |
| 6 | `unique_players` | integer |  | Y | — |
| 7 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_ad_spend_by_source`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `ad_source` | character varying(50) |  | Y | — |
| 2 | `dt_min` | date |  | Y | — |
| 3 | `dt_max` | date |  | Y | — |
| 4 | `dias` | bigint |  | Y | — |
| 5 | `cost_brl_total` | numeric |  | Y | — |
| 6 | `impressions_total` | bigint |  | Y | — |
| 7 | `clicks_total` | bigint |  | Y | — |
| 8 | `conversions_total` | numeric |  | Y | — |
| 9 | `campaigns` | bigint |  | Y | — |
| 10 | `cpc_medio` | numeric |  | Y | — |
| 11 | `cpa_medio` | numeric |  | Y | — |

### `vw_ad_spend_daily`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 10

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `ad_source` | character varying(50) |  | Y | — |
| 3 | `affiliate_id` | character varying |  | Y | — |
| 4 | `cost_brl` | numeric |  | Y | — |
| 5 | `impressions` | bigint |  | Y | — |
| 6 | `clicks` | bigint |  | Y | — |
| 7 | `conversions` | numeric |  | Y | — |
| 8 | `campaigns` | bigint |  | Y | — |
| 9 | `cpc` | numeric |  | Y | — |
| 10 | `ctr_pct` | numeric |  | Y | — |

### `vw_aquisicao_trafego`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 17

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `canal` | character varying(30) |  | Y | — |
| 3 | `cadastros` | integer |  | Y | — |
| 4 | `ftd` | integer |  | Y | — |
| 5 | `ftd_amount` | numeric(14,2) |  | Y | — |
| 6 | `conversao_ftd_pct` | numeric(5,1) |  | Y | — |
| 7 | `ticket_medio_ftd` | numeric(14,2) |  | Y | — |
| 8 | `deposito` | numeric(14,2) |  | Y | — |
| 9 | `saque` | numeric(14,2) |  | Y | — |
| 10 | `net_deposit` | numeric(14,2) |  | Y | — |
| 11 | `ggr_casino` | numeric(14,2) |  | Y | — |
| 12 | `ggr_sport` | numeric(14,2) |  | Y | — |
| 13 | `ggr_total` | numeric |  | Y | — |
| 14 | `bonus_cost` | numeric(14,2) |  | Y | — |
| 15 | `ngr` | numeric(14,2) |  | Y | — |
| 16 | `players_ativos` | integer |  | Y | — |
| 17 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_attribution_metrics`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `c_tracker_id` | character varying(255) |  | Y | — |
| 3 | `qty_registrations` | integer |  | Y | — |
| 4 | `qty_ftds` | integer |  | Y | — |
| 5 | `ggr` | numeric(18,2) |  | Y | — |
| 6 | `marketing_spend` | numeric(18,2) |  | Y | — |
| 7 | `cpa` | numeric |  | Y | — |
| 8 | `cac` | numeric |  | Y | — |
| 9 | `roas` | numeric |  | Y | — |
| 10 | `roi_pct` | numeric |  | Y | — |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_casino_by_category`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 12

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `category` | text |  | Y | — |
| 3 | `qty_games` | bigint |  | Y | — |
| 4 | `qty_players` | bigint |  | Y | — |
| 5 | `total_rounds` | bigint |  | Y | — |
| 6 | `turnover_real` | numeric |  | Y | — |
| 7 | `wins_real` | numeric |  | Y | — |
| 8 | `ggr_real` | numeric |  | Y | — |
| 9 | `ggr_total` | numeric |  | Y | — |
| 10 | `hold_rate_pct` | numeric |  | Y | — |
| 11 | `rtp_pct` | numeric |  | Y | — |
| 12 | `jackpot_win` | numeric |  | Y | — |

### `vw_casino_by_provider`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `provider` | text |  | Y | — |
| 3 | `qty_games` | bigint |  | Y | — |
| 4 | `qty_players` | bigint |  | Y | — |
| 5 | `total_rounds` | bigint |  | Y | — |
| 6 | `turnover_real` | numeric |  | Y | — |
| 7 | `wins_real` | numeric |  | Y | — |
| 8 | `ggr_real` | numeric |  | Y | — |
| 9 | `turnover_total` | numeric |  | Y | — |
| 10 | `ggr_total` | numeric |  | Y | — |
| 11 | `hold_rate_pct` | numeric |  | Y | — |
| 12 | `rtp_pct` | numeric |  | Y | — |
| 13 | `jackpot_win` | numeric |  | Y | — |
| 14 | `jackpot_contribution` | numeric |  | Y | — |
| 15 | `free_spins_bet` | numeric |  | Y | — |
| 16 | `free_spins_win` | numeric |  | Y | — |

### `vw_casino_kpis`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 15

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `qty_players` | integer |  | Y | — |
| 3 | `total_rounds` | bigint |  | Y | — |
| 4 | `casino_real_bet` | numeric(18,2) |  | Y | — |
| 5 | `casino_bonus_bet` | numeric(18,2) |  | Y | — |
| 6 | `casino_total_bet` | numeric(18,2) |  | Y | — |
| 7 | `casino_real_win` | numeric(18,2) |  | Y | — |
| 8 | `casino_bonus_win` | numeric(18,2) |  | Y | — |
| 9 | `casino_total_win` | numeric(18,2) |  | Y | — |
| 10 | `casino_real_ggr` | numeric(18,2) |  | Y | — |
| 11 | `casino_bonus_ggr` | numeric(18,2) |  | Y | — |
| 12 | `casino_total_ggr` | numeric(18,2) |  | Y | — |
| 13 | `ggr_per_player` | numeric |  | Y | — |
| 14 | `hold_rate_pct` | numeric |  | Y | — |
| 15 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_casino_top_games`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `game_id` | character varying(50) |  | Y | — |
| 3 | `game_name` | character varying |  | Y | — |
| 4 | `provider` | text |  | Y | — |
| 5 | `category` | text |  | Y | — |
| 6 | `qty_players` | integer |  | Y | — |
| 7 | `total_rounds` | integer |  | Y | — |
| 8 | `rounds_per_player` | numeric(10,2) |  | Y | — |
| 9 | `turnover_real` | numeric(18,2) |  | Y | — |
| 10 | `wins_real` | numeric(18,2) |  | Y | — |
| 11 | `ggr_real` | numeric(18,2) |  | Y | — |
| 12 | `hold_rate_pct` | numeric(10,4) |  | Y | — |
| 13 | `rtp_pct` | numeric(10,4) |  | Y | — |
| 14 | `jackpot_win` | numeric(18,2) |  | Y | — |
| 15 | `free_spins_bet` | numeric(18,2) |  | Y | — |
| 16 | `free_spins_win` | numeric(18,2) |  | Y | — |

### `vw_cohort_roi`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 13

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `month_of_ftd` | character varying(7) |  | Y | — |
| 2 | `source` | character varying(100) |  | Y | — |
| 3 | `qty_players` | bigint |  | Y | — |
| 4 | `avg_ftd_amount` | numeric |  | Y | — |
| 5 | `total_ggr_d0` | numeric |  | Y | — |
| 6 | `total_ggr_d7` | numeric |  | Y | — |
| 7 | `total_ggr_d30` | numeric |  | Y | — |
| 8 | `avg_ltv_d30` | numeric |  | Y | — |
| 9 | `pct_2nd_deposit` | numeric |  | Y | — |
| 10 | `monthly_spend` | numeric |  | Y | — |
| 11 | `roi_d30_pct` | numeric |  | Y | — |
| 12 | `payback_ratio` | numeric |  | Y | — |
| 13 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_front_api_games`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `gameId` | character varying(50) |  | Y | — |
| 2 | `name` | character varying(255) |  | Y | — |
| 3 | `gameSlug` | character varying(200) |  | Y | — |
| 4 | `gamePath` | character varying(200) |  | Y | — |
| 5 | `image` | character varying(500) |  | Y | — |
| 6 | `provider` | character varying(100) |  | Y | — |
| 7 | `category` | character varying(30) |  | Y | — |
| 8 | `categoryDescription` | character varying(50) |  | Y | — |
| 9 | `totalBets` | bigint |  | Y | — |
| 10 | `uniquePlayers` | integer |  | Y | — |
| 11 | `totalBet` | numeric |  | Y | — |
| 12 | `totalWins` | numeric |  | Y | — |
| 13 | `rank` | integer |  | Y | — |
| 14 | `live_subtype` | character varying(30) |  | Y | — |
| 15 | `has_jackpot` | boolean |  | Y | — |
| 16 | `windowEndUtc` | timestamp with time zone |  | Y | — |

### `vw_front_by_category`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 10

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `category` | character varying(30) |  | Y | — |
| 2 | `category_desc` | character varying(50) |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `vendor` | character varying(100) |  | Y | — |
| 5 | `live_subtype` | character varying(30) |  | Y | — |
| 6 | `image_url` | character varying(500) |  | Y | — |
| 7 | `slug` | character varying(200) |  | Y | — |
| 8 | `rounds_24h` | bigint |  | Y | — |
| 9 | `rank` | integer |  | Y | — |
| 10 | `has_jackpot` | boolean |  | Y | — |

### `vw_front_by_vendor`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 10

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `vendor` | character varying(100) |  | Y | — |
| 2 | `sub_vendor` | character varying(50) |  | Y | — |
| 3 | `game_name` | character varying(255) |  | Y | — |
| 4 | `category` | character varying(30) |  | Y | — |
| 5 | `live_subtype` | character varying(30) |  | Y | — |
| 6 | `image_url` | character varying(500) |  | Y | — |
| 7 | `slug` | character varying(200) |  | Y | — |
| 8 | `rounds_24h` | bigint |  | Y | — |
| 9 | `rank` | integer |  | Y | — |
| 10 | `has_jackpot` | boolean |  | Y | — |

### `vw_front_jackpot`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 7

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `game_name` | character varying(255) |  | Y | — |
| 2 | `vendor` | character varying(100) |  | Y | — |
| 3 | `category` | character varying(30) |  | Y | — |
| 4 | `image_url` | character varying(500) |  | Y | — |
| 5 | `slug` | character varying(200) |  | Y | — |
| 6 | `rounds_24h` | bigint |  | Y | — |
| 7 | `rank` | integer |  | Y | — |

### `vw_front_live_casino`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 10

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `game_name` | character varying(255) |  | Y | — |
| 2 | `vendor` | character varying(100) |  | Y | — |
| 3 | `sub_vendor` | character varying(50) |  | Y | — |
| 4 | `live_subtype` | character varying(30) |  | Y | — |
| 5 | `subtipo_raw` | character varying(100) |  | Y | — |
| 6 | `image_url` | character varying(500) |  | Y | — |
| 7 | `slug` | character varying(200) |  | Y | — |
| 8 | `rounds_24h` | bigint |  | Y | — |
| 9 | `players_24h` | integer |  | Y | — |
| 10 | `rank` | integer |  | Y | — |

### `vw_front_top_24h`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `rank` | integer |  | Y | — |
| 2 | `game_name` | character varying(255) |  | Y | — |
| 3 | `vendor` | character varying(100) |  | Y | — |
| 4 | `sub_vendor` | character varying(50) |  | Y | — |
| 5 | `category` | character varying(30) |  | Y | — |
| 6 | `live_subtype` | character varying(30) |  | Y | — |
| 7 | `image_url` | character varying(500) |  | Y | — |
| 8 | `slug` | character varying(200) |  | Y | — |
| 9 | `rounds_24h` | bigint |  | Y | — |
| 10 | `players_24h` | integer |  | Y | — |
| 11 | `window_end_utc` | timestamp with time zone |  | Y | — |

### `vw_ltv_cac_ratio`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `month_of_ftd` | character varying(7) |  | Y | — |
| 2 | `source` | character varying(100) |  | Y | — |
| 3 | `qty_players` | bigint |  | Y | — |
| 4 | `avg_ltv_d30` | numeric |  | Y | — |
| 5 | `cac` | numeric |  | Y | — |
| 6 | `ltv_cac_ratio` | numeric |  | Y | — |

### `vw_odds_performance_by_range`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 15

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `odds_range` | character varying(20) |  | Y | — |
| 2 | `odds_order` | smallint |  | Y | — |
| 3 | `total_bets` | bigint |  | Y | — |
| 4 | `bets_casa_ganha` | bigint |  | Y | — |
| 5 | `bets_casa_perde` | bigint |  | Y | — |
| 6 | `pct_casa_ganha` | numeric |  | Y | — |
| 7 | `total_stake` | numeric |  | Y | — |
| 8 | `total_payout` | numeric |  | Y | — |
| 9 | `ggr` | numeric |  | Y | — |
| 10 | `hold_rate_pct` | numeric |  | Y | — |
| 11 | `avg_ticket` | numeric |  | Y | — |
| 12 | `pct_ggr_total` | numeric |  | Y | — |
| 13 | `first_dt` | date |  | Y | — |
| 14 | `last_dt` | date |  | Y | — |
| 15 | `last_refresh` | timestamp with time zone |  | Y | — |

### `vw_odds_performance_summary`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 17

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `odds_range` | character varying(20) |  | Y | — |
| 2 | `odds_order` | smallint |  | Y | — |
| 3 | `bet_mode` | character varying(10) |  | Y | — |
| 4 | `total_bets` | bigint |  | Y | — |
| 5 | `unique_players_sum` | bigint |  | Y | — |
| 6 | `bets_casa_ganha` | bigint |  | Y | — |
| 7 | `bets_casa_perde` | bigint |  | Y | — |
| 8 | `pct_casa_ganha` | numeric |  | Y | — |
| 9 | `total_stake` | numeric |  | Y | — |
| 10 | `total_payout` | numeric |  | Y | — |
| 11 | `ggr` | numeric |  | Y | — |
| 12 | `hold_rate_pct` | numeric |  | Y | — |
| 13 | `avg_ticket` | numeric |  | Y | — |
| 14 | `first_dt` | date |  | Y | — |
| 15 | `last_dt` | date |  | Y | — |
| 16 | `dias_cobertos` | integer |  | Y | — |
| 17 | `last_refresh` | timestamp with time zone |  | Y | — |

### `vw_player_performance_period`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 11

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `user_id` | bigint |  | Y | — |
| 2 | `period` | character varying(15) |  | Y | — |
| 3 | `period_label` | character varying(40) |  | Y | — |
| 4 | `period_start` | date |  | Y | — |
| 5 | `period_end` | date |  | Y | — |
| 6 | `vertical` | character varying(15) |  | Y | — |
| 7 | `player_result` | numeric(18,2) |  | Y | — |
| 8 | `turnover` | numeric(18,2) |  | Y | — |
| 9 | `deposit_total` | numeric(18,2) |  | Y | — |
| 10 | `qty_sessions` | integer |  | Y | — |
| 11 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_roi_by_source`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 20

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `ad_source` | character varying(50) |  | Y | — |
| 3 | `affiliate_id` | character varying |  | Y | — |
| 4 | `cost_brl` | numeric |  | Y | — |
| 5 | `impressions` | bigint |  | Y | — |
| 6 | `clicks` | bigint |  | Y | — |
| 7 | `conversions_google` | numeric |  | Y | — |
| 8 | `registrations` | integer |  | Y | — |
| 9 | `ftds` | integer |  | Y | — |
| 10 | `ftd_amount_brl` | numeric |  | Y | — |
| 11 | `dep_amount_brl` | numeric |  | Y | — |
| 12 | `net_deposit_brl` | numeric |  | Y | — |
| 13 | `ggr_total_brl` | numeric |  | Y | — |
| 14 | `bonus_cost_brl` | numeric |  | Y | — |
| 15 | `ngr_brl` | numeric |  | Y | — |
| 16 | `cpc` | numeric |  | Y | — |
| 17 | `cpa_ftd` | numeric |  | Y | — |
| 18 | `cpa_reg` | numeric |  | Y | — |
| 19 | `roi_pct` | numeric |  | Y | — |
| 20 | `profit_brl` | numeric |  | Y | — |

### `vw_segmentacao_hibrida`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 34

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `user_id` | character varying(50) |  | Y | — |
| 2 | `signup_date` | timestamp without time zone |  | Y | — |
| 3 | `status` | character varying(50) |  | Y | — |
| 4 | `score_original` | numeric(5,1) |  | Y | — |
| 5 | `score_final` | numeric |  | Y | — |
| 6 | `delta_score` | numeric |  | Y | — |
| 7 | `segmento_original` | character varying(20) |  | Y | — |
| 8 | `segmento_final` | text |  | Y | — |
| 9 | `mudanca_segmento` | text |  | Y | — |
| 10 | `classificacao_risco` | text |  | Y | — |
| 11 | `score_bruto_risco` | integer |  | Y | — |
| 12 | `ggr_total` | numeric(12,2) |  | Y | — |
| 13 | `deposit_lifetime` | numeric(12,2) |  | Y | — |
| 14 | `deposit_count` | integer |  | Y | — |
| 15 | `days_active` | integer |  | Y | — |
| 16 | `recency_days` | integer |  | Y | — |
| 17 | `tem_fast_cashout` | boolean |  | Y | — |
| 18 | `tem_potencial_abuser` | boolean |  | Y | — |
| 19 | `tem_rollback` | boolean |  | Y | — |
| 20 | `tem_cashout_run` | boolean |  | Y | — |
| 21 | `tem_promo_only` | boolean |  | Y | — |
| 22 | `tem_multi_game` | boolean |  | Y | — |
| 23 | `is_mixed_player` | boolean |  | Y | — |
| 24 | `is_active_recent` | boolean |  | Y | — |
| 25 | `is_high_priority` | boolean |  | Y | — |
| 26 | `is_blocked` | boolean |  | Y | — |
| 27 | `activity_rate_pct` | numeric(5,2) |  | Y | — |
| 28 | `margin_ggr_turnover` | numeric(8,4) |  | Y | — |
| 29 | `bonus_sensitivity_pct` | numeric(5,2) |  | Y | — |
| 30 | `withdrawal_deposit_ratio` | numeric(8,4) |  | Y | — |
| 31 | `computed_at` | timestamp without time zone |  | Y | — |
| 32 | `risk_computed_at` | timestamp without time zone |  | Y | — |
| 33 | `usuarios_no_segmento` | bigint |  | Y | — |
| 34 | `ggr_total_segmento` | numeric |  | Y | — |

### `vw_sportsbook_by_sport`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 19

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `sport_name` | text |  | Y | — |
| 3 | `sport_category` | text |  | Y | — |
| 4 | `qty_bets` | integer |  | Y | — |
| 5 | `qty_players` | integer |  | Y | — |
| 6 | `turnover` | numeric(18,2) |  | Y | — |
| 7 | `total_return` | numeric(18,2) |  | Y | — |
| 8 | `ggr` | numeric(18,2) |  | Y | — |
| 9 | `margin_pct` | numeric(10,4) |  | Y | — |
| 10 | `avg_ticket` | numeric(18,2) |  | Y | — |
| 11 | `avg_odds` | double precision |  | Y | — |
| 12 | `qty_pre_match` | integer |  | Y | — |
| 13 | `qty_live` | integer |  | Y | — |
| 14 | `turnover_pre_match` | numeric(18,2) |  | Y | — |
| 15 | `turnover_live` | numeric(18,2) |  | Y | — |
| 16 | `pct_pre_match` | numeric(10,4) |  | Y | — |
| 17 | `pct_live` | numeric(10,4) |  | Y | — |
| 18 | `ggr_per_player` | numeric |  | Y | — |
| 19 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_sportsbook_exposure`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `snapshot_dt` | date |  | Y | — |
| 2 | `sport_name` | text |  | Y | — |
| 3 | `qty_open_bets` | integer |  | Y | — |
| 4 | `total_stake_open` | double precision |  | Y | — |
| 5 | `avg_odds_open` | double precision |  | Y | — |
| 6 | `projected_liability` | double precision |  | Y | — |
| 7 | `projected_ggr` | double precision |  | Y | — |
| 8 | `pct_stake_total` | numeric |  | Y | — |
| 9 | `refreshed_at` | timestamp with time zone |  | Y | — |

### `vw_sportsbook_kpis`
**Tipo:** view &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 16

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `dt` | date |  | Y | — |
| 2 | `qty_players` | integer |  | Y | — |
| 3 | `qty_bets` | integer |  | Y | — |
| 4 | `sports_real_bet` | numeric(18,2) |  | Y | — |
| 5 | `sports_bonus_bet` | numeric(18,2) |  | Y | — |
| 6 | `sports_total_bet` | numeric(18,2) |  | Y | — |
| 7 | `sports_real_win` | numeric(18,2) |  | Y | — |
| 8 | `sports_bonus_win` | numeric(18,2) |  | Y | — |
| 9 | `sports_total_win` | numeric(18,2) |  | Y | — |
| 10 | `sports_real_ggr` | numeric(18,2) |  | Y | — |
| 11 | `sports_bonus_ggr` | numeric(18,2) |  | Y | — |
| 12 | `sports_total_ggr` | numeric(18,2) |  | Y | — |
| 13 | `ggr_per_player` | numeric |  | Y | — |
| 14 | `avg_ticket` | numeric |  | Y | — |
| 15 | `margin_pct` | numeric |  | Y | — |
| 16 | `refreshed_at` | timestamp with time zone |  | Y | — |
