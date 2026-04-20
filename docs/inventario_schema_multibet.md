# Inventario Schema `multibet` + `play4` — Super Nova DB

**Data:** 2026-04-19 (v2 — refresh automatico)
**Responsavel:** Mateus Fabro (Squad Intelligence Engine)
**Fonte:** `scripts/inventario_schema_refresh.py` rodado 19/04/2026 contra Super Nova DB
**Host:** `supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com:5432` / DB `supernova_db`
**JSON bruto:** `reports/inventario_schema_refresh_20260419.json`

---

## Resumo Quantitativo

| Schema  | Tipo | 30/03/2026 (v1) | 19/04/2026 (v2) | Delta |
|---------|------|-----------------|------------------|-------|
| multibet | Tabelas | ~54 | **74**  | +20 |
| multibet | Views | 8 | **43** | +35 |
| multibet | Matviews | 0 | **3**  | +3 |
| multibet | **Subtotal** | **~62** | **120** | **+58** |
| play4   | Foreign tables | 9 | **10** | +1 |
| **Total geral** | | **~71** | **130** | **+59** |

**Maiores mudancas estruturais:**
- **Camada Bronze descontinuada** — todas as 24 tabelas `bronze_*` do inventario anterior foram removidas. O time passou a consumir Athena diretamente nos pipelines (leitura) e persistir apenas agregados no Super Nova DB.
- **Explosao de views** (+35) — consolidacao de fronts (CTO Castrin), matrizes financeiras, PCR, odds performance, segmentacao hibrida.
- **Novas tabelas massivas:** `risk_tags` (2.37M rows, 793MB), `tab_user_daily` (4.94M, 1.35GB), `tab_dep_user` (1.68M, 1.18GB), `fct_player_performance_by_period` (693K), `silver_jogadores_ganhos` (2.10M).

---

## 1. SCHEMA `multibet` — camadas

Convencao de prefixos consolidada:
- `fact_*`, `fct_*`  → fatos (dia/player/evento)
- `agg_*`            → agregacoes derivadas
- `dim_*`            → dimensoes
- `silver_*`         → staging intermediario
- `tab_*`            → tabelas auxiliares das matrizes (base de views)
- `vw_*`             → views analiticas
- `vw_front_*`       → views para camada de apresentacao (CTO/Produto)
- `mv_*`             → materialized views
- `crm_*`            → tabelas CRM v1 (report diario)
- `pcr_*`, `risk_*`, `segment_*` → risco / segmentacao / CRM push

### 1.1 Materialized views (3)

| Nome | Linhas (est.) | Tamanho | Colunas | Proposito |
|------|---------------|---------|---------|-----------|
| `mv_aquisicao` | 2.238 | 803 KB | — | Snapshot de aquisicao (refresh manual) |
| `mv_cohort_aquisicao` | 23.616 | 5 MB | — | Cohort de aquisicao por safra |
| `mv_cohort_retencao_ftd` | 58.016 | 13 MB | — | Retencao por cohort de FTD |

### 1.2 Tabelas fact (produto & performance)

| Tabela | Linhas (est.) | Tamanho | Cols | Pipeline |
|--------|---------------|---------|------|----------|
| `fact_casino_rounds` | 122.910 | 37 MB | 25 | `pipelines/fact_casino_rounds.py` |
| `fact_sports_bets` | 172 | 82 KB | 17 | `pipelines/fact_sports_bets.py` |
| `fact_sports_bets_by_sport` | 1.961 | 647 KB | 17 | `pipelines/fact_sports_bets.py` |
| `fact_sports_open_bets` | 0 | 24 KB | 8 | `pipelines/fact_sports_bets.py` |
| `fact_sports_odds_performance` | 879 | 303 KB | 16 | `ec2_deploy/pipelines/fact_sports_odds_performance.py` (EC2 05:00 BRT) |
| `fact_live_casino` | 11.674 | 3 MB | 17 | `pipelines/fact_live_casino.py` |
| `fact_jackpots` | 0 | 57 KB | 12 | `pipelines/fact_jackpots.py` |
| `fct_casino_activity` | 174 | 66 KB | 12 | `pipelines/fct_casino_activity.py` |
| `fct_sports_activity` | 173 | 66 KB | 13 | `pipelines/fct_sports_activity.py` |
| `fct_active_players_by_period` | 18 | 24 KB | 7 | — (novo 2026-04) |
| `fct_player_performance_by_period` | 693.429 | 218 MB | 11 | — (novo 2026-04) |
| `mv_top_sports_events_daily` | 19.598 | 9 MB | 16 | (tabela, nome legado com prefixo mv_) |
| `mv_sports_open_bets_by_odds` | 0 | 24 KB | 6 | (tabela, nome legado) |

### 1.3 Tabelas fact (player & aquisicao)

| Tabela | Linhas (est.) | Tamanho | Cols | Pipeline |
|--------|---------------|---------|------|----------|
| `fact_player_activity` | 142 | 82 KB | 11 | `pipelines/fact_player_activity.py` |
| `fact_gaming_activity_daily` | 116.739 | 56 MB | 15 | `pipelines/fact_gaming_activity_daily.py` |
| `fact_player_engagement_daily` | 154.651 | 25 MB | 14 | `pipelines/fact_player_engagement_daily.py` |
| `fact_redeposits` | 154.980 | 23 MB | 14 | `pipelines/fact_redeposits.py` |
| `fact_registrations` | 171 | 57 KB | 11 | `pipelines/fact_registrations.py` |
| `fact_ftd_deposits` | 29.833 | 12 MB | 11 | `pipelines/fact_ftd_deposits.py` |
| `fact_attribution` | 154.684 | 70 MB | 8 | `pipelines/fact_attribution.py` |
| `fact_affiliate_revenue` | 0 | 24 KB | 14 | — (novo, pendente carga) |

### 1.4 Tabelas CRM

| Tabela | Linhas (est.) | Tamanho | Cols | Pipeline / Obs |
|--------|---------------|---------|------|-----------|
| `fact_crm_daily_performance` | 2.179 | 3 MB | 13 | `pipelines/crm_daily_performance.py` (versao principal, JSONB) |
| `dim_crm_friendly_names` | 1.443 | 336 KB | 6 | De-Para de campanhas |
| `crm_campaign_daily` | 3.037 | 2 MB | 48 | v1 |
| `crm_campaign_segment_daily` | 0 | 24 KB | 12 | v1 |
| `crm_campaign_game_daily` | 20 | 106 KB | 11 | v1 |
| `crm_campaign_comparison` | 0 | 24 KB | 13 | v1 |
| `crm_dispatch_budget` | 0 | 57 KB | 12 | Custos fixos (SMS/WhatsApp) |
| `crm_vip_group_daily` | 0 | 57 KB | 9 | v1 |
| `crm_recovery_daily` | 0 | 57 KB | 11 | v1 |
| `crm_player_vip_tier` | 0 | 33 KB | 8 | Elite/Key Account/High Value |

### 1.5 Dimensoes & mapeamento

| Tabela | Linhas (est.) | Tamanho | Cols | Pipeline |
|--------|---------------|---------|------|----------|
| `dim_games_catalog` | 381 | 156 KB | 16 | `pipelines/dim_games_catalog.py` |
| `game_image_mapping` | 2.715 | 2 MB | 23 | `pipelines/game_image_mapper.py` (enriquecido v3 04/2026) |
| `dim_marketing_mapping` | 3.241 | 2 MB | 9 | `pipelines/dim_marketing_mapping_canonical.py` |
| `dim_marketing_mapping_bkp_20260319` | 0 | 16 KB | 5 | Backup anterior ao canonical (pode ser arquivada) |
| `dim_campaign_affiliate` | 0 | 33 KB | 6 | `pipelines/sync_google_ads_spend.py` |
| `dim_affiliate_source` | 4.579 | 1 MB | 8 | Mapa canonico affiliate→source (Google/Meta/TikTok/organico) |

### 1.6 Agregacoes

| Tabela | Linhas (est.) | Tamanho | Cols | Pipeline |
|--------|---------------|---------|------|----------|
| `agg_cohort_acquisition` | 203.413 | 37 MB | 11 | `pipelines/agg_cohort_acquisition.py` |
| `agg_game_performance` | 27.622 | 6 MB | 17 | `pipelines/agg_game_performance.py` |
| `agg_btr_by_utm_campaign` | 51 | 33 KB | 11 | `scripts/btr_by_utm_campaign.py` |

### 1.7 Ad spend & trafego

| Tabela | Linhas (est.) | Tamanho | Cols | Pipeline |
|--------|---------------|---------|------|----------|
| `fact_ad_spend` | 2.605 | 1 MB | 11 | `ec2_deploy/pipelines/sync_meta_spend.py` + `pipelines/sync_google_ads_spend.py` (Google+Meta, EC2 01:00/01:15 BRT) |
| `aquisicao_trafego_diario` | 111 | 123 KB | 16 | `pipelines/etl_aquisicao_trafego_diario.py` (cron 60min) |
| `trackings` | 50.680 | 11 MB | 7 | Espelho da tabela original da plataforma Super Nova (UTMs de aquisicao por jogador) |

### 1.8 Risco, PCR & segmentacao

| Tabela | Linhas (est.) | Tamanho | Cols | Pipeline / Obs |
|--------|---------------|---------|------|-----------|
| `risk_tags` | 2.376.997 | 793 MB | 30 | Matriz Risco v2 — tags agregadas dos 155K jogadores + push S2S Smartico (EC2 02:30 BRT) |
| `risk_tags_pgs` | 398.570 | 131 MB | 26 | Variante filtrada (provedor/segmento) |
| `segment_tags` | 6.373 | 2 MB | 26 | Tags de segmentacao (Smartico push) |
| `pcr_ratings` | 151.572 | 37 MB | 25 | Player Credit Rating D-AAA (valor+risco+outlook) — HTML v1 pronto |

### 1.9 Silver & staging

| Tabela | Linhas (est.) | Tamanho | Cols | Uso |
|--------|---------------|---------|------|-----|
| `silver_game_activity` | 332.550 | 43 MB | 9 | Atividade por jogo x dia |
| `silver_game_15min` | 110 | 49 KB | 9 | Granularidade 15min (live ops) |
| `silver_jogadores_ganhos` | 2.105.849 | 272 MB | 8 | Historico de ganhos por jogador |
| `silver_jogos_jogadores_ativos` | 7.266 | 655 KB | 5 | Base jogo x jogador ativo |
| `silver_tab_user_ftd` | 217.298 | 27 MB | 3 | Staging FTD por jogador |

### 1.10 Tabelas auxiliares das matrizes (`tab_*`)

Base das views `matriz_*` (diario, semanal, mensal, hora).

| Tabela | Linhas (est.) | Tamanho | Cols | Descricao |
|--------|---------------|---------|------|-----------|
| `tab_user_daily` | 4.940.444 | **1.35 GB** | 14 | Fato diario por jogador (maior tabela do schema) |
| `tab_dep_user` | 1.679.098 | **1.18 GB** | 4 | Depositos por jogador |
| `tab_user_affiliate` | 1.100.181 | 167 MB | 4 | Relacao jogador x afiliado |
| `tab_with_user` | 381.826 | 192 MB | 2 | Saques por jogador |
| `tab_user_ftd` | 475 | 238 KB | 7 | FTD metricas por dia |
| `tab_dep_with` | 474 | 131 KB | 10 | Dep+saque por dia |
| `tab_affiliate` | 3.233 | 475 KB | 2 | Afiliados base |
| `tab_ativos` | 176 | 98 KB | 2 | Jogadores ativos (betting) por dia |
| `tab_cassino` | 176 | 213 KB | 5 | KPIs casino por dia |
| `tab_sports` | 174 | 82 KB | 9 | KPIs sports por dia |
| `tab_btr` | 173 | 115 KB | 2 | BTR (Bonus Turnover Ratio) por dia |
| `tab_atualizacao` | 0 | 8 KB | 3 | Controle de ultima atualizacao |
| `tab_hour_ativos` | 4.144 | 1 MB | 3 | Granularidade horaria |
| `tab_hour_cassino` | 4.156 | 2 MB | 6 | Granularidade horaria |
| `tab_hour_sports` | 4.132 | 2 MB | 10 | Granularidade horaria |
| `tab_hour_dep_with` | 11.329 | 6 MB | 11 | Granularidade horaria |
| `tab_hour_user_ftd` | 11.326 | 4 MB | 8 | Granularidade horaria |

### 1.11 Operacionais / ETL / utilitarios

| Tabela | Linhas (est.) | Tamanho | Cols | Uso |
|--------|---------------|---------|------|-----|
| `grandes_ganhos` | 50 | 82 KB | 11 | Big wins diarios (cron 00:30 BRT) |
| `etl_active_player_retention_weekly` | 26 | 24 KB | 9 | Base da view semanal de retencao |
| `etl_control` | 0 | 24 KB | 6 | Controle generico de ETL |
| `migrations` | 0 | 33 KB | 3 | Historico de migracoes DDL |

---

## 2. Views (43)

### 2.1 Matrizes financeiras

| View | Cols | Proposito |
|------|------|-----------|
| `matriz_financeiro` | 26 | KPIs financeiros agregados (diario) |
| `matriz_financeiro_hora` | 22 | KPIs financeiros por hora |
| `matriz_financeiro_semanal` | 22 | KPIs semanais |
| `matriz_financeiro_mensal` | 22 | KPIs mensais |
| `matriz_aquisicao` | 20 | Aquisicao consolidada |
| `matriz_risco` | 8 | Matriz de risco v2 (Smartico push) |

### 2.2 Casino & Sportsbook

| View | Cols | Proposito |
|------|------|-----------|
| `vw_casino_kpis` | 15 | KPIs casino diarios (GGR/Jogador, Hold Rate) |
| `vw_casino_by_provider` | 16 | Casino por provedor |
| `vw_casino_by_category` | 12 | Slots vs Live vs Outros |
| `vw_casino_top_games` | 16 | Detalhe por jogo |
| `vw_sportsbook_kpis` | 16 | KPIs sportsbook diarios |
| `vw_sportsbook_by_sport` | 19 | Por esporte (46 esportes, pre/live) |
| `vw_sportsbook_exposure` | 9 | Risco por esporte (apostas abertas) |
| `vw_odds_performance_by_range` | 15 | Win/Loss por faixa de odds |
| `vw_odds_performance_summary` | 17 | Resumo consolidado odds |

### 2.3 Views Front (CTO Castrin / categories-api)

| View | Cols | Proposito |
|------|------|-----------|
| `vw_front_api_games` | 16 | Catalogo para GameResponseDto (shape padrao API) |
| `vw_front_by_category` | 10 | Jogos por categoria (front) |
| `vw_front_by_vendor` | 10 | Jogos por vendor (front) |
| `vw_front_jackpot` | 7 | Jackpots ativos |
| `vw_front_live_casino` | 10 | Live casino jogos |
| `vw_front_top_24h` | 11 | Top jogos ultimas 24h |

### 2.4 Aquisicao / atribuicao / ROI

| View | Cols | Proposito |
|------|------|-----------|
| `vw_acquisition_channel` | 9 | Canal de aquisicao consolidado |
| `vw_attribution_metrics` | 11 | Metricas por modelo de atribuicao |
| `vw_aquisicao_trafego` | 17 | Trafego/aquisicao formatado |
| `vw_ad_spend_daily` | 10 | Ad spend diario (multi-canal) |
| `vw_ad_spend_by_source` | 11 | Ad spend por fonte |
| `vw_cohort_roi` | 13 | ROI por cohort de FTD |
| `vw_roi_by_source` | 20 | ROI por source |
| `vw_ltv_cac_ratio` | 6 | LTV/CAC por cohort |

### 2.5 Segmentacao, PCR, retencao

| View | Cols | Proposito |
|------|------|-----------|
| `vw_segmentacao_hibrida` | 34 | Segmentacao hibrida (multi-criterio) |
| `pcr_atual` | 25 | Ultima foto PCR por jogador |
| `pcr_resumo` | 11 | Resumo agregado PCR |
| `vw_player_performance_period` | 11 | Performance por periodo |
| `vw_active_players_period` | 7 | Ativos por periodo |
| `vw_active_player_retention_weekly` | 9 | Retencao semanal depositantes |
| `cohort_aquisicao` | 6 | Cohort de aquisicao (view, nao confundir com mv) |
| `cohort_retencao_ftd` | 6 | Retencao FTD |

### 2.6 Operacionais / live ops

| View | Cols | Proposito |
|------|------|-----------|
| `active_users` | 4 | Jogadores ativos no momento |
| `atualizacao` | 2 | Ultima atualizacao (live ops) |
| `heatmap_hour` | 9 | Heatmap hora x dia |
| `game_paid_15min` | 3 | Pagamentos granularidade 15min |
| `jogo_total_pago_hoje` | 4 | Total pago hoje por jogo |
| `top_jogadores_ganhos` | 7 | Top jogadores por ganho |
| `user_game_activity_30d` | 9 | Atividade jogador x jogo 30d |

---

## 3. Schema `play4` — foreign tables (10)

Foreign tables via `supernova_bet_server` → `supernova-bet-db` / `supernova_bet` (PKR, Play4Tune Paquistao).
Dados agregados — para granularidade completa usar `db/supernova_bet.py`.

| Foreign table | Cols | Origem espelhada |
|---------------|------|------------------|
| `heatmap_hour` | 9 | Agregado por hora |
| `matriz_aquisicao` | 14 | Aquisicao Play4Tune |
| `matriz_financeiro` | 26 | Financeiro Play4Tune |
| `matriz_financeiro_hora` | 22 | Financeiro por hora |
| `mv_aquisicao` | 14 | Snapshot aquisicao |
| `mv_cohort_aquisicao` | 6 | Cohort aquisicao |
| `tab_cassino` | 2 | KPIs casino |
| `tab_sports` | 2 | KPIs sports |
| `vw_active_player_retention_weekly` | 8 | Retencao semanal |
| `vw_ggr_player_game_daily` | 31 | **NOVA** (2026-04) — GGR por jogador/jogo/dia com flags outlier |

---

## 4. Pipelines em producao (EC2 — 54.197.63.138)

| Pipeline | Cron BRT | Destino | Status |
|----------|----------|---------|--------|
| `grandes_ganhos.py` | 00:30 | `multibet.grandes_ganhos` | Ativo |
| `game_image_mapper.py` | pre-req grandes_ganhos | `multibet.game_image_mapping` | Ativo |
| `etl_aquisicao_trafego_diario.py` | a cada 60min | `multibet.aquisicao_trafego_diario` | Ativo |
| `sync_google_ads_spend.py` | 01:00 | `multibet.fact_ad_spend` (parcial Google) | Ativo |
| `sync_meta_spend.py` | 01:15 | `multibet.fact_ad_spend` (parcial Meta) | Ativo |
| `fact_sports_odds_performance.py` | 05:00 | `multibet.fact_sports_odds_performance` | Ativo |
| `push_risk_to_smartico.py` | 02:30 | Smartico S2S (le `risk_tags`) | Ativo |
| `anti_abuse_multiverso.py` | loop 5min (systemd) | — (monitoramento CRM) | Ativo |

---

## 5. Mudancas 30/03 → 19/04 (delta 20 dias)

### 5.1 Removido

- **Camada Bronze inteira** — 24 tabelas `bronze_*` descontinuadas. Pipelines agora leem Athena diretamente. `bronze_crm_*` (Fase 2 do plano anterior) **nao sera mais criada** porque BigQuery Smartico foi desativado em 19/04/2026.

### 5.2 Adicionado (19/04)

**Tabelas:**
- `fact_sports_odds_performance`, `fact_ad_spend` (multi-canal), `fact_affiliate_revenue`
- `fct_active_players_by_period`, `fct_player_performance_by_period`
- `pcr_ratings`, `risk_tags`, `risk_tags_pgs`, `segment_tags`
- `silver_game_activity`, `silver_game_15min`, `silver_jogadores_ganhos`, `silver_jogos_jogadores_ativos`, `silver_tab_user_ftd`
- `tab_user_daily`, `tab_dep_user`, `tab_user_affiliate`, `tab_with_user`, `tab_affiliate`, `tab_btr`, `tab_hour_*` (5 tabelas horarias)
- `dim_affiliate_source`, `dim_marketing_mapping_bkp_20260319`
- `game_image_mapping` (enriquecido v3)
- `mv_top_sports_events_daily`, `mv_sports_open_bets_by_odds` (nomes com prefixo mv_ mas sao tabelas)
- `etl_control`, `migrations`

**Materialized views (3 novas):**
- `mv_aquisicao`, `mv_cohort_aquisicao`, `mv_cohort_retencao_ftd`

**Views (+35):**
- Bloco `vw_front_*` (6 views) — CTO Castrin / categories-api
- Bloco `matriz_*` (6 views — financeiro/aquisicao/risco, diario/hora/semanal/mensal)
- Bloco `vw_odds_performance_*` (2)
- Bloco `vw_ad_spend_*`, `vw_roi_by_source`, `vw_cohort_roi`, `vw_ltv_cac_ratio` (ROI/atribuicao)
- `vw_segmentacao_hibrida`, `pcr_atual`, `pcr_resumo`
- Varias views de live ops (`active_users`, `heatmap_hour`, `top_jogadores_ganhos`, etc.)

**Schema `play4`:**
- Nova foreign table `vw_ggr_player_game_daily` (31 cols — GGR granular Play4Tune)

### 5.3 Alteracoes

- `trackings`: 39K → 50.680 rows (+30%)
- `fact_casino_rounds`: crescimento constante (122K)
- `fact_attribution`, `fact_player_engagement_daily`, `fact_redeposits`: em torno de 154K cada (base de jogadores)

---

## 6. Notas tecnicas

- **Read vs Write:** Super Nova DB **somente destino** de escrita (regra do time). Fonte canonica continua Athena.
- **Timezone:** todas as tabelas armazenam timestamps em UTC. Views de matriz ja convertem para BRT.
- **Upsert:** maioria usa `INSERT ... ON CONFLICT DO UPDATE` para idempotencia.
- **TRUNCATE+INSERT:** fact de produto (casino_rounds, sports_bets, live_casino, jackpots) fazem full reload.
- **JSONB:** `fact_crm_daily_performance` usa colunas JSONB (funil/financeiro/comparativo).
- **LGPD:** `grandes_ganhos` hasheia nomes (ex: "Ri***s").
- **BigQuery desativado:** 19/04/2026 — pipelines CRM dependentes migrados para Athena + Smartico S2S (push `risk_tags`).
- **Refresh deste inventario:** rodar `scripts/inventario_schema_refresh.py` para atualizacao automatica.

---

## 7. Dependencias entre objetos

```
Matriz financeiro
  matriz_financeiro          --> tab_dep_with, tab_user_ftd, tab_cassino, tab_sports, tab_ativos, tab_btr
  matriz_financeiro_hora     --> tab_hour_* (5 tabelas)
  matriz_financeiro_semanal  --> (mesmas de diario, agregadas semana)
  matriz_financeiro_mensal   --> (mesmas, agregadas mes)
  matriz_aquisicao           --> mv_aquisicao + tab_user_daily
  matriz_risco               --> risk_tags

Views front (CTO)
  vw_front_api_games         --> game_image_mapping + dim_games_catalog
  vw_front_by_category       --> fact_casino_rounds + game_image_mapping
  vw_front_top_24h           --> fact_casino_rounds (ultimas 24h)

Analytics / ROI
  vw_cohort_roi              --> agg_cohort_acquisition
  vw_roi_by_source           --> fact_ad_spend + dim_affiliate_source + agg_cohort_acquisition
  vw_ltv_cac_ratio           --> agg_cohort_acquisition + fact_ad_spend

PCR & risco
  pcr_atual                  --> pcr_ratings (ultima snapshot por jogador)
  pcr_resumo                 --> pcr_atual
  risk_tags_pgs              --> risk_tags (filtrado)

Pipelines
  grandes_ganhos.py          --> game_image_mapping (pre-req)
  agg_cohort_acquisition.py  --> dim_marketing_mapping (lookup)
  push_risk_to_smartico.py   --> risk_tags (leitura)
```
