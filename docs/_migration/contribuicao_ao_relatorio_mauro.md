# Contribuição ao Relatório do Mauro (16/04) — Análise Completa

**Relatório base do Mauro:** 114 objetos | 33 em uso (28%) | 81 sem referência em código

**Proposta:** complementar o mapeamento já feito pelo Mauro, cobrindo os objetos cuja autoria está no meu escopo (Intelligence Engine) via 3 PRs em sequência.

---

## 🎯 RESUMO — Evolução da cobertura com as contribuições propostas

| Estado | Cobertura | Delta |
|---|---|---|
| Baseline do relatório (16/04 manhã) | 33/114 (28%) | — |
| Após PR #1 (mergeado) | 36/114 (**31%**) | +3 tabelas |
| Após PR #2 (pipelines Intelligence Engine) | ~51/114 (**~45%**) | +15 tabelas |
| Após PR #3 (DDLs de views) | ~61/114 (**~53%**) | +10 views |
| **Contribuição total (meu escopo)** | ~61/114 (~53%) | **+28 objetos** |

**Interpretação:** a contribuição do meu escopo (Intelligence Engine) quase dobra a cobertura documentada. Os demais objetos pertencem a outras áreas da squad (sync/ingest, APIs, dashboards do Gusta) — complementares aos meus pipelines, fazem parte do mesmo ecossistema.

---

## ✅ PR #1 — Já contribui (em revisão)

### 3 tabelas que passam de "Sem ref" → "Em uso"

| Tabela | Tamanho | Pipeline que passa a referenciar |
|---|---|---|
| `fact_sports_odds_performance` | 280 kB, 855 regs | `pipelines/fact_sports_odds_performance.py` |
| `pcr_ratings` | 37 MB, 156K regs | `pipelines/pcr_pipeline.py` |
| `fact_ad_spend` | 944 kB, 2.464 regs | `pipelines/sync_google_ads_spend.py` + `sync_meta_spend.py` |

**Bônus no PR #1:** 15 pipelines + 10 views Casino+Sportsbook (handoff Gusta 08/04) + 3 conectores db + DEPLOY.md expandido.

---

## 🟡 PR #2 — Pipelines do meu escopo que podem ir pro repo time

Tabelas do relatório "Sem ref" cujos pipelines Python estão no meu ambiente local:

### Dimensões (2)
| Tabela | Tamanho | Pipeline local |
|---|---|---|
| `dim_marketing_mapping` | 1.968 kB, 3.241 regs | `pipelines/dim_marketing_mapping.py` + `_canonical.py` |
| `dim_games_catalog` | 152 kB, 381 regs | `pipelines/dim_games_catalog.py` |

### Fatos granulares (9)
| Tabela | Tamanho | Pipeline local |
|---|---|---|
| `fact_attribution` | 67 MB, 154K regs | `pipelines/fact_attribution.py` |
| `fact_ftd_deposits` | 12 MB, 29K regs | `pipelines/fact_ftd_deposits.py` |
| `fact_redeposits` | 22 MB, 154K regs | `pipelines/fact_redeposits.py` |
| `fact_registrations` | 56 kB, 171 regs | `pipelines/fact_registrations.py` |
| `fact_player_activity` | 80 kB, 142 regs | `pipelines/fact_player_activity.py` |
| `fact_player_engagement_daily` | 24 MB, 154K regs | `pipelines/fact_player_engagement_daily.py` |
| `fact_gaming_activity_daily` | 54 MB, 116K regs | `pipelines/fact_gaming_activity_daily.py` |
| `fact_jackpots` | 56 kB, 1 reg | `pipelines/fact_jackpots.py` |
| `fact_live_casino` | 2.936 kB, 11K regs | `pipelines/fact_live_casino.py` |

### Agregações (3)
| Tabela | Tamanho | Pipeline local |
|---|---|---|
| `agg_cohort_acquisition` | 32 MB, 189K regs | `pipelines/agg_cohort_acquisition.py` |
| `agg_game_performance` | 5.736 kB, 27K regs | `pipelines/agg_game_performance.py` |
| `agg_btr_by_utm_campaign` | 32 kB, 51 regs | relacionado ao `clustering-btr-utm-campaign` (confirmar com o autor) |

### CRM — Campaign tables (8)
| Tabela | Tamanho | Pipeline local |
|---|---|---|
| `crm_campaign_comparison` | 24 kB | `pipelines/crm_report_daily_v3_agent.py` |
| `crm_campaign_daily` | 2.376 kB, 3K regs | `pipelines/crm_report_daily.py` + `ddl_crm_report.py` |
| `crm_campaign_game_daily` | 104 kB, 20 regs | idem |
| `crm_campaign_segment_daily` | 24 kB | idem |
| `crm_dispatch_budget` | 56 kB, 6 regs | `pipelines/report_crm_promocoes.py` (confirmar) |
| `crm_player_vip_tier` | 32 kB | idem |
| `crm_recovery_daily` | 56 kB, 1 reg | `pipelines/crm_report_daily_v3_agent.py` (confirmar) |
| `crm_vip_group_daily` | 56 kB, 3 regs | idem |

**Total PR #2:** ~22 tabelas documentáveis.
**Pré-requisito:** validar empiricamente quais pipelines rodam em produção (excluir drafts/experimentos) + alinhar com Mauro/Gusta pra evitar duplicação com trabalhos em curso.

---

## 🟠 PR #3 — DDLs de views criadas via DBeaver

Views do relatório "Sem ref" derivadas dos meus pipelines:

### Derivadas diretas (6 views — contexto claro)
| View | Tabela fonte | Projeto origem |
|---|---|---|
| `pcr_atual` | `pcr_ratings` | PCR — Player Credit Rating |
| `pcr_resumo` | `pcr_ratings` | PCR — Player Credit Rating |
| `vw_ad_spend_daily` | `fact_ad_spend` | Ad Spend multicanal |
| `vw_ad_spend_by_source` | `fact_ad_spend` | Ad Spend multicanal |
| `vw_odds_performance_by_range` | `fact_sports_odds_performance` | Sports Odds Performance |
| `vw_odds_performance_summary` | `fact_sports_odds_performance` | Sports Odds Performance |

### Derivadas de dims/fatos do meu escopo (confirmar autoria)
| View | Tabela fonte | Projeto origem provável |
|---|---|---|
| `vw_acquisition_channel` | `dim_marketing_mapping` | dim_marketing_mapping |
| `vw_attribution_metrics` | `fact_attribution` | fact_attribution |
| `vw_ltv_cac_ratio` | `fact_attribution` | fact_attribution |
| `vw_player_performance_period` | `fct_player_performance_by_period` | views_casino_sportsbook |

### Possivelmente compartilhadas (alinhar com Gusta/Mauro)
- `vw_casino_by_category`, `vw_casino_by_provider`, `vw_casino_top_games` (fontes: `fact_casino_rounds`) — podem ser parte de views_casino_sportsbook
- `vw_roi_by_source` (fonte: `fact_affiliate_revenue`) — autoria incerta
- `vw_segmentacao_hibrida` (fonte: `risk_tags_pgs`) — pode ser Smartico / matriz de risco

**Total PR #3:** ~10 views.
**Método:** extrair DDL via `psql \d+ nome_da_view` ou `pg_dump --schema-only --view` e commitar em `multibet_pipelines/sql/views/`.

---

## ℹ️ Outros objetos do relatório — escopos complementares da squad

Os ~47 objetos restantes no "Sem ref" pertencem a outras áreas da squad e já têm repos próprios. O scan identifica corretamente que não estão em `multibet_pipelines` — fazem parte do ecossistema mais amplo:

### Camada de sync/ingest (repos próprios do Gusta)
- `tab_*` (19 tabelas: `tab_affiliate`, `tab_ativos`, `tab_atualizacao`, `tab_btr`, `tab_cassino`, `tab_dep_user`, `tab_dep_with`, `tab_hour_*` 6x, `tab_sports`, `tab_user_affiliate`, `tab_user_ftd`, `tab_with_user`) → repos `sync_all`, `sync_all_aquisicao`, `sync_user_daily`
- `silver_tab_user_ftd`, `migrations`, `etl_control` → infraestrutura de sync
- `silver_*` (`silver_game_activity`, `silver_game_15min`, `silver_jogadores_ganhos`, `silver_jogos_jogadores_ativos`) → repos `top_wins` e `game_activity_30d`

### Views de apoio a dashboards (escopo compartilhado)
- `matriz_financeiro` + variantes (mensal/semanal/hora)
- `matriz_aquisicao`, `matriz_risco` (view)
- `cohort_aquisicao`, `cohort_retencao_ftd`, `heatmap_hour`
- `active_users`, `game_paid_15min`, `jogo_total_pago_hoje`, `top_jogadores_ganhos`, `user_game_activity_30d`

### Sem origem identificada (investigar com a squad)
- `dim_affiliate_source`, `dim_campaign_affiliate`, `dim_crm_friendly_names`
- `fact_affiliate_revenue`, `segment_tags`

> Nota: essa separação é puramente organizacional (escopo de trabalho individual dentro da squad). Todos os objetos fazem parte do mesmo ecossistema da Squad 3 Intelligence Engine + áreas adjacentes.

---

## 📊 Resumo da contribuição possível

| Categoria | Total no relatório | Contribuição do meu escopo | Outros escopos da squad |
|---|---|---|---|
| Tabelas sem ref | 52 | **~24** (3 no PR #1 + ~21 no PR #2) | ~28 (sync/ingest) |
| Views sem ref | 29 | **~10** (PR #3) | ~19 (views de dashboard) |
| **TOTAL** | **81** | **~34 (42% dos órfãos)** | ~47 |

---

## 📝 Mensagem sugerida pro Mauro (complementar ao PR #1)

> "Mauro, valeu pelo relatório de 16/04. Fiz uma análise completa pra ver o que consigo complementar:
>
> - **PR #1 (em revisão)** já documenta 3 tabelas: fact_sports_odds_performance, pcr_ratings, fact_ad_spend.
> - **PR #2 (planejado)** pode cobrir mais ~21 tabelas do meu escopo — dims, facts granulares, aggs, CRM.
> - **PR #3 (planejado)** traz ~10 DDLs de views criadas via DBeaver (PCR, ad_spend, odds, attribution).
>
> No total, consigo complementar ~34 dos 81 órfãos (~42%) via Intelligence Engine. Os outros ~47 pertencem a outras áreas da squad (sync_all, sync_all_aquisicao, matriz_*, etc.) — o relatório reflete certinho, é só o escopo do multibet_pipelines que não cobre elas.
>
> Antes do PR #2 vale alinhar rápido (30 min) — quero evitar duplicar pipeline com algo que você ou o Gusta já tenham em vista. Topa?
>
> Análise completa: `docs/_migration/contribuicao_ao_relatorio_mauro.pdf`"

---

## 🎯 Próxima ação sugerida

1. **Fase B.2 (PR #2)** — varredura crontab + systemd na EC2 ETL pra validar quais dos ~21 pipelines locais rodam em prod
2. **Fase B.3 (PR #3)** — script Python pra extrair DDL das ~10 views via `db/supernova.py` → commitar em `multibet_pipelines/sql/views/`
3. **Call com Mauro (30 min)** antes do PR #2 pra alinhar escopo e evitar duplicação com trabalhos em curso da squad
