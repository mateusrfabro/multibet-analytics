# PR #1 multibet_pipelines — Delta vs Relatório de 16/04

**Gerado em:** 2026-04-16 17:30 | **Repo:** `GL-Analytics-M-L/multibet_pipelines`
**Branch:** `sync-ec2-prod-20260416` | **PR:** https://github.com/GL-Analytics-M-L/multibet_pipelines/pull/new/sync-ec2-prod-20260416

**Resumo:** 8 commits atômicos | 44 arquivos | +7.336 linhas | -1.412 linhas
**Impacto no seu relatório:** 3 objetos saem de "sem referência" → "em uso" | 15 novos artefatos no repo

---

## 1. Objetos do seu relatório que passam de `[ ] Sem ref` para `[x] Em uso` após merge deste PR

| Status antes | Status depois | Objeto | Pipeline/Script que passa a referenciar |
|---|---|---|---|
| [ ] Sem ref | [x] Em uso | `fact_sports_odds_performance` (280 kB) | `pipelines/fact_sports_odds_performance.py` |
| [ ] Sem ref | [x] Em uso | `pcr_ratings` (37 MB, 156K regs) | `pipelines/pcr_pipeline.py` |
| [ ] Sem ref | [x] Em uso | `fact_ad_spend` (944 kB, 2.464 regs) | `pipelines/sync_google_ads_spend.py` + `pipelines/sync_meta_spend.py` |

**Delta no cálculo do seu relatório:**
- Antes: 33/114 encontrados (28%)
- Depois: **36/114 encontrados (31%)** — ganho de +3 tabelas documentadas

---

## 2. Novos arquivos no repo `multibet_pipelines` (15 itens produtivos + 29 suporte)

### 2.1 Conectores (`db/`) — 3 arquivos
| Arquivo | Tamanho | Função |
|---|---|---|
| `db/google_ads.py` | 7.252 B | Conector Google Ads API (alimenta `fact_ad_spend`) |
| `db/meta_ads.py` | 6.760 B | Conector Meta Graph API (alimenta `fact_ad_spend`) |
| `db/smartico_api.py` | 13.588 B | Conector Smartico S2S API (alimenta push de tags) |

### 2.2 Pipelines produtivos (`pipelines/`) — 6 arquivos novos + 1 atualizado
| Arquivo | Cron (BRT → UTC) | Tabela destino | Observação |
|---|---|---|---|
| `pipelines/grandes_ganhos.py` | diário 00:30 → 03:30 | `grandes_ganhos` | **Atualizado +197 linhas** (Athena migration + file lock + CDN auto-discovery) |
| `pipelines/sync_google_ads_spend.py` | diário 01:00 → 04:00 | `fact_ad_spend` (Google) | Novo |
| `pipelines/sync_meta_spend.py` | diário 01:15 → 04:15 | `fact_ad_spend` (Meta) | Novo |
| `pipelines/push_risk_to_smartico.py` | diário 02:30 → 05:30 | Push externo (Smartico) | Novo |
| `pipelines/export_smartico_sent_today.py` | under-demand | Support/audit | Novo |
| `pipelines/pcr_pipeline.py` | diário 03:30 → 06:30 | `pcr_ratings` | Novo — Player Credit Rating D-AAA |
| `pipelines/fact_sports_odds_performance.py` | diário 05:00 → 08:00 | `fact_sports_odds_performance` | Novo — Win/Loss por faixa de odds |

### 2.3 Views Casino+Sportsbook (`views_casino_sportsbook/`) — pasta nova completa
| Arquivo | Cron | Alimenta |
|---|---|---|
| `create_views_casino_sportsbook.py` (DDL orchestrator) | manual | 10 tabelas fact/fct |
| `fact_casino_rounds.py` | diário 04:30 BRT | `fact_casino_rounds` |
| `fact_sports_bets.py` | diário 04:30 BRT | `fact_sports_bets` |
| `fact_sports_bets_by_sport.py` | diário 04:30 BRT | `fact_sports_bets_by_sport` |
| `fct_casino_activity.py` | diário 04:30 BRT | `fct_casino_activity` |
| `fct_sports_activity.py` | diário 04:30 BRT | `fct_sports_activity` |
| `fct_active_players_by_period.py` | intraday 12:07+18:07 | `fct_active_players_by_period` |
| `fct_player_performance_by_period.py` | intraday 12:07+18:07 | `fct_player_performance_by_period` |
| `vw_active_player_retention_weekly.py` | diário 04:30 BRT | view |
| `agg_cohort_acquisition.py` | diário 04:30 BRT | `mv_cohort_aquisicao` |
| `deploy_views_casino_sportsbook.sh`, `run_*.sh`, `rollback_*.sh` | - | Deploy scripts |

### 2.4 Scripts deploy/run (`./`) — 7 arquivos novos
- `deploy_fact_sports_odds_performance.sh`, `deploy_push_smartico.sh`
- `run_fact_sports_odds_performance.sh`, `run_pcr_pipeline.sh`, `run_push_smartico.sh`, `run_sync_google_ads.sh`, `run_sync_meta_ads.sh`

### 2.5 Documentação
- `DEPLOY.md` expandido de 5 → **10 pipelines** documentados (cron, paths EC2, deploy steps)

---

## 3. Bug estrutural corrigido (commit 1)

- Removido `db/db/` e `pipelines/pipelines/` (pastas duplicadas de commit antigo acidental)
- Zero imports usavam `db.db.*` (validado via grep) — remoção safe
- **1.412 linhas removidas** (código morto)

---

## 4. Ainda `[ ] Sem ref` após este PR (fica para PRs seguintes)

### PR #2 — Pipelines existentes no projeto local mas ainda precisam de validação em prod
- `dim_marketing_mapping` (3.241 regs) → tem `pipelines/dim_marketing_mapping.py` + `_canonical.py`
- `dim_games_catalog` (381 regs) → tem `pipelines/dim_games_catalog.py`
- `fact_attribution` (67 MB, 154K regs) → tem `pipelines/fact_attribution.py`
- `fact_ftd_deposits`, `fact_redeposits`, `fact_registrations`, `fact_player_activity`, `fact_player_engagement_daily`, `fact_gaming_activity_daily`, `fact_jackpots`, `fact_live_casino` → todos têm pipeline local
- `fact_crm_daily_performance` → tem `pipelines/crm_daily_performance.py`
- `agg_cohort_acquisition`, `agg_game_performance` → têm pipelines
- `crm_campaign_*` (7 tables) → têm `pipelines/crm_report_daily*.py` + `report_crm_promocoes.py`

**Ação:** próxima sessão — varredura sistemática crontab+systemd pra confirmar quais rodam em prod antes de commitar.

### PR #3 — DDLs de views criadas manualmente via DBeaver (falta SQL versionado)
- `pcr_atual`, `pcr_resumo` (fontes: `pcr_ratings`)
- `vw_ad_spend_daily`, `vw_ad_spend_by_source` (fontes: `fact_ad_spend`)
- `vw_odds_performance_by_range`, `vw_odds_performance_summary` (fontes: `fact_sports_odds_performance`)
- `vw_acquisition_channel` (fontes: `dim_marketing_mapping`)

**Ação:** extrair DDL via `psql \d+ view` e commitar em `multibet_pipelines/sql/views/`.

### Fora do escopo deste PR — outras áreas da squad (já têm repos próprios)
- `matriz_financeiro` (+ mensal/semanal/hora), `matriz_aquisicao`, `cohort_aquisicao`, `cohort_retencao_ftd`, `heatmap_hour` → views de dashboard
- Tabelas `tab_*` → alimentadas por `sync_all`/`sync_all_aquisicao`/`sync_user_daily`
- Tabelas `silver_*` → alimentadas por `top_wins`/`game_activity_30d`

---

## 5. Validações de segurança executadas antes do PR

| Item | Status |
|---|---|
| SSH EC2 ETL (54.197.63.138) para confirmar versões em produção | ✅ |
| Snapshot EC2 antes do trabalho (`multibet_pipelines_snapshot_20260416.tar.gz`, 141 MB) | ✅ |
| Scan de credenciais hard-coded (AKIA, ghp_, Bearer, PASSWORD=, etc.) | ✅ Zero ocorrências |
| Scan de imports quebrados (`db.db.*`) antes de remover duplicação | ✅ Zero ocorrências |
| Ordem de commits (`db/` antes dos pipelines que importam) | ✅ Previne broken checkout |
| Diff de arquivos já existentes no repo vs EC2 (ignorando CRLF/LF) | ✅ 12 iguais, 1 atualizado no commit 8 (`grandes_ganhos.py`) |
| `origin/main` sem commit novo desde clone | ✅ Zero conflito de rebase |
| Processo: **EC2 → git** (nunca git → EC2) | ✅ Nenhum bit da EC2 foi tocado |

---

## 6. Impacto em produção

**Zero.** Este PR apenas espelha no git o que já está rodando há semanas na EC2. Não altera EC2.

Após o merge, o git passa a ser a fonte de verdade → base necessária para qualquer deploy futuro seguir o fluxo "git primeiro, depois EC2" (regra formalizada em `CLAUDE.md` do projeto).

---

## 7. Sugestão de reviewers

- **Mauro** — pipelines/ + conectores db/
- **Gusta** — deploy/run scripts + paths EC2
- **Castrin** — visão executiva / aprovação final merge

---

*Gerado após auditoria completa: SSH EC2 + SSM EC2 Apps + revisão por 2 agentes (auditor + best-practices).*
*Documento técnico completo disponível no projeto local: `docs/_migration/mapping_arquivo_repo.md`.*
