# Mapping de Migração — MultiBet ↔ Repos GitHub

**Gerado em:** 2026-04-16 | **Status:** ⏳ Aguardando validação do Mateus antes de qualquer push

Base: SSH EC2 ETL + SSM EC2 Apps + clone de 19 repos GL-Analytics-M-L + token pessoal mateusrfabro.

---

## 1. Sumário Executivo

### 3 fontes de verdade
| Fonte | Status |
|---|---|
| **EC2 Produção** (ETL + Apps) | ✅ 100% atualizada (crons rodando) |
| **Local** (`c:/Users/NITRO/.../MultiBet`) | ⚠️ Parcial: tem tudo que está na EC2 + staging (`ec2_deploy/`) + muitas coisas ad-hoc |
| **Git Time** (`GL-Analytics-M-L/multibet_pipelines`) | 🔴 **DESATUALIZADO vs EC2** — 9+ arquivos de produção faltando |
| **Git Pessoal** (`mateusrfabro/MultiBet-Scripts---SQLs`) | ✅ Espelho do local (commit 5d6adb0) |

### Descoberta crítica (confirma sua preocupação)
> "sei que as automações/etls que rodamos na ec2 está 100% atualizado, não sei dizer se no git está igual"

**Resposta:** não está. Vários pipelines em produção foram editados e nunca commitados. Exemplo `grandes_ganhos.py` tem 3 backups na EC2 sem equivalente no git:
- `grandes_ganhos.py.bak_20260416_172257` (hoje, 17:22)
- `grandes_ganhos.py.bkp_20260415_093737` (ontem, 09:37)
- `grandes_ganhos.py.bkp_20260406_180011` (10 dias atrás)

A partir de agora: **git primeiro, EC2 depois** (já formalizado em CLAUDE.md + `memory/feedback_git_first_then_ec2_deploy.md`).

### Observação importante sobre repo pessoal
Os dois repos pessoais `mateusrfabro/multibet-analytics` e `mateusrfabro/MultiBet-Scripts---SQLs` têm **EXATAMENTE o mesmo HEAD** (`5d6adb0b3fe67e4241b3b8886d3203e270100018`).

Possibilidades:
- (a) Você fez push duplicado no passado → dois repos espelhados
- (b) Um foi fork do outro

**Pergunta pra você:** qual dos dois manter? Recomendação: **arquivar `multibet-analytics`** (manter só `MultiBet-Scripts---SQLs`) pra evitar divergência futura.

---

## 2. Escopo Intelligence Engine na org GL-Analytics-M-L (27 repos totais)

| Repo | Escopo | Ação proposta |
|---|---|---|
| **multibet_pipelines** | ✅ **Meu escopo** — grandes ganhos, ETL aquisicao, risk matrix, PCR, Google/Meta, Smartico, sports odds, views casino/sportsbook | **PR para atualizar com EC2** |
| **clustering-btr-utm-campaign** | ✅ **Meu escopo** — projeto BTR por UTM campaign | Verificar sync local vs repo |
| **alerta-ftd** | ✅ **Meu escopo** — alerta FTD+BTR+rollback (já tem `.git` próprio em `alerta-ftd/` local) | Verificar sync (já commit recentes lá) |
| risk-matrix | ⚠️ API Flask (service EC2 Apps) — Domain-driven. É FAT (infra/não nossa) | Não mexer |
| alert_fraud | ❌ Time (Gusta) | Não mexer |
| top_wins | ❌ Time | Não mexer |
| game_activity_30d | ❌ Time | Não mexer |
| sync_all / sync_all_aquisicao / sync_user_daily | ❌ Time (infra Gusta) | Não mexer |
| refresh_mv | ❌ Time | Não mexer |
| freshness_check | ❌ Time | Não mexer |
| deposit_monitoring | ❌ Time | Não mexer |
| crm_onboarding | ❌ Time (API CRM onboarding) | Não mexer |
| Supernova-Dashboard | ❓ Dashboard web interno. **Investigar se `dashboards/crm_report/` ou `dashboards/google_ads/` é nosso ou se pertence a este repo** | Verificar |
| webhook-multibet---keitaro | ❌ Time | Não mexer |
| onboarding / worker_missions / Missions_API | ❌ Time | Não mexer |
| clustering-athena-api / supernova-machine-learning / prediction-market-api | ❌ Time (APIs/ML) | Não mexer |
| odds-predict-machine-learning / url-audit-script / personalize-sdk-api | ❌ Time (antigos) | Não mexer |

**Conclusão:** dos 27 repos da org, **3** são do meu escopo individual (Intelligence Engine): `multibet_pipelines`, `clustering-btr-utm-campaign`, `alerta-ftd`. Os demais pertencem a outros escopos da squad (infra de sync, APIs, ML, webhooks, etc.) — todos complementares.

---

## 3. Arquivos FALTANDO em `multibet_pipelines` (git time) vs EC2

Prioridade **ALTA** — esses arquivos estão rodando em produção na EC2 há dias e não têm histórico no git.

### pipelines/ (falta 6 arquivos)
| Arquivo | EC2 | Git time | Ação |
|---|---|---|---|
| `export_smartico_sent_today.py` | ✅ | ❌ | **commitar** |
| `fact_sports_odds_performance.py` | ✅ | ❌ | **commitar** (cron 08:00 UTC) |
| `pcr_pipeline.py` | ✅ | ❌ | **commitar** (cron 06:30 UTC) |
| `push_risk_to_smartico.py` | ✅ | ❌ | **commitar** (cron 05:30 UTC) |
| `sync_google_ads_spend.py` | ✅ | ❌ | **commitar** (cron 04:00 UTC) |
| `sync_meta_spend.py` | ✅ | ❌ | **commitar** (cron 04:15 UTC) |

### db/ (falta 3 arquivos)
| Arquivo | EC2 | Git time | Ação |
|---|---|---|---|
| `db/google_ads.py` | ✅ | ❌ | **commitar** |
| `db/meta_ads.py` | ✅ | ❌ | **commitar** |
| `db/smartico_api.py` | ✅ | ❌ | **commitar** |

### scripts raiz (falta 7 arquivos)
| Arquivo | EC2 | Git time | Ação |
|---|---|---|---|
| `deploy_fact_sports_odds_performance.sh` | ✅ | ❌ | **commitar** |
| `deploy_push_smartico.sh` | ✅ | ❌ | **commitar** |
| `run_fact_sports_odds_performance.sh` | ✅ | ❌ | **commitar** |
| `run_pcr_pipeline.sh` | ✅ | ❌ | **commitar** |
| `run_push_smartico.sh` | ✅ | ❌ | **commitar** |
| `run_sync_google_ads.sh` | ✅ | ❌ | **commitar** |
| `run_sync_meta_ads.sh` | ✅ | ❌ | **commitar** |

### views_casino_sportsbook/ (10 pipelines gold)
EC2 tem:
- `create_views_casino_sportsbook.py`
- `agg_cohort_acquisition.py`
- `fact_casino_rounds.py`, `fact_sports_bets.py`, `fact_sports_bets_by_sport.py`
- `fct_casino_activity.py`, `fct_sports_activity.py`
- `fct_active_players_by_period.py`, `fct_player_performance_by_period.py`
- `vw_active_player_retention_weekly.py`
- `deploy_views_casino_sportsbook.sh`, `run_views_casino_sportsbook.sh`, `run_views_intraday.sh`, `rollback_views_casino_sportsbook.sh`

**Git time status:** ❓ Ainda não verificado no repo clonado (precisa conferir). Ação: **checar → commitar faltantes**.

### Backups EC2 (pré-vigente) a preservar
| Arquivo | Preservar em |
|---|---|
| `grandes_ganhos.py.bak_20260416_172257` | Git time (commit separado: "chore: preserve EC2 backups") |
| `grandes_ganhos.py.bkp_20260415_093737` | Idem |
| `grandes_ganhos.py.bkp_20260406_180011` | Idem |

### Bug de estrutura no repo `multibet_pipelines`
O repo tem pastas duplicadas **`db/db/`** (dentro de `db/`) e **`pipelines/pipelines/`** (dentro de `pipelines/`) — commit errado no passado. Contém cópia antiga de arquivos.

**Ação sugerida:** commit separado: "chore: remove duplicate db/db and pipelines/pipelines subfolders". Isso também afeta a EC2 (que tem mesmo problema — possivelmente por cp sem cuidado).

---

## 4. Mapping Pastas Locais → Destino

### 4.1 Pastas com repo próprio no time (não mexer, só sync)
| Pasta Local | Repo Time | Status |
|---|---|---|
| `alerta-ftd/` | `GL-Analytics-M-L/alerta-ftd` | Já tem `.git` nested. Comparar com remote. |
| `clustering-btr-utm-campaign/` | `GL-Analytics-M-L/clustering-btr-utm-campaign` | Verificar sync. |

### 4.2 Pastas que vão pro repo `multibet_pipelines`
| Pasta/Arquivo Local | Destino Git Time |
|---|---|
| `ec2_deploy/pipelines/*.py` | `multibet_pipelines/pipelines/` |
| `ec2_deploy/db/*.py` | `multibet_pipelines/db/` |
| `ec2_deploy/sql/risk_matrix/*.sql` | `multibet_pipelines/sql/risk_matrix/` |
| `ec2_deploy/views_casino_sportsbook/` | `multibet_pipelines/views_casino_sportsbook/` |
| `ec2_deploy/deploy_*.sh` + `run_*.sh` | `multibet_pipelines/` (raiz) |
| `ec2_deploy/requirements.txt` | `multibet_pipelines/requirements.txt` |
| `ec2_deploy/DEPLOY.md` | `multibet_pipelines/DEPLOY.md` |
| `pipelines/pcr_pipeline.py` | idem (em prod EC2 ≠ git time) |
| `pipelines/fact_sports_odds_performance.py` | idem |
| `pipelines/sync_google_ads_spend.py` + `sync_meta_spend.py` | idem |
| `pipelines/fact_casino_rounds.py` + `fact_sports_bets.py` + `fct_*.py` | `multibet_pipelines/views_casino_sportsbook/pipelines/` (já está na EC2 lá) |
| `pipelines/vw_active_player_retention_weekly.py` | idem |

### 4.3 Pastas que vão pro repo pessoal `MultiBet-Scripts---SQLs`
Como o repo pessoal já é espelho do local, basicamente **atualizar com o estado atual**:

| Pasta/Arquivo Local | Ação |
|---|---|
| `scripts/` (167 arquivos — análises ad-hoc, auditorias, investigações) | Atualizar repo pessoal |
| `reports/` (103 arquivos — HTML, MD, PDF, TXT, CSV, XLSX) | Atualizar — **filtrar PII** (CSV/XLSX) antes |
| `docs/` | Atualizar — `.md` só |
| `pipelines/` modificados locais (ver git diff) | Atualizar repo pessoal (commit) |
| `validacoes/` | Atualizar (manter como histórico) |
| `solicitacoes_pontuais/` (6781 files — MAIORIA CSV de output) | **NÃO subir output**, já gitignored |
| `segmentacao/` (4346 files — outputs de seg.) | **NÃO subir output**, já gitignored |
| `temp/` (128 files — scripts exploratórios) | Revisar antes (alguns têm sentido histórico) |
| `analysis/` (9 files — antigos) | Arquivar |
| Arquivos `*.html` da raiz (`PCR_Player_Credit_Rating_v*.html`) | Mover para `reports/` → subir |

### 4.4 Pastas para `_archive/` (mover, não deletar)
Todas confirmadas obsoletas, mas **preservar histórico**:

| Pasta | Motivo |
|---|---|
| `anti_abuse_deploy/` | Staging antigo — versão vigente está em `pipelines/anti_abuse_multiverso.py` (mas bot NÃO está em prod na EC2 atual — verificar se foi descontinuado) |
| `matriz_de_risco/` | Docs iniciais (PDF v1_4, v1_5_1, Excel, zip). Código vivo está em `risk_matrix_pipeline.py` + SQLs. Docs podem subir no repo `multibet_pipelines/docs/` ou ficar em `_archive/`. |
| `anotacoes/` (2 files) | Notas antigas |
| `analysis/` (9 files) | Análises ad-hoc antigas |
| `google ads/` (1 file com espaço no nome) | Resíduo — substituído por `sync_google_ads_spend.py` |
| `crm_dashboard/` (v0) | Superseded por `crm_report_daily*.py` |

### 4.5 Pastas a **INVESTIGAR** antes de qualquer decisão
| Pasta | Dúvida |
|---|---|
| `dashboards/crm_report/` | É Flask app. Está em prod na EC2 Apps? Pertence a `Supernova-Dashboard` ou repo próprio? |
| `dashboards/google_ads/` | Idem. |
| `crm_dashboard/` | Relação com `crm_onboarding` (repo time) ou é nosso? |
| `pipelines/anti_abuse_multiverso*.py` | Está rodando em algum lugar? (crontab ETL não menciona) |

### 4.6 Pastas de outros escopos da squad que estão no seu local (contexto histórico — não mexer)
- `ec2_etl_automacao.md` (memory) aponta: são projetos de infra (Gusta/Mauro)
- Nada pra commitar pra você

---

## 5. Scripts de Deploy/Setup que devem subir no `multibet_pipelines`
- `ec2_deploy/DEPLOY.md` (atualizar com 9 pipelines)
- `ec2_deploy/.env.example`
- `ec2_deploy/requirements.txt`
- `ec2_deploy/deploy_*.sh` (10 scripts)
- `ec2_deploy/run_*.sh` (10 scripts)
- `ec2_deploy/views_casino_sportsbook/` (pasta completa)

---

## 6. Plano de Execução (após validação)

### FASE A — Limpeza segura (pode fazer agora)
- [ ] Commit local do `.gitignore` reforçado
- [ ] Mover pastas obsoletas para `_archive/` (com `git mv`)
- [ ] Commit "chore: organize project structure, archive obsolete folders"

### FASE B — Sync `multibet_pipelines` (time) com EC2
Ordem:
1. [ ] `git clone` do repo time em `_workdir_migration/multibet_pipelines/`
2. [ ] Branch `sync-ec2-prod-2026-04-16`
3. [ ] Commit 1: "chore: remove duplicate db/db and pipelines/pipelines"
4. [ ] Commit 2: "feat: add missing pipelines (fact_sports_odds, pcr, push_smartico, sync_google/meta, fact_sports_odds_performance, export_smartico)"
5. [ ] Commit 3: "feat: add db/ connectors (google_ads, meta_ads, smartico_api)"
6. [ ] Commit 4: "feat: add deploy/run scripts for EC2"
7. [ ] Commit 5: "feat: add views_casino_sportsbook pipelines and scripts"
8. [ ] Commit 6: "chore: preserve EC2 backups (grandes_ganhos bak)"
9. [ ] Commit 7: "docs: update DEPLOY.md with full pipeline list"
10. [ ] Push → abrir **PR** pra review do Mauro/Gusta/Castrin
11. [ ] Você faz o merge manual após review

### FASE C — Sync `alerta-ftd` e `clustering-btr-utm-campaign`
- [ ] Comparar local vs remoto
- [ ] Se local > remoto: PR com as diffs
- [ ] Se remoto > local: pull

### FASE D — Atualizar repo pessoal `MultiBet-Scripts---SQLs`
- [ ] Verificar remotes (são espelhados — escolher um como principal)
- [ ] Scan PII em `reports/` (sanitizar CSV/XLSX com dados de jogadores)
- [ ] Commit local de tudo relevante
- [ ] Push (você faz manual, eu preparo)

### FASE E — Verificações finais
- [ ] SSH EC2 + confirmar git pull funciona após commits
- [ ] Documentar processo no README principal

---

## 7. Dúvidas abertas pra você responder

### ✅ Já respondidas por investigação empírica (16/04 pós-SSH/SSM)

**Dúvida 3 — `anti_abuse_multiverso.py` em prod? → NÃO, descontinuado.**
Evidência EC2 ETL: zero systemd service, zero cron, zero processo, zero pasta `/home/ec2-user/anti_abuse*`. Bot não roda há dias (sem logs ativos). **Ação: arquivar `anti_abuse_deploy/` local + `pipelines/anti_abuse_multiverso*.py` sem risco.**

**Dúvida 2 — `dashboards/crm_report/` e `dashboards/google_ads/` em prod? → PROVAVELMENTE NÃO.**
Services ativos na EC2 Apps (via SSM):
- `btr-dashboard` → `/home/ubuntu/btr-dashboard/` (gunicorn `app:server` — padrão Dash/Plotly — **time**, talvez não versionado em repo)
- `monitor` → `/home/ubuntu/monitor/` (Supernova Monitor Dashboard — **time**)
- `crm-onboarding-api` → `/home/ubuntu/crm_onboarding/api/` (webhook receiver — **time**, repo `GL-Analytics-M-L/crm_onboarding`)
- `deposit-monitoring` → **time**
- `alerta-ftd` → nosso (repo próprio)
- `risk-matrix` → API service (time, repo `risk-matrix`)

Nenhum dos dashboards da EC2 Apps bate com `dashboards/crm_report/` ou `dashboards/google_ads/` local (ambos usam path `/home/ubuntu/` — usuário diferente do ETL). **Hipótese:** ambos nunca foram para produção ou foram superseded pelo `btr-dashboard`/`monitor` do time.

**Ação proposta:** arquivar `dashboards/crm_report/` e `dashboards/google_ads/` locais em `_archive/`. Se você confirmar que eram POCs/rascunhos, sem risco.

### ❓ Ainda pendem sua resposta

1. **Repos pessoais espelhados** (`multibet-analytics` + `MultiBet-Scripts---SQLs` têm mesmo HEAD `5d6adb0`): manter qual? Arquivar o outro? Recomendação: manter `MultiBet-Scripts---SQLs` (nome melhor), arquivar `multibet-analytics`.
4. **`matriz_de_risco/` (docs antigos, PDF, Excel)**: subir pro `multibet_pipelines/docs/` ou arquivar só no pessoal?
5. **`pcr_pipeline.py`**: em `multibet_pipelines` (já é nosso repo pipelines) ou repo próprio? Recomendo: **ficar em `multibet_pipelines`** (evita proliferar repos pequenos).
6. **PR vs push direto** no `multibet_pipelines`: abrir PR pra review (recomendado) ou push direto na main?
7. **Scan PII**: você tem padrão de sanitização? Se não, recomendo **não subir** CSV/XLSX/TXT com nomes de jogadores, subir só HTML/MD/PDF (esses tipicamente mostram agregados).
8. **`scripts/` (167 arquivos)**: subir tudo ou filtrar só os "produtivos" (excluir investigações ad-hoc de data específica)?

---

## 8. Próximo passo

**Você revisa este documento** (pode editar inline com comentários tipo `>>> NÃO fazer isso` ou `>>> OK`). Quando aprovar, eu executo por Fase (A → B → C → D → E), com snapshot e commit atômico, pausando entre fases pra validação.

**Nenhum commit em repo compartilhado** (time ou pessoal) até seu OK explícito.
