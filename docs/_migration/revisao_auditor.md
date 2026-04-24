# Revisao Auditor — Plano de Migracao MultiBet -> GitHub

**Auditor:** Auditor (squad QA) | **Data:** 2026-04-16 | **Doc revisado:** `docs/_migration/mapping_arquivo_repo.md`

## 1. Checklist item-a-item

| # | Item | Status | Justificativa |
|---|---|---|---|
| 1 | Listagem EC2 vs Git (secao 3) | **WARNING** | SSH para 54.197.63.138 foi **BLOQUEADO** no meu ambiente (permissao Bash negada). Nao consegui validar empiricamente `ls /home/ec2-user/multibet_pipelines/`. Validei indiretamente via `ec2_deploy/` local que e o staging da EC2: **todos os 16 arquivos listados existem** localmente em `ec2_deploy/` (9 pipelines/db + 7 scripts raiz). Plausivel, mas **precisa reconfirmacao empirica antes do push**. |
| 2 | Pipelines faltantes (6) | **OK** | Todos existem em `ec2_deploy/pipelines/` ou `pipelines/` (validei via Glob). |
| 3 | db/ faltantes (3) | **OK** | `ec2_deploy/db/meta_ads.py`, `smartico_api.py` presentes. `google_ads.py` nao apareceu no Glob — **VERIFICAR** se esta em outro path ou faltando de fato. |
| 4 | Scripts raiz (7) | **OK** | Todos os `deploy_*.sh` e `run_*.sh` presentes em `ec2_deploy/`. |
| 5 | views_casino_sportsbook (secao 3) | **WARNING** | Documento admite "nao verificado no repo clonado". Antes da Fase B commit 5, **rodar diff explicito** clone-local vs `ec2_deploy/views_casino_sportsbook/`. |
| 6 | Segredos hard-coded | **OK** | Grep por `AKIA*`, `ghp_`, `sk-`, `Bearer`, `-----BEGIN`, JWTs, `password=\"...\"`, `token=\"...\"` em todos `pipelines/**` e `ec2_deploy/**`: **zero matches**. `smartico_api.py` usa `os.getenv("SMARTICO_API_TOKEN")`. Padrao `.env` consistente. |
| 7 | Escopo 3 repos "nossos" | **OK com ressalva** | Memoria confirma: `risk-matrix` (API Flask), `alert_fraud`, `top_wins`, `refresh_mv`, `freshness_check` sao do time (Gusta/Mauro). `alerta-ftd` local ja tem `.git` propria (confirmado). **Ressalva:** `project_alerta_sportsbook.md` menciona alerta SB — verificar se tem repo proprio esquecido. |
| 8 | Ordem commits Fase B | **ISSUE** | **Ordem proposta quebra dependencias.** Commit 2 (pipelines novos) vem ANTES do Commit 3 (db connectors) — `sync_google_ads_spend.py` importa de `db/google_ads.py`. Se alguem fizer checkout no Commit 2, codigo quebra (ImportError). |
| 9 | Commit backups `.bak/.bkp` | **ISSUE** | Ma pratica: polui historico git, git ja e o sistema de versionamento. Se quer preservar, criar **tag** `pre-sync-ec2-20260416` no commit anterior ou zip em release/artefato fora do repo. |
| 10 | Scan PII em reports/ (Fase D) | **OK** | Previsto no plano. Critico para repo pessoal com CSV de jogadores. |
| 11 | Duplicatas `db/db/` e `pipelines/pipelines/` | **OK** | Commit dedicado para limpeza e correto (atomicidade). |
| 12 | Merge manual pos-review (Fase B passo 11) | **OK** | Alinhado com `feedback_git_first_then_ec2_deploy.md` e `feedback_validar_antes_deploy_ec2.md`. |

## 2. Achados criticos (priorizados)

**P0 — BLOQUEANTE:**
1. **Ordem dos commits Fase B esta errada.** `db/` connectors DEVEM vir antes dos pipelines que os importam. Reordenar:
   - Commit 1: remocao duplicatas (OK)
   - Commit 2: `db/` connectors (era commit 3)
   - Commit 3: pipelines novos (era commit 2)
   - Commit 4: views_casino_sportsbook
   - Commit 5: deploy/run scripts
   - Commit 6: DEPLOY.md
   - (Commit de backups: REMOVER — ver P1)

**P1 — ALTA:**
2. **NAO commitar os 3 backups `.bak/.bkp` de `grandes_ganhos.py`.** Alternativas: (a) criar tag git `pre-sync-ec2-20260416` na main antes do sync; (b) mover os .bak para `_archive/` local (fora do push para repo time); (c) atachar zip em uma release GitHub. Mexer em `.gitignore` para garantir que `*.bak*` e `*.bkp*` nao entrem em commits futuros.
3. **SSH empirico pendente.** Auditoria nao pode ser 100% sem `ls` real na EC2. Rodar antes da Fase B: `ssh ec2-user@54.197.63.138 "ls /home/ec2-user/multibet_pipelines/{pipelines,db}/ ; crontab -l | grep -E 'pcr|sports_odds|smartico|google_ads|meta'"`.

**P2 — MEDIA:**
4. **`db/google_ads.py` nao apareceu no Glob local.** Confirmar se esta na EC2 e copiar para `ec2_deploy/db/` antes do commit, senao deploy da Fase B gera ImportError.
5. **views_casino_sportsbook diff nao feito.** Executar `diff -r` clone-do-time vs `ec2_deploy/views_casino_sportsbook/` antes do commit 4.
6. **Repos pessoais espelhados** (`multibet-analytics` vs `MultiBet-Scripts---SQLs`) — decisao do Mateus pendente (secao 7 do doc). Nao e bloqueante para Fase B, mas definir antes da Fase D.

**P3 — BAIXA:**
7. `anti_abuse_multiverso.py` status em prod — documento ja marcou "investigar" (secao 4.5). OK.

## 3. Recomendacao final

**PROCEDER COM AJUSTES.**

Condicoes para autorizar Fase B:
1. Reordenar commits (db/ antes de pipelines) — **P0**.
2. Remover commit de backups; usar tag git como alternativa — **P1**.
3. Rodar SSH real para validar lista faltantes (Mateus executa e cola output, ou libera sandbox) — **P1**.
4. Confirmar `db/google_ads.py` existe e esta em `ec2_deploy/db/` — **P2**.
5. Diff explicito `views_casino_sportsbook/` antes do commit 4 — **P2**.

Fase A (limpeza local + archive) pode prosseguir **agora** — nao toca repo compartilhado.
Fase D (repo pessoal) condicionada ao scan PII (ja previsto) e decisao sobre repo espelhado.

Nenhum commit em repo da org `GL-Analytics-M-L` ate P0+P1 resolvidos.
