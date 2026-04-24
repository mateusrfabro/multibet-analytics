# Revisao Best Practices — Plano de Migracao MultiBet

**Data:** 2026-04-16 | **Revisor:** best-practices agent | **Alvo:** `mapping_arquivo_repo.md`

## Resumo Executivo
Plano solido, mas tem 4 riscos criticos a mitigar antes de push: (1) `.gitignore` nao cobre `*.pem` nem `ec2_deploy/.env`, (2) bug `db/db/` precisa `git rm -r` em commit SEPARADO do sync (senao polui history), (3) DEPLOY.md so cobre 5 de 10 pipelines vivos, (4) nao ha mecanismo anti-desync futuro. Recomendacao forte: branch com escopo unico, squash merge desabilitado (preservar commits atomicos), PR obrigatoria no `multibet_pipelines`, scan PII via regex CPF/email antes do push pessoal.

## 1. Estrategia de branches/PR — Prioridade ALTA
**Situacao:** branch `sync-ec2-prod-2026-04-16` proposta, 7 commits atomicos, merge "manual".
**Recomendacao:** (a) renomear para `chore/sync-ec2-prod` (sem data — Git ja tem timestamp); (b) usar **merge commit (--no-ff)** no `multibet_pipelines`, NAO squash — os 7 commits atomicos sao o valor do PR e squash joga fora a rastreabilidade de "o que veio de onde"; (c) commit 1 (remover `db/db/`) vai em PR SEPARADO e merged ANTES — evita conflito de rename/delete no PR principal; (d) proteger `main` no GitHub (Settings → Branches → require PR + 1 review do Mauro/Gusta).

## 2. Convencoes de commit — Prioridade MEDIA
**Situacao:** Conventional Commits aplicado corretamente nos 7 commits.
**Recomendacao:** (a) commit 2 esta gigante — quebrar em 2: `feat: add marketing pipelines (google_ads, meta, smartico_push)` + `feat: add analytics pipelines (pcr, fact_sports_odds, export_smartico)`; (b) commit 6 "preserve EC2 backups" deveria ser `chore: archive grandes_ganhos legacy versions` e ir para subpasta `_archive/` — commitar `.bak` na raiz eh anti-pattern; (c) incluir escopo: `feat(marketing):`, `feat(risk):`, `chore(cleanup):`; (d) corpo de commit obrigatorio citando commit EC2 equivalente (se houver) ou data de edicao em prod.

## 3. Seguranca `.gitignore` — Prioridade ALTA (CRITICO)
**Situacao:** cobre `.env`, `*credentials*.json`, `dwh-ext-24105.json`. **FALHA grave:** nao ignora `*.pem`, `*.key`, `*_rsa`, `*token*`, nem `ec2_deploy/.env` especifico.
**Recomendacao:** adicionar bloco:
```
# SSH keys e tokens
*.pem
*.key
*_rsa
*_rsa.pub
id_ed25519*
bastion-analytics-key*
etl-key*
*token*.txt
*.p12
*.pfx

# Envs especificos
ec2_deploy/.env
anti_abuse_deploy/.env
**/.env.local
**/.env.production

# Output sensivel
output/risk_matrix_*.csv
reports/**/*_users_*.csv
reports/**/*_players_*.xlsx
```
**Antes do push:** rodar `git log --all --full-history -- "*.pem" "*credentials*.json"` no clone limpo pra garantir que nada vazou historicamente. Se vazou: usar `git filter-repo` + rotacionar credencial.

## 4. Refatoracao `db/db/` e `pipelines/pipelines/` — Prioridade ALTA
**Situacao:** pastas duplicadas por erro de commit antigo. EC2 tambem tem o bug.
**Recomendacao:** (a) antes de qualquer coisa, SSH na EC2 e rodar `diff -r /home/ec2-user/multibet/db/ /home/ec2-user/multibet/db/db/` — se forem identicas, sao seguras para remover; se divergirem, comparar hash/mtime e escolher a mais recente; (b) no repo: `git rm -r db/db pipelines/pipelines` em PR proprio, com titulo explicito "fix: remove duplicate nested folders (legacy commit error)"; (c) validar que nenhum import usa `from db.db.x` ou `from pipelines.pipelines.x` via `grep -r "db.db\|pipelines.pipelines" --include="*.py"`; (d) apos merge, SSH EC2 e `rm -rf db/db pipelines/pipelines` — mas SO depois de confirmar que git pull nao vai recriar.

## 5. DEPLOY.md desatualizado — Prioridade ALTA
**Situacao:** documenta 5 pipelines (grandes_ganhos, anti_abuse, etl_aquisicao, risk_matrix, sync_meta). Faltam 5: `sync_google_ads_spend`, `pcr_pipeline`, `fact_sports_odds_performance`, `push_risk_to_smartico`, `export_smartico_sent_today`.
**Recomendacao:** (a) criar tabela-indice no topo com **todos** os pipelines (nome, cron, BRT/UTC, tabela destino, owner); (b) padronizar bloco por pipeline: Arquivos / Deploy / Teste manual / Crontab / Logs / Destino / Rollback; (c) adicionar secao "Crontab completo consolidado" com todas as linhas num unico bloco — hoje cada pipeline tem sua secao e eh facil ter conflito de horario; (d) adicionar secao "Como validar pos-deploy" (smoke test padronizado); (e) mover DEPLOY.md para `multibet_pipelines/docs/DEPLOY.md` na raiz do repo.

## 6. Anti-desync git ↔ EC2 — Prioridade ALTA
**Situacao:** plano conserta o gap atual mas nao previne reincidencia. Risco de repetir daqui 2 meses.
**Recomendacao (pragmatico, implementar em 1 dia):**
1. **Cron diario de drift check na EC2** (06:00 UTC, antes do deploy humano): script que faz `cd /home/ec2-user/multibet && git fetch && git status --porcelain` e, se houver diff, posta no Slack `#data-alerts` com lista de arquivos modificados. Tempo de setup: 30min.
2. **Pre-commit hook no repo** (`.pre-commit-config.yaml`): bloquear commit de arquivos `*.pem`, `.env`, `*credentials*.json`, e rodar `detect-secrets scan`. Zero custo de CI.
3. **CHANGELOG.md obrigatorio** no `multibet_pipelines` — toda alteracao em prod vai para changelog em commit, forca disciplina.
4. **(opcional, fase 2) GitHub Action** que abre issue automaticamente se arquivo em `pipelines/` nao foi modificado ha >90 dias (detecta pipeline esquecido).

## 7. Scan PII em reports — Prioridade ALTA
**Situacao:** `reports/` tem 103 arquivos, muitos CSV/XLSX com user_id, external_id, nomes, CPF, email, mobile de jogadores.
**Recomendacao pragmatica (30min de trabalho, zero dependencia externa):**
1. **Nao subir CSV/XLSX de `reports/` no repo pessoal** (regra simples) — `.gitignore` ja tem `reports/*.csv`, estender para `reports/**/*.xlsx` e `reports/**/*.txt`.
2. **Script Python de scan** (`scripts/scan_pii.py`) que percorre arquivos a commitar e aplica regex: CPF `\d{3}\.\d{3}\.\d{3}-\d{2}|\d{11}`, email `[\w.+-]+@[\w-]+\.[\w.-]+`, mobile BR `\+?55?\s?\(?\d{2}\)?\s?9?\d{4}-?\d{4}`, external_id 8+ digitos em contexto de coluna `user_id|external_id|cpf|email|phone`. Abortar commit se match.
3. **Para HTML de report ja publicados** (PCR, risk matrix, CRM dashboard): subir apenas template/codigo, nunca o HTML renderizado com dados reais.
4. **Manual review final** — 15min rodando `git diff --cached | grep -iE "cpf|email|@.*\..*|55\d{10,}"` antes do push.

---

**Conclusao:** plano aprovado com ressalvas. Bloqueios para iniciar Fase B: reforcar `.gitignore` (item 3), quebrar commit 2, separar PR do bug `db/db/`. Com esses 3 ajustes, execucao fica segura.
