# Handoff Gusta — Atualizar pipeline `risk_matrix_pipeline` no Prefect/Orquestrador

**De:** Mateus
**Para:** Gusta
**Data:** 14/05/2026
**Prioridade:** P1 (correcoes de tech-debt — sem incidente em producao)

---

## Contexto / por que essa mudanca

Apos a entrega de hoje 14/05 (matriz rodou local, 183.749 jogadores, 21 tags, push pro Smartico OK com 7.857 diffs), rodei uma **auditoria multi-fonte** com 3 agentes especializados (auditor tecnico, risk-analyst iGaming, statistician) + spot-check empirico na tabela `multibet.risk_tags`.

Os 5 fixes aplicados aqui sao todos de **tech-debt tecnico baixo risco** — nao mudam contrato com Smartico nem comportamento de tier. Decisoes de produto (substituir tags, fundir tags, adicionar novas tags antifraude) ficaram fora deste pacote e estao listadas como follow-up no final.

---

## O que tem que substituir no orquestrador

**Commit alvo (HEAD `main`):**
```
9bfa02275cf3ba900d19d20d2b4af770be22671a
feat(risk_matrix): v2.2 — 5 fixes de auditoria + handoff Gusta
```

**Repo:** https://github.com/mateusrfabro/multibet-analytics
**Branch:** `main`

### Link direto raw dos arquivos principais

- https://raw.githubusercontent.com/mateusrfabro/multibet-analytics/9bfa022/ec2_deploy/pipelines/risk_matrix_pipeline.py
- https://raw.githubusercontent.com/mateusrfabro/multibet-analytics/9bfa022/ec2_deploy/sql/risk_matrix/PROMO_CHAINER.sql

### Arquivos que mudaram

| Arquivo | Status | Mudanca |
|---|---|---|
| `ec2_deploy/pipelines/risk_matrix_pipeline.py` | substituir | Fix #3, #4, #5 |
| `ec2_deploy/sql/risk_matrix/ENGAGED_PLAYER.sql` | substituir | Fix #1 timezone BRT |
| `ec2_deploy/sql/risk_matrix/RG_ALERT_PLAYER.sql` | substituir | Fix #1 timezone BRT |
| `ec2_deploy/sql/risk_matrix/PROMO_CHAINER.sql` | substituir | Fix #1 timezone BRT + Fix #2 ratio bug |
| `ec2_deploy/sql/risk_matrix/PLAYER_REENGAGED.sql` | substituir | Fix #1 timezone BRT |
| `ec2_deploy/sql/risk_matrix/SLEEPER_LOW_PLAYER.sql` | substituir | Fix #1 timezone BRT |
| `ec2_deploy/sql/risk_matrix/REGULAR_DEPOSITOR.sql` | substituir | Fix #1 timezone BRT (DATE_TRUNC) |
| `ec2_deploy/sql/risk_matrix/PROMO_ONLY.sql` | substituir | Fix #1 timezone BRT (2 ocorrencias) |
| `ec2_deploy/sql/risk_matrix/VIP_WHALE_PLAYER.sql` | substituir | Fix #1 timezone BRT |
| `ec2_deploy/sql/risk_matrix/ROLLBACK_PLAYER.sql` | substituir | Fix #1 timezone BRT |
| `ec2_deploy/sql/risk_matrix/WINBACK_HI_VAL_PLAYER.sql` | substituir | Fix #1 timezone BRT |

Os arquivos sob `scripts/sql/risk_matrix/` foram sincronizados automaticamente — sao identicos aos `ec2_deploy/sql/risk_matrix/`.

---

## Os 5 fixes em detalhe

### Fix #1 — Timezone BRT em 10 SQLs (regra de ouro CLAUDE.md)

**Antes:**
```sql
CAST(t.c_start_time AS DATE)
DATE_TRUNC('month', c.c_created_time)
```

**Depois:**
```sql
CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
DATE_TRUNC('month', c.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
```

**Why:** Antes a "fronteira do dia" era 00:00 UTC = 21:00 BRT. Jogador que jogava as 22h BRT caia no dia seguinte. Distorcia contagens de `active_days`, `monthly_deposits`, `bonus_day_count`, `rollback_days` em ate ~3% dos casos na virada de dia. Agora alinha com o timezone do negocio.

**Como aplicar:** substituicao literal nos 10 SQLs listados.

### Fix #2 — PROMO_CHAINER ratio bug

**Bug:** as CTEs `bonus_days` e `activity_days` eram independentes. Podia haver dia de bonus SEM aposta, gerando `bonus_day_count > active_day_count` e ratio > 1 — flag sempre disparava.

**Fix:** reescrita do SQL completo (mantive comentarios documentando a mudanca).
- `bonus_days_raw` e `activity_days_raw` continuam separadas.
- Adicionei `day_flags` que faz `UNION ALL` para gerar `(user_id, day, has_bonus, has_bet)`.
- `agg` calcula `bonus_days`, `active_days`, `bonus_and_bet_days` (intersecao).
- Filtro agora usa `bonus_and_bet_days / active_days >= 0.80` — ratio sempre <= 1.

**Impacto esperado:** queda **moderada** na contagem de PROMO_CHAINER (jogadores que so tinham bonus em dias sem aposta nao serao mais flagados — eram falsos positivos). A magnitude exata depende dos dados; rodar smoke test e comparar.

### Fix #3 — Legenda RG_ALERT (+1 → -1)

`risk_matrix_pipeline.py:472` dizia `" +1 | 10+ sessoes/dia (alerta jogo responsavel)"` na legenda exportada. O codigo (TAG_SCORES) e o SQL ja usavam `-1` desde o fix do dia 07/04 (compliance Mauro). So a doc estava desalinhada.

Mudei pra `-1`. Apenas cosmetico — Smartico recebia o valor certo.

### Fix #4 — Remover `player_not_valid` (coluna fantasma)

Coluna `player_not_valid` existia em `multibet.risk_tags` mas nunca foi populada nas execucoes da v2 (auditoria mostrou 100% NULL/0). Vestigio da v1.

Adicionei migracao defensiva no `save_to_postgres()`:
```python
legacy_cols = {"player_not_valid"}
for col in legacy_cols:
    if col in existing:
        cur.execute(f"ALTER TABLE ... DROP COLUMN IF EXISTS {col};")
```

Idempotente — roda 1x e zera. Apos a primeira execucao na EC2, pode remover o bloco se quiser limpar o codigo.

### Fix #5 — Log pos-merge

**Bug de observabilidade:** o log do pipeline antes mostrava so `len(df)` do SQL bruto, antes do merge com `user_base` (cohort financeiro 90d). Auditoria revelou diferencas grandes:

| Tag | Log (SQL raw) | Tabela (pos-merge) | Diferenca |
|---|---:|---:|---:|
| POTENCIAL_ABUSER | 4.007 | 1.141 | **-71%** |
| SLEEPER_LOW_PLAYER | 51.431 | 44.777 | -13% |
| ENGAGED_PLAYER | 35.099 | 33.757 | -4% |

Nao e bug do pipeline (POTENCIAL_ABUSER por design pega signup<2d, muitos ainda nao depositaram), mas a leitura do log enganava. Agora o pipeline tambem loga `Distribuicao por tag (POS-merge cohort 90d)` antes do COPY, com as contagens reais que vao pro Smartico.

---

## Como validar pos-deploy

Apos substituir no orquestrador, rodar manual e conferir no log:

1. **Cabecalho de cada SQL com timezone:** os logs nao mostram o SQL em si, mas o resultado deve ser **levemente diferente** vs ontem (algumas contagens variam ~1-3% pelo realinhamento BRT).
2. **PROMO_CHAINER:** contagem deve cair vs historico (ex: hoje foi 11.523 pos-merge → esperar algo menor ainda). Se cair > 50%, me chamar — pode ter regressao logica.
3. **Bloco novo no log:** procurar a linha `Distribuicao por tag (POS-merge cohort 90d)` apos a `Distribuicao por tier`. Deve listar as 21 tags com count + %.
4. **DROP COLUMN:** primeira execucao deve logar `DROP COLUMN (legacy) player_not_valid`. Execucoes seguintes nao logam mais (idempotente).

Se algo der erro de sintaxe Athena (timezone), me chamar — fix #1 e #2 nao foram smoke-testados na cadeia completa, so com `--dry-run --only` em 3 tags.

---

## Schedule

Mantem o cron atual. Sem mudanca de horario.

---

## Decisoes de produto NAO inclusas neste handoff (follow-up)

Auditoria identificou ainda 5 itens de **decisao de produto** que ficaram FORA deste pacote por afetarem contrato com Smartico / regras de scoring. Estao listados aqui pra ficarem registrados pro alinhamento com Castrin/CGO:

| Item | Achado | Acao sugerida |
|---|---|---|
| A | `zero_risk_player` peso 0 = invisivel no Smartico (31.471 players sem essa tag no CRM) | Mudar peso para +1 OU mudar storage para 0/1 + score separado |
| B | `BEHAV_SLOTGAMER` em 55% da base — nao discrimina | Substituir por `slotgamer_heavy` (top quartil em bet em slot) |
| C | Cluster correlacionado (regular_dep + reinvest + multi_game + rg_alert + sustained, phi ~0.5) | Fundir / hierarquizar — 5 tags medem o mesmo perfil |
| D | Gap antifraude: faltam tags MULTI_ACCOUNT_SUSPECT, SHARED_PAYMENT_METHOD, STRUCTURING_AML, NEW_PLAYER_FAST_WITHDRAW, CHARGEBACK_HISTORY | Desenhar SQLs novos no proximo sprint |
| E | RG_ALERT em 12% da base — compliance vai questionar | Revisar limiar de "sessao" no SQL |

---

## Re-entrega de hoje

A entrega de hoje 14/05 ja esta no ar com o pipeline v2.1 (anterior). Os fixes deste handoff entram na **proxima rodada (15/05 ou seguinte)** assim que o orquestrador estiver atualizado. Nao precisa retificacao manual hoje.

---

## Arquivos relevantes

- Auditoria completa: `logs/audit_risk_matrix_2026-05-14.log` + `logs/diag_tags_2026-05-14.log`
- Pipeline atualizado: `ec2_deploy/pipelines/risk_matrix_pipeline.py`
- SQLs atualizados: `ec2_deploy/sql/risk_matrix/*.sql` (10 arquivos)
- Smoke test: `logs/smoke_v2_2_2026-05-14.log`
