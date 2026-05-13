# Handoff Gusta — Matriz de Risco v2.1 (substituir no Prefect)

**De:** Mateus
**Data:** 13/05/2026
**Prioridade:** P1 (matriz nao rodou hoje no Prefect, sem log de erro)

---

## TL;DR

Substituir no Prefect os 2 grupos de arquivos desta pasta:

1. [`pipelines/risk_matrix_pipeline.py`](pipelines/risk_matrix_pipeline.py) (1 arquivo)
2. [`sql/risk_matrix/`](sql/risk_matrix/) (6 SQLs alterados — manter os outros 15 como estao)

Pipeline corrige bugs de discriminacao detectados em auditoria + adiciona logica de non-stacking de tags. Push Smartico (`push_risk_to_smartico.py`) NAO mudou — segue como esta.

---

## O que aconteceu hoje (13/05)

Snapshot diario nao foi gerado em `multibet.risk_tags` desde 11/05 (ja sao 2 dias). Prefect nao reportou erro. Hipoteses (precisam de checagem do seu lado):

- Job timeout silencioso (Athena demora mais com FAST_CASHOUT atual — JOIN cartesiano).
- Out of memory no executor.
- Falha de conexao SSH com o bastion (intermitente).
- Job desativado/pausado.

Os fixes desta entrega **reduzem o custo Athena do FAST_CASHOUT** (era O(N x M)), entao pode ate resolver o problema de timeout caso fosse essa a causa raiz.

---

## Auditoria que motivou as mudancas

Snapshot 11/05 (183.745 jogadores) mostrou:

| Tag | Cobertura ANTES | Problema |
|---|---:|---|
| `rg_alert_player` | 61.2% | Score +1 em flag de Jogo Responsavel — **risco regulatorio** (Mauro flagou em 07/04) |
| `behav_slotgamer` | 60.2% | Threshold 70% ativa em quase toda a base — tag virou ruido |
| `sustained_player` | 31.2% | "Sacou e jogou depois" = qualquer player ativo |
| `fast_cashout` | 29.2% | Qualquer par dep/saque <1h, com 16.536 jogadores tambem em CASHOUT_AND_RUN — penalidade dupla -50 |
| `reinvest_player` | 19.3% | "Sacou e depositou em 7d" = ciclo normal de player engajado |
| `zero_risk_player` | 0.0% | Tag dead code — threshold 30% e inalcancavel em base real |

Concentracao em **Mediano (35.6%) + Bom (31.2%) = 66.8%** confirma falta de discriminacao no miolo da base.

Relatorio completo (uso interno): `reports/audit_risk_matrix_2026-05-13.md`.

---

## Resultados do dry-run local (13/05/2026)

Pipeline rodado contra o Athena em dry-run, comparado com snapshot 11/05 do Postgres:

| Tag | ANTES (11/05) | DEPOIS (dry-run 13/05) | Direcao |
|---|---:|---:|---|
| `rg_alert_player` | 112.382 (61.2%) | 21.857 (11.9%) | OK — caiu, score agora -1 |
| `behav_slotgamer` | 110.539 (60.2%) | 100.995 (54.9%) | Caiu pouco — ver nota abaixo |
| `sustained_player` | 57.289 (31.2%) | 16.231 (8.8%) | OK — caiu como esperado |
| `fast_cashout` | 53.731 (29.2%) | 14.260 (7.8%) | OK — caiu como esperado |
| `reinvest_player` | 35.436 (19.3%) | 19.900 (10.8%) | OK — caiu como esperado |
| `cashout_and_run` | 21.078 (11.5%) | 5.519 (3.0%) | (nao mexi, variacao normal) |
| `zero_risk_player` | 0 (0.0%) | 31.383 (17.1%) | OK — revivida |

**Observacoes:**

- Cobertura agregada das 7 tags caiu de ~252k para ~210k (incidencia, nao players unicos).
- `cashout_and_run` caiu mesmo sem mudanca de SQL — provavelmente reflete movimentacao real da base entre 11/05 e 13/05.
- **`behav_slotgamer` ainda em 54.9%**: o fix do `total_bets` (era so casino, agora casino+SB) foi semanticamente correto, mas a base MultiBet e predominantemente casino — entao a maioria dos jogadores realmente tem >=90% das apostas em casino. O threshold 90% + min 20 bets ja apertou bem (`110k -> 101k`), mas a tag continua descritiva mais do que rara. Score +5 (baixo) reflete isso: e flag de perfil, nao de eventos. Se quiser raridade real (~10% da base), subir threshold para 95% + min 50 bets (sugestao P3 — nao bloqueante).

---

## O que mudou — diff conceitual

### 1. `RG_ALERT_PLAYER` — score +1 → -1 (compliance regulatorio)

Score positivo em flag de Jogo Responsavel e contradicao regulatoria. Mauro pediu correcao em 07/04. **Mudanca minima** (1 ponto), mas tira o sinal contraditorio.

Arquivos:
- [`sql/risk_matrix/RG_ALERT_PLAYER.sql`](sql/risk_matrix/RG_ALERT_PLAYER.sql) — `score = -1`
- [`pipelines/risk_matrix_pipeline.py`](pipelines/risk_matrix_pipeline.py) — `TAG_SCORES["RG_ALERT_PLAYER"] = -1`

### 2. `BEHAV_SLOTGAMER` — threshold 70% → 90% + min 20 bets (era 10)

Reduz cobertura de **60.2% → ~12-15%** (estimativa). Tag volta a discriminar o slotgamer puro.

Arquivo: [`sql/risk_matrix/BEHAV_SLOTGAMER.sql`](sql/risk_matrix/BEHAV_SLOTGAMER.sql)

### 3. `SUSTAINED_PLAYER` — exige >=3 dias distintos de atividade pos-saque

Antes: "qualquer aposta apos ultimo saque". Agora: "atividade sustentada em pelo menos 3 dias distintos depois do saque". Reduz cobertura de **31.2% → ~10-15%** (estimativa).

Arquivo: [`sql/risk_matrix/SUSTAINED_PLAYER.sql`](sql/risk_matrix/SUSTAINED_PLAYER.sql)

### 4. `REINVEST_PLAYER` — exige >=2 ciclos saque→deposito (era 1)

Antes: "qualquer 1 ciclo saque→deposito em 7d" = ciclo normal de player engajado. Agora: "padrao de pelo menos 2 ciclos distintos". Reduz cobertura de **19.3% → ~5-8%** (estimativa).

Score continua **+15** (nao mexi). Reduzir score nao foi necessario porque o threshold ja restringe.

Arquivo: [`sql/risk_matrix/REINVEST_PLAYER.sql`](sql/risk_matrix/REINVEST_PLAYER.sql)

### 5. `FAST_CASHOUT` — exige >=3 ocorrencias + EXISTS no lugar de JOIN cartesiano

Mudanca dupla:
- **Semantica:** captura padrao, nao evento isolado (3+ pares dep-saque <1h).
- **Custo Athena:** JOIN `deposits × cashouts` virou EXISTS correlacionado.

Reduz cobertura de **29.2% → ~5-10%** (estimativa). Pode tambem destravar timeouts no Prefect (causa raiz potencial do nao-run de hoje).

Arquivo: [`sql/risk_matrix/FAST_CASHOUT.sql`](sql/risk_matrix/FAST_CASHOUT.sql)

### 6. `ZERO_RISK_PLAYER` — threshold 30% → 50% (saia da "dead code")

Tag retornava **0 jogadores** na producao — em base real, saque medio sempre diverge >30% do deposito medio. Subindo pra 50% a tag volta a capturar perfil conservador. **Score continua 0** (neutro), entao impacto no tier e zero — so flag descritiva.

Arquivo: [`sql/risk_matrix/ZERO_RISK_PLAYER.sql`](sql/risk_matrix/ZERO_RISK_PLAYER.sql)

### 7. Non-stacking `FAST_CASHOUT` + `CASHOUT_AND_RUN` no Python

16.536 jogadores tinham AMBAS (31% dos FC). Soma de -25 + -25 = **-50** joga player legitimo direto para Muito Ruim, sem justificativa adicional.

Logica: se ambas ativas, zera `fast_cashout` na hora do `compute_scores` (mantem `cashout_and_run`, que tem o sinal extra de "inativo 48h+").

Arquivo: [`pipelines/risk_matrix_pipeline.py`](pipelines/risk_matrix_pipeline.py) — funcao `compute_scores`, bloco novo

---

## O que substituir no Prefect

| Substituir no orquestrador | Por este arquivo |
|---|---|
| versao antiga de `risk_matrix_pipeline.py` | [`pipelines/risk_matrix_pipeline.py`](pipelines/risk_matrix_pipeline.py) |
| `sql/risk_matrix/RG_ALERT_PLAYER.sql` | [`sql/risk_matrix/RG_ALERT_PLAYER.sql`](sql/risk_matrix/RG_ALERT_PLAYER.sql) |
| `sql/risk_matrix/BEHAV_SLOTGAMER.sql` | [`sql/risk_matrix/BEHAV_SLOTGAMER.sql`](sql/risk_matrix/BEHAV_SLOTGAMER.sql) |
| `sql/risk_matrix/SUSTAINED_PLAYER.sql` | [`sql/risk_matrix/SUSTAINED_PLAYER.sql`](sql/risk_matrix/SUSTAINED_PLAYER.sql) |
| `sql/risk_matrix/REINVEST_PLAYER.sql` | [`sql/risk_matrix/REINVEST_PLAYER.sql`](sql/risk_matrix/REINVEST_PLAYER.sql) |
| `sql/risk_matrix/FAST_CASHOUT.sql` | [`sql/risk_matrix/FAST_CASHOUT.sql`](sql/risk_matrix/FAST_CASHOUT.sql) |
| `sql/risk_matrix/ZERO_RISK_PLAYER.sql` | [`sql/risk_matrix/ZERO_RISK_PLAYER.sql`](sql/risk_matrix/ZERO_RISK_PLAYER.sql) |
| `sql/risk_matrix/POTENCIAL_ABUSER.sql` | [`sql/risk_matrix/POTENCIAL_ABUSER.sql`](sql/risk_matrix/POTENCIAL_ABUSER.sql) |

**NAO mudar (14 SQLs nao tocados):**
REGULAR_DEPOSITOR, PROMO_ONLY, NON_BONUS_DEPOSITOR, PROMO_CHAINER, CASHOUT_AND_RUN, NON_PROMO_PLAYER, ENGAGED_PLAYER, BEHAV_RISK_PLAYER, PLAYER_REENGAGED, SLEEPER_LOW_PLAYER, VIP_WHALE_PLAYER, WINBACK_HI_VAL_PLAYER, MULTI_GAME_PLAYER, ROLLBACK_PLAYER.

### Bug descoberto rodando v2.1 em prod (13/05)

`POTENCIAL_ABUSER.sql` quebrou com `COLUMN_NOT_FOUND: 'u.c_created_time' cannot be resolved`. Coluna correta validada: `c_signup_time`. O "fix" de 20/04 que tinha trocado `first_deposit` por `c_created_time` **nunca foi exercitado em prod** — provavelmente o Prefect rodava versao ainda mais antiga (usando proxy de primeiro deposito), por isso ninguem viu o erro ate hoje. Ja corrigido. **IMPORTANTE pro Gusta**: ao subir, garantir que o `POTENCIAL_ABUSER.sql` desta pasta esta na versao com `c_signup_time`.

**Dependencias** (continuam iguais, ja existem no Prefect):
- `db/athena.py`
- `db/supernova.py`
- `db/smartico_api.py` (so usado pelo push)

---

## Schedule

**Mantem `0 5 * * *` (05:00 UTC = 02:00 BRT)** para o `risk_matrix_pipeline.py`.
**Mantem `30 5 * * *` (05:30 UTC = 02:30 BRT)** para o `push_risk_to_smartico.py` (NAO mudou).

---

## Como validar pos-deploy

1. **Rodar manual** o pipeline e conferir no log a secao final:
   ```
   Distribuicao por tier:
     Muito Bom      :  X (Y.Y%)
     Bom            :  X (Y.Y%)
     Mediano        :  X (Y.Y%)
     Ruim           :  X (Y.Y%)
     Muito Ruim     :  X (Y.Y%)
     SEM SCORE      :  X (Y.Y%)
   ```

2. **Esperar este shift na distribuicao** (vs snapshot 11/05):
   - `Muito Bom`: pode cair ~1-2% (SUSTAINED, REINVEST menos generosos)
   - `Bom`: pode cair ~1-2% pelo mesmo motivo
   - `Mediano`: estavel ou +1%
   - `Ruim` / `Muito Ruim`: pode cair ~1-2% (FAST_CASHOUT mais restritivo + non-stacking FC+CR)
   - `SEM SCORE`: pode subir ~1-3% (tags mais restritivas reduzem ativacao)

3. **Conferir no Postgres apos o run:**
   ```sql
   SELECT tier, COUNT(*) FROM multibet.risk_tags
   WHERE snapshot_date = CURRENT_DATE GROUP BY tier ORDER BY 2 DESC;

   -- Cobertura por tag (validacao especifica das mudancas)
   SELECT
     SUM(CASE WHEN rg_alert_player <> 0 THEN 1 ELSE 0 END) AS rg_alert,
     SUM(CASE WHEN behav_slotgamer <> 0 THEN 1 ELSE 0 END) AS slotgamer,
     SUM(CASE WHEN sustained_player <> 0 THEN 1 ELSE 0 END) AS sustained,
     SUM(CASE WHEN reinvest_player <> 0 THEN 1 ELSE 0 END) AS reinvest,
     SUM(CASE WHEN fast_cashout <> 0 THEN 1 ELSE 0 END) AS fc,
     SUM(CASE WHEN cashout_and_run <> 0 THEN 1 ELSE 0 END) AS cr,
     SUM(CASE WHEN fast_cashout <> 0 AND cashout_and_run <> 0 THEN 1 ELSE 0 END) AS overlap_fc_cr,
     SUM(CASE WHEN zero_risk_player <> 0 THEN 1 ELSE 0 END) AS zero_risk
   FROM multibet.risk_tags
   WHERE snapshot_date = CURRENT_DATE;
   ```

   **Sucesso:** `overlap_fc_cr = 0` (non-stacking funcionando), `behav_slotgamer < 30k`, `zero_risk > 0`.

4. **Push Smartico** deve rodar normalmente 30min depois. Confere `pipelines/logs/push_smartico_2026-05-XX.log` — espera `sent=` na faixa de 5-15k (mais que de costume porque varias tags mudam de status).

---

## Commit de referencia

```
<sera preenchido apos push>
```

**Repo:** https://github.com/mateusrfabro/multibet-analytics
**Branch:** `main`
**Pasta no repo:** `handoffs/risk_matrix_v2_1_para_gusta/`

Qualquer duvida me chama. Valeu Gusta!
