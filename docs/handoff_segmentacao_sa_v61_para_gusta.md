# Handoff Gusta — Atualizar pipeline `segmentacao_sa_diaria` no Prefect/Orquestrador

**De:** Mateus
**Para:** Gusta
**Data:** 13/05/2026
**Prioridade:** P0 (entrega CRM diaria quebrada hoje)

---

## Contexto / por que urgente

A entrega de hoje (`players_segmento_SA_2026-05-13_FINAL.csv` no Slack) saiu com **9 colunas-chave 100% zeradas** porque o orquestrador rodou uma versao **anterior ao commit `c049890` (10/05)**.

Auditoria empirica confirmada:
- `ps_bi.fct_player_activity_daily` esta em **gap dbt ha 37 dias** (ultima data 06/04/2026)
- A versao antiga do pipeline ainda consultava essa tabela no Bloco 1+2 e Bloco 5b → tudo veio zero
- A versao nova (commit `c049890`) ja migrou esses blocos para `bireports_ec2.tbl_ecr_wise_daily_bi_summary` (saudavel, ultima data 13/05)

| Campo | Entrega quebrada 13/05 | Esperado (validado local) |
|---|---:|---:|
| `GGR_30D` soma | R$ 0 | R$ 9.137.818 |
| `DEPOSIT_AMOUNT_30D` soma | R$ 0 | R$ 22.399.569 |
| `BET_AMOUNT_30D` soma | R$ 0 | R$ 113.825.306 |
| `BONUS_ISSUED_30D` soma | R$ 0 | R$ 981.042 |
| Colunas | 57 | 61 |

---

## O que tem que substituir no orquestrador

**Commit alvo (HEAD atual em `main`):**
```
c049890673cc877472285992bc16596523ac588f
feat(segmentacao_sa): GGR/NGR_LIFETIME + DESCONHECIDO_id + sanity coverage + BTR tech-debt
```

**Repo:** https://github.com/mateusrfabro/multibet-analytics
**Branch:** `main`

### Arquivos que mudaram (316 insertions, 74 deletions)

| Arquivo | Status |
|---|---|
| `ec2_deploy/pipelines/segmentacao_sa_diaria.py` | substituir |
| `ec2_deploy/pipelines/segmentacao_sa_enriquecimento.py` | substituir |

(os dois sob `pipelines/` sao identicos aos `ec2_deploy/pipelines/` — substituir versao do orquestrador pelos arquivos de `ec2_deploy/pipelines/` do repo)

### Link direto raw dos arquivos

- https://raw.githubusercontent.com/mateusrfabro/multibet-analytics/c049890/ec2_deploy/pipelines/segmentacao_sa_diaria.py
- https://raw.githubusercontent.com/mateusrfabro/multibet-analytics/c049890/ec2_deploy/pipelines/segmentacao_sa_enriquecimento.py

---

## O que muda no comportamento

### A) Bloco 1+2 e 5b migrados de `ps_bi.fct_player_activity_daily` → `bireports_ec2.tbl_ecr_wise_daily_bi_summary`
Resolve o problema central. Bireports esta atualizado D-1, fct_player_activity_daily nao.

### B) Duas novas colunas: `GGR_LIFETIME`, `NGR_LIFETIME`
Pedido da operacao VIP em 09/05. Receita bruta/liquida desde abertura da conta.

### C) Duas novas colunas: `first_name`, `last_name`
Enriquecimento PII so no CSV (nao persiste em DB). Vem de `ps_bi.dim_user`.

### D) Prefixo `DESCONHECIDO_<id>` em `TOP_GAME_*` quando o mapping nao reconhece o game_id
Antes: aparecia ID cru (ex: `8842`). Agora: `DESCONHECIDO_8842` com log warning.

### E) Sanity coverage check no final do log (ALERTA AUTOMATICO)
O pipeline agora loga em ERROR se cobertura ficar abaixo do esperado:
```
SANITY COVERAGE A+S
  DEPOSIT_COUNT_30D > 0: 86.0%   ← se <50% dispara log.error
  KYC_STATUS notna:    100.0%
  TOP_GAME_1 notna:    100.0%
  first_name notna:    100.0%
  GGR_LIFETIME > 0:     91.1%
  BONUS_ISSUED_30D > 0: 27.3%
```
Isso teria evitado a quebra silenciosa de hoje. Se voce conseguir parsear o log no Prefect e mandar alerta no Slack quando aparecer `[ERROR]` na linha SANITY, melhor ainda.

### F) Validacao defensiva no `to_csv`
```python
if len(cols_csv) != 61:
    raise RuntimeError(f"CSV deveria ter 61 colunas — tem {len(cols_csv)}")
```
Falha rapido se algum bloco upstream nao popular uma coluna.

---

## Tech debt conhecido (deixado intencional)

`BTR_30D`, `BTR_CASINO_30D`, `BTR_SPORT_30D` continuam **NULL em 100%**. Motivo documentado:

A query oficial do projeto `tab_btr` mede *bonus emitido* (type=20), nao *bonus turned-real*. A fonte canonica do BTR seria `ps_bi.fct_player_activity_daily.bonus_turnedreal_base` — **mesma tabela que esta em gap dbt**. `bireports_ec2` nao expoe esse campo.

→ So volta quando dbt voltar. Acao Mauro/Gusta — ja listado em `squad_update_2026-04-25.md`.

---

## Schedule

Mantem `0 7 * * *` (07:00 UTC = 04:00 BRT — 30min apos PCR upstream). Sem mudanca de cron.

---

## Como validar pos-deploy

Apos substituir no orquestrador, rodar manual e conferir log:
```
PIPELINE CONCLUIDO — 11,984 jogadores A+S processados
```

E checar no log a secao SANITY COVERAGE A+S — todos os checks devem estar >50% (BONUS_ISSUED_30D pode ficar baixo, e OK).

Se algum check ficar em `[ERROR]`, NAO distribuir o CSV — me chamar pra investigar.

---

## Re-entrega de hoje

Ja rodei o pipeline corrigido local. CSV correto esta em:
`output/players_segmento_SA_2026-05-13_FINAL.csv` (11.984 linhas x 61 cols, 5.87 MB)

Vou subir manual no Slack do CRM como retificacao da entrega de manha.
