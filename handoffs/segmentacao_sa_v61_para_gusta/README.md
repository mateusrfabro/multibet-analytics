# Handoff Gusta — Pipeline `segmentacao_sa_diaria` (substituir no Prefect/Orquestrador)

**De:** Mateus
**Data:** 13/05/2026
**Prioridade:** P0 (entrega CRM diaria quebrada hoje)

---

## TL;DR

Substituir no orquestrador os 2 arquivos da pasta [`pipelines/`](pipelines/) ao lado deste README. Sem isso, **amanha (14/05) quebra de novo** do mesmo jeito que quebrou hoje.

---

## O que aconteceu hoje

A entrega `players_segmento_SA_2026-05-13_FINAL.csv` saiu com **9 colunas-chave 100% zeradas** (GGR_30D, NGR_30D, Deposito 30D, Aposta 30D, Saque 30D, Bonus 30D, e os respectivos counts).

| Campo | Entrega quebrada 13/05 | Esperado (validado local agora) |
|---|---:|---:|
| `GGR_30D` soma | R$ 0 | R$ 9.137.818 |
| `NGR_30D` soma | R$ 0 | R$ 8.156.775 |
| `DEPOSIT_AMOUNT_30D` | R$ 0 | R$ 22.399.569 |
| `BET_AMOUNT_30D` | R$ 0 | R$ 113.825.306 |
| `BONUS_ISSUED_30D` | R$ 0 | R$ 981.042 |
| Colunas | 57 | 61 |

CSV corrigido ja foi re-enviado manual no canal CRM do Slack as 10h32.

---

## Root cause

O orquestrador roda uma versao do pipeline **anterior ao commit `c049890` (10/05)** que ainda consulta `ps_bi.fct_player_activity_daily` no Bloco 1+2 e Bloco 5b.

Validacao empirica feita hoje contra Athena:
```
ps_bi.fct_player_activity_daily               ultima data = 2026-04-06  (37 dias de gap)
bireports_ec2.tbl_ecr_wise_daily_bi_summary   ultima data = 2026-05-13  (saudavel)
```

A versao nova do pipeline (commit `c049890`, que esta nesta pasta) ja migrou todos os blocos 30D para `bireports_ec2`, que esta atualizado D-1.

---

## O que tem nesta pasta

```
handoffs/segmentacao_sa_v61_para_gusta/
├── README.md                          (este arquivo)
└── pipelines/
    ├── segmentacao_sa_diaria.py       (entry point, executar este)
    └── segmentacao_sa_enriquecimento.py  (modulo importado pelo diaria)
```

Os dois arquivos sao copias **identicas** ao que esta em [`ec2_deploy/pipelines/`](../../ec2_deploy/pipelines/) no commit `c049890`. Mantive copia separada aqui pra facilitar o substituicao no orquestrador sem voce precisar navegar o repo.

---

## O que substituir

| Substituir no orquestrador | Por este arquivo |
|---|---|
| versao antiga de `segmentacao_sa_diaria.py` | [`pipelines/segmentacao_sa_diaria.py`](pipelines/segmentacao_sa_diaria.py) |
| versao antiga de `segmentacao_sa_enriquecimento.py` | [`pipelines/segmentacao_sa_enriquecimento.py`](pipelines/segmentacao_sa_enriquecimento.py) |

**Dependencias** (continuam iguais, ja existem no orquestrador):
- `db/athena.py`
- `db/supernova.py`
- `db/slack_uploader.py`
- `db/email_sender.py`
- `pipelines/segmentacao_sa_smartico.py` (nao mudou)

---

## Schedule

**Mantem `0 7 * * *` (07:00 UTC = 04:00 BRT — 30min apos PCR upstream).** Sem mudanca de cron.

---

## O que muda no comportamento

### A) Bloco 1+2 e 5b migrados — fix do problema principal
`ps_bi.fct_player_activity_daily` → `bireports_ec2.tbl_ecr_wise_daily_bi_summary`. Resolve o zero em GGR/NGR/Deposito/Aposta/Saque/Bonus.

### B) +4 colunas novas (CSV passou de 57 → 61)
- `first_name`, `last_name` — PII so no CSV, nao persiste em DB (vem de `ps_bi.dim_user`)
- `GGR_LIFETIME`, `NGR_LIFETIME` — pedido da operacao VIP em 09/05

### C) Sanity coverage no log final (alerta automatico)
O pipeline agora loga em `[ERROR]` se cobertura ficar abaixo do esperado. Exemplo da rodada de hoje as 10h11:
```
SANITY COVERAGE A+S
  DEPOSIT_COUNT_30D > 0:  86.0%
  KYC_STATUS notna:      100.0%
  TOP_GAME_1 notna:      100.0%
  first_name notna:      100.0%
  GGR_LIFETIME > 0:       91.1%
  BONUS_ISSUED_30D > 0:   27.3%   ← unico que pode ficar baixo, e OK
```

**Sugestao:** se voce conseguir parsear o log no Prefect e alertar no Slack quando aparecer `[ERROR]` na secao SANITY, isso teria evitado a quebra silenciosa de hoje. Nao bloqueante, mas seria util.

### D) Validacao defensiva no `to_csv` (linha 864-866)
```python
missing = [c for c in cols_csv if c not in df_csv.columns]
if missing:
    raise RuntimeError(f"CSV missing colunas: {missing}")
if len(cols_csv) != 61:
    raise RuntimeError(f"CSV deveria ter 61 colunas — tem {len(cols_csv)}")
```
Falha rapido se algum bloco upstream nao popular uma coluna esperada.

### E) Prefixo `DESCONHECIDO_<id>` em `TOP_GAME_*`
Quando `multibet.game_image_mapping` nao tem o game_id, marca com `DESCONHECIDO_8842` em vez de soltar ID cru. Tech-debt formal: popular o mapping (acao Mauro/Gusta).

---

## Tech-debt conhecido (continua NULL — intencional)

`BTR_30D`, `BTR_CASINO_30D`, `BTR_SPORT_30D` continuam **NULL em 100%**.

Motivo: a fonte canonica REAL de bonus_turned_real seria `ps_bi.fct_player_activity_daily.bonus_turnedreal_base` — **mesma tabela que esta em gap dbt ha 37 dias**. `bireports_ec2` nao expoe esse campo. So volta quando o dbt voltar.

---

## Como validar pos-deploy

Apos substituir os 2 arquivos no orquestrador e rodar manual, conferir no log:

1. Linha final: `PIPELINE CONCLUIDO — 11,984 jogadores A+S processados` (numero varia D-1, mas tem que aparecer "CONCLUIDO")
2. Secao `SANITY COVERAGE A+S` — todos os checks `>50%` exceto BONUS_ISSUED_30D que pode ficar baixo
3. Mensagem final: `CSV salvo: output/players_segmento_SA_<DATA>_FINAL.csv (... linhas x 61 cols ...)`

Se aparecer `[ERROR]` na secao SANITY ou `RuntimeError: CSV deveria ter 61 colunas`, **NAO distribuir o CSV** — me chama.

---

## Commit de referencia

```
c049890673cc877472285992bc16596523ac588f
feat(segmentacao_sa): GGR/NGR_LIFETIME + DESCONHECIDO_id + sanity coverage + BTR tech-debt
```

**Repo:** https://github.com/mateusrfabro/multibet-analytics
**Branch:** `main`

Qualquer duvida me chama. Valeu Gusta!
