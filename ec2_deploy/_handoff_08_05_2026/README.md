# Handoff Segmentacao A+S — 08/05/2026

**Foco:** (a) atender pedido do head — incluir `first_name` e `last_name` no CSV diario do Slack; (b) corrigir 21 colunas zeradas em produ-cao desde ~08/04 (auditoria 08/05 detectou que `ps_bi.fct_player_activity_daily` parou de materializar em 06/04).

## 2 arquivos desta pasta vao pra EC2:

| De (aqui) | Para (EC2) | Acao |
|---|---|---|
| `pipelines/segmentacao_sa_diaria.py` | `~/multibet-analytics/pipelines/segmentacao_sa_diaria.py` | substitui |
| `pipelines/segmentacao_sa_enriquecimento.py` | `~/multibet-analytics/pipelines/segmentacao_sa_enriquecimento.py` | substitui |

Nao mexe em mais nada: sem novo `.env`, sem mudanca de cron, sem schema/DDL no Super Nova DB, sem dependencia nova.

## O que mudou

### A) Pedido do head — `first_name` / `last_name` (PII)
- CSV passou de **57 -> 59 colunas**: adicionadas `first_name` e `last_name` logo apos `external_id` no bloco IDENTIFICACAO.
- Dado vem de `ps_bi.dim_user` via Athena (query leve, ~12k linhas A+S).
- PII **nao vai pro `multibet.segmentacao_sa_diaria`** (Postgres) — fica somente no CSV (LGPD/minimizacao).

### B) Fix Bloco 1+2 — metricas 30d (financeiras + aposta)
**Antes:** 13 colunas `*_30D` zeradas em prod (GGR_30D, NGR_30D, DEPOSIT_AMOUNT_30D, BET_AMOUNT_30D, etc).
**Causa raiz:** `ps_bi.fct_player_activity_daily` parou de materializar em 06/04/2026 (mesmo gap dbt que afetou o PCR upstream — fix em commit `dfaf4ed` de 05/05).
**Fix:** migrado para `bireports_ec2.tbl_ecr_wise_daily_bi_summary` (mesma fonte usada pelo PCR pos-fix).
**Validacao empirica:** smoke test 08/05 retornou **87.0%** de cobertura `deposit_30d > 0` (antes: 0.0%).

### C) Fix Bloco 5b — BTR + bonus extras
**Antes:** 8 colunas zeradas/vazias (BONUS_ISSUED_30D, BTR_*, BONUS_DEPENDENCY_RATIO_LIFETIME, NGR_PER_BONUS_REAL_30D, LAST_BONUS_DATE/TYPE).
**Fix:** migrado pra `bireports_ec2.tbl_ecr_wise_daily_bi_summary`.
- 5 cols voltam 100% (BONUS_ISSUED_30D, BONUS_DEPENDENCY_RATIO_LIFETIME, NGR_PER_BONUS_REAL_30D, LAST_BONUS_DATE, LAST_BONUS_TYPE).
- 3 cols ficam **NULL temporariamente**: `BTR_30D`, `BTR_CASINO_30D`, `BTR_SPORT_30D` — bireports_ec2 nao expoe `bonus_turned_real`. Reativacao requer integrar `bonus_ec2` (tech-debt formal). Documentado na legenda `_legenda.txt`.

## Smoke test pos-deploy

```bash
cd ~/multibet-analytics && source venv/bin/activate
python3 pipelines/segmentacao_sa_diaria.py --no-db --no-email
```

Esperado nos logs:
```
Athena (bireports_ec2): 11,XXX players (janela ...)
Cobertura atividade 30d (deposit > 0): >70%       <-- antes: 0.0%
BTR_*_30D = NULL (bonus_turned_real ausente em bireports_ec2 ...)
Enriquecendo PII (first_name/last_name) ...
CSV salvo: output/players_segmento_SA_<data>_FINAL.csv (XXX linhas x 59 cols ...)
```

Verificar o CSV:
```bash
head -1 output/players_segmento_SA_<data>_FINAL.csv
# deve comecar com: player_id;external_id;first_name;last_name;registration_date;...

awk -F';' 'NR>1 && $9!="0" && $9!="0,00" {c++} END {print "GGR_30D > 0:", c+0}' output/players_segmento_SA_<data>_FINAL.csv
# esperado: numero alto (>=70% do total)
```

## Cron — sem alteracao

Continua `0 7 * * *` (07:00 UTC = 04:00 BRT, 30min apos PCR upstream). Nao precisa tocar no crontab.

## Pendencias conhecidas (fora do escopo deste handoff)

1. **`pipelines/crm_report_daily_v3_agent.py`** tambem usa `ps_bi.fct_player_activity_daily` — auditoria 08/05 confirmou que esta zerando `crm_campaign_daily` desde ~08/04. Sera fix separado (proximo handoff). **Nao incluido aqui** pra manter este handoff focado/atomico.
2. **`pipelines/dim_marketing_mapping.py`** (NGR por afiliado) — mesmo problema, prioridade P1.
3. **BTR_30D / BTR_CASINO_30D / BTR_SPORT_30D ficam NULL** ate integrar `bonus_ec2` (tech-debt formal).

## Nao precisa mexer em nada fora dessa pasta

Os outros pipelines do orquestrador (pcr_pipeline, segmentacao_sa_smartico, grandes_ganhos, fact_sports_odds, sync_meta_spend, etc.) **nao mudaram**.
