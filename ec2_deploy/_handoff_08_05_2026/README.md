# Handoff Segmentacao A+S — 08/05/2026 (atualizado 09/05/2026)

**Foco original (08/05):** (a) atender pedido do head — incluir `first_name` e `last_name` no CSV diario do Slack; (b) corrigir 21 colunas zeradas em produ-cao desde ~08/04 (auditoria 08/05 detectou que `ps_bi.fct_player_activity_daily` parou de materializar em 06/04).

**Atualizacao 10/05 (pos-queixa operacao VIP — sabado tarde + investigacao completa BTR):**
- Adicionadas **GGR_LIFETIME** e **NGR_LIFETIME** (feature pedida pelo head: valor historico total do jogador — chave pra Tier S). 96.9% cobertura empirica.
- **BTR_30D / BTR_CASINO_30D / BTR_SPORT_30D continuam NULL** — investigacao consolidada do dia descobriu que a "query oficial" do projeto medi a coisa errada. Fonte canonica real (`fct_player_activity_daily.bonus_turnedreal_base`) esta em gap dbt. Sem fallback. Tech-debt formal documentado.
- **TOP_GAME_1** com IDs cru ("8842", "Wild Ape 3258") ganha prefixo `DESCONHECIDO_` + log de warning, marcando que o `multibet.game_image_mapping` esta incompleto pra esses IDs (acao paralela: pingar Mauro/Gusta pra popular).
- Legenda + Notion atualizados (61 cols, BTR documentado, LIFETIME).

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
- **BTR_30D / BTR_CASINO_30D / BTR_SPORT_30D continuam NULL** — investigacao consolidada de 10/05/2026 (auditoria empirica + 3 agentes especializados):
  - A "query oficial" do projeto (`btr_oficial_20abr.py` -> `multibet.tab_btr`) mede BONUS EMITIDO, nao `bonus_turned_real` (validado: agregado A+S 30d = R$ 918.921 em type=20 vs R$ 919.045 em bireports.c_bonus_issued, delta 0.01%).
  - **Fonte canonica real** identificada: `ps_bi.fct_player_activity_daily.bonus_turnedreal_base` com split `casino_*`/`sports_book_*`. Validada empiricamente pelo head em `.tmp_ssm/validate_btr_step6.py`. Universo: 136.870 players, R$ 14M total (jun/25-06/04/26).
  - **PROBLEMA:** essa tabela esta em GAP DBT desde 06/04/2026 (mesmo gap que afetou Bloco 1+2 e foi migrado pra bireports). bireports nao expoe `bonus_turnedreal_base` -> nao ha fallback.
  - **UNICA SOLUCAO:** resolver gap dbt (acao Mauro/Gusta — ja listado em `docs/_handoff/squad_update_2026-04-25.md:129`). Quando voltar, reativacao do BTR no pipeline e trivial (~10 linhas em `bloco_5b_btr_bonus`).
  - Memoria atualizada com aprendizado completo: `memory/feedback_btr_valor_na_subfund.md`.

### D) NEW — GGR_LIFETIME + NGR_LIFETIME (operacao VIP, 09/05)
- 2 colunas novas adicionadas a CTE `metrics_lifetime` (custo zero — ja existia para ticket lifetime).
- Schema CSV: 59 -> 61 colunas. DDL atualizado (`ALTER TABLE IF NOT EXISTS` adiciona ambas idempotente).
- Persiste em `multibet.segmentacao_sa_diaria` (diferente de PII, que fica so no CSV).

### E) NEW — Fix enriquecimento jogo (DESCONHECIDO_<id>)
- Quando `multibet.game_image_mapping` nao tem o `provider_game_id`, em vez de cair com ID cru ("8842"), agora marca como `DESCONHECIDO_<id>` + warning no log.
- Acao paralela (Mauro/Gusta): popular o mapping para os IDs em warning.

## Smoke test pos-deploy

```bash
cd ~/multibet-analytics && source venv/bin/activate
python3 pipelines/segmentacao_sa_diaria.py --no-db --no-email
```

Esperado nos logs:
```
Athena (bireports_ec2): 11,XXX players (janela ...)
Cobertura atividade 30d (deposit > 0): >70%       <-- antes: 0.0%
BTR_*_30D = NULL (auditoria 10/05 mostrou que c_txn_type=20 mede bonus emitido — tech-debt formal)
Enriquecendo PII (first_name/last_name) ...
N game_ids sem nome no game_image_mapping (ex: [...]) — marcando DESCONHECIDO_<id>
CSV salvo: output/players_segmento_SA_<data>_FINAL.csv (XXX linhas x 61 cols ...)
======================================================================
SANITY COVERAGE A+S
======================================================================
  DEPOSIT_COUNT_30D > 0: 8X.X%
  KYC_STATUS notna:      100.0%
  TOP_GAME_1 notna:      100.0%
  first_name notna:       9X.X%
  GGR_LIFETIME > 0:       9X.X%
  BONUS_ISSUED_30D > 0:   2X.X%
```

Verificar o CSV:
```bash
head -1 output/players_segmento_SA_<data>_FINAL.csv
# deve comecar com: player_id;external_id;first_name;last_name;registration_date;...

# valida 61 colunas
head -1 output/players_segmento_SA_<data>_FINAL.csv | tr ';' '\n' | wc -l   # esperado: 61

# valida GGR_LIFETIME / NGR_LIFETIME populados (esperado ~96%)
awk -F';' 'NR==1{for(i=1;i<=NF;i++){if($i=="GGR_LIFETIME") g=i; if($i=="NGR_LIFETIME") n=i}; next} {tot++; if($g!="0,00" && $g!="") cg++; if($n!="0,00" && $n!="") cn++} END {print "GGR_LIFETIME > 0:", cg, "/", tot; print "NGR_LIFETIME > 0:", cn, "/", tot}' output/players_segmento_SA_<data>_FINAL.csv

# valida BTR_30D vazio (esperado 100% vazio — fonte canonica em gap dbt)
awk -F';' 'NR==1{for(i=1;i<=NF;i++) if($i=="BTR_30D") b=i; next} {n++; if($b=="") c++} END {print "BTR_30D vazio:", c, "de", n, "(esperado 100%)"}' output/players_segmento_SA_<data>_FINAL.csv
```

## Cron — sem alteracao

Continua `0 7 * * *` (07:00 UTC = 04:00 BRT, 30min apos PCR upstream). Nao precisa tocar no crontab.

## Pendencias conhecidas (fora do escopo deste handoff)

1. **`pipelines/crm_report_daily_v3_agent.py`** tambem usa `ps_bi.fct_player_activity_daily` — auditoria 08/05 confirmou que esta zerando `crm_campaign_daily` desde ~08/04. Sera fix separado (proximo handoff). **Nao incluido aqui** pra manter este handoff focado/atomico.
2. **`pipelines/dim_marketing_mapping.py`** (NGR por afiliado) — mesmo problema, prioridade P1.
3. **BTR_30D / BTR_CASINO_30D / BTR_SPORT_30D ficam NULL** ate gap dbt em `fct_player_activity_daily.bonus_turnedreal_base` ser resolvido (acao Mauro/Gusta). Investigacao consolidada do dia 10/05 confirmou que NAO ha fonte alternativa em Athena — bireports nao expoe `bonus_turnedreal_base`. Memoria atualizada: `feedback_btr_valor_na_subfund.md`. Quando o dbt voltar, reativacao no pipeline e trivial (~10 linhas em `bloco_5b_btr_bonus`).
4. **TOP_GAME / DOMINANT_TIMEBUCKET com cardinalidade baixa** — Bloco 3 ainda usa `ps_bi.fct_casino_activity_daily/hourly`, mesma fato dbt parada que afetou o Bloco 1+2. Fix definitivo depende de Mauro/Gusta destravarem o dbt. Por hora, doc na legenda informando que reflete janela 06/02-06/04.
5. **multibet.game_image_mapping incompleto** — IDs como "8842" e "13097" sem nome amigavel. Pingar Mauro/Gusta pra rodar `pipelines/fix_missing_game_images.py` (ou equivalente).

## Nao precisa mexer em nada fora dessa pasta

Os outros pipelines do orquestrador (pcr_pipeline, segmentacao_sa_smartico, grandes_ganhos, fact_sports_odds, sync_meta_spend, etc.) **nao mudaram**.
