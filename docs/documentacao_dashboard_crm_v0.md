# Dashboard CRM Performance v0 - Documentacao Tecnica

**Autor:** Mateus F. (Squad Intelligence Engine)
**Data:** 31/03/2026
**Status:** v0 para validacao com CRM (Raphael M.)
**Dashboard:** http://localhost:5051 | Login: multibet / mb2026

## Objetivo
Dashboard de performance de campanhas CRM para marco/2026 (01/03 a 30/03).
Dados extraidos como CSVs de BigQuery (Smartico CRM) e Athena (Data Lake).
Apos validacao com CRM, os dados serao persistidos no Super Nova DB para automacao.

## Periodo
01/03/2026 a 30/03/2026 (BRT = America/Sao_Paulo)

---

## Numeros Macro Marco/2026

| Metrica | Valor | Calculo |
|---------|-------|---------|
| Campanhas no periodo | 1.580 | entity_ids unicos com atividade |
| Players ativos | 30.518 | Users com turnover > 0 |
| Coorte CRM total | 37.110 | Users unicos que completaram campanhas |
| GGR | R$ 5,15M | SUM(ggr_brl) da coorte |
| NGR | R$ 3,79M | SUM(ngr_brl) = GGR - BTR |
| Depositos | R$ 38,3M | SUM(depositos_brl) |
| Custo Bonus (BTR) | R$ 363K | SUM(bonus_cost_value) do j_bonuses |
| Custo Disparos | R$ 375K | Volume x custo unitario por canal |
| Custo CRM Total | R$ 738K | BTR + Disparos |
| ROI (GGR/Custo) | 7.0x | R$ 5.15M / R$ 738K |
| ARPU | R$ 169 | GGR / Players ativos |

---

## Funil de Conversao

| Etapa | O que mede | Fonte | Valor | % do topo |
|-------|-----------|-------|-------|-----------|
| Oferecidos | Receberam oferta de bonus | j_bonuses status=1 | 105.312 | 100% |
| Completaram | Cumpriram condicao | j_bonuses status=3 | 103.163 | 98% |
| Ativados | Depositaram OU apostaram | ps_bi (dep+turn>0) | 34.252 | 32,5% |
| Monetizados | Depositaram E apostaram | ps_bi (dep>0 AND turn>0) | 28.032 | 26,6% |

**Insight:** 67% dos users que receberam bonus NAO tiveram atividade financeira.
Gap de 69K users que o CRM pode atacar com reativacao.

---

## CSVs e Fontes

### 1. campanhas_diarias.csv (232 KB, 2.712 registros)
**Fonte:** BigQuery `smartico-bq6.dwh_ext_24105.j_bonuses`
**Grao:** 1 linha por entity_id x dia

| Coluna | Descricao | Fonte |
|--------|-----------|-------|
| report_date | Data BRT | DATE(fact_date, 'America/Sao_Paulo') |
| campaign_id | entity_id do Smartico | j_bonuses.entity_id |
| campaign_name | Nome da campanha | JSON_EXTRACT_SCALAR(activity_details, '$.campaign_name') |
| oferecidos | Users que receberam oferta | COUNT(DISTINCT user_ext_id) WHERE bonus_status_id = 1 |
| opt_in | Users com bonus criado | COUNT(DISTINCT user_ext_id) WHERE bonus_status_id IN (1,3) |
| completaram | Users que completaram | COUNT(DISTINCT user_ext_id) WHERE bonus_status_id = 3 |
| expiraram | Users que expiraram | COUNT(DISTINCT user_ext_id) WHERE bonus_status_id = 4 |
| custo_bonus_brl | BTR (Bonus Turned Real) em R$ | SUM(bonus_cost_value) WHERE bonus_status_id = 3 |
| campaign_type | Classificacao por nome | Challenge, Cashback_VIP, RETEM, etc. |

### 2. financeiro_coorte.csv (14 MB, 231.720 registros)
**Fonte:** BigQuery (coorte) + Athena ps_bi (financeiro)
**Grao:** 1 linha por user x dia
**Coorte:** Users que completaram campanhas (j_bonuses bonus_status_id = 3)
**Bridge:** user_ext_id (BigQuery) = external_id (ps_bi.dim_user)

| Coluna | Descricao | Unidade |
|--------|-----------|---------|
| user_ext_id | ID do jogador | Smartico external_id |
| report_date | Data da atividade | UTC truncado (ps_bi) |
| ggr_brl | Gross Gaming Revenue | BRL reais |
| ngr_brl | Net Gaming Revenue (GGR - BTR) | BRL reais |
| turnover_brl | Total apostado | BRL reais |
| depositos_brl | Depositos confirmados | BRL reais |
| saques_brl | Saques confirmados | BRL reais |
| net_deposit_brl | Depositos - Saques | BRL reais |
| sessoes | Contagem de logins | Inteiro |

**Query Athena (por batch de 500 users):**
```sql
SELECT du.external_id AS user_ext_id,
    p.activity_date AS report_date,
    SUM(p.ggr_base) AS ggr_brl,
    SUM(p.ngr_base) AS ngr_brl,
    SUM(p.bet_amount_base) AS turnover_brl,
    SUM(p.deposit_success_base) AS depositos_brl,
    SUM(p.cashout_success_base) AS saques_brl,
    SUM(p.login_count) AS sessoes
FROM ps_bi.dim_user du
JOIN ps_bi.fct_player_activity_daily p ON p.player_id = du.ecr_id
WHERE du.external_id IN (...)
  AND du.is_test = false
  AND p.activity_date BETWEEN DATE '2026-03-01' AND DATE '2026-03-30'
GROUP BY du.external_id, p.activity_date
```

### 3. financeiro_por_campanha.csv (1.579 campanhas)
**Fonte:** Cruzamento BigQuery (coorte user->campanha) x financeiro_coorte.csv
**Grao:** 1 linha por campaign_id (agregado mensal)

| Coluna | Descricao |
|--------|-----------|
| campaign_id | entity_id do Smartico |
| users | Users unicos na campanha |
| ggr_brl | GGR total dos users (toda atividade, nao so campanha) |
| turnover_brl | Turnover total dos users |
| depositos_brl | Depositos total dos users |

**Top 5 campanhas por GGR:**
| ID | Users | GGR | Tipo provavel |
|----|-------|-----|---------------|
| 754 | 6.848 | R$ 2,93M | Cashback VIP |
| 1403485 | 7.913 | R$ 2,04M | Lifecycle |
| 1340568 | 8.748 | R$ 1,38M | Lifecycle |
| 792 | 1.899 | R$ 998K | Cashback VIP |
| 2120332 | 663 | R$ 922K | Cashback |

### 4. disparos_custos.csv (216 registros)
**Fonte:** BigQuery `j_communication` (fact_type_id = 1)

| Canal | Provedor | Custo/envio | Envios Marco | Custo Total |
|-------|----------|-------------|-------------|-------------|
| SMS | DisparoPro | R$ 0,045 | 2.037.610 | R$ 87.185 |
| SMS | PushFY | R$ 0,060 | 743.040 | R$ 44.582 |
| SMS | Comtele | R$ 0,063 | 2.739.771 | R$ 173.128 |
| WhatsApp | Loyalty | R$ 0,160 | 325.003 | R$ 52.000 |
| Push | PushFY | R$ 0,060 | 1.059.087 | R$ 63.545 |
| Popup | Smartico | R$ 0,000 | 6.965.741 | R$ 0 |
| **TOTAL** | | | **15.615.052** | **R$ 375.513** |

### 5. top_jogos.csv (30 jogos)
**Fonte:** Athena ps_bi.fct_casino_activity_daily + DE-PARA manual
**Coorte:** Mesmos users do financeiro (completaram campanhas CRM)
**DE-PARA:** 35 jogos mapeados em data/crm_csvs/depara_jogos.csv

### 6. vip_groups.csv (37.110 users)
**Fonte:** Derivado do financeiro_coorte.csv (NGR acumulado marco)

| Tier | Criterio | Users | NGR Total | NGR/User | APD |
|------|----------|-------|-----------|----------|-----|
| Elite | NGR >= R$ 10.000 | 99 | R$ 2,12M | R$ 21.375 | 15.3 |
| Key Account | NGR >= R$ 5.000 | 176 | R$ 1,19M | R$ 6.738 | 14.8 |
| High Value | NGR >= R$ 3.000 | 284 | R$ 1,09M | R$ 3.843 | 13.2 |
| Standard | NGR < R$ 3.000 | 36.551 | -R$ 601K | -R$ 16 | 6.1 |

**Insight:** 559 VIPs (1.5%) geram R$ 4,39M de NGR = 100%+ da receita liquida.

---

## Classificacao de Campanhas

| Tipo | Campanhas | Oferecidos | Custo Bonus | Exemplo |
|------|-----------|-----------|-------------|---------|
| Challenge | 1.073 | 26.734 | R$ 0 | [PGS] Challenge Fortune Tiger |
| Cashback_VIP | 7 | 21.079 | R$ 353K | IDs 754, 792, 755, 793 |
| DailyFS | 103 | 17.974 | R$ 8.6K | Gire e Ganhe Ratinho |
| RETEM | 279 | 16.372 | R$ 0 | RETEM Corujao 27/03 |
| Lifecycle | 41 | 8.835 | R$ 0 | C&S_LifeCycle_2ndDeposit |
| Gamificacao | 4 | 8.315 | R$ 0 | IDs 23053, 33670 |
| CrossSell_Sports | 14 | 1.251 | R$ 498 | Sportsbook_25GatesOfOlympus |
| Reativacao_FTD | 8 | 676 | R$ 133 | UsuariosSemFTD_Missoes |
| CX_Recovery | 1 | 135 | R$ 0 | CX_ChamadosAbertos_30Giros |
| Sem_Classificacao | 33 | 3.183 | R$ 0 | IDs sem nome |

---

## Pendencias para o CRM validar

| # | Pendencia | Responsavel | Impacto |
|---|-----------|-------------|---------|
| 1 | Confirmar IDs 754/792/755/793 = Cashback VIP | Raphael M. | 99% do custo bonus |
| 2 | Mapeamento entity_id -> campanha ativa | CRM/Smartico | Filtrar so ativas |
| 3 | bonus_cost_value = BTR? Ou BG? | CRM/Smartico | Calculo ROI |
| 4 | Verba mensal de disparos | CRM/Financeiro | % utilizado |
| 5 | Custos Popup/Smartico = R$ 0 mesmo? | CRM | Custo real |
| 6 | Classificacao campanhas sem nome (33) | CRM | Tipo correto |
| 7 | Definicao de inativo (15d?) | CRM | Modulo recuperacao |

---

## Roadmap

| Fase | Entrega | Estimativa |
|------|---------|-----------|
| v0 (atual) | Dashboard + CSVs + doc para CRM avaliar | Pronto |
| v0.5 | Corrigir pendencias CRM + comparativo periodo | 3-5 dias |
| v1 | Pipeline D+1 + Super Nova DB + GGR incremental | 1-2 semanas |
| v2 | Dashboard em producao (EC2) + alertas | 2-3 semanas |

---

## Arquivos do Projeto

| Arquivo | Funcao |
|---------|--------|
| scripts/extract_crm_csvs_marco.py | Extracao dos CSVs de BigQuery+Athena |
| scripts/fix_crossref.py | Cruzamento campanha x financeiro |
| scripts/extract_saques.py | Extracao de saques (cashout) |
| data/crm_csvs/*.csv | 6 CSVs + depara_jogos.csv |
| dashboards/crm_report/app.py | Flask backend (porta 5051) |
| dashboards/crm_report/queries_csv.py | Logica de leitura dos CSVs |
| dashboards/crm_report/templates/dashboard.html | Frontend |
| dashboards/crm_report/config.py | Configuracao e tipos |
| docs/documentacao_dashboard_crm_v0.pdf | PDF para apresentacao |

## Regras aplicadas
- **Timezone:** BigQuery usa DATE(fact_date, 'America/Sao_Paulo'). Athena ps_bi activity_date = UTC truncado.
- **Valores:** ps_bi em BRL reais. bireports_ec2 em centavos (/100).
- **Test users:** is_test = false (ps_bi). Filtrado em todas as queries.
- **GGR por campanha:** Toda atividade do player no periodo (nao filtrado por jogo da campanha).
