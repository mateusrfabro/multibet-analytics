# Dashboard CRM Performance v0 — Handoff para Head

**Autor:** Mateus F. (Squad Intelligence Engine)
**Data:** 31/03/2026
**Dashboard:** http://localhost:5051 | Login: multibet / mb2026

---

## 1. O que foi construido

Dashboard Flask que mostra performance de campanhas CRM com dados de marco/2026 (01-30/03).
Dados extraidos de BigQuery (Smartico CRM) e Athena (Data Lake) como CSVs.
Apos validacao com CRM, sera automatizado via ETL para Super Nova DB.

### Fontes de dados utilizadas

| Fonte | Banco | Tabela/View | O que fornece |
|-------|-------|-------------|---------------|
| Smartico CRM | BigQuery `smartico-bq6.dwh_ext_24105` | `dm_automation_rule` | Campanhas reais (rule_id, rule_name, is_active) |
| Smartico CRM | BigQuery | `j_automation_rule_progress` | Bridge campanha -> users (automation_rule_id, user_ext_id, dt_executed) |
| Smartico CRM | BigQuery | `j_bonuses` | Bonus oferecidos/completados, custo BTR (entity_id, bonus_status_id, bonus_cost_value) |
| Smartico CRM | BigQuery | `j_communication` | Disparos de comunicacao (activity_type_id, label_provider_id, fact_type_id) |
| Data Lake | Athena `ps_bi` | `fct_player_activity_daily` | GGR, NGR, turnover, depositos, saques, sessoes (por player x dia) |
| Data Lake | Athena `ps_bi` | `dim_user` | Bridge IDs (external_id = BigQuery user_ext_id, ecr_id = player_id) |
| Data Lake | Athena `ps_bi` | `fct_casino_activity_daily` | Top jogos por player (game_id, bet_amount, ggr) |
| Data Lake | Athena `ps_bi` | `dim_game` | Nomes dos jogos (cobertura parcial, PG Soft mapeado manualmente) |

### CSVs extraidos (data/crm_csvs/)

| CSV | Registros | Fonte | Descricao |
|-----|-----------|-------|-----------|
| campanhas_v3.csv | 48 campanhas | BQ + Athena | Campanhas reais com GGR, depositos, custo, ROI |
| campanhas_v2.csv | 48 x 30 dias | BQ dm_automation_rule | Campanhas reais por dia |
| financeiro_coorte.csv | 231K | BQ + Athena ps_bi | Financeiro por user x dia (com saques) |
| disparos_custos.csv | 216 | BQ j_communication | Custos por canal/provedor/dia |
| top_jogos.csv | 30 | Athena ps_bi | Top jogos da coorte CRM |
| vip_groups.csv | 37K users | Derivado financeiro | Classificacao VIP |
| depara_jogos.csv | 35 | Manual | DE-PARA game_id -> nome |
| LEGENDA_crm_csvs.txt | - | - | Dicionario completo de colunas |

---

## 2. O que o dashboard mostra (v0)

| Secao | Status | Dados reais? |
|-------|--------|-------------|
| KPIs header (Campanhas, Players, Custo, GGR, ROI, ARPU) | OK | Sim |
| Tabela de campanhas (48 campanhas reais com GGR e ROI) | OK | Sim |
| Funil CRM (Oferecidos > Completaram > Ativados > Monetizados) | OK | Sim |
| Funil por tipo de campanha | OK | Sim |
| Volume diario por etapa | OK | Sim (30 dias) |
| ROI e Custos (bonus BTR + disparos) | OK | Sim |
| Conversao por tipo + Oferecidos vs Completaram | OK | Sim |
| Orcamento de disparos por canal/provedor | OK | Sim |
| Top jogos da base impactada | OK | Sim |
| Analise VIP (Elite/Key Account/High Value) | OK | Sim |

---

## 3. O que falta vs PRD original

### 3a. Entregue parcialmente (precisa refinar)

| Item PRD | Status | O que falta |
|----------|--------|-------------|
| 1. Identificacao campanha | Parcial | Canal de disparo por campanha (activity_type_id na dm_automation_rule, pendente validar) |
| 2. Funil de conversao | Parcial | Funil classico (Segmentados > Entregues > Abertos > Clicados) nao vincula a campanha no j_communication. Usamos funil alternativo. |
| 4. Resultado financeiro 6 recortes | Parcial | Temos: Geral + Campanha + Jogo. Faltam: Cassino isolado, Sportsbook isolado, por Segmento |
| 5. ROI e custo | Parcial | ROI calculado. Falta: meta vs realizado (input CRM), CPA por campanha |

### 3b. Nao entregue (precisa de dados/confirmacao)

| Item PRD | Status | O que precisa |
|----------|--------|---------------|
| 1. Segmento alvo da acao | Pendente | dm_automation_rule tem segment_id mas nao extraimos. Query pronta. |
| 3. Segmentacoes da base (Ativacao/Monetizacao/Retencao/Recuperacao) | Pendente | Precisa classificacao do CRM por campanha |
| 3. Opt-in vs nao opt-in (economia gerada) | Pendente | Precisa logica de quem apostou SEM opt-in |
| 5. Meta vs realizado | Pendente | Input manual do CRM antes de cada disparo |
| 6. % verba mensal utilizado + projecao | Pendente | Verba mensal nao informada pelo CRM |
| 7. Comparativo antes/durante/depois | Pendente | Logica definida, nao implementada (precisa datas inicio/fim por campanha) |
| 8a. Controle sobreposicao VIP | Pendente | Users em 2+ campanhas simultaneas |
| 8b. Recuperacao (inativos reengajados) | Parcial | Definicao: inativo = 15d. Implementacao pendente |
| 8c. GGR negativo + RTP medio | Pendente | Dados disponiveis no top_jogos, falta detalhe por campanha |

---

## 4. Tabelas e colunas necessarias para o ETL (Super Nova DB)

### 4.1 Fonte: BigQuery (Smartico CRM)

| Tabela | Coluna | Uso |
|--------|--------|-----|
| `dm_automation_rule` | rule_id, rule_name, is_active, activity_type_id, segment_id | Campanhas reais |
| `dm_segment` | segment_id, segment_name | Nome do segmento |
| `j_automation_rule_progress` | automation_rule_id, user_ext_id, dt_executed | Bridge campanha -> users |
| `j_bonuses` | entity_id, user_ext_id, bonus_status_id, bonus_cost_value, fact_date | Bonus e custo BTR |
| `j_communication` | user_ext_id, fact_type_id (1-5), activity_type_id, label_provider_id, fact_date | Funil + disparos |
| `dm_bonus_template` | label_bonus_template_id, internal_name | Tipo de bonus |

### 4.2 Fonte: Athena (Data Lake)

| Tabela | Coluna | Uso |
|--------|--------|-----|
| `ps_bi.dim_user` | ecr_id, external_id, is_test | Bridge IDs + filtro test users |
| `ps_bi.fct_player_activity_daily` | player_id, activity_date, ggr_base, ngr_base, bet_amount_base, deposit_success_base, cashout_success_base, login_count | Financeiro |
| `ps_bi.fct_casino_activity_daily` | player_id, game_id, activity_date, bet_amount_base, ggr_base | Top jogos |
| `ps_bi.dim_game` | game_id, game_desc | Nomes jogos (cobertura parcial) |

### 4.3 Destino: Super Nova DB (schema multibet)

| Tabela destino | Ja criada? | Fonte principal |
|----------------|-----------|-----------------|
| crm_campaign_daily | Sim (DDL existe) | BQ j_automation_rule_progress + Athena ps_bi |
| crm_dispatch_budget | Sim | BQ j_communication |
| crm_campaign_game_daily | Sim | Athena fct_casino_activity_daily |
| crm_vip_group_daily | Sim | Derivado do financeiro |
| crm_player_vip_tier | Sim | Derivado do financeiro |
| crm_campaign_comparison | Sim | Athena ps_bi (antes/durante/depois) |
| crm_recovery_daily | Sim | Athena ps_bi (inativos) |

---

## 5. Numeros macro marco/2026

| Metrica | Valor | Fonte |
|---------|-------|-------|
| Campanhas reais | 48 (26 ativas) | dm_automation_rule |
| Players na coorte | 37.110 | j_bonuses (completaram) |
| Players ativos (turnover>0) | 30.518 | ps_bi |
| GGR coorte | R$ 5,15M | ps_bi |
| NGR coorte | R$ 3,79M | ps_bi |
| Depositos | R$ 38,32M | ps_bi |
| Saques | R$ 33,72M | ps_bi |
| Net Deposit | R$ 4,60M | calculado |
| Custo bonus (BTR) | R$ 363K | j_bonuses |
| Custo disparos | R$ 375K | j_communication |
| Custo CRM total | R$ 738K | calculado |
| ROI (GGR/Custo) | 7.0x | calculado |

### VIP
| Tier | Users | NGR |
|------|-------|-----|
| Elite (>=R$10K) | 99 | R$ 2,12M |
| Key Account (>=R$5K) | 176 | R$ 1,19M |
| High Value (>=R$3K) | 284 | R$ 1,09M |
| Standard (<R$3K) | 36.551 | -R$ 601K |

---

## 6. Proximos passos

1. **Validar com CRM (Raphael M.)** — mostrar dashboard + pendencias
2. **Coletar inputs do CRM** — metas, verba, classificacao campanhas, segmentos
3. **Implementar itens pendentes** — comparativo antes/durante/depois, recuperacao
4. **ETL para Super Nova DB** — adaptar scripts para persistir nas tabelas existentes
5. **Front Gusta** — consumir tabelas do Super Nova DB direto

---

## Arquivos do projeto

| Arquivo | Funcao |
|---------|--------|
| scripts/extract_crm_csvs_marco.py | Extracao v1 (bonus) |
| scripts/extract_crm_campanhas_v2.py | Extracao v2 (campanhas reais) |
| scripts/build_v3_fast.py | Cruzamento v3 (campanha + financeiro) |
| scripts/extract_saques.py | Extracao de saques |
| scripts/fix_crossref.py | Cruzamento bonus -> financeiro |
| data/crm_csvs/*.csv | Todos os CSVs |
| dashboards/crm_report/ | Dashboard Flask |
| pipelines/ddl_crm_report.py | DDL das tabelas Super Nova DB |
| docs/documentacao_dashboard_crm_v0.pdf | PDF documentacao |
| docs/roteiro_apresentacao_crm_v0.md | Roteiro de apresentacao |
