# Draft — Report Diário de Performance CRM

**Task:** 86ag3994u (Board BI)
**Autor:** Mateus F. (Squad Intelligence Engine)
**Data:** 25/03/2026
**Status:** Draft para validação com Head (Castrin)

---

## 1. Objetivo

Entregar um **report recorrente diário (D+1)** com visão completa do desempenho de cada campanha executada pelo CRM — cobrindo funil de conversão, impacto financeiro, ROI, segmentações e comparativo antes/durante/depois.

**Consumidores:** Time CRM (Raphael M.), BI, liderança (CGO/CTO).

---

## 2. O que já temos pronto no Super Nova DB (schema `multibet`)

### 2.1 Tabela fato — `multibet.fact_crm_daily_performance`

| Item | Detalhe |
|------|---------|
| DDL | `pipelines/ddl/ddl_crm_daily_performance.sql` |
| Grão | 1 linha por `campanha_id` + `period` (BEFORE / DURING / AFTER) |
| Colunas JSONB | `funil`, `financeiro`, `comparativo` — flexíveis, sem alterar schema |
| Indexes | B-tree em campanha_id + period; GIN nos JSONBs |
| Constraint | UNIQUE (campanha_id, period) — UPSERT seguro |

**O que já armazena hoje:**

- **Funil:** comunicações enviadas, entregues, abertas, clicadas, convertidas, canais (WhatsApp/SMS/push)
- **Financeiro:** total_users, depósitos (BRL + qtd), GGR, BTR, RCA, NGR, avg_play_days, sessões
- **Comparativo:** NGR incremental, variação %, custos por canal, custo total, ROI

### 2.2 Tabela dimensão — `multibet.dim_crm_friendly_names`

Mapeia `entity_id` → nome amigável da campanha + categoria (RETEM, MULTIVERSO, WELCOME...) + responsável. **Pendência:** Raphael precisa validar/completar o mapeamento.

### 2.3 Pipelines existentes

| Pipeline | Arquivo | Status | Fonte financeira |
|----------|---------|--------|------------------|
| CRM Daily Performance v1 | `pipelines/fact_crm_daily_performance.py` | Funcional | ⚠️ Redshift |
| CRM Daily Performance v2 | `pipelines/crm_daily_performance.py` | Funcional | ⚠️ Redshift |
| Report CRM Promoções | `pipelines/report_crm_promocoes.py` | Completo | Athena (fund_ec2) |
| Report Multiverso | `pipelines/report_multiverso_campanha.py` | Completo | BigQuery + Athena |
| Anti-Abuse Multiverso | `pipelines/anti_abuse_multiverso.py` | Em produção | Athena |

### 2.4 Regras de negócio documentadas e validadas

| Regra | Referência | Impacto |
|-------|-----------|---------|
| Duplo filtro CRM (entity_id + template_id) | `memory/feedback_crm_bonus_isolation.md` | Evita inflação de ~39% nos completadores |
| Semântica do funil (fact_type_id 1-5) | `memory/feedback_crm_funil_semantics.md` | Define exatamente o que cada etapa mede |
| Sub-fund isolation (Real vs Bonus) | `memory/feedback_subfund_isolation.md` | GGR Real precisão de 0.000% vs referência |
| Glossário de KPIs (GGR, NGR, BTR, etc.) | `memory/glossario_kpis.md` | Padroniza definições na entrega |
| Fuso horário UTC → BRT | CLAUDE.md | Obrigatório em toda query Athena |
| Exclusão de test users | `memory/feedback_test_users_filter.md` | Evita 3% de divergência |

### 2.5 Infraestrutura de dados disponível

| Fonte | O que fornece | Conexão |
|-------|--------------|---------|
| **BigQuery (Smartico)** | Funil CRM, coorte de bônus, opt-in, journeys, disparos | `db/bigquery.py` ✅ |
| **Athena (Iceberg)** | GGR, depósitos, turnover, sessões, jogos, sportsbook | `db/athena.py` ✅ |
| **Super Nova DB** | Persistência de resultados (destino) | `db/supernova.py` ✅ |
| **ps_bi (dbt)** | Camada pré-agregada em BRL — `fct_player_activity_daily`, `fct_casino_activity_daily`, `dim_user` | Via Athena ✅ |

### 2.6 Tabelas de suporte já criadas no `multibet`

| Tabela | DDL | Uso no report |
|--------|-----|---------------|
| `fact_casino_rounds` | `ddl_fact_casino_rounds.sql` | Top jogos, GGR casino, RTP |
| `fact_sports_bets` | `ddl_fact_sports_bets.sql` | Sportsbook por esporte |
| `dim_games_catalog` | `ddl_dim_games_catalog.sql` | Catálogo com flags jackpot/freespin |
| `agg_financial_monthly` | `ddl_agg_financial_monthly.sql` | Baseline financeiro mensal |
| `fact_ftd_deposits` | `ddl_fact_ftd_deposits.sql` | FTDs para campanhas de ativação |

---

## 3. Gap Analysis — PRD vs. infraestrutura atual

### 3.1 Gaps CRÍTICOS (bloqueiam entrega)

| # | Requisito do PRD | O que falta | Esforço |
|---|-----------------|-------------|---------|
| G1 | **Migração Redshift → Athena** | Ambas as pipelines CRM (v1 e v2) ainda usam Redshift para métricas financeiras. Redshift está descontinuado. | **Alto** — reescrever Step 3 para usar `ps_bi` ou `bireports_ec2` via Athena |
| G2 | **Grão diário por campanha** | Hoje o grão é campanha + period (3 linhas). O PRD pede **1 linha por campanha por dia** com atualização D+1. | **Médio** — novo DDL ou evolução do existente |
| G3 | **6 recortes financeiros** | Hoje temos apenas "geral". PRD pede: Geral, Cassino, Sportsbook, Segmento, Jogo, Campanha. | **Médio** — expandir JSONB `financeiro` ou criar tabela auxiliar |
| G4 | **Custos por provedor diferenciados** | Pipeline usa custo fixo R$0,16. PRD diferencia: SMS DisparosPro R$0,045 · SMS Pushfy R$0,06 · WhatsApp Loyalty R$0,16 | **Baixo** — parametrizar mapeamento de custos |

### 3.2 Gaps IMPORTANTES (enriquecem a entrega)

| # | Requisito do PRD | O que falta | Esforço |
|---|-----------------|-------------|---------|
| G5 | **Opt-in tracking** | Separar quem apostou + fez opt-in vs. quem apostou sem opt-in (economia gerada) | **Médio** — cruzar `j_automation_rule_progress` com `j_bonuses` |
| G6 | **Segmentação por perfil** | Tipo segmento (Ativação/Monetização/Retenção/Recuperação), preferência produto, ticket VIP | **Médio** — join com `j_user` e dimensões de player |
| G7 | **Comparativo Antes/Durante/Depois com baseline M-1** | Lógica de M-1 já está no v2, mas precisa validar que funciona com Athena e incluir APD + sessões | **Baixo** — já temos a lógica, migrar fonte |
| G8 | **Budget tracking de disparos** | Consumo mensal por canal, % da verba, projeção até fim do mês | **Médio** — novo bloco JSONB ou tabela `fact_crm_budget` |
| G9 | **Análise VIP (Elite/Key/High)** | Quebra por faixas NGR (≥10K, ≥5K, ≥3K) com controle de sobreposição | **Baixo** — CTE com CASE WHEN no NGR acumulado |
| G10 | **Recuperação (reengajamento)** | Inativos reengajados, tempo até 1º depósito, churn D+7, comparativo por canal | **Alto** — requer tracking longitudinal |
| G11 | **Dados casino detalhados** | Top jogos, GGR casino isolado, proporção casino/sports, GGR negativo, RTP | **Baixo** — `fact_casino_rounds` + `fct_casino_activity_daily` já existem |
| G12 | **Meta vs. Realizado** | Campo de meta por campanha (definido pelo CRM pré-disparo) | **Baixo** — coluna na dim ou input manual |

---

## 4. Plano de execução proposto

### Fase 1 — Fundação (prioridade máxima)

**Objetivo:** Pipeline funcional com Athena, grão correto, dados financeiros confiáveis.

| Step | Entrega | Dependência |
|------|---------|-------------|
| 1.1 | Migrar Step 3 (financeiro) de Redshift → Athena (`ps_bi.fct_player_activity_daily`) | Nenhuma |
| 1.2 | Evoluir DDL para suportar grão diário (`campanha_id + execution_date + period`) | Nenhuma |
| 1.3 | Parametrizar custos por provedor (SMS DisparosPro, Pushfy, WhatsApp Loyalty) | Tabela de custos do CRM |
| 1.4 | Implementar os 6 recortes financeiros no JSONB | 1.1 concluído |
| 1.5 | Validar pipeline completa com campanha RETEM como piloto | 1.1 a 1.4 concluídos |

### Fase 2 — Enriquecimento

**Objetivo:** Segmentações, opt-in, VIP, comparativo completo.

| Step | Entrega | Dependência |
|------|---------|-------------|
| 2.1 | Implementar opt-in tracking (economia gerada) | Fase 1 |
| 2.2 | Segmentação por perfil (tipo, produto, ticket) | Fase 1 |
| 2.3 | Quebra VIP (Elite / Key Account / High Value) | Fase 1 |
| 2.4 | Comparativo M-1 validado com APD + sessões | Fase 1 |
| 2.5 | Budget tracking mensal por canal | Fase 1 |

### Fase 3 — Análises avançadas

**Objetivo:** Recuperação, casino detalhado, dashboard visual.

| Step | Entrega | Dependência |
|------|---------|-------------|
| 3.1 | Módulo de recuperação (reengajamento, churn D+7, tempo até depósito) | Fase 2 |
| 3.2 | Casino detalhado (top jogos, RTP, GGR negativo) | Fase 1 |
| 3.3 | Meta vs. Realizado (input do CRM + comparação automática) | Alinhamento com Raphael |
| 3.4 | Dashboard Flask (visualização diária consolidada) | Fase 2 concluída |

---

## 5. Arquitetura de dados proposta

```
┌─────────────────────────────────────────────────────────────────┐
│                        FONTES DE DADOS                         │
├─────────────────┬───────────────────┬──────────────────────────┤
│   BigQuery      │     Athena        │     Input CRM            │
│   (Smartico)    │   (Iceberg/ps_bi) │   (manual/Smartico)      │
├─────────────────┼───────────────────┼──────────────────────────┤
│ • j_bonuses     │ • fct_player_     │ • Metas por campanha     │
│ • j_communic.   │   activity_daily  │ • Verba mensal           │
│ • j_automation  │ • fct_casino_     │ • Tabela de custos       │
│ • j_user        │   activity_daily  │   por provedor           │
│ • dm_bonus_tmpl │ • dim_user        │                          │
│                 │ • dim_game        │                          │
│                 │ • bireports_ec2   │                          │
└────────┬────────┴────────┬──────────┴────────┬─────────────────┘
         │                 │                   │
         ▼                 ▼                   ▼
┌─────────────────────────────────────────────────────────────────┐
│              PIPELINE: crm_daily_report.py                      │
│                                                                 │
│  1. Extrair coorte (BigQuery j_bonuses + entity_id isolation)   │
│  2. Extrair funil (BigQuery j_communication fact_type_id 1-5)   │
│  3. Extrair financeiro (Athena ps_bi — Real vs Bonus split)     │
│  4. Extrair casino detail (Athena fct_casino_activity_daily)    │
│  5. Calcular opt-in vs não opt-in (economia)                    │
│  6. Calcular ROI, CPA, NGR incremental                         │
│  7. Montar comparativo M-1 (BEFORE/DURING/AFTER)               │
│  8. Segmentar (VIP, produto, tipo, ticket)                      │
│  9. Persistir no Super Nova DB (UPSERT JSONB)                   │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                    SUPER NOVA DB (PostgreSQL)                    │
│                      schema: multibet                           │
├─────────────────────────────────────────────────────────────────┤
│  fact_crm_daily_performance  (grão: campanha × dia × periodo)   │
│  ├── funil (JSONB)                                              │
│  ├── financeiro (JSONB) — 6 recortes                            │
│  ├── segmentacao (JSONB) — VIP, produto, tipo                   │
│  ├── comparativo (JSONB) — deltas, custos, ROI                  │
│  └── budget (JSONB) — consumo mensal por canal                  │
│                                                                 │
│  dim_crm_friendly_names     (entity_id → nome + categoria)      │
│  dim_crm_campaign_meta      (NEW — metas + config por campanha) │
│  fact_crm_budget_monthly    (NEW — orçamento mensal de disparos) │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     CAMADA DE ENTREGA                           │
├─────────────────────────────────────────────────────────────────┤
│  Fase 1-2: Report Excel/CSV diário (D+1 manhã)                 │
│  Fase 3:   Dashboard Flask (#bi_reports ou canal CRM dedicado)  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 6. Decisões técnicas relevantes

| Decisão | Justificativa |
|---------|---------------|
| **ps_bi como fonte principal financeira** | Valores já em BRL (não centavos), Real vs Bonus já separados, mais rápido que fund_ec2 raw. Divergência conhecida: ps_bi `casino_ggr` = só realcash; fund_ec2 tipo 27-45 inclui bonus. Para NGR, ps_bi é a fonte correta. |
| **JSONB para métricas** | Já validado na tabela atual. Permite adicionar métricas sem ALTER TABLE. GIN index garante performance em queries. |
| **Coorte sempre via BigQuery** | entity_id + template_id isolation evita 39% de inflação. Regra crítica já documentada e validada. |
| **Comparativo M-1 (não D-7)** | PRD define: baseline = mesmo intervalo do mês anterior. Captura sazonalidade real (dia da semana, pagamento, etc.). |
| **Custos de disparo parametrizáveis** | Valores mudam por provedor. Criar dict/config em vez de hardcode. PRD já fornece tabela de referência. |

---

## 7. Dependências externas

| Dependência | Responsável | Status |
|-------------|-------------|--------|
| Validar `dim_crm_friendly_names` (mapeamento entity_id → nome) | Raphael M. (CRM) | Pendente |
| Definir metas por campanha (campo `meta_xxx` pré-disparo) | Time CRM | Pendente |
| Confirmar verba mensal de disparos (baseline para budget tracking) | Time CRM / Financeiro | Pendente |
| Confirmar tabela de custos por provedor (SMS DisparosPro/Pushfy/WhatsApp) | Time CRM | PRD já fornece referência |
| Acesso a dados de opt-in granular no Smartico | Time CRM / Smartico | A verificar |

---

## 8. Riscos identificados

| Risco | Mitigação |
|-------|-----------|
| Divergência GGR fund_ec2 vs ps_bi (até R$ 3.3M por incluir bonus) | Usar ps_bi para NGR (só realcash) — alinhado com definição de negócio |
| Sobreposição de campanhas (mesmo usuário em 2+ campanhas no dia) | Last Click attribution já implementado na v2 — manter |
| Dados de opt-in podem não estar granulares o suficiente no BigQuery | Validar com Smartico; fallback: inferir via j_automation_rule_progress |
| Custo real de disparo pode variar vs. tabela de referência | Começar com valores fixos do PRD; evoluir para integração com provedores |
| Delay D+1: dados no Athena podem não estar completos até de manhã | Validar horário de refresh do Iceberg (tipicamente completa até 6h BRT) |

---

## 9. Próximos passos imediatos

1. **Validar este draft com Castrin** — alinhar prioridades e timeline
2. **Iniciar Fase 1.1** — migrar fonte financeira de Redshift para Athena (ps_bi)
3. **Agendar alinhamento com Raphael** — validar dim_crm_friendly_names + metas + custos
4. **Pilotar com campanha RETEM** — primeira execução end-to-end com dados reais
5. **Definir formato de entrega** — Excel D+1 ou canal Slack (#bi_reports)

---

## Anexo A — Tabela de custos de disparo (referência PRD)

| Canal | Provedor | Custo por envio |
|-------|----------|-----------------|
| SMS | Disparos Pro | R$ 0,045 |
| SMS | Pushfy | R$ 0,060 |
| WhatsApp | Loyalty | R$ 0,16 |

## Anexo B — Faixas VIP (referência PRD)

| Grupo | Critério NGR no período |
|-------|------------------------|
| Elite | NGR ≥ R$ 10.000 |
| Key Account | NGR ≥ R$ 5.000 e < R$ 10.000 |
| High Value | NGR ≥ R$ 3.000 e < R$ 5.000 |

## Anexo C — Funil CRM — definição exata de cada etapa

| Etapa | Fonte | Significado real |
|-------|-------|-----------------|
| Segmentados | Smartico segment | Base total da campanha (100%) |
| Enviado | j_communication fact_type_id=1 | Sistema empurrou a mensagem (não é opt-in) |
| Entregue | j_communication fact_type_id=2 | Chegou ao dispositivo (não significa que viu) |
| Visualizado | j_communication fact_type_id=3 | Sessão ativa, popup/msg apareceu |
| Clicou | j_communication fact_type_id=4 | Qualquer interação (fechar também conta) |
| Converteu (CTA) | j_communication fact_type_id=5 | Clicou no botão de ação ("Participar") |
| Apostou | j_automation_rule_progress | Realizou aposta nos jogos da campanha |
| Completou | j_bonuses (redeem_date NOT NULL) | Cumpriu condição e recebeu benefício |