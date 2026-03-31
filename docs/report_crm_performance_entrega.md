# Report CRM Performance de Campanhas — Documentacao de Entrega

**Data:** 28/03/2026
**Squad:** Intelligence Engine (Mateus F.)
**Para:** Castrin (Head de Dados) + Gusta (Front-end)
**Task:** Report diario consolidado de performance CRM
**Status:** Back-end e preview entregues. Pendente: front no Super Nova DB (Gusta)

---

## 1. O que foi entregue

### Arquivos criados

| Arquivo | Descricao |
|---------|-----------|
| `pipelines/ddl_crm_report.py` | DDL das 8 tabelas no Super Nova DB (schema multibet) |
| `pipelines/crm_report_daily.py` | Pipeline ETL completo (BigQuery + Athena → Super Nova DB) |
| `dashboards/crm_report/index.html` | Preview HTML no padrao do Super Nova DB front |
| `scripts/investigacao_crm_report.py` | Script de investigacao das 3 fontes de dados |
| `docs/report_crm_performance_entrega.md` | Este documento |

### Tabelas criadas no Super Nova DB (schema multibet)

| Tabela | Descricao | Consumo do Front |
|--------|-----------|------------------|
| `crm_campaign_daily` | 1 linha por campanha x dia (principal) | Tabela principal do report |
| `crm_campaign_segment_daily` | Quebra por segmento/produto/ticket | Graficos de segmentacao |
| `crm_campaign_game_daily` | Top jogos da base impactada | Tabela de jogos |
| `crm_campaign_comparison` | Antes/durante/depois | Cards comparativos |
| `crm_dispatch_budget` | Orcamento de disparos por canal/provedor | Tabela de custos |
| `crm_vip_group_daily` | Analise por grupo VIP (Elite/Key/High) | Cards VIP |
| `crm_recovery_daily` | Usuarios de recuperacao por canal | Tabela recuperacao |
| `crm_player_vip_tier` | Classificacao VIP calculada por NGR | Dimensao auxiliar |

### Preview HTML

Abrir `dashboards/crm_report/index.html` no navegador. Usa dados mockados realistas para demonstrar como ficaria no Super Nova DB front. Segue o MESMO padrao visual: dark theme, KPI cards, funil, tabelas com paginacao, graficos Chart.js.

**IMPORTANTE:** O HTML e apenas um preview. O Gusta vai consumir as tabelas do banco e implementar no front real.

---

## 2. Fontes de dados

### BigQuery (Smartico CRM) — Fonte primaria para CRM

| Tabela BigQuery | O que fornece | Volume 30d |
|----------------|---------------|------------|
| `j_communication` | Funil (fact_type_id 1-5), canal, provider | ~40M registros |
| `j_bonuses` | Custos (bonus_cost_value), coorte de users | ~220K registros |
| `dm_automation_rule` | Nomes e regras das campanhas | Ativo |
| `dm_segment` | Nomes dos segmentos | Ativo |
| `dm_bonus_template` | Templates de bonus | Ativo |
| `j_user` | 143 colunas de perfil do jogador | Ativo |

**Canais identificados (activity_type_id):**
- 50 = Popup (13.3M), 60 = SMS (16.7M), 64 = WhatsApp (970K)
- 30/40 = Push (4.6M), 31 = Inbox (4.7M)

**Providers com custo (label_provider_id):**
- 1536 = DisparoPro SMS (R$ 0,045)
- 1545 = PushFY SMS (R$ 0,060)
- 1268 = Comtele SMS (R$ 0,063)
- 1261 = WhatsApp Loyalty (R$ 0,160)

### Athena (Data Lake) — Fonte para financeiro

| Tabela Athena | O que fornece |
|---------------|---------------|
| `bireports_ec2.tbl_ecr_wise_daily_bi_summary` | GGR, depositos, saques, sessoes (centavos /100) |
| `ps_bi.dim_user` | Bridge de IDs (external_id = BigQuery user_ext_id) |
| `ps_bi.fct_casino_activity_daily` | GGR por jogo |

### Super Nova DB — Destino

Tabelas bronze CRM (`bronze_crm_campaigns`, `bronze_crm_communications`, `bronze_crm_player_responses`) estao **VAZIAS** — os ETLs ainda nao rodaram. Por isso o pipeline consulta BigQuery diretamente.

Tabela `bronze_real_fund_txn` tem 188M registros e pode ser alternativa futura ao Athena para reduzir custos.

---

## 3. Pipeline — Como funciona

```
python pipelines/crm_report_daily.py [--date YYYY-MM-DD] [--days N] [--dry-run]
```

### Fluxo

```
STEP 1: BigQuery → Campanhas com claims no dia (entity_ids)
STEP 2: BigQuery → Enriquecer com nomes (dm_automation_rule + dm_segment)
STEP 3: BigQuery → Funil de conversao (j_communication fact_type_id 1-5)
STEP 4: BigQuery → Custos de disparo por canal/provedor
STEP 5: Athena  → Metricas financeiras da coorte (GGR, depositos, saques)
STEP 6: Super Nova DB → Persistir em tabelas normalizadas (upsert)
```

### Regras de negocio aplicadas
- Timestamps UTC → BRT (AT TIME ZONE)
- bireports_ec2 em centavos → /100.0, ps_bi ja em BRL
- Filtro obrigatorio: test users (is_test = false)
- GGR casino = realcash_bets - realcash_wins (sem bonus)
- NGR = GGR - custo_bonus
- ROI = NGR / custo_total
- Classificacao de tipo por padrao no rule_name ("[RETEM]", "Cashback", etc.)

---

## 4. O que NAO temos (pendencias)

| Item | Motivo | Como resolver |
|------|--------|---------------|
| **Metas de campanha** | CRM define antes de cada disparo, nao esta no banco | Campo `meta_conversao_pct` aguardando input manual |
| **Verba mensal de disparos** | Decisao orcamentaria do CRM | Campo `budget_monthly_brl` na tabela `crm_dispatch_budget` |
| **Custos Google/Meta/TikTok** | Sem API conectada | Integracao futura (Google Ads API ja existe no dashboard de trafego) |
| **VIP tiers nativos** | `dm_level` nao existe no BigQuery | Tabela `crm_player_vip_tier` calcula automaticamente pelo NGR |
| **Opt-in detalhado** | Precisa validar campo especifico no j_user | Investigar colunas de consent/notification |
| **Classificacao padronizada** | Tipo inferido pelo nome (pode falhar) | Ideal: campo dedicado no Smartico ou tabela De-Para manual |

---

## 5. Handoff para Gusta (Front-end)

### Tabela principal: `multibet.crm_campaign_daily`

Colunas que o front deve consumir:

```sql
SELECT
    report_date, campaign_name, campaign_type, channel, segment_name, status,
    segmentados, msg_entregues, msg_abertos, msg_clicados, convertidos,
    cumpriram_condicao, apostaram,
    turnover_total_brl, ggr_brl, ggr_pct, ngr_brl, net_deposit_brl,
    depositos_brl, saques_brl,
    turnover_casino_brl, ggr_casino_brl, turnover_sports_brl, ggr_sports_brl,
    custo_bonus_brl, custo_disparos_brl, custo_total_brl, cpa_medio_brl, roi,
    disparos_sms, disparos_whatsapp, disparos_push, disparos_popup
FROM multibet.crm_campaign_daily
WHERE report_date BETWEEN '2026-03-01' AND '2026-03-27'
ORDER BY report_date DESC, turnover_total_brl DESC
```

### Filtros a implementar
- Date picker (report_date)
- Dropdown tipo campanha (campaign_type)
- Dropdown canal (channel)

### KPIs (calcular via SQL)
```sql
SELECT
    COUNT(DISTINCT campaign_id) AS campanhas_ativas,
    SUM(segmentados) AS usuarios_impactados,
    ROUND(SUM(convertidos)::NUMERIC / NULLIF(SUM(segmentados), 0) * 100, 1) AS taxa_conversao,
    SUM(ggr_brl) AS ggr_total,
    ROUND(SUM(ngr_brl) / NULLIF(SUM(custo_total_brl), 0), 1) AS roi_medio,
    SUM(economia_optin_brl) AS economia_optin
FROM multibet.crm_campaign_daily
WHERE report_date = '2026-03-27'
```

### Tabelas auxiliares
- `crm_dispatch_budget` — filtrar por `month_ref`
- `crm_campaign_comparison` — filtrar por `campaign_id`
- `crm_campaign_game_daily` — filtrar por `report_date` e `campaign_id`
- `crm_vip_group_daily` — filtrar por `report_date`
- `crm_recovery_daily` — filtrar por `report_date`

---

## 6. Como rodar

### Pre-requisito (1 vez)
```bash
python pipelines/ddl_crm_report.py
```

### Execucao diaria (D+1 pela manha)
```bash
python pipelines/crm_report_daily.py
```

### Backfill (ex: ultimos 30 dias)
```bash
python pipelines/crm_report_daily.py --days 30
```

### Teste sem persistir
```bash
python pipelines/crm_report_daily.py --dry-run
```

---

## 7. Proximos passos

1. **Gusta** implementa no front do Super Nova DB consumindo as tabelas
2. **CRM (Raphael)** preenche metas e verba mensal
3. **Pipeline** roda diariamente (cron ou agendamento)
4. **Validacao** — comparar numeros do report com Smartico Data Studio
5. **Evolucao** — quando bronze_crm_* tiverem dados, migrar fonte para economizar BigQuery
