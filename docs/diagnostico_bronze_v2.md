# Diagnostico Bronze v2 — Gap Analysis Completa e Mapeamento KPIs

**Data:** 2026-03-23
**Responsavel:** Mateus Fabro + Squad Intelligence Engine (Data Architect)
**Referencia:** `docs/bronze_selects_kpis_CORRIGIDO_v2.md` (21 SELECTs validados)
**Validacao empirica:** `output/validacao_bronze_colunas.csv` + `output/validacao_bronze_sqls_v2.csv`

---

## 1. STATUS DAS TABELAS BRONZE NO BANCO

### 1.1 Tabelas COM dados (6):

| Tabela | Linhas | PG cols | Athena cols | Match |
|--------|--------|---------|-------------|-------|
| bronze_ecr_flags | 1,119,911 | - | 13 | OK |
| bronze_bonus_sub_fund | 4,232,397 | 29 | 38 | 29/38 |
| bronze_ecr_banner | 336,525 | - | 16 | OK |
| bronze_instrument | 120,479 | - | - | OK |
| bronze_games_mapping_data | 2,718 | - | 14 | OK |
| bronze_fund_txn_type_mst | 157 | - | 26 | OK |

### 1.2 Tabelas VAZIAS (18):

| Tabela | PG cols | Athena cols | Match | DDL OK? |
|--------|---------|-------------|-------|---------|
| bronze_ecr | 17 | 19 | 16/19 | SIM - 3 cols Athena extras nao criticas |
| bronze_cashier_deposit | 26 | 66 | 25/66 | SIM - trazemos so as necessarias |
| bronze_cashier_cashout | 24 | 62 | 23/62 | SIM |
| bronze_real_fund_txn | 27 | 51 | 26/51 | SIM |
| bronze_realcash_sub_fund | 17 | 21 | 17/21 | SIM |
| bronze_daily_payment_summary | 28 | 54 | 27/54 | SIM |
| bronze_gaming_sessions | 20 | 23 | 19/23 | SIM |
| bronze_sports_bets | 22 | 22 | 20/22 | SIM |
| bronze_bonus_details | 34 | 46 | 33/46 | SIM |
| bronze_ccf_score | 8 | 7 | 7/7 | SIM - perfeito |
| bronze_kyc_level | 14 | 13 | 13/13 | SIM - perfeito |
| bronze_games_catalog | 20 | 24 | 20/24 | SIM |
| bronze_fund_txn_casino | 22 | (mesma real_fund_txn) | - | SIM - subconjunto filtrado |
| bronze_games_catalog_full | - | - | - | AVALIAR: possivelmente duplicata |
| bronze_big_wins | - | - | - | AVALIAR: provavelmente Silver, nao Bronze |
| bronze_crm_campaigns | - | BigQuery | - | Fase 2 |
| bronze_crm_communications | - | BigQuery | - | Fase 2 |
| bronze_crm_player_responses | - | BigQuery | - | Fase 2 |

### 1.3 Tabelas que NAO EXISTEM no banco (3 - precisam CREATE):

| Tabela a criar | Fonte Athena | Justificativa |
|---------------|-------------|---------------|
| bronze_sports_bet_details | vendor_ec2.tbl_sports_book_bet_details | Unica fonte de c_sport_type_name (nome do esporte) |
| bronze_dim_game | ps_bi.dim_game | Dimensao (lookup) - catalogo de jogos |
| bronze_dim_user | ps_bi.dim_user | Dimensao (lookup) - unica fonte de country_code |

---

## 2. VALIDACAO DDLs vs DOCUMENTO v2

### 2.1 Contexto

O documento v1 do Mauro tinha **69 colunas com nome errado**. Essas foram corrigidas no documento v2 (`bronze_selects_kpis_CORRIGIDO_v2.md`). A questao e: as DDLs no banco foram baseadas no v1 ou no v2?

**Resultado do diagnostico anterior:** DDLs estao RAZOAVELMENTE OK. As tabelas no PG tem mais colunas que o minimo do v2, e os nomes das colunas criticas estao corretos. Isso indica que as DDLs provavelmente ja foram criadas com base nas colunas reais do Athena (SHOW COLUMNS), nao apenas no documento v1.

### 2.2 Pontos de Verificacao por Tabela

Mesmo com DDLs OK, confirmar estes pontos CRITICOS antes de popular:

| Tabela | Verificacao | Importancia |
|--------|------------|-------------|
| bronze_real_fund_txn | TEM c_amount_in_ecr_ccy? | CRITICA - sem ela nao calcula GGR |
| bronze_real_fund_txn | TEM c_txn_status? | ALTA - filtro SUCCESS |
| bronze_real_fund_txn | TEM c_op_type? | ALTA - DB/CR (debito/credito) |
| bronze_real_fund_txn | TEM c_product_id? | ALTA - separar casino/sports |
| bronze_real_fund_txn | TEM c_sub_vendor_id (NAO c_vendor_id)? | MEDIA - c_vendor_id nao existe no Athena |
| bronze_daily_payment_summary | TEM c_created_date (NAO c_date)? | ALTA - nome real |
| bronze_daily_payment_summary | TEM c_success_cashout_amount (NAO c_withdrawal_amount_brl)? | ALTA - nome real |
| bronze_bonus_details | TEM c_issue_type (NAO c_bonus_type)? | ALTA - nome real |
| bronze_bonus_details | TEM c_drp/crp/wrp/rrp_in_ecr_ccy (NAO c_bonus_amount_brl)? | ALTA - sub-wallets |
| bronze_gaming_sessions | TEM c_session_length_in_sec (NAO c_session_duration_sec)? | MEDIA |
| bronze_gaming_sessions | TEM c_game_played_count (NAO c_round_count)? | MEDIA |
| bronze_games_catalog | TEM c_game_desc (NAO c_game_name)? | MEDIA |
| bronze_ccf_score | TEM c_ccf_timestamp (NAO c_calculated_date)? | MEDIA |
| bronze_kyc_level | TEM c_level (NAO c_kyc_level)? | MEDIA |

### 2.3 Decisao Arquitetural: bronze_fund_txn_casino

**Problema:** bronze_fund_txn_casino e subconjunto filtrado de bronze_real_fund_txn (WHERE c_product_id = 'CASINO'). Viola principio Bronze = dados brutos sem filtros.

**Recomendacao:** ELIMINAR. Separacao casino/sports deve ser feita na camada Silver via WHERE ou views.

---

## 3. DDLs PARA TABELAS NOVAS (CREATE)

### 3.1 bronze_sports_bet_details

```sql
-- Fonte: vendor_ec2.tbl_sports_book_bet_details (SELECT #11 do v2)
-- Justificativa: unica fonte de c_sport_type_name (nome do esporte)
CREATE TABLE IF NOT EXISTS multibet.bronze_sports_bet_details (
    c_customer_id       BIGINT,
    c_bet_slip_id       BIGINT,
    c_transaction_id    BIGINT,
    c_bet_id            BIGINT,
    c_sport_type_name   VARCHAR(255),
    c_sport_id          INTEGER,
    c_event_name        VARCHAR(500),
    c_market_name       VARCHAR(500),
    c_selection_name    VARCHAR(500),
    c_odds              VARCHAR(50),          -- VARCHAR no Athena
    c_leg_status        VARCHAR(10),          -- O=Open, W=Won, L=Lost
    c_tournament_name   VARCHAR(500),
    c_is_live           BOOLEAN,
    c_created_time      TIMESTAMPTZ,          -- UTC
    c_leg_settlement_date TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_bsbd_bet_slip ON multibet.bronze_sports_bet_details (c_bet_slip_id);
CREATE INDEX IF NOT EXISTS idx_bsbd_sport ON multibet.bronze_sports_bet_details (c_sport_type_name);
```

### 3.2 bronze_dim_game

```sql
-- Fonte: ps_bi.dim_game (SELECT #20 do v2)
-- Dimensao pura - lookup de jogos
CREATE TABLE IF NOT EXISTS multibet.bronze_dim_game (
    game_id             VARCHAR(50) PRIMARY KEY,
    game_desc           VARCHAR(255),
    vendor_id           VARCHAR(50),
    product_id          VARCHAR(30),
    game_category       VARCHAR(100),
    game_category_desc  VARCHAR(100),
    game_type_id        INTEGER,
    game_type_desc      VARCHAR(255),
    status              VARCHAR(30),
    updated_time        TIMESTAMPTZ,
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);
```

### 3.3 bronze_dim_user

```sql
-- Fonte: ps_bi.dim_user (SELECT #21 do v2)
-- Unica fonte de country_code + external_id (Smartico join)
CREATE TABLE IF NOT EXISTS multibet.bronze_dim_user (
    ecr_id              BIGINT PRIMARY KEY,
    external_id         BIGINT,               -- = Smartico user_ext_id
    registration_date   DATE,
    country_code        VARCHAR(10),
    last_deposit_date   DATE,
    last_deposit_amount_inhouse NUMERIC(15,2),
    loaded_at           TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 4. ALTERs PARA TABELAS COM DADOS (se necessario)

As tabelas carregadas podem precisar de colunas adicionais do v2. Executar apenas se a verificacao confirmar que faltam.

```sql
-- bronze_ecr_flags (1.1M) — v2 adicionou colunas extras
ALTER TABLE multibet.bronze_ecr_flags
    ADD COLUMN IF NOT EXISTS c_referral_ban BOOLEAN,
    ADD COLUMN IF NOT EXISTS c_withdrawl_allowed BOOLEAN,
    ADD COLUMN IF NOT EXISTS c_two_factor_auth_enabled BOOLEAN,
    ADD COLUMN IF NOT EXISTS c_hide_username_feed BOOLEAN;

-- bronze_bonus_sub_fund (4.2M) — v2 adicionou c_ecr_id
ALTER TABLE multibet.bronze_bonus_sub_fund
    ADD COLUMN IF NOT EXISTS c_ecr_id BIGINT;

-- bronze_ecr_banner (336K) — v2 adicionou colunas
ALTER TABLE multibet.bronze_ecr_banner
    ADD COLUMN IF NOT EXISTS c_ecr_id BIGINT,
    ADD COLUMN IF NOT EXISTS c_affiliate_name VARCHAR(255),
    ADD COLUMN IF NOT EXISTS c_banner_id VARCHAR(100),
    ADD COLUMN IF NOT EXISTS c_reference_url VARCHAR(2000),
    ADD COLUMN IF NOT EXISTS c_custom1 VARCHAR(500),
    ADD COLUMN IF NOT EXISTS c_custom2 VARCHAR(500),
    ADD COLUMN IF NOT EXISTS c_custom3 VARCHAR(500),
    ADD COLUMN IF NOT EXISTS c_custom4 VARCHAR(500);

-- bronze_games_mapping_data (2.7K) — v2 adicionou colunas
ALTER TABLE multibet.bronze_games_mapping_data
    ADD COLUMN IF NOT EXISTS c_game_desc VARCHAR(255),
    ADD COLUMN IF NOT EXISTS c_game_category_desc VARCHAR(100),
    ADD COLUMN IF NOT EXISTS c_product_id VARCHAR(30),
    ADD COLUMN IF NOT EXISTS c_status VARCHAR(30);
```

NOTA: Apos ALTERs, recarregar dados para popular as novas colunas.

---

## 5. MAPEAMENTO KPIs POR CAMADA (Bronze/Silver/Gold)

### 5.1 BRONZE (dados brutos - sem calculos)

Todas as colunas abaixo sao dados crus do Athena. Valores em centavos, timestamps em UTC. Sem conversoes, sem filtros (exceto test_user=false nos JOINs com ecr_flags).

| Dominio | Tabela Bronze | Colunas-Chave |
|---------|--------------|---------------|
| Jogadores | bronze_ecr | c_ecr_id, c_external_id, c_signup_time, c_tracker_id, c_affiliate_id |
| Flags | bronze_ecr_flags | c_ecr_id, c_test_user (boolean) |
| Depositos | bronze_cashier_deposit | c_ecr_id, c_txn_id, c_initial_amount (centavos), c_txn_status |
| Saques | bronze_cashier_cashout | c_ecr_id, c_txn_id, c_initial_amount (centavos), c_txn_status |
| Transacoes Gaming | bronze_real_fund_txn | c_ecr_id, c_txn_type, c_amount_in_ecr_ccy (centavos), c_op_type, c_product_id |
| Sub-Fund Real | bronze_realcash_sub_fund | c_fund_txn_id, c_ecr_id, c_amount_in_house_ccy (centavos) |
| Sub-Fund Bonus | bronze_bonus_sub_fund | c_fund_txn_id, c_ecr_id, c_drp/crp/wrp/rrp (centavos) |
| Tipos Transacao | bronze_fund_txn_type_mst | c_txn_type, c_internal_description, c_is_gaming_txn, c_is_cancel_txn |
| Bonus | bronze_bonus_details | c_ecr_id, c_bonus_id, c_issue_type, c_bonus_status, wallets |
| Sports Bets | bronze_sports_bets | c_customer_id, c_total_stake (BRL!), c_total_return, c_bet_type |
| Sports Details | bronze_sports_bet_details (CRIAR) | c_sport_type_name, c_odds, c_leg_status |
| Gaming Sessions | bronze_gaming_sessions | c_ecr_id, c_game_id, c_session_length_in_sec |
| Pagamentos Diarios | bronze_daily_payment_summary | c_ecr_id, c_deposit_amount, c_success_cashout_amount |
| Instrumentos | bronze_instrument | c_ecr_id, c_instrument, c_deposit_success, c_chargeback |
| Risco (CCF) | bronze_ccf_score | c_ecr_id, c_ccf_score, c_bet_factor |
| KYC | bronze_kyc_level | c_ecr_id, c_level, c_grace_action_status |
| Catalogo Jogos | bronze_games_catalog | c_game_id, c_game_desc, c_vendor_id, c_game_category_desc |
| Game Images | bronze_games_mapping_data | c_game_id, c_game_desc, c_vendor_id |
| Dim Game | bronze_dim_game (CRIAR) | game_id, game_desc, vendor_id, game_category |
| Dim User | bronze_dim_user (CRIAR) | ecr_id, external_id, country_code |

### 5.2 SILVER (views com joins e conversoes)

A Silver faz as transformacoes basicas sobre a Bronze:

| Transformacao | Exemplo |
|--------------|---------|
| Centavos -> BRL | c_amount_in_ecr_ccy / 100.0 AS amount_brl |
| UTC -> BRT | c_signup_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' |
| Filtro test users | JOIN bronze_ecr_flags WHERE c_test_user = false |
| FTD detection | ROW_NUMBER() OVER (PARTITION BY c_ecr_id ORDER BY c_created_time) = 1 |
| Sub-Fund Isolation | JOIN realcash + bonus sub-funds com type_mst para separar Real vs Bonus |
| Casino Bets | SUM(realcash+drp) WHERE product_id='CASINO' AND op_type='DB' |
| Casino Wins | SUM(realcash+drp) WHERE product_id='CASINO' AND op_type='CR' |
| Sports GGR by Sport | JOIN sports_bets + bet_details por c_bet_slip_id |
| Net Deposit | SUM(deposits) - SUM(cashouts) |
| Chargeback Rate | c_cb_amount / c_deposit_amount |
| Days Inactive | date_diff(last_active, current_date) |

### 5.3 GOLD (KPIs calculados)

| KPI | Formula | Tabelas Base (via Silver) |
|-----|---------|--------------------------|
| GGR Real | Real Bets - Real Wins (Sub-Fund Isolation) | real_fund_txn + realcash sub-fund + type_mst |
| GGR Bonus | Bonus Bets - Bonus Wins (crp+wrp+rrp) | real_fund_txn + bonus sub-fund + type_mst |
| GGR Total | GGR Real + GGR Bonus | Todas acima |
| NGR | GGR Real (sem bonus) | real_fund_txn + realcash sub-fund |
| New NGR | GGR - BTR - RCA | GGR + bonus_summary_details + cashier |
| Hold Rate | GGR / Total Bets * 100 | Derivado de GGR |
| RTP | Total Wins / Total Bets * 100 | Derivado de GGR |
| FTD Conversion Rate | Qty FTDs / Qty Registrations * 100 | ecr + cashier_deposit |
| CPA | Marketing Spend / Qty FTDs | dim_marketing_mapping + cashier_deposit |
| CPR | Marketing Spend / Qty Registrations | dim_marketing_mapping + ecr |
| ROAS | GGR / Marketing Spend | GGR + dim_marketing_mapping |
| LTV D7/D30 | SUM(GGR) primeiros 7/30 dias por cohort | real_fund_txn + cashier_deposit |
| Stickiness | DAU / MAU * 100 | real_fund_txn |
| Churn Rate | Inativos >30d / Base ativa * 100 | real_fund_txn + cashier_deposit |
| Avg Bets per Player | Total Bets / DAU | real_fund_txn |
| GGR per DAU | GGR / DAU | real_fund_txn |
| Net Deposit | Total Deposits - Total Cashouts | cashier_deposit + cashier_cashout |
| FTD Value Bands | COUNT por faixa (<50, 50-500, >500) | cashier_deposit |
| Bonus/GGR Ratio | BTR / GGR * 100 | bonus_details + GGR |

### 5.4 Mapeamento Detalhado KPIs Excel -> Camadas (linhas 34-82)

| # | KPI | Bronze | Silver | Gold |
|---|-----|--------|--------|------|
| 34 | Qty Registrations | bronze_ecr + bronze_ecr_flags | COUNT DISTINCT c_ecr_id/dia BRT | - |
| 35 | Qty FTDs | bronze_cashier_deposit + bronze_ecr_flags | ROW_NUMBER rn=1, UTC->BRT | - |
| 36 | FTD Conversion Rate | - | - | ftds / regs * 100 |
| 37 | Time to FTD | bronze_ecr + bronze_cashier_deposit | date_diff(signup, ftd) horas | - |
| 38 | KYC Completion Rate | bronze_kyc_level + bronze_ecr | COUNT KYC_1,KYC_2 / total | - |
| 39 | Device Distribution | FALTA: tbl_ecr_signup_info | COUNT_IF por canal | - |
| 40 | Avg FTD Value | bronze_cashier_deposit | AVG(amount/100) rn=1 | - |
| 41 | FTD Payment Method | bronze_cashier_deposit (add c_processor_name) | GROUP BY processor | - |
| 42 | FTD Value Bands | - | - | COUNT_IF por faixas |
| 43 | FTD with Bonus | bronze_cashier_deposit + bronze_bonus_details | JOIN timing | - |
| 44-46 | Gaming Bets/Wins | bronze_real_fund_txn + sub-funds + type_mst + flags | Sub-Fund Isolation | - |
| 47 | Hold Rate | - | - | GGR / bets * 100 |
| 48 | DAU (gaming) | bronze_real_fund_txn | COUNT DISTINCT/dia | - |
| 49 | Max Single Win | bronze sub-funds | MAX(real+bonus) CR | - |
| 50 | Rollbacks | bronze_real_fund_txn + type_mst | COUNT cancel=true | - |
| 51 | Bonus GGR | bronze_bonus_sub_fund | crp+wrp+rrp por op_type | - |
| 52 | NGR | - | - | GGR Real |
| 53-58 | Attribution | bronze_ecr + cashier + fund + dim_marketing | Joins | CPA, CPR, ROAS |
| 59-62 | Marketing Mapping | dim_marketing_mapping (ja existe) | - | - |
| 63-64 | Acquisition Channel | vw_acquisition_channel (ja existe) | - | - |
| 65-68 | Cohort Metrics | bronze_cashier_deposit + bronze_real_fund_txn | Cohort por mes FTD | LTV, payback |
| 69-70 | DAU/MAU | bronze_real_fund_txn | Janela 1d/30d | - |
| 71 | Stickiness | - | - | DAU/MAU * 100 |
| 72 | Avg Bets/Player | - | - | bets / DAU |
| 73 | Session Duration | bronze_gaming_sessions | AVG(length_in_sec) | - |
| 74 | GGR per DAU | - | - | GGR / DAU |
| 75-78 | Redeposit | bronze_cashier_deposit | ROW_NUMBER, COUNT, AVG | - |
| 79-80 | Churn | bronze_real_fund_txn | days_since_last | Taxa >30d |
| 81-82 | Reactivation | BigQuery Smartico (fase 2) | CRM campaigns | - |

---

## 6. PRIORIDADE DE CARGA

### P1 - CRITICA (bloqueia ~60% dos KPIs P2):

| Ordem | Tabela | Justificativa | Volume Estimado |
|-------|--------|--------------|-----------------|
| 1 | bronze_real_fund_txn | GGR, NGR, Hold Rate, DAU, tudo gaming | ~100M+ |
| 2 | bronze_realcash_sub_fund | Sub-Fund Isolation (GGR Real vs Bonus) | ~50M+ |
| 3 | bronze_cashier_deposit | FTD, depositos, cohorts, CPA | ~5M+ |
| 4 | bronze_ecr | Registrations, time-to-FTD, atribuicao | ~1.5M+ |
| 5 | bronze_cashier_cashout | Saques, Net Deposit | ~3M+ |

### P2 - ALTA (complementa KPIs P2):

| Ordem | Tabela | Justificativa | Volume |
|-------|--------|--------------|--------|
| 6 | bronze_dim_user (CRIAR) | country_code, external_id (Smartico) | ~1.5M |
| 7 | bronze_bonus_details | Bonus cost, wagering, BTR | ~5M+ |
| 8 | bronze_daily_payment_summary | Financial summary, chargebacks | ~10M+ |
| 9 | bronze_kyc_level | KYC rate | ~1.5M |

### P3 - MEDIA (KPIs P3 e especificos):

| Ordem | Tabela | Volume |
|-------|--------|--------|
| 10 | bronze_sports_bets | ~2M+ |
| 11 | bronze_sports_bet_details (CRIAR) | ~5M+ |
| 12 | bronze_gaming_sessions | ~20M+ |
| 13-14 | bronze_games_catalog + bronze_dim_game (CRIAR) | ~3K cada |
| 15 | bronze_ccf_score | ~1.5M |

### P4 - BAIXA:

| Tabela | Acao |
|--------|------|
| bronze_crm_* (3 tabelas) | Fase 2 (BigQuery) |
| bronze_fund_txn_casino | ELIMINAR (redundante) |
| bronze_big_wins | AVALIAR: mover para Silver |
| bronze_games_catalog_full | AVALIAR: possivelmente duplicata |

---

## 7. RISCOS E RECOMENDACOES

### 7.1 Riscos

| # | Risco | Severidade | Mitigacao |
|---|-------|-----------|-----------|
| R1 | Volume: real_fund_txn >100M linhas | ALTA | Carregar por mes. Paginacao na API Athena |
| R2 | Custo Athena: full scans em tabelas grandes | ALTA | Filtrar por data SEMPRE. SELECT colunas especificas |
| R3 | Colunas erradas se DDL baseada em v1 | MEDIA | Verificar pontos criticos (secao 2.2) antes de popular |
| R4 | Falta tbl_bonus_summary_details (para BTR) | MEDIA | Adicionar ao v2 proxima revisao |
| R5 | Falta tbl_ecr_signup_info (device distribution) | BAIXA | Verificar existencia no Athena |
| R6 | Falta c_processor_name em bronze_cashier_deposit | BAIXA | Adicionar coluna (KPI #41) |
| R7 | Tabelas carregadas podem precisar recarga apos ALTER | MEDIA | Planejar recarga das 6 tabelas |

### 7.2 Tabelas Faltantes no Documento v2 (para proxima revisao)

| Tabela | Database | Justificativa |
|--------|----------|---------------|
| tbl_bonus_summary_details | bonus_ec2 | Necessaria para BTR (c_actual_issued_amount) |
| tbl_ecr_signup_info | ecr_ec2 | Device distribution (c_channel) |
| tbl_real_fund_session | fund_ec2 | Session-level metrics (se existir) |
| tbl_sports_book_info | vendor_ec2 | Transacoes financeiras granulares SB |

### 7.3 Recomendacoes Arquiteturais

1. **Padrao Bronze:** prefixo `bronze_`, coluna `loaded_at`, nomes originais Athena (c_*), sem calculos
2. **Carga incremental** para tabelas grandes: primeira carga por mes, depois D-1 diario
3. **Adicionar coluna `batch_date DATE`** para controle de carga incremental
4. **Eliminar bronze_fund_txn_casino** (redundante com bronze_real_fund_txn)
5. **Validacao pos-carga:** COUNT no Athena vs COUNT no Super Nova DB para cada tabela

### 7.4 Proximos Passos

1. [ ] Verificar pontos criticos (secao 2.2) — confirmar nomes de colunas no banco
2. [ ] CREATE 3 tabelas novas (sports_bet_details, dim_game, dim_user)
3. [ ] Pipeline de carga P1 (5 tabelas criticas)
4. [ ] Testar pipeline com LIMIT 1000 antes de carga full
5. [ ] Carga full P1 (por mes para tabelas grandes)
6. [ ] Validar contagens Athena vs Super Nova DB
7. [ ] ALTER + recarga das 6 tabelas com dados (se necessario)
8. [ ] Criar views Silver sobre Bronze
9. [ ] Criar views Gold com KPIs calculados
10. [ ] Documentar no schema_multibet_database com nova secao "Camada Bronze"

---

## ANEXO A: Resumo Quantitativo

| Metrica | Valor |
|---------|-------|
| Tabelas Bronze com dados | 6 |
| Tabelas Bronze vazias | 18 |
| Tabelas novas para CRIAR | 3 |
| Tabelas para ELIMINAR | 1 (fund_txn_casino) |
| Tabelas para AVALIAR | 2 (big_wins, games_catalog_full) |
| Total Bronze v2 (alvo) | 21 (+3 CRM fase 2) |
| Colunas v1 corrigidas no v2 | 69 |
| KPIs mapeados | ~80+ (linhas 34-82 do Excel + casino/sports) |
| KPIs bloqueados sem P1 | ~60% dos KPIs P2 |
| Volume estimado P1 | ~160M linhas (5 tabelas) |

## ANEXO B: Mapeamento Tabela v2 -> Bronze DB

| # v2 | Fonte Athena | Tabela Bronze | Status |
|------|-------------|--------------|--------|
| 1 | ecr_ec2.tbl_ecr | bronze_ecr | VAZIA |
| 2 | cashier_ec2.tbl_cashier_deposit | bronze_cashier_deposit | VAZIA |
| 3 | ecr_ec2.tbl_ecr_banner | bronze_ecr_banner | CARREGADA 336K |
| 4 | ecr_ec2.tbl_ecr_flags | bronze_ecr_flags | CARREGADA 1.1M |
| 5 | fund_ec2.tbl_real_fund_txn | bronze_real_fund_txn | VAZIA (CRITICA) |
| 6 | fund_ec2.tbl_realcash_sub_fund_txn | bronze_realcash_sub_fund | VAZIA |
| 7 | fund_ec2.tbl_bonus_sub_fund_txn | bronze_bonus_sub_fund | CARREGADA 4.2M |
| 8 | fund_ec2.tbl_real_fund_txn_type_mst | bronze_fund_txn_type_mst | CARREGADA 157 |
| 9 | vendor_ec2.tbl_vendor_games_mapping_mst | bronze_games_catalog | VAZIA |
| 10 | vendor_ec2.tbl_sports_book_bets_info | bronze_sports_bets | VAZIA |
| 11 | vendor_ec2.tbl_sports_book_bet_details | NAO EXISTE -> CRIAR | - |
| 12 | bireports_ec2.tbl_ecr_gaming_sessions | bronze_gaming_sessions | VAZIA |
| 13 | cashier_ec2.tbl_cashier_cashout | bronze_cashier_cashout | VAZIA |
| 14 | cashier_ec2.tbl_cashier_ecr_daily_payment_summary | bronze_daily_payment_summary | VAZIA |
| 15 | cashier_ec2.tbl_instrument | bronze_instrument | CARREGADA 120K |
| 16 | bonus_ec2.tbl_ecr_bonus_details | bronze_bonus_details | VAZIA |
| 17 | risk_ec2.tbl_ecr_ccf_score | bronze_ccf_score | VAZIA |
| 18 | ecr_ec2.tbl_ecr_kyc_level | bronze_kyc_level | VAZIA |
| 19 | bireports_ec2.tbl_vendor_games_mapping_data | bronze_games_mapping_data | CARREGADA 2.7K |
| 20 | ps_bi.dim_game | NAO EXISTE -> CRIAR | - |
| 21 | ps_bi.dim_user | NAO EXISTE -> CRIAR | - |

---

**Documento gerado por:** Data Architect — igaming-data-squad
**Proximo passo:** Verificar colunas criticas (secao 2.2), CREATE 3 tabelas novas, iniciar pipeline P1
