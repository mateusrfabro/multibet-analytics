# Schema Document — Camada Bronze MultiBet
## Super Nova DB (PostgreSQL) — Schema `multibet`

**Versao:** 1.0
**Data:** 2026-03-23
**Responsavel:** Mateus Fabro — Squad Intelligence Engine
**Empresa:** Super Nova Gaming
**Base de validacao:** `bronze_selects_kpis_CORRIGIDO_v2.md` (SHOW COLUMNS + SELECT LIMIT 5 no Athena, 2026-03-20)
**Referencia documental:** Pragmatic Solutions Database Schema Documents v1.0-v1.3

---

## 1. Summary

### 1.1 Descricao

A camada Bronze do Data Warehouse MultiBet armazena dados brutos replicados do Athena
(Iceberg Data Lake) no Super Nova DB (PostgreSQL). Nenhum calculo, transformacao ou
agregacao e realizado nesta camada -- os dados sao copiados "as-is" das fontes originais.

### 1.2 Objetivo

- Prover uma copia persistente e consultavel dos dados transacionais brutos
- Servir como fonte unica de verdade para as camadas Silver (transformacoes) e Gold (agregacoes)
- Permitir rastreabilidade completa (lineage) de qualquer KPI ate o dado bruto original
- Habilitar validacao cruzada: Silver calculada vs Gold pre-agregada pelo dbt/Pragmatic

### 1.3 Principio Canonical

A camada Bronze foi projetada para ser **replicavel a qualquer operador** que use a
plataforma Pragmatic Solutions. As tabelas seguem a estrutura nativa do Athena (_ec2),
com apenas 2 dimensoes auxiliares do ps_bi (dim_user, dim_game) incluidas por nao
existirem equivalente no _ec2.

### 1.4 Convencoes

| Convencao | Detalhe |
|-----------|---------|
| **Prefixo** | `bronze_` para todas as tabelas |
| **Valores monetarios** | Centavos (menor denominacao BRL) — dividir por 100.0 na Silver |
| **Excecao monetaria** | Sports (vendor_ec2.tbl_sports_book_*): valores ja em BRL real |
| **Timestamps** | UTC — converter para BRT na Silver com `AT TIME ZONE` |
| **Test users** | Filtrados na ingestao (`c_test_user = false`) quando aplicavel |
| **Tipos Athena** | Mapeados para PostgreSQL (bigint, varchar, timestamp, boolean, double, date) |

### 1.5 Estrategia de Carga

- **Metodo:** TRUNCATE + INSERT (full refresh diario)
- **Fonte:** Athena via `db/athena.py` (boto3 SDK, read-only)
- **Destino:** Super Nova DB via `db/supernova.py` (SSH tunnel + psycopg2)
- **Filtros na ingestao:** test users excluidos, apenas status relevantes

---

## 2. Schema Overview

### 2.1 Tabelas por Dominio

| # | Tabela Bronze | Fonte Athena | Dominio | Descricao | Colunas |
|---|---------------|-------------|---------|-----------|---------|
| 1 | bronze_ecr | ecr_ec2.tbl_ecr | Aquisicao | Registro de jogadores — cadastro core | 9 |
| 2 | bronze_cashier_deposit | cashier_ec2.tbl_cashier_deposit | Aquisicao/FTD | Depositos individuais com status e valor | 6 |
| 3 | bronze_ecr_banner | ecr_ec2.tbl_ecr_banner | Marketing | Banners, trackers, sinais de trafego | 11 |
| 4 | bronze_ecr_flags | ecr_ec2.tbl_ecr_flags | Filtros | Flags do jogador (test, ban, 2FA) | 6 |
| 5 | bronze_real_fund_txn | fund_ec2.tbl_real_fund_txn | Gaming Core | Transacoes de jogo (aposta, win, rollback) | 12 |
| 6 | bronze_realcash_sub_fund_txn | fund_ec2.tbl_realcash_sub_fund_txn | Sub-Fund Real | Sub-transacoes de dinheiro real | 3 |
| 7 | bronze_bonus_sub_fund_txn | fund_ec2.tbl_bonus_sub_fund_txn | Sub-Fund Bonus | Sub-transacoes de bonus (DRP/CRP/WRP/RRP) | 6 |
| 8 | bronze_real_fund_txn_type_mst | fund_ec2.tbl_real_fund_txn_type_mst | Catalogo | Master de tipos de transacao | 10 |
| 9 | bronze_vendor_games_mapping_mst | vendor_ec2.tbl_vendor_games_mapping_mst | Catalogo | Catalogo master de jogos | 14 |
| 10 | bronze_sports_book_bets_info | vendor_ec2.tbl_sports_book_bets_info | Sports | Apostas esportivas (header) | 17 |
| 11 | bronze_sports_book_bet_details | vendor_ec2.tbl_sports_book_bet_details | Sports | Detalhes/legs de apostas esportivas | 15 |
| 12 | bronze_ecr_gaming_sessions | bireports_ec2.tbl_ecr_gaming_sessions | Sessoes | Sessoes de jogo (duracao, rodadas) | 10 |
| 13 | bronze_cashier_cashout | cashier_ec2.tbl_cashier_cashout | Financeiro | Saques individuais com status e valor | 6 |
| 14 | bronze_cashier_ecr_daily_payment_summary | cashier_ec2.tbl_cashier_ecr_daily_payment_summary | Financeiro | Resumo diario de pagamentos por jogador | 12 |
| 15 | bronze_instrument | cashier_ec2.tbl_instrument | Financeiro | Instrumentos de pagamento do jogador | 13 |
| 16 | bronze_ecr_bonus_details | bonus_ec2.tbl_ecr_bonus_details | Bonus | Bonus ativos com wallets e wagering | 18 |
| 17 | bronze_ecr_ccf_score | risk_ec2.tbl_ecr_ccf_score | Risco | Fraud score CCF | 6 |
| 18 | bronze_ecr_kyc_level | ecr_ec2.tbl_ecr_kyc_level | Compliance | Nivel KYC do jogador | 7 |
| 19 | bronze_vendor_games_mapping_data | bireports_ec2.tbl_vendor_games_mapping_data | Catalogo | Catalogo de jogos (view BI) | 6 |
| 20 | bronze_ps_bi_dim_game | ps_bi.dim_game | Dimensao | Dimensao de jogos (catalogo dbt) | 10 |
| 21 | bronze_ps_bi_dim_user | ps_bi.dim_user | Dimensao | Dimensao de jogadores (lookup dbt) | 6 |
| 22 | bronze_ecr_signup_info | ecr_ec2.tbl_ecr_signup_info | Device | Device e canal de cadastro | 7 |
| 23 | bronze_bonus_summary_details | bonus_ec2.tbl_bonus_summary_details | Bonus | Resumo financeiro de bonus (BTR) | 11 |

### 2.2 Distribuicao por Database Athena

| Database Athena | Qtd Tabelas | Tabelas |
|-----------------|-------------|---------|
| ecr_ec2 | 5 | bronze_ecr, bronze_ecr_banner, bronze_ecr_flags, bronze_ecr_kyc_level, bronze_ecr_signup_info |
| fund_ec2 | 4 | bronze_real_fund_txn, bronze_realcash_sub_fund_txn, bronze_bonus_sub_fund_txn, bronze_real_fund_txn_type_mst |
| cashier_ec2 | 4 | bronze_cashier_deposit, bronze_cashier_cashout, bronze_cashier_ecr_daily_payment_summary, bronze_instrument |
| vendor_ec2 | 3 | bronze_vendor_games_mapping_mst, bronze_sports_book_bets_info, bronze_sports_book_bet_details |
| bonus_ec2 | 2 | bronze_ecr_bonus_details, bronze_bonus_summary_details |
| bireports_ec2 | 2 | bronze_ecr_gaming_sessions, bronze_vendor_games_mapping_data |
| risk_ec2 | 1 | bronze_ecr_ccf_score |
| ps_bi | 2 | bronze_ps_bi_dim_game, bronze_ps_bi_dim_user |

### 2.3 Distribuicao por Dominio

| Dominio | Qtd | Descricao |
|---------|-----|-----------|
| Aquisicao e Marketing | 4 | Registro, depositos FTD, banners, flags |
| Gaming e Performance | 5 | Transacoes, sub-funds, tipos, sessoes |
| Financeiro | 3 | Saques, resumo diario, instrumentos |
| Bonus e Custos | 2 | Bonus details, bonus summary (BTR) |
| Risco e Compliance | 2 | Fraud score, KYC |
| Catalogos e Dimensoes | 4 | Jogos (2 fontes), dimensao jogador, signup info |
| Sports | 2 | Apostas esportivas header + legs |

---

## 3. Data Dictionary

---

### 3.1 bronze_ecr

**Fonte Athena:** `ecr_ec2.tbl_ecr`
**Descricao:** Tabela mestre de registro de jogadores. Contem o cadastro core com IDs interno (c_ecr_id) e externo (c_external_id), dados de afiliacao e timestamps de cadastro. E a tabela raiz para joins com qualquer tabela transacional.
**Frequencia de atualizacao:** Diaria (full refresh)
**Filtro de ingestao:** JOIN com tbl_ecr_flags WHERE c_test_user = false
**Volume estimado:** ~500K-1M linhas (todos os jogadores reais registrados)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (PK, 18 digitos). Usado em TODAS as tabelas transacionais | 123456789012345678 |
| 2 | c_external_id | BIGINT | bigint | ID externo do jogador (15 digitos). = Smartico user_ext_id para joins CRM | 987654321012345 |
| 3 | c_tracker_id | VARCHAR(50) | varchar | ID do tracker de aquisicao (fonte de trafego) | tracker_google_01 |
| 4 | c_affiliate_id | VARCHAR(50) | varchar | ID do afiliado (fallback quando tracker nao disponivel) | aff_12345 |
| 5 | c_jurisdiction | VARCHAR(20) | varchar | Jurisdicao regulatoria do jogador. Substitui c_country_code (que NAO existe nesta tabela) | BR |
| 6 | c_language | VARCHAR(10) | varchar | Idioma preferido do jogador | pt-BR |
| 7 | c_ecr_status | VARCHAR(20) | varchar | Status da conta: 'play' (demo) ou 'real' (dinheiro real) | real |
| 8 | c_signup_time | TIMESTAMP | timestamp | Data/hora de cadastro em UTC. Converter para BRT na Silver | 2026-01-15 14:30:00 |
| 9 | dt | DATE | date (calculado) | Data de cadastro truncada: CAST(c_signup_time AS DATE) | 2026-01-15 |

**Notas:**
- c_country_code NAO existe nesta tabela. Para pais do jogador, usar `ps_bi.dim_user.country_code` (tabela #21)
- c_ecr_id e a chave universal de join com todas as tabelas transacionais
- c_external_id e a chave de join com BigQuery Smartico (user_ext_id)

---

### 3.2 bronze_cashier_deposit

**Fonte Athena:** `cashier_ec2.tbl_cashier_deposit`
**Descricao:** Depositos individuais do jogador. Inclui valor, status e timestamp. Tabela usada para calcular FTD (First Time Deposit), NRC (New Registered with Cash), volume de depositos e taxas de aprovacao.
**Frequencia de atualizacao:** Diaria (full refresh)
**Filtro de ingestao:** c_txn_status = 'txn_confirmed_success' AND c_test_user = false
**Volume estimado:** ~5-10M linhas (depositos com sucesso de todos os jogadores reais)
**Nota sobre fonte Athena:** Tabela original tem 66 colunas; apenas 6 relevantes para Bronze.

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (FK para bronze_ecr) | 123456789012345678 |
| 2 | c_txn_id | BIGINT | bigint | ID unico da transacao de deposito (PK) | 98765432101234 |
| 3 | c_initial_amount | BIGINT | bigint | Valor do deposito em CENTAVOS. Dividir por 100.0 para BRL | 5000 (= R$ 50,00) |
| 4 | c_created_time | TIMESTAMP | timestamp | Data/hora do deposito em UTC | 2026-02-10 18:45:00 |
| 5 | c_txn_status | VARCHAR(50) | varchar | Status da transacao. Filtrado para 'txn_confirmed_success' na ingestao | txn_confirmed_success |
| 6 | dt | DATE | date (calculado) | Data truncada: CAST(c_created_time AS DATE) | 2026-02-10 |

**Notas:**
- Valor em CENTAVOS: R$ 50,00 = 5000 no banco. Sempre dividir por 100.0
- Status 'txn_confirmed_success' e o unico status de sucesso final (nao usar 'SUCCESS' que e do fund_ec2)
- Para FTD: MIN(c_created_time) por jogador = primeiro deposito
- Para NRC: jogadores que fizeram pelo menos 1 deposito no periodo

---

### 3.3 bronze_ecr_banner

**Fonte Athena:** `ecr_ec2.tbl_ecr_banner`
**Descricao:** Registros de banners e sinais de marketing associados ao jogador. Contem tracker_id, affiliate_id, reference_url (onde click IDs como gclid/fbclid estao embutidos) e campos custom para UTMs. Principal fonte para atribuicao de trafego.
**Frequencia de atualizacao:** Diaria (full refresh)
**Filtro de ingestao:** c_tracker_id IS NOT NULL
**Volume estimado:** ~2-5M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador | 123456789012345678 |
| 2 | c_tracker_id | VARCHAR(100) | varchar | ID do tracker (fonte de trafego). Chave para dim_marketing_mapping | tracker_google_01 |
| 3 | c_affiliate_id | VARCHAR(50) | varchar | ID do afiliado (QUEM trouxe o trafego) | aff_12345 |
| 4 | c_affiliate_name | VARCHAR(200) | varchar | Nome descritivo do afiliado | Google Ads BR |
| 5 | c_banner_id | VARCHAR(100) | varchar | ID do banner/criativo (QUAL peça publicitaria) | banner_456 |
| 6 | c_reference_url | VARCHAR(2000) | varchar | URL de referencia completa. Contem click IDs (gclid, fbclid, ttclid) embutidos | https://multibet.com/?gclid=abc123 |
| 7 | c_custom1 | VARCHAR(500) | varchar | Campo customizavel 1 — usado para UTMs ou parametros extras | utm_source=google |
| 8 | c_custom2 | VARCHAR(500) | varchar | Campo customizavel 2 | utm_medium=cpc |
| 9 | c_custom3 | VARCHAR(500) | varchar | Campo customizavel 3 | utm_campaign=spring2026 |
| 10 | c_custom4 | VARCHAR(500) | varchar | Campo customizavel 4 | utm_content=variant_a |
| 11 | c_created_time | TIMESTAMP | timestamp | Data/hora do registro do banner em UTC | 2026-01-20 10:15:00 |

**Notas:**
- c_click_id, c_utm_source, c_utm_medium, c_utm_campaign NAO existem como colunas. Click IDs estao em c_reference_url; UTMs em c_custom1..c_custom4
- Hierarquia Pragmatic: affiliate_id (QUEM) -> tracker_id (DE ONDE) -> banner_id (QUAL criativo)
- Tabelas mestre de affiliates/trackers NAO sao replicadas no _ec2 (decisao confirmada 19/03/2026)
- Fonte de verdade para mapeamento: `multibet.dim_marketing_mapping` no Super Nova DB

---

### 3.4 bronze_ecr_flags

**Fonte Athena:** `ecr_ec2.tbl_ecr_flags`
**Descricao:** Flags booleanas do jogador. Principal uso: identificar test users (c_test_user = false). Tambem contem flags de ban, saque permitido, 2FA e privacidade.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~500K-1M linhas (1 por jogador)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (PK) | 123456789012345678 |
| 2 | c_test_user | BOOLEAN | boolean | Flag de usuario de teste. CRITICO: filtro obrigatorio em todas as queries (= false para reais) | false |
| 3 | c_referral_ban | BOOLEAN | boolean | Jogador banido de programa de referral | false |
| 4 | c_withdrawl_allowed | BOOLEAN | boolean | Saque permitido (true = liberado) | true |
| 5 | c_two_factor_auth_enabled | BOOLEAN | boolean | 2FA ativado | false |
| 6 | c_hide_username_feed | BOOLEAN | boolean | Esconder username no feed publico | false |

**Notas:**
- c_test_user e BOOLEAN (nao integer). Comparar com `= false`, nao `= 0`
- c_flag_name e c_flag_value NAO existem. Flags sao colunas individuais, nao key-value
- Esta tabela e usada em JOIN com praticamente todas as queries Bronze para excluir test users
- Sem filtro de test users, divergencia de ~3% nos numeros

---

### 3.5 bronze_real_fund_txn

**Fonte Athena:** `fund_ec2.tbl_real_fund_txn`
**Descricao:** Tabela PRINCIPAL de transacoes de jogo. Cada linha representa uma transacao: aposta (27), win (45), rollback (72), deposito (1), saque (2), bonus, etc. E o coracaoda camada Bronze para calcular GGR, NGR, Turnover, Hold Rate na Silver.
**Frequencia de atualizacao:** Diaria (full refresh)
**Filtro de ingestao:** JOIN com tbl_ecr_flags WHERE c_test_user = false
**Volume estimado:** ~50-100M+ linhas (todas as transacoes de todos os jogadores reais)
**Nota sobre fonte Athena:** Tabela original tem 51 colunas; 12 relevantes para Bronze.

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (FK para bronze_ecr) | 123456789012345678 |
| 2 | c_txn_id | BIGINT | bigint | ID unico da transacao (PK) | 55667788990011 |
| 3 | c_txn_type | INTEGER | int | Tipo numerico da transacao. Ver tabela #8 para mapeamento completo | 27 |
| 4 | c_txn_status | VARCHAR(30) | varchar | Status: 'SUCCESS' (sucesso), 'INIT' (iniciado), 'FAILURE' (falha) | SUCCESS |
| 5 | c_amount_in_ecr_ccy | BIGINT | bigint | Valor em CENTAVOS BRL. CRITICO: unico campo de valor nesta tabela | 5000 (= R$ 50,00) |
| 6 | c_op_type | VARCHAR(5) | varchar | Tipo de operacao: 'DB' (debito/saida) ou 'CR' (credito/entrada) | DB |
| 7 | c_game_id | VARCHAR(50) | varchar | ID do jogo. Join com bronze_vendor_games_mapping_mst ou dim_game | 4776 |
| 8 | c_sub_vendor_id | VARCHAR(50) | varchar | ID do sub-vendor/agregador (pgsoft, betsoft, etc.) | alea_pgsoft |
| 9 | c_product_id | VARCHAR(30) | varchar | Produto: 'CASINO' ou 'SPORTSBOOK' | CASINO |
| 10 | c_game_category | VARCHAR(50) | varchar | Categoria do jogo (slots, table, live, etc.) | slots |
| 11 | c_start_time | TIMESTAMP | timestamp | Data/hora da transacao em UTC | 2026-03-15 22:30:45 |
| 12 | dt | DATE | date (calculado) | Data truncada: CAST(c_start_time AS DATE) | 2026-03-15 |

**ALERTAS CRITICOS (validados empiricamente 17-20/03/2026):**
- `c_confirmed_amount_in_inhouse_ccy` NAO EXISTE nesta tabela
- `c_tracker_id` NAO EXISTE (tracker esta em ecr_ec2.tbl_ecr)
- `c_vendor_id` NAO EXISTE (usar c_sub_vendor_id)
- `c_round_id` NAO EXISTE
- Coluna de particao `dt` NAO existe como coluna visivel (nao ha filtro de particao disponivel)
- Status de sucesso e `'SUCCESS'` (nao 'txn_confirmed_success' que e do cashier)
- Tipos principais: 1=deposito, 27=aposta casino, 45=win casino, 72=rollback, 59=aposta SB, 112=win SB

---

### 3.6 bronze_realcash_sub_fund_txn

**Fonte Athena:** `fund_ec2.tbl_realcash_sub_fund_txn`
**Descricao:** Sub-transacoes de dinheiro real (real cash). Para cada transacao na tbl_real_fund_txn, esta tabela detalha a parcela em dinheiro real. Essencial para Sub-Fund Isolation: separar GGR real do GGR bonus.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~50-100M+ linhas (1:1 com tbl_real_fund_txn)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_fund_txn_id | BIGINT | bigint | FK para bronze_real_fund_txn.c_txn_id | 55667788990011 |
| 2 | c_ecr_id | BIGINT | bigint | ID interno do jogador (facilita joins diretos) | 123456789012345678 |
| 3 | c_amount_in_house_ccy | BIGINT | bigint | Valor em dinheiro real (centavos). Parcela real cash da transacao | 3000 (= R$ 30,00) |

**Notas:**
- Join com bronze_real_fund_txn: `c_fund_txn_id = c_txn_id`
- GGR Real Cash = SUM(aposta real cash) - SUM(win real cash)
- Sub-Fund Isolation validado com AWS Console e Mauro (18/03/2026)

---

### 3.7 bronze_bonus_sub_fund_txn

**Fonte Athena:** `fund_ec2.tbl_bonus_sub_fund_txn`
**Descricao:** Sub-transacoes de bonus. Detalha as parcelas de bonus por wallet (DRP, CRP, WRP, RRP) para cada transacao. Essencial para Sub-Fund Isolation e calculo de custo de bonus (BTR).
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~20-50M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_fund_txn_id | BIGINT | bigint | FK para bronze_real_fund_txn.c_txn_id | 55667788990011 |
| 2 | c_ecr_id | BIGINT | bigint | ID interno do jogador | 123456789012345678 |
| 3 | c_drp_amount_in_house_ccy | BIGINT | bigint | DRP = Deposit Restricted Points (real cash restrito). Centavos | 1000 |
| 4 | c_crp_amount_in_house_ccy | BIGINT | bigint | CRP = Casino Restricted Points (bonus casino). Centavos | 500 |
| 5 | c_wrp_amount_in_house_ccy | BIGINT | bigint | WRP = Wagering Restricted Points (bonus wagering). Centavos | 200 |
| 6 | c_rrp_amount_in_house_ccy | BIGINT | bigint | RRP = Restricted Reward Points (bonus reward). Centavos | 0 |

**Notas:**
- DRP e tecnicamente dinheiro real restrito (nao e bonus puro)
- CRP + WRP + RRP = bonus total na transacao
- GGR Bonus = SUM(aposta bonus) - SUM(win bonus) usando CRP+WRP+RRP
- Tipo 36 (CASHOUT_REVERSAL) NAO e bonus — cuidado na classificacao

---

### 3.8 bronze_real_fund_txn_type_mst

**Fonte Athena:** `fund_ec2.tbl_real_fund_txn_type_mst`
**Descricao:** Tabela master (lookup) de tipos de transacao. Mapeia c_txn_type numerico para descricao textual, tipo de operacao, e flags (gaming, cancel, free spin, refund, settlement).
**Frequencia de atualizacao:** Rara (referencia estatica, muda somente quando PS adiciona novos tipos)
**Volume estimado:** ~150 linhas (um por tipo de transacao)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_txn_type | INTEGER | int | Codigo numerico do tipo de transacao (PK) | 27 |
| 2 | c_internal_description | VARCHAR(200) | varchar | Descricao interna do tipo. NOTA: c_txn_type_name NAO existe | CASINO_BUYIN |
| 3 | c_op_type | VARCHAR(5) | varchar | Tipo de operacao: 'DB' (debito) ou 'CR' (credito) | DB |
| 4 | c_is_gaming_txn | BOOLEAN | boolean | Flag: transacao de jogo (aposta, win, rollback) | true |
| 5 | c_is_cancel_txn | BOOLEAN | boolean | Flag: transacao de cancelamento/rollback | false |
| 6 | c_is_free_spin_txn | BOOLEAN | boolean | Flag: transacao de free spin | false |
| 7 | c_is_refund_txn_type | BOOLEAN | boolean | Flag: transacao de reembolso | false |
| 8 | c_is_settlement_txn_type | BOOLEAN | boolean | Flag: transacao de liquidacao (sportsbook) | false |
| 9 | c_product_id | VARCHAR(30) | varchar | Produto: 'CASINO', 'SPORTSBOOK', null | CASINO |
| 10 | c_txn_identifier_key | VARCHAR(100) | varchar | Chave textual identificadora do tipo | CASINO_BUYIN |

**Tipos principais para KPIs:**

| c_txn_type | Constante | Op | Uso no KPI |
|------------|-----------|-----|------------|
| 1 | REAL_CASH_DEPOSIT | CR | Volume de depositos |
| 2 | REAL_CASH_WITHDRAW | DB | Volume de saques |
| 27 | CASINO_BUYIN | DB | Turnover casino / GGR |
| 45 | CASINO_WIN | CR | GGR (subtrai do turnover) |
| 59 | SB_BUYIN | DB | Turnover sportsbook |
| 65 | JACKPOT_WIN | CR | Jackpots |
| 72 | CASINO_BUYIN_CANCEL | CR | Rollbacks (excluir do GGR) |
| 80 | CASINO_FREESPIN_WIN | CR | Free spin wins |
| 112 | SB_WIN | CR | GGR sportsbook |

---

### 3.9 bronze_vendor_games_mapping_mst

**Fonte Athena:** `vendor_ec2.tbl_vendor_games_mapping_mst`
**Descricao:** Catalogo master de jogos. Cada jogo com seu vendor, sub-vendor, categoria, tipo, tecnologia e flags (jackpot, free spin, feature trigger). Referencia para enriquecer transacoes de jogo com nome e detalhes.
**Frequencia de atualizacao:** Rara (atualiza quando PS cadastra novos jogos, cron 4:30 AM)
**Volume estimado:** ~5K-10K linhas (todos os jogos cadastrados)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_game_id | VARCHAR(50) | varchar | ID unico do jogo. Join com bronze_real_fund_txn.c_game_id | 4776 |
| 2 | c_game_desc | VARCHAR(200) | varchar | Nome/descricao do jogo. NOTA: c_game_name NAO existe | Fortune Tiger |
| 3 | c_vendor_id | VARCHAR(50) | varchar | ID do vendor principal (pragmaticplay, hub88, etc.) | hub88 |
| 4 | c_sub_vendor_id | VARCHAR(50) | varchar | ID do sub-vendor/agregador (pgsoft, betsoft, etc.) | alea_pgsoft |
| 5 | c_product_id | VARCHAR(30) | varchar | Produto: 'CASINO' ou 'SPORTSBOOK' | CASINO |
| 6 | c_game_category_desc | VARCHAR(100) | varchar | Descricao da categoria. NOTA: c_game_category NAO existe nesta tabela | Slots |
| 7 | c_game_type_id | INTEGER | int | ID do tipo de jogo | 1 |
| 8 | c_game_type_desc | VARCHAR(100) | varchar | Descricao do tipo de jogo | Video Slots |
| 9 | c_status | VARCHAR(30) | varchar | Status: 'active' ou 'inactive' | active |
| 10 | c_has_jackpot | BOOLEAN | boolean/tinyint | Flag: jogo tem jackpot | false |
| 11 | c_free_spin_game | BOOLEAN | boolean/tinyint | Flag: jogo aceita free spins. NOTA: c_has_free_spins NAO existe | true |
| 12 | c_feature_trigger_game | BOOLEAN | boolean/tinyint | Flag: jogo tem feature trigger. NOTA: c_feature_trigger NAO existe | false |
| 13 | c_game_technology | VARCHAR(30) | varchar | Tecnologia: 'H5' (HTML5), 'F' (Flash). NOTA: c_technology NAO existe | H5 |
| 14 | c_updated_time | TIMESTAMP | timestamp | Data/hora de atualizacao em UTC. NOTA: c_updated_dt NAO existe | 2026-03-01 04:30:00 |

**Notas:**
- 6 nomes de colunas estavam errados na documentacao original (corrigidos na v2)
- Fortune Tiger (PG Soft): c_game_id = '4776', vendor = hub88, sub_vendor = alea_pgsoft
- vs1tigers = "Triple Tigers" (Pragmatic Play) — NAO e Fortune Tiger
- Sportsbook tem c_game_id = '0' sempre

---

### 3.10 bronze_sports_book_bets_info

**Fonte Athena:** `vendor_ec2.tbl_sports_book_bets_info`
**Descricao:** Header de apostas esportivas. Cada linha e uma aposta com valor total, odds, status e tipo (PreLive/Live/Mixed). Valores ja em BRL real (NAO centavos).
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~5-20M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_bet_id | VARCHAR(100) | varchar | ID unico da aposta | bet_123456 |
| 2 | c_bet_slip_id | VARCHAR(150) | varchar | ID do bet slip. Join com bronze_sports_book_bet_details | slip_789012 |
| 3 | c_customer_id | BIGINT | bigint | External ID do jogador (= c_external_id do ecr, = Smartico user_ext_id) | 987654321012345 |
| 4 | c_total_stake | DOUBLE PRECISION | double | Valor total apostado em BRL REAL (NAO centavos!) | 50.00 |
| 5 | c_total_return | DOUBLE PRECISION | double | Retorno total em BRL REAL | 125.50 |
| 6 | c_total_odds | VARCHAR(50) | varchar | Odds totais como VARCHAR. PRECISA TRY_CAST para numerico! | 2.51 |
| 7 | c_bonus_amount | DOUBLE PRECISION | double | Valor de bonus utilizado na aposta (BRL) | 0.00 |
| 8 | c_is_free | BOOLEAN | boolean | Flag: aposta gratis (freebet) | false |
| 9 | c_is_live | BOOLEAN | boolean | Flag: aposta feita ao vivo | true |
| 10 | c_bet_type | VARCHAR(20) | varchar | Tipo: 'PreLive', 'Live', 'Mixed' | PreLive |
| 11 | c_bet_state | VARCHAR(5) | varchar | Estado: 'O'=Open, 'C'=Closed | C |
| 12 | c_bet_slip_state | BOOLEAN | boolean | Estado do slip. ATENCAO: e BOOLEAN, nao 'C'/'O' | true |
| 13 | c_transaction_type | VARCHAR(5) | varchar | Tipo: 'M'=Commit, 'P'=Payout | M |
| 14 | c_transaction_id | VARCHAR(100) | varchar | ID da transacao financeira | txn_456789 |
| 15 | c_bet_closure_time | TIMESTAMP | timestamp | Data/hora de fechamento da aposta em UTC | 2026-03-15 20:45:00 |
| 16 | c_created_time | TIMESTAMP | timestamp | Data/hora de criacao da aposta em UTC | 2026-03-15 18:30:00 |
| 17 | dt | DATE | date (calculado) | Data truncada do fechamento: CAST(c_bet_closure_time AS DATE) | 2026-03-15 |

**Notas:**
- Valores em BRL REAL (excecao na camada Bronze — vendor_ec2 sports nao usa centavos)
- c_total_odds e VARCHAR! Precisa TRY_CAST(c_total_odds AS DOUBLE) para calculos
- c_bet_slip_state e BOOLEAN (nao 'C'/'O' como documentado originalmente)
- c_customer_id = external_id (nao ecr_id). Para join com fund, passar por ecr_ec2.tbl_ecr

---

### 3.11 bronze_sports_book_bet_details

**Fonte Athena:** `vendor_ec2.tbl_sports_book_bet_details`
**Descricao:** Detalhes/legs de apostas esportivas. Cada linha e uma selecao (leg) dentro de um bet slip. Contem o nome do esporte, evento, mercado, selecao e odds individuais. Necessario para analise por esporte/evento.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~10-40M linhas (multiplas legs por aposta)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_customer_id | BIGINT | bigint | External ID do jogador | 987654321012345 |
| 2 | c_bet_slip_id | VARCHAR(150) | varchar | FK para bronze_sports_book_bets_info.c_bet_slip_id | slip_789012 |
| 3 | c_transaction_id | VARCHAR(100) | varchar | ID da transacao | txn_456789 |
| 4 | c_bet_id | VARCHAR(100) | varchar | ID da aposta | bet_123456 |
| 5 | c_sport_type_name | VARCHAR(100) | varchar | Nome do esporte. Principal campo para analise por esporte | Futebol |
| 6 | c_sport_id | INTEGER | int | ID numerico do esporte | 1 |
| 7 | c_event_name | VARCHAR(500) | varchar | Nome do evento (ex: "Flamengo vs Palmeiras") | Flamengo vs Palmeiras |
| 8 | c_market_name | VARCHAR(200) | varchar | Nome do mercado (ex: "Resultado Final", "Ambas Marcam") | Resultado Final |
| 9 | c_selection_name | VARCHAR(200) | varchar | Selecao apostada (ex: "Flamengo", "Empate") | Flamengo |
| 10 | c_odds | DOUBLE PRECISION | double | Odds dessa leg especifica | 1.85 |
| 11 | c_leg_status | VARCHAR(5) | varchar | Status da leg: 'O'=Open, 'W'=Won, 'L'=Lost | W |
| 12 | c_tournament_name | VARCHAR(200) | varchar | Nome do torneio/liga | Brasileirao Serie A |
| 13 | c_is_live | BOOLEAN | boolean | Flag: leg feita ao vivo | false |
| 14 | c_created_time | TIMESTAMP | timestamp | Data/hora de criacao da leg em UTC | 2026-03-15 18:30:00 |
| 15 | c_leg_settlement_date | TIMESTAMP | timestamp | Data/hora de liquidacao da leg em UTC | 2026-03-15 21:00:00 |

**Notas:**
- c_sport_name NAO existe. O nome do esporte e c_sport_type_name
- Join com bets_info via c_bet_slip_id
- Uma aposta combinada (multipla) tem N linhas nesta tabela (uma por leg)

---

### 3.12 bronze_ecr_gaming_sessions

**Fonte Athena:** `bireports_ec2.tbl_ecr_gaming_sessions`
**Descricao:** Sessoes individuais de jogo. Cada linha e uma sessao com duracao em segundos e contagem de rodadas jogadas. Usado para calcular metricas de engajamento (sessoes/dia, tempo medio, rodadas/sessao).
**Frequencia de atualizacao:** Diaria (full refresh)
**Filtro de ingestao:** c_product_id = 'CASINO'
**Volume estimado:** ~10-30M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador | 123456789012345678 |
| 2 | c_game_id | VARCHAR(50) | varchar | ID do jogo nessa sessao | 4776 |
| 3 | c_session_start_time | TIMESTAMP | timestamp | Inicio da sessao em UTC | 2026-03-15 20:00:00 |
| 4 | c_session_end_time | TIMESTAMP | timestamp | Fim da sessao em UTC | 2026-03-15 20:45:30 |
| 5 | c_session_length_in_sec | INTEGER | int | Duracao da sessao em segundos. NOTA: c_session_duration_sec NAO existe | 2730 |
| 6 | c_game_played_count | INTEGER | int | Numero de rodadas jogadas na sessao. NOTA: c_round_count NAO existe | 150 |
| 7 | c_product_id | VARCHAR(30) | varchar | Produto. Filtrado para 'CASINO' na ingestao | CASINO |
| 8 | c_vendor_id | VARCHAR(50) | varchar | ID do vendor do jogo | hub88 |
| 9 | c_game_category | VARCHAR(50) | varchar | Categoria do jogo | slots |
| 10 | dt | DATE | date (calculado) | Data truncada: CAST(c_session_start_time AS DATE) | 2026-03-15 |

**Notas:**
- c_session_duration_sec NAO existe — usar c_session_length_in_sec
- c_round_count NAO existe — usar c_game_played_count
- Apenas sessoes CASINO sao ingeridas (filtro na query Bronze)

---

### 3.13 bronze_cashier_cashout

**Fonte Athena:** `cashier_ec2.tbl_cashier_cashout`
**Descricao:** Saques individuais do jogador. Complementar ao bronze_cashier_deposit. Usado para calcular Net Deposit (depositos - saques), velocidade de saque e churn risk.
**Frequencia de atualizacao:** Diaria (full refresh)
**Filtro de ingestao:** c_txn_status = 'co_success' AND c_test_user = false
**Volume estimado:** ~2-5M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (FK para bronze_ecr) | 123456789012345678 |
| 2 | c_txn_id | BIGINT | bigint | ID unico da transacao de saque (PK) | 44556677889900 |
| 3 | c_initial_amount | BIGINT | bigint | Valor do saque em CENTAVOS. Dividir por 100.0 para BRL | 10000 (= R$ 100,00) |
| 4 | c_created_time | TIMESTAMP | timestamp | Data/hora do saque em UTC | 2026-03-10 14:20:00 |
| 5 | c_txn_status | VARCHAR(50) | varchar | Status. Filtrado para 'co_success' (sucesso final) | co_success |
| 6 | dt | DATE | date (calculado) | Data truncada: CAST(c_created_time AS DATE) | 2026-03-10 |

**Notas:**
- Status de sucesso do cashier e 'co_success' (diferente de deposit que e 'txn_confirmed_success')
- Valor em CENTAVOS (mesma regra do deposito)

---

### 3.14 bronze_cashier_ecr_daily_payment_summary

**Fonte Athena:** `cashier_ec2.tbl_cashier_ecr_daily_payment_summary`
**Descricao:** Resumo diario de pagamentos por jogador. Consolida depositos e saques do dia com contagens, valores e detalhes de metodo/provedor. Util para analise de gateway, taxa de aprovacao e chargebacks.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~5-15M linhas (1 por jogador x dia x metodo)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador | 123456789012345678 |
| 2 | c_created_date | DATE | date | Data do resumo. NOTA: c_date NAO existe | 2026-03-15 |
| 3 | c_deposit_amount | BIGINT | bigint | Total depositado no dia em CENTAVOS. NOTA: c_deposit_amount_brl NAO existe | 15000 (= R$ 150,00) |
| 4 | c_deposit_amount_inhouse | BIGINT | bigint | Total depositado (moeda interna, centavos) | 15000 |
| 5 | c_deposit_count | INTEGER | int | Quantidade de depositos no dia | 2 |
| 6 | c_success_cashout_amount | BIGINT | bigint | Total sacado no dia em CENTAVOS. NOTA: c_withdrawal_amount_brl NAO existe | 5000 (= R$ 50,00) |
| 7 | c_success_cashout_amount_inhouse | BIGINT | bigint | Total sacado (moeda interna, centavos) | 5000 |
| 8 | c_success_cashout_count | INTEGER | int | Quantidade de saques no dia. NOTA: c_withdrawal_count NAO existe | 1 |
| 9 | c_cb_amount | BIGINT | bigint | Total de chargebacks no dia em CENTAVOS | 0 |
| 10 | c_cb_count | INTEGER | int | Quantidade de chargebacks no dia | 0 |
| 11 | c_option | VARCHAR(50) | varchar | Metodo de pagamento: 'PIXP2F', 'PIXPB', 'VISA', 'BANK_TRANSFER' | PIXP2F |
| 12 | c_provider | VARCHAR(100) | varchar | Nome do provedor/gateway de pagamento | pay4fun |

**Notas:**
- 6 de 8 nomes de colunas estavam errados na documentacao original (corrigidos na v2)
- Campos calculados (net_deposit, avg_ticket) NAO existem nesta tabela — calcular na Silver
- Valores em CENTAVOS

---

### 3.15 bronze_instrument

**Fonte Athena:** `cashier_ec2.tbl_instrument`
**Descricao:** Instrumentos de pagamento cadastrados pelo jogador (cartoes, PIX, contas bancarias). NAO contem metricas de transacao — e um cadastro de metodos. Inclui contadores de sucesso/falha por instrumento.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~1-3M linhas (multiplos instrumentos por jogador)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador | 123456789012345678 |
| 2 | c_instrument | VARCHAR(50) | varchar | Tipo do instrumento: PIX, Credit, Debit, BankTransfer | PIX |
| 3 | c_first_part | VARCHAR(50) | varchar | Primeiros digitos (mascarado para seguranca) | 1234** |
| 4 | c_last_part | VARCHAR(50) | varchar | Ultimos digitos (mascarado) | **5678 |
| 5 | c_status | VARCHAR(30) | varchar | Status do instrumento: active, blocked, expired | active |
| 6 | c_use_in_deposit | BOOLEAN | boolean | Instrumento habilitado para depositos | true |
| 7 | c_use_in_cashout | BOOLEAN | boolean | Instrumento habilitado para saques | true |
| 8 | c_last_deposit_date | TIMESTAMP | timestamp | Data do ultimo deposito com este instrumento | 2026-03-14 16:00:00 |
| 9 | c_deposit_success | INTEGER | int | Contagem de depositos com sucesso | 15 |
| 10 | c_deposit_attempted | INTEGER | int | Contagem de tentativas de deposito | 18 |
| 11 | c_payout_success | INTEGER | int | Contagem de saques com sucesso | 5 |
| 12 | c_payout_attempted | INTEGER | int | Contagem de tentativas de saque | 6 |
| 13 | c_chargeback | INTEGER | int | Contagem de chargebacks neste instrumento | 0 |

**Notas:**
- Esta tabela e de CADASTRO de instrumentos, nao de transacoes
- Taxa de aprovacao = c_deposit_success / c_deposit_attempted (calcular na Silver)
- KPIs como taxa de aprovacao devem ser calculados na Silver cruzando com tbl_cashier_deposit

---

### 3.16 bronze_ecr_bonus_details

**Fonte Athena:** `bonus_ec2.tbl_ecr_bonus_details`
**Descricao:** Bonus ativos atribuidos a jogadores. Contem detalhes por wallet (DRP/CRP/WRP/RRP), status, wagering, free spins e datas de emissao/expiracao. Principal fonte para calcular custo de bonus e eficacia de campanhas.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~2-5M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador | 123456789012345678 |
| 2 | c_bonus_id | INTEGER | int | ID do bonus global (cadastrado no backoffice) | 101 |
| 3 | c_ecr_bonus_id | BIGINT | bigint | ID do bonus atribuido ao jogador (PK) | 9988776655 |
| 4 | c_issue_type | VARCHAR(50) | varchar | Tipo de emissao. NOTA: c_bonus_type NAO existe | AUTOMATIC |
| 5 | c_criteria_type | VARCHAR(50) | varchar | Criterio de elegibilidade do bonus | DEPOSIT |
| 6 | c_bonus_status | VARCHAR(50) | varchar | Status: 'BONUS_OFFER' (ativo), 'EXPIRED', 'DROPPED', 'BONUS_ISSUED_OFFER' | BONUS_OFFER |
| 7 | c_is_freebet | BOOLEAN | boolean | Flag: bonus e freebet | false |
| 8 | c_drp_in_ecr_ccy | BIGINT | bigint | DRP (Deposit Restricted Points) em centavos | 2000 |
| 9 | c_crp_in_ecr_ccy | BIGINT | bigint | CRP (Casino Restricted Points) em centavos | 1000 |
| 10 | c_wrp_in_ecr_ccy | BIGINT | bigint | WRP (Wagering Restricted Points) em centavos | 500 |
| 11 | c_rrp_in_ecr_ccy | BIGINT | bigint | RRP (Restricted Reward Points) em centavos | 0 |
| 12 | c_wager_amount | BIGINT | bigint | Requisito de apostas (wagering) em centavos. NOTA: c_rollover_requirement NAO existe | 50000 |
| 13 | c_wager_amount_in_inhouse_ccy | BIGINT | bigint | Wagering em moeda interna (centavos) | 50000 |
| 14 | c_created_time | TIMESTAMP | timestamp | Data de criacao/atribuicao em UTC. NOTA: c_issued_date NAO existe | 2026-03-01 12:00:00 |
| 15 | c_bonus_expired_date | TIMESTAMP | timestamp | Data de expiracao do bonus em UTC. NOTA: c_expiry_date NAO existe | 2026-03-31 23:59:59 |
| 16 | c_claimed_date | TIMESTAMP | timestamp | Data em que o jogador reivindicou o bonus | 2026-03-01 12:05:00 |
| 17 | c_free_spin_used | INTEGER | int | Quantidade de free spins ja utilizados | 10 |
| 18 | c_vendor_id | VARCHAR(50) | varchar | Vendor do jogo associado ao bonus | hub88 |

**Notas:**
- 6 nomes de colunas estavam errados na documentacao original (corrigidos na v2)
- Para bonus inativos (historico), existe `tbl_ecr_bonus_details_inactive` (nao incluida na Bronze)
- NUNCA filtrar bonus so por template_id; usar duplo filtro entity_id + template_id (caso Multiverso/RETEM)
- BTR calculado a partir de bronze_bonus_summary_details (#23), nao desta tabela

---

### 3.17 bronze_ecr_ccf_score

**Fonte Athena:** `risk_ec2.tbl_ecr_ccf_score`
**Descricao:** Score de fraude CCF (Customer Confidence Factor) do jogador. Tabela muito limitada — apenas score numerico e bet_factor. Classificacao de risco (baixo/medio/alto) deve ser calculada na Silver.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~500K-1M linhas (1 por jogador com score)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (PK) | 123456789012345678 |
| 2 | c_ccf_score | DOUBLE PRECISION | double | Score CCF numerico. Maior = mais confiavel | 85.5 |
| 3 | c_bet_factor | DOUBLE PRECISION | double | Fator de aposta usado no calculo do score | 1.2 |
| 4 | c_ccf_timestamp | TIMESTAMP | timestamp | Data/hora do calculo do score em UTC. NOTA: c_calculated_date NAO existe | 2026-03-15 03:00:00 |
| 5 | c_created_time | TIMESTAMP | timestamp | Data/hora de criacao do registro | 2026-01-15 14:30:00 |
| 6 | c_updated_time | TIMESTAMP | timestamp | Data/hora de ultima atualizacao | 2026-03-15 03:00:00 |

**Notas:**
- Tabela MUITO limitada — 7 colunas total no Athena, 6 selecionadas
- c_risk_level NAO existe | c_fraud_indicators_json NAO existe | c_aml_flags_json NAO existe
- Classificacao de risco (baixo/medio/alto/critico) deve ser definida por faixas na Silver
- Para flags AML detalhadas, usar ecr_ec2.tbl_ecr_aml_flags (nao incluida na Bronze)

---

### 3.18 bronze_ecr_kyc_level

**Fonte Athena:** `ecr_ec2.tbl_ecr_kyc_level`
**Descricao:** Nivel KYC (Know Your Customer) do jogador. Indica o grau de verificacao de identidade. Usado para compliance regulatorio e segmentacao de risco.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~500K-1M linhas (1 por jogador)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (PK) | 123456789012345678 |
| 2 | c_level | VARCHAR(20) | varchar | Nivel KYC: 'KYC_0', 'KYC_1', 'KYC_2'. NOTA: c_kyc_level NAO existe | KYC_1 |
| 3 | c_desc | VARCHAR(200) | varchar | Descricao do nivel KYC | Verificacao basica |
| 4 | c_grace_action_status | VARCHAR(50) | varchar | Status da acao de graca (pendente/completo). NOTA: c_verification_status NAO existe | COMPLETED |
| 5 | c_kyc_limit_nearly_reached | BOOLEAN | boolean | Flag: jogador proximo do limite KYC | false |
| 6 | c_kyc_reminder_count | INTEGER | int | Quantidade de lembretes KYC enviados | 2 |
| 7 | c_updated_time | TIMESTAMP | timestamp | Data/hora de ultima atualizacao em UTC. NOTA: c_updated_date NAO existe | 2026-03-10 09:00:00 |

**Notas:**
- 5 de 6 nomes de colunas estavam errados na documentacao original (corrigidos na v2)
- KYC_0 = sem verificacao | KYC_1 = verificacao basica | KYC_2 = verificacao completa

---

### 3.19 bronze_vendor_games_mapping_data

**Fonte Athena:** `bireports_ec2.tbl_vendor_games_mapping_data`
**Descricao:** Catalogo de jogos na visao BI (view/copia da tabela master #9). Contem menos colunas que a mst mas e atualizada pelo cron job diario do bireports. Util como fallback ou para imagens de jogos.
**Frequencia de atualizacao:** Diaria (cron job 4:30 AM)
**Volume estimado:** ~5K-10K linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_game_id | VARCHAR(50) | varchar | ID unico do jogo (PK) | 4776 |
| 2 | c_game_desc | VARCHAR(200) | varchar | Nome/descricao do jogo. NOTA: c_game_name NAO existe | Fortune Tiger |
| 3 | c_vendor_id | VARCHAR(50) | varchar | ID do vendor | hub88 |
| 4 | c_game_category_desc | VARCHAR(100) | varchar | Descricao da categoria do jogo | Slots |
| 5 | c_product_id | VARCHAR(30) | varchar | Produto: 'CASINO' ou 'SPORTSBOOK' | CASINO |
| 6 | c_status | VARCHAR(30) | varchar | Status: 'active' ou 'inactive' | active |

---

### 3.20 bronze_ps_bi_dim_game

**Fonte Athena:** `ps_bi.dim_game`
**Descricao:** Dimensao de jogos da camada dbt (Gold). Incluida na Bronze como lookup pois tem campos enriquecidos nao disponiveis nas tabelas _ec2. Sem prefixo `c_` nas colunas (padrao dbt).
**Frequencia de atualizacao:** Diaria (full refresh)
**Justificativa de inclusao:** Dimensao pura (lookup), nao calculo
**Volume estimado:** ~5K-10K linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | game_id | VARCHAR(50) | varchar | ID unico do jogo (PK). Join com c_game_id das tabelas _ec2 | 4776 |
| 2 | game_desc | VARCHAR(200) | varchar | Nome/descricao do jogo | Fortune Tiger |
| 3 | vendor_id | VARCHAR(50) | varchar | ID do vendor | hub88 |
| 4 | product_id | VARCHAR(30) | varchar | Produto: 'CASINO' ou 'SPORTSBOOK' | CASINO |
| 5 | game_category | VARCHAR(50) | varchar | Codigo da categoria | slots |
| 6 | game_category_desc | VARCHAR(100) | varchar | Descricao da categoria | Slots |
| 7 | game_type_id | INTEGER | int | ID do tipo de jogo | 1 |
| 8 | game_type_desc | VARCHAR(100) | varchar | Descricao do tipo | Video Slots |
| 9 | status | VARCHAR(30) | varchar | Status: 'active' ou 'inactive' | active |
| 10 | updated_time | TIMESTAMP | timestamp | Data/hora de atualizacao (UTC) | 2026-03-01 04:30:00 |

**Notas:**
- Colunas SEM prefixo `c_` (padrao dbt, diferente das tabelas _ec2)
- Equivalente enriquecido da tbl_vendor_games_mapping_mst

---

### 3.21 bronze_ps_bi_dim_user

**Fonte Athena:** `ps_bi.dim_user`
**Descricao:** Dimensao de jogadores da camada dbt (Gold). Incluida na Bronze pois e a UNICA fonte de country_code. Contem external_id para join direto com Smartico sem passar pelo ecr_ec2.
**Frequencia de atualizacao:** Diaria (full refresh)
**Justificativa de inclusao:** Unica fonte de country_code. Dimensao (lookup), nao calculo
**Volume estimado:** ~500K-1M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | ecr_id | BIGINT | bigint | ID interno do jogador (PK). = c_ecr_id nas tabelas _ec2 | 123456789012345678 |
| 2 | external_id | VARCHAR(50) | varchar | ID externo = Smartico user_ext_id (join direto com CRM) | 987654321012345 |
| 3 | registration_date | DATE | date | Data de registro (truncamento UTC — impacto <2.1% vs BRT) | 2026-01-15 |
| 4 | country_code | VARCHAR(10) | varchar | Codigo do pais (ISO). UNICA fonte no ecossistema | BR |
| 5 | last_deposit_date | DATE | date | Data do ultimo deposito | 2026-03-14 |
| 6 | last_deposit_amount_inhouse | DOUBLE PRECISION | double | Valor do ultimo deposito em BRL (ja convertido, nao centavos) | 100.00 |

**Notas:**
- Colunas SEM prefixo `c_` (padrao dbt)
- external_id = Smartico user_ext_id (join direto, sem precisar passar pelo ecr_ec2)
- ecr_id = c_ecr_id das tabelas _ec2
- is_test existe na tabela original ps_bi.dim_user (filtro obrigatorio!) mas nao incluida na Bronze pois filtro e feito via tbl_ecr_flags
- Campos DATE sao truncamento UTC — usar datetime + AT TIME ZONE para precisao BRT

---

### 3.22 bronze_ecr_signup_info

**Fonte Athena:** `ecr_ec2.tbl_ecr_signup_info`
**Descricao:** Informacoes de device e canal no momento do cadastro. Unica fonte para Device Distribution (Mobile/Desktop/Tablet), sistema operacional, browser e dominio de origem.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~500K-1M linhas (1 por jogador)

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador (PK) | 123456789012345678 |
| 2 | c_channel | VARCHAR(30) | varchar | Canal de cadastro: Mobile, Desktop, Tablet | Mobile |
| 3 | c_sub_channel | VARCHAR(50) | varchar | Detalhe do canal (app, browser, etc.) | HTML5 |
| 4 | c_os_browser_type | VARCHAR(100) | varchar | Sistema operacional e browser | Android Chrome |
| 5 | c_device_id | VARCHAR(200) | varchar | ID unico do dispositivo | device_abc123 |
| 6 | c_hostname | VARCHAR(200) | varchar | Dominio/hostname de origem do cadastro | www.multibet.com |
| 7 | c_created_time | TIMESTAMP | timestamp | Data/hora do cadastro em UTC | 2026-01-15 14:30:00 |

---

### 3.23 bronze_bonus_summary_details

**Fonte Athena:** `bonus_ec2.tbl_bonus_summary_details`
**Descricao:** Resumo financeiro de bonus emitidos. Contem c_actual_issued_amount (valor real convertido quando wagering e cumprido) — campo ESSENCIAL para calcular BTR (Bonus Total Redeemed). Detalha por wallet (DRP/CRP/WRP/RRP) valores emitidos e oferecidos.
**Frequencia de atualizacao:** Diaria (full refresh)
**Volume estimado:** ~2-5M linhas

| # | Coluna | Tipo PG | Tipo Athena | Descricao | Exemplo |
|---|--------|---------|-------------|-----------|---------|
| 1 | c_ecr_id | BIGINT | bigint | ID interno do jogador | 123456789012345678 |
| 2 | c_bonus_id | INTEGER | int | ID do bonus global | 101 |
| 3 | c_ecr_bonus_id | BIGINT | bigint | ID do bonus atribuido ao jogador (FK para bronze_ecr_bonus_details) | 9988776655 |
| 4 | c_actual_issued_amount | BIGINT | bigint | Valor REAL emitido quando wagering cumprido. CENTAVOS. Campo-chave para BTR | 5000 (= R$ 50,00) |
| 5 | c_issued_drp | BIGINT | bigint | DRP emitido (centavos) | 2000 |
| 6 | c_issued_crp | BIGINT | bigint | CRP emitido (centavos) | 1000 |
| 7 | c_issued_wrp | BIGINT | bigint | WRP emitido (centavos) | 500 |
| 8 | c_issued_rrp | BIGINT | bigint | RRP emitido (centavos) | 0 |
| 9 | c_offered_crp | BIGINT | bigint | CRP oferecido/prometido (centavos) | 1500 |
| 10 | c_offered_rrp | BIGINT | bigint | RRP oferecido (centavos) | 0 |
| 11 | c_offered_drp | BIGINT | bigint | DRP oferecido (centavos) | 3000 |

**Notas:**
- BTR (Bonus Total Redeemed) = SUM(c_actual_issued_amount) / 100.0
- Diferenca entre offered e issued = bonus nao convertido (wagering nao cumprido)

---

## 4. Relacionamentos

### 4.1 Diagrama de Relacionamentos (textual)

```
                         ┌──────────────────────┐
                         │  bronze_ecr_flags     │
                         │  PK: c_ecr_id         │
                         │  c_test_user (filtro)  │
                         └──────────┬───────────┘
                                    │ 1:1
                                    │
┌──────────────────┐     ┌──────────▼───────────┐     ┌──────────────────────┐
│ bronze_ecr_banner │────▶│    bronze_ecr         │◀────│ bronze_ecr_signup_info│
│ FK: c_ecr_id      │     │  PK: c_ecr_id         │     │ FK: c_ecr_id          │
│ marketing/trafego │     │  c_external_id         │     │ device/canal          │
└──────────────────┘     │  (tabela mestre)       │     └──────────────────────┘
                         └──┬───┬───┬───┬───┬───┘
                            │   │   │   │   │
           ┌────────────────┘   │   │   │   └────────────────────┐
           │                    │   │   │                        │
           ▼                    ▼   │   ▼                        ▼
┌──────────────────┐ ┌─────────────┐│┌──────────────────┐ ┌──────────────────┐
│bronze_cashier_   │ │bronze_real_ │││bronze_ecr_bonus_ │ │bronze_ecr_       │
│deposit           │ │fund_txn     │││details           │ │kyc_level         │
│FK: c_ecr_id      │ │FK: c_ecr_id │││FK: c_ecr_id      │ │FK: c_ecr_id      │
│depositos         │ │transacoes   │││bonus ativos      │ │KYC               │
└──────────────────┘ └──┬───┬─────┘│└──────────────────┘ └──────────────────┘
                        │   │      │
           ┌────────────┘   │      │
           │                │      │
           ▼                ▼      ▼
┌──────────────────┐ ┌─────────────────┐ ┌──────────────────────┐
│bronze_realcash_  │ │bronze_bonus_    │ │bronze_cashier_cashout │
│sub_fund_txn      │ │sub_fund_txn     │ │FK: c_ecr_id           │
│FK: c_fund_txn_id │ │FK: c_fund_txn_id│ │saques                 │
│real cash detail  │ │bonus detail     │ └──────────────────────┘
└──────────────────┘ └─────────────────┘

┌──────────────────────────┐     ┌────────────────────────────────┐
│bronze_real_fund_txn_     │     │bronze_vendor_games_mapping_mst │
│type_mst                  │     │PK: c_game_id                   │
│PK: c_txn_type            │     │catalogo de jogos               │
│lookup tipo transacao     │     └────────────────────────────────┘
└──────────────────────────┘                ▲
    ▲ lookup                                │ lookup
    │                                       │
    │ c_txn_type                    c_game_id
    │                                       │
┌───┴──────────────────────────────────────┴──┐
│              bronze_real_fund_txn            │
│  c_txn_type → bronze_real_fund_txn_type_mst │
│  c_game_id  → bronze_vendor_games_mapping_mst│
└─────────────────────────────────────────────┘
```

### 4.2 Joins Principais

| Tabela Origem | Coluna | Tabela Destino | Coluna | Tipo | Descricao |
|---------------|--------|----------------|--------|------|-----------|
| bronze_ecr | c_ecr_id | (TODAS as tabelas) | c_ecr_id | 1:N | Chave universal do jogador |
| bronze_ecr | c_external_id | BigQuery Smartico | user_ext_id | 1:1 | Join com CRM |
| bronze_real_fund_txn | c_txn_id | bronze_realcash_sub_fund_txn | c_fund_txn_id | 1:1 | Sub-fund real cash |
| bronze_real_fund_txn | c_txn_id | bronze_bonus_sub_fund_txn | c_fund_txn_id | 1:1 | Sub-fund bonus |
| bronze_real_fund_txn | c_txn_type | bronze_real_fund_txn_type_mst | c_txn_type | N:1 | Tipo de transacao (lookup) |
| bronze_real_fund_txn | c_game_id | bronze_vendor_games_mapping_mst | c_game_id | N:1 | Jogo (lookup) |
| bronze_sports_book_bets_info | c_bet_slip_id | bronze_sports_book_bet_details | c_bet_slip_id | 1:N | Header -> Legs |
| bronze_sports_book_bets_info | c_customer_id | bronze_ecr | c_external_id | N:1 | Sports usa external_id |
| bronze_ecr_bonus_details | c_ecr_bonus_id | bronze_bonus_summary_details | c_ecr_bonus_id | 1:1 | Bonus ativo -> resumo financeiro |
| bronze_ps_bi_dim_user | ecr_id | bronze_ecr | c_ecr_id | 1:1 | Dimensao dbt -> cadastro |
| bronze_ps_bi_dim_game | game_id | bronze_vendor_games_mapping_mst | c_game_id | 1:1 | Dimensao dbt -> catalogo |
| bronze_ecr | c_ecr_id | bronze_ecr_flags | c_ecr_id | 1:1 | Flags (filtro test user) |

### 4.3 Cuidados nos Joins

1. **Sports usa external_id, nao ecr_id:** `bronze_sports_book_bets_info.c_customer_id` = `bronze_ecr.c_external_id`
2. **Sub-funds via txn_id:** `c_fund_txn_id` (sub-fund) = `c_txn_id` (fund principal)
3. **dim_user e dim_game sem prefixo c_:** colunas do ps_bi nao tem prefixo `c_`
4. **Filtro de test user sempre via JOIN:** `JOIN bronze_ecr_flags f ON x.c_ecr_id = f.c_ecr_id WHERE f.c_test_user = false`

---

## 5. Notas Tecnicas

### 5.1 Valores Monetarios

| Tabela | Campo de Valor | Unidade | Conversao BRL |
|--------|---------------|---------|---------------|
| bronze_cashier_deposit | c_initial_amount | Centavos | / 100.0 |
| bronze_cashier_cashout | c_initial_amount | Centavos | / 100.0 |
| bronze_real_fund_txn | c_amount_in_ecr_ccy | Centavos | / 100.0 |
| bronze_realcash_sub_fund_txn | c_amount_in_house_ccy | Centavos | / 100.0 |
| bronze_bonus_sub_fund_txn | c_drp/crp/wrp/rrp_amount_in_house_ccy | Centavos | / 100.0 |
| bronze_cashier_ecr_daily_payment_summary | c_deposit_amount, c_success_cashout_amount | Centavos | / 100.0 |
| bronze_ecr_bonus_details | c_drp/crp/wrp/rrp_in_ecr_ccy, c_wager_amount | Centavos | / 100.0 |
| bronze_bonus_summary_details | c_actual_issued_amount, c_issued_*, c_offered_* | Centavos | / 100.0 |
| bronze_sports_book_bets_info | c_total_stake, c_total_return, c_bonus_amount | **BRL Real** | Direto |
| bronze_ps_bi_dim_user | last_deposit_amount_inhouse | **BRL Real** | Direto |

**REGRA:** Na camada Bronze, valores ficam como vieram da fonte. A conversao para BRL e feita na Silver.

### 5.2 Timestamps e Fuso Horario

- **Todos os timestamps na Bronze estao em UTC**
- Conversao para BRT na Silver: `AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'`
- Campos DATE (registration_date, ftd_date, dt) sao truncamento UTC — impacto <2.1% vs BRT preciso
- Para precisao maxima em periodos especificos, usar campo timestamp original + AT TIME ZONE

### 5.3 Test Users

- Filtro obrigatorio em todas as queries: `c_test_user = false`
- c_test_user e **BOOLEAN** (nao integer). Comparar com `= false`, nao `= 0`
- Sem filtro, divergencia de ~3% nos numeros
- Filtro aplicado na ingestao (tabelas 1, 2, 5, 12, 13) via JOIN com bronze_ecr_flags

### 5.4 Sub-Fund Isolation

Para calcular GGR/NGR corretamente, separar real cash de bonus:

```
GGR Real Cash = SUM(real_cash_bet) - SUM(real_cash_win)
  usando: bronze_realcash_sub_fund_txn

GGR Bonus = SUM(bonus_bet) - SUM(bonus_win)
  usando: bronze_bonus_sub_fund_txn (CRP + WRP + RRP)

GGR Total = GGR Real Cash + GGR Bonus
  ATENCAO: fund_ec2 tipos 27-45 inclui ambos. ps_bi.casino_ggr so realcash.
  Divergencia de ate R$ 3.3M entre as duas abordagens.
```

### 5.5 Status por Tabela

| Tabela | Campo | Valor Sucesso | Observacao |
|--------|-------|---------------|------------|
| bronze_cashier_deposit | c_txn_status | 'txn_confirmed_success' | Padrao cashier |
| bronze_cashier_cashout | c_txn_status | 'co_success' | Padrao cashier cashout |
| bronze_real_fund_txn | c_txn_status | 'SUCCESS' | Padrao fund |

### 5.6 Nomes de Colunas — Correcoes Validadas

A documentacao original da Pragmatic Solutions e/ou IAs anteriores continham nomes de colunas incorretos. Abaixo as correcoes validadas empiricamente (SHOW COLUMNS + SELECT LIMIT 5 no Athena, 20/03/2026):

| Tabela | Coluna ERRADA (doc original) | Coluna CORRETA (validada) |
|--------|------------------------------|---------------------------|
| bronze_ecr | c_country_code | NAO EXISTE (usar ps_bi.dim_user.country_code) |
| bronze_real_fund_txn | c_confirmed_amount_in_inhouse_ccy | NAO EXISTE (usar c_amount_in_ecr_ccy) |
| bronze_real_fund_txn | c_vendor_id | NAO EXISTE (usar c_sub_vendor_id) |
| bronze_real_fund_txn | c_tracker_id | NAO EXISTE (tracker em ecr_ec2.tbl_ecr) |
| bronze_real_fund_txn | c_round_id | NAO EXISTE |
| bronze_real_fund_txn | dt (como particao) | NAO EXISTE como coluna visivel |
| bronze_ecr_gaming_sessions | c_session_duration_sec | c_session_length_in_sec |
| bronze_ecr_gaming_sessions | c_round_count | c_game_played_count |
| bronze_vendor_games_mapping_mst | c_game_name | c_game_desc |
| bronze_vendor_games_mapping_mst | c_game_category | c_game_category_desc |
| bronze_vendor_games_mapping_mst | c_has_free_spins | c_free_spin_game |
| bronze_vendor_games_mapping_mst | c_feature_trigger | c_feature_trigger_game |
| bronze_vendor_games_mapping_mst | c_technology | c_game_technology |
| bronze_vendor_games_mapping_mst | c_updated_dt | c_updated_time |
| bronze_vendor_games_mapping_data | c_game_name | c_game_desc |
| bronze_real_fund_txn_type_mst | c_txn_type_name | c_internal_description |
| bronze_cashier_ecr_daily_payment_summary | c_date | c_created_date |
| bronze_cashier_ecr_daily_payment_summary | c_deposit_amount_brl | c_deposit_amount |
| bronze_cashier_ecr_daily_payment_summary | c_withdrawal_amount_brl | c_success_cashout_amount |
| bronze_cashier_ecr_daily_payment_summary | c_withdrawal_count | c_success_cashout_count |
| bronze_ecr_bonus_details | c_bonus_type | c_issue_type |
| bronze_ecr_bonus_details | c_rollover_requirement | c_wager_amount |
| bronze_ecr_bonus_details | c_issued_date | c_created_time |
| bronze_ecr_bonus_details | c_expiry_date | c_bonus_expired_date |
| bronze_ecr_flags | c_flag_name / c_flag_value | NAO EXISTEM (flags sao colunas individuais) |
| bronze_ecr_ccf_score | c_risk_level | NAO EXISTE |
| bronze_ecr_ccf_score | c_calculated_date | c_ccf_timestamp |
| bronze_ecr_kyc_level | c_kyc_level | c_level |
| bronze_ecr_kyc_level | c_verification_status | c_grace_action_status |
| bronze_ecr_kyc_level | c_updated_date | c_updated_time |
| bronze_ecr_banner | c_click_id | NAO EXISTE (usar c_reference_url) |
| bronze_ecr_banner | c_utm_source/medium/campaign | NAO EXISTEM (usar c_custom1..c_custom4) |
| bronze_sports_book_bets_info | c_sport_name | NAO EXISTE (usar bet_details.c_sport_type_name) |

### 5.7 Tabelas de Validacao Cruzada (NAO Bronze)

As seguintes tabelas Gold/pre-agregadas NAO sao replicadas na Bronze, mas devem ser usadas para validar os calculos da Silver:

| Tabela Gold | Uso de Validacao |
|-------------|-----------------|
| ps_bi.fct_casino_activity_daily | Validar GGR casino calculado na Silver vs dbt |
| ps_bi.fct_player_activity_daily | Validar DAU/depositos vs fund_ec2 |
| ps_bi.fct_bonus_activity_daily | Validar custo bonus vs bonus_ec2 |
| bireports_ec2.tbl_ecr_wise_daily_bi_summary | Validar metricas diarias consolidadas |
| bireports_ec2.tbl_ecr | Validar dados do jogador vs ecr_ec2.tbl_ecr |

---

## 6. Apendice

### 6.1 Mapeamento Completo de c_txn_type

Ver secao 3.8 para tipos principais. Para mapeamento completo (141+ tipos), consultar:
- `memory/schema_fund.md` — documentacao detalhada por categoria
- `fund_ec2.tbl_real_fund_txn_type_mst` — tabela master no Athena

### 6.2 Glossario Rapido

| Termo | Significado |
|-------|------------|
| GGR | Gross Gaming Revenue = Apostas - Ganhos do jogador |
| NGR | Net Gaming Revenue = GGR - Custos de bonus |
| BTR | Bonus Total Redeemed = Valor de bonus convertido em real cash |
| FTD | First Time Deposit = Primeiro deposito do jogador |
| NRC | New Registered with Cash = Jogadores que registraram e depositaram |
| NDC | New Depositing Customer = Novo cliente com deposito (periodo) |
| DAU/MAU | Daily/Monthly Active Users |
| DRP | Deposit Restricted Points (real cash restrito) |
| CRP | Casino Restricted Points (bonus casino) |
| WRP | Wagering Restricted Points (bonus wagering) |
| RRP | Restricted Reward Points (bonus reward) |
| KYC | Know Your Customer (verificacao de identidade) |
| CCF | Customer Confidence Factor (score de fraude) |
| AML | Anti-Money Laundering (anti-lavagem de dinheiro) |

### 6.3 Fontes Documentais

| Documento | Versao | Conteudo |
|-----------|--------|----------|
| Fund Database Schema Document | v1.3 | Tabelas fund_ec2 (carteira, transacoes) |
| ECR Database Schema Document | v1.2 | Tabelas ecr_ec2 (cadastro, KYC) |
| Cashier Database Schema Document | v1.0 | Tabelas cashier_ec2 (pagamentos) |
| Bonus Database Schema Document | v1.0 | Tabelas bonus_ec2 (bonus) |
| BI Reports Database Schema Document | v1.2 | Tabelas bireports_ec2 (agregados BI) |
| Vendor Database Schema Document | v1.0 | Tabelas vendor_ec2 (jogos, sports) |
| CSM Database Schema Document | v1.0 | Tabelas csm_ec2 (risco, alertas) |
| bronze_selects_kpis_CORRIGIDO_v2.md | v2.1 | SELECTs validados empiricamente (Mateus, 20/03/2026) |

---

**Fim do documento — Schema Bronze MultiBet v1.0**
