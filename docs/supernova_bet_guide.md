# Guia Super Nova Bet DB (Play4Tune) — v1.0

> **Data:** 09/04/2026 | **Autor:** Mateus Fabro (via investigacao empirica)
> **Banco:** PostgreSQL 15.14 | **Tamanho:** 101 MB | **Moeda:** PKR (Rupia Paquistanesa)

---

## 1. Visao Geral

O Super Nova Bet DB e o banco **transacional** da operacao Play4Tune (Paquistao).
Diferente das foreign tables do schema `play4` no Super Nova DB (que sao views agregadas),
este banco contem **dados granulares** por jogador, por jogo e por transacao.

### Comparacao com o acesso anterior (foreign tables play4)

| Aspecto | Foreign Tables (play4) | Acesso Direto (supernova_bet) |
|---------|----------------------|-------------------------------|
| Granularidade | Dia/hora agregado | Transacao individual |
| Dados por jogador | NAO | SIM (users, bets, transactions) |
| Dados por jogo | NAO | SIM (casino_games, casino_game_metrics) |
| Catalogo jogos | NAO | SIM (136 jogos, providers, categorias) |
| Bonus detalhado | NAO | SIM (activations, coupons, programs) |
| Marketing/UTM | NAO | SIM (user_marketing_events, sessions) |
| Wallets/saldos | NAO | SIM (wallets, real + bonus) |
| Webhooks | NAO | SIM (deliveries, endpoints) |
| Views financeiras | 9 views | 15+ views (inclui as mesmas + novas) |

### Schemas

| Schema | Objetos | Descricao |
|--------|---------|-----------|
| `public` | 68 | Tabelas transacionais + views financeiras |
| `platform` | 3 | Multi-tenant: tenants, users admin, access_log |

---

## 2. Conexao

### Via Python (recomendado)
```python
from db.supernova_bet import execute_supernova_bet

# Leitura
rows = execute_supernova_bet("SELECT * FROM users LIMIT 5", fetch=True)

# O modulo usa SSH tunnel via bastion automaticamente
# Credenciais no .env (SUPERNOVA_BET_*)
```

### Env vars necessarias (.env)
```
BASTION_HOST=34.238.84.114          # IP pode mudar (sem Elastic IP)
SUPERNOVA_BET_HOST=supernova-bet-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com
SUPERNOVA_BET_DB=supernova_bet
SUPERNOVA_BET_USER=supernova_bet_admin
SUPERNOVA_BET_PASS=<ver .env>
```

### Via DBeaver
- **SSH:** bastion `34.238.84.114:22`, user `ec2-user`, key `bastion-analytics-key.pem`
- **DB:** host `supernova-bet-db...`, port 5432, db `supernova_bet`, SSL require
- Ver instrucoes completas em `memory/schema_supernova_bet.md`

---

## 3. Tabelas Transacionais (dados granulares)

### 3.1 users (1.451 registros — 09/04)
Cadastro de jogadores. **Chave primaria:** `id` (UUID).

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| id | uuid PK | ID unico do jogador |
| username | varchar(50) | Username (obrigatorio, unico) |
| public_id | varchar(9) | ID publico curto (ex: "ZIiCKQDKi") |
| name | varchar(255) | Nome completo (pode ser NULL) |
| email | varchar(255) | Email (pode ser NULL) |
| phone | varchar(20) | Telefone com DDI (ex: +923075941703) |
| role | varchar(100) | Role: USER (padrao) |
| active | boolean | Conta ativa |
| blocked | boolean | Conta bloqueada |
| is_affiliate | boolean | E afiliado? |
| affiliate_code | varchar(50) | Codigo do afiliado |
| affiliate_id | uuid | FK para afiliado responsavel |
| referred_by | uuid | FK para quem indicou |
| created_at | timestamp | Data cadastro (UTC, sem timezone!) |
| updated_at | timestamp | Ultima atualizacao |

**ATENCAO:** `created_at` e `timestamp without time zone` — confirmar se UTC.

### 3.2 transactions (~112.399 registros)
Todas as movimentacoes financeiras. **Tabela mais importante para analytics.**

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| id | uuid PK | ID da transacao |
| user_id | uuid FK | Jogador |
| type | varchar | Tipo: DEPOSIT, WITHDRAWAL, BONUS_CREDIT, MANUAL_CREDIT, etc. |
| status | varchar | Status: COMPLETED, FAILED, PENDING, CANCELLED, REVERSED |
| amount | numeric(36,18) | Valor em PKR (ja em unidades, NAO centavos) |
| real_amount | numeric(36,18) | Valor real processado |
| balance_before | numeric(36,18) | Saldo antes |
| balance_after | numeric(36,18) | Saldo depois |
| locked_amount | numeric(36,18) | Valor bloqueado |
| fee_amount | numeric(36,18) | Taxa cobrada |
| currency_id | uuid FK | Moeda (currencies) |
| wallet_id | uuid FK | Carteira usada |
| payment_method_id | uuid FK | Metodo de pagamento |
| external_id | varchar | ID externo (gateway) |
| gateway_order_no | varchar | Numero do pedido no gateway |
| ip_address | varchar | IP do jogador |
| flagged_for_review | boolean | Marcado para revisao |
| gateway_flagged | boolean | Gateway sinalizou |
| turnover_met | boolean | Rollover atingido |
| processed_at | timestamp | Quando processou |
| created_at | timestamp | Quando criou |
| metadata | jsonb | Dados extras (URL gateway, sessionId, etc.) |

**Valores:** PKR direto (nao centavos). `amount` com 18 casas decimais.

### 3.3 bets (83.986 registros)
Apostas individuais no casino.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| id | uuid PK | ID da aposta |
| user_id | uuid FK | Jogador |
| game_id | uuid FK | Jogo (casino_games) |
| provider_id | varchar | Provider (ex: "2j") |
| session_id | varchar | Sessao de jogo |
| round_id | varchar | Rodada |
| amount | numeric(18,4) | Valor apostado (PKR) |
| bonus_amount | numeric(18,4) | Valor bonus usado |
| win_amount | numeric(18,4) | Valor ganho |
| category | varchar | WAGER ou WIN |
| status | varchar | PLACED ou SETTLED |
| pre_cash_balance | numeric(18,4) | Saldo real antes |
| pre_bonus_balance | numeric(18,4) | Saldo bonus antes |
| post_cash_balance | numeric(18,4) | Saldo real depois |
| post_bonus_balance | numeric(18,4) | Saldo bonus depois |
| created_at | timestamp | Sem timezone |

**GGR por jogo:** `SUM(amount) - SUM(win_amount)` agrupado por game_id.

### 3.4 casino_games (136 jogos)
Catalogo completo de jogos.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| id | uuid PK | ID do jogo |
| external_id | varchar | ID externo no provider (bate com "Game ID" da planilha 2J) |
| provider_id | uuid FK | Provider (casino_providers) |
| name | varchar | Nome do jogo (ex: "FISHDOM", "CRASH") |
| slug | varchar | Slug URL |
| image_url | varchar | URL imagem horizontal |
| vertical_image_url | varchar | URL imagem vertical |
| **rtp** | numeric(8,2) | **RTP OBSERVADO ACUMULADO** (calculado internamente pelo backend, NAO o RTP contratual do provider). Ver nota abaixo. |
| pot | numeric(8,2) | Pot/house edge — espelho de (100 - rtp), mesma logica calculada |
| min_bet / max_bet | numeric(18,4) | Limites (NULL = sem limite) |
| active | boolean | Jogo ativo |
| demo | boolean | Modo demo disponivel |

**Provider unico ativo:** 2J Games (slug: "2j") — atua como aggregator (tem slots proprios + agrega jogos Pragmatic Play, Crash games, etc.).

> **ATENCAO — `casino_games.rtp` e CALCULADO INTERNAMENTE pelo backend (confirmado em 24/04/2026):**
>
> O campo e um snapshot observado acumulado gerado pelo backend da plataforma, NAO o RTP teorico/contratual do provider. Por isso aparecem valores absurdos em jogos de amostra baixa (THREE DICE banco 200%, BACCARAT 180%, CALL BREAK 0%, POINT RUMMY 1.01%) e matches exatos com o RTP observado em jogos de boa amostra (FRUIT MARY banco 86.0% = obs 86.0%, HALLOWEEN 86.1%/86.1%, WILD BANDITO 97.3%/97.3%). **NAO usar como fonte de RTP contratual.**
>
> Para RTP teorico (contratual) use a planilha oficial 2J:
> - XLSX: `C:\Users\NITRO\Downloads\API Games List.xlsx` (5 abas, 123 jogos)
> - CSV parseado: `reports/rtp_oficial_2j_games.csv`
> - Script parser: `scripts/build_rtp_oficial_2j_csv.py`
> - Detalhe: `memory/project_rtp_oficial_2j_sheet.md`
>
> Para RTP real observado no periodo desejado, calcular on-the-fly via `casino_user_game_metrics` (`SUM(win)/SUM(bet)`).

### 3.5 casino_game_metrics (632 registros, particionado por mes)
Metricas diarias por jogo (agregado).

| Coluna | Descricao |
|--------|-----------|
| game_id | FK jogo |
| date | Data |
| game_clicks, created_sessions, played_sessions, played_rounds | Engajamento |
| total_bet_amount, total_win_amount, net_revenue | Financeiro (PKR) |

Tabelas particionadas: `casino_game_metrics_2026_03`, `_2026_04`, `_2026_05`, `_default`.

### 3.6 casino_user_game_metrics (1.224 registros, particionado por mes)
Metricas diarias **por usuario por jogo** — granularidade maxima.

Mesmas colunas de `casino_game_metrics` + `user_id`.

### 3.7 wallets (2.900 registros)
Carteiras dos jogadores (REAL e BONUS separadas).

| Coluna | Descricao |
|--------|-----------|
| id | UUID PK |
| user_id | FK jogador |
| type | REAL ou BONUS |
| balance | Saldo atual (PKR) |
| locked_balance | Saldo bloqueado |
| blocked / blocked_reason | Carteira bloqueada |

**Cada jogador tem 2 wallets:** REAL + BONUS (por isso 2.900 para ~1.450 users).

### 3.8 bonus_activations (77 registros)
Ativacoes de bonus com rollover tracking.

| Coluna | Descricao |
|--------|-----------|
| user_id, program_id | FK jogador e programa |
| deposit_amount, bonus_amount | Valores deposito/bonus |
| rollover_target, rollover_progress | Meta e progresso wagering |
| max_withdraw_amount | Limite saque do bonus |
| status | ACTIVE, COMPLETED, CANCELLED |

**Programa ativo:** Welcome Bonus 100% (match 100%, rollover 75x, max saque 3x, min dep 200 PKR).

---

## 4. Tabelas de Suporte

| Tabela | Registros | Descricao |
|--------|-----------|-----------|
| bonus_programs | 1 | Programas de bonus (Welcome 100%) |
| bonus_coupons | 1 | Cupons (ex: WILLIAMN = 50 PKR) |
| bonus_coupon_redemptions | 0 | Resgates de cupom |
| casino_providers | 1 | Provider: 2J Game |
| casino_provider_configs | 2 | Config providers: Alea (off), 2J (on) |
| casino_game_categories | 7 | Slots, Crypto, Multiplayer, etc. |
| casino_game_categories_map | 182 | Jogo → categoria (N:N) |
| casino_game_types | 0 | Tipos de jogo (nao usado) |
| currencies | 2 | BRL + PKR |
| gateways | 1 | PK Gateway (gateway unico) |
| payment_methods | 5 | Jazzcash, Easypaisa, etc. |
| user_payment_accounts | 202 | Contas bancarias dos jogadores |
| user_favorite_games | 124 | Jogos favoritos |
| user_sessions | 2.741 | Sessoes login (com UTM!) |
| user_marketing_events | 1.965 | Eventos marketing (Register, Deposit, FTD + UTMs) |
| user_addresses | 0 | Enderecos (nao usado) |
| referrals / referral_config | 0 | Sistema referral (nao ativo) |
| responsible_gaming_limits | 0 | Limites jogo responsavel |
| responsible_gaming_self_exclusions | 0 | Auto-exclusoes |
| admin_audit_logs | 366 | Audit trail de acoes admin |
| webhook_endpoints | 2 | Nubo integration + Keitaro |
| webhook_deliveries | 5.610 | Entregas webhook (Register, Login, Deposit, FTD) |
| roles | 9 | SUPER_ADMIN, ADMIN, MAINTAINER, etc. |
| role_permissions | 258 | Permissoes por role |
| site_settings | 1 | Config do site (Play4Tune) |
| site_assets | 12 | Imagens/logos do site |
| themes | 4 | Temas: Multibet, Supernova, Zona de Jogo, etc. |
| banners | 4 | Banners homepage |
| carousels | 0 | Carroseis (nao usado) |
| legal_documents | 2 | Termos de uso + Politica privacidade |
| storage_config | 1 | Config S3 (play4tune-assets) |
| location_service_config | 2 | GeoIP + impossible travel |
| migrations | 53 | Historico migracoes DB |
| timezone_migration_log | 0 | Log migracao timezone |

---

## 5. Views Financeiras (15 views)

### Views ja conhecidas (foreign tables play4)
| View | Descricao |
|------|-----------|
| `matriz_financeiro` | KPIs diarios: deposit, withdrawal, GGR, NGR, FTD, ativos, hold |
| `matriz_financeiro_hora` | Mesmos KPIs por hora |
| `heatmap_hour` | Dep, withdrawal, GGR por hora |
| `matriz_aquisicao` | Registros + FTD/STD/TTD por canal |
| `mv_aquisicao` | Mesma coisa (MV = materialized view como view) |
| `mv_cohort_aquisicao` | Cohort retencao por canal |
| `tab_cassino` | Bonus bet casino por dia |
| `tab_sports` | Bonus bet sportsbook por dia |
| `vw_active_player_retention_weekly` | Retencao semanal |

### Views NOVAS (nao disponiveis nas foreign tables)
| View | Descricao |
|------|-----------|
| `vw_ativos` | Ativos casino por dia |
| `vw_cadastros_ftd` | Novos usuarios + FTD + conversao por dia |
| `vw_carteiras_saldo` | Saldo total real + bonus + total |
| `vw_casino_resumo` | Bets, wins, GGR, rodadas, clientes ativos por dia |
| `vw_creditacoes` | Adicoes/remocoes manuais + bonus ativados/removidos/convertidos por dia |
| `vw_movimentacao_financeira` | Dep + saque por processadora (Jazzcash, Easypaisa) por dia |
| `vw_top_jogos_ggr` | Top jogos por GGR diario |
| `vw_totais_gerais` | Totais: apostado, pago, GGR, hold%, NGR, clientes, bonus convertidos |

---

## 6. Regras Empiricas Validadas (09/04/2026)

### Moeda e valores
- **Moeda principal:** PKR (Rupia Paquistanesa), mas BRL tambem cadastrado
- **Valores ja em unidades** — NAO dividir por 100 (diferente do Athena fund_ec2)
- `transactions.amount` = numeric(36,18) — 18 casas decimais, usar `ROUND(amount, 2)`
- `bets.amount` = numeric(18,4) — 4 casas, mais simples

### Timezone
- **CRITICO:** colunas `created_at` na maioria das tabelas sao `timestamp without time zone`
- Confirmar empiricamente se UTC ou horario local Pakistan (PKT = UTC+5)
- Views financeiras usam `date` (sem hora), possivelmente ja em PKT

### Status transacoes
- Depositos/saques: `COMPLETED`, `FAILED`, `PENDING`, `CANCELLED`, `REVERSED`
- Bets: category `WAGER` (aposta) + `WIN` (ganho), status `PLACED` / `SETTLED`
- Bonus: `ACTIVE`, `COMPLETED`, `CANCELLED`

### Provider
- Unico provider ativo: **2J Games** (slug: "2j")
- Alea cadastrado mas `enabled = false`
- 136 jogos catalogados

### IDs
- Tudo UUID (formato `019d...`)
- `users.public_id` = ID curto 9 chars (uso publico)
- `users.username` = username de login
- Currency ID unico PKR: `019cdd15-5c7d-74df-a586-bdf7026b5df5`

### Marketing
- `user_marketing_events` tem UTMs reais (utm_source, utm_medium, utm_campaign, fbclid)
- Canais identificados: Facebook, Organico, Instagram, Afiliados, Google, Kwai
- `user_sessions.utm` = JSONB com UTMs da sessao
- Webhook para Keitaro ativo (tracking)

### Multi-tenant
- Schema `platform` tem config multi-tenant
- Tenant unico: `T-0050 = Play4` (schema: public)
- Preparado para multiplos clientes no futuro

---

## 7. Queries Uteis

### Depositos por dia (status COMPLETED)
```sql
SELECT DATE(created_at) AS dia,
       COUNT(*) AS qtd,
       ROUND(SUM(amount)::numeric, 2) AS total_pkr
FROM transactions
WHERE type = 'DEPOSIT' AND status = 'COMPLETED'
GROUP BY 1 ORDER BY 1;
```

### GGR por jogo (top 10)
```sql
SELECT g.name AS jogo,
       COUNT(*) AS rodadas,
       ROUND(SUM(b.amount)::numeric, 2) AS apostado,
       ROUND(SUM(b.win_amount)::numeric, 2) AS pago,
       ROUND((SUM(b.amount) - SUM(b.win_amount))::numeric, 2) AS ggr
FROM bets b
JOIN casino_games g ON g.id = b.game_id
WHERE b.category = 'WAGER'
GROUP BY g.name
ORDER BY ggr DESC
LIMIT 10;
```

### FTD (primeiro deposito) por usuario
```sql
SELECT user_id, MIN(created_at) AS ftd_date,
       (SELECT amount FROM transactions t2
        WHERE t2.user_id = t.user_id AND t2.type = 'DEPOSIT' AND t2.status = 'COMPLETED'
        ORDER BY created_at LIMIT 1) AS ftd_amount
FROM transactions t
WHERE type = 'DEPOSIT' AND status = 'COMPLETED'
GROUP BY user_id;
```

### Saldo total por tipo de carteira
```sql
SELECT type,
       COUNT(*) AS qtd,
       ROUND(SUM(balance)::numeric, 2) AS saldo_total,
       ROUND(AVG(balance)::numeric, 2) AS saldo_medio
FROM wallets
WHERE balance > 0
GROUP BY type;
```

---

## 8. Limitacoes Conhecidas

1. **Somente casino** — zero atividade sportsbook ate 09/04/2026
2. **Provider unico** — apenas 2J Games ativo, Alea desabilitado
3. **Base pequena** — ~1.450 usuarios, ~84K bets, ~112K transacoes
4. **Timezone ambiguo** — `created_at` sem timezone na maioria das tabelas, precisa validar
5. **Sem foreign tables/servers** — banco isolado, sem conexoes externas
6. **Credenciais admin** — user `supernova_bet_admin` tem acesso total, usar com cuidado
7. **Tabelas sensiveis** — `storage_config` e `casino_provider_configs` contem secrets
