# Handoff: Views Gold Casino & Sportsbook — SuperNova DB Web

**Data:** 2026-04-08
**De:** Mateus (Squad Intelligence Engine)
**Para:** Gusta (front-end SuperNova DB Web)
**Solicitante:** Castrin (Head de Dados)
**Pipeline:** `pipelines/create_views_casino_sportsbook.py`

---

## TL;DR

7 views PostgreSQL prontas para consumo no front-end, organizadas em 2 telas:

**Tela Casino (4 views):**
1. `vw_casino_kpis` — resumo financeiro diario (cards + grafico tendencia)
2. `vw_casino_by_provider` — performance por provedor (Pragmatic, Evolution, PG Soft...)
3. `vw_casino_by_category` — performance por categoria (Slots vs Live vs Crash)
4. `vw_casino_top_games` — detalhe por jogo (tabela com busca)

**Tela Sportsbook (3 views):**
5. `vw_sportsbook_kpis` — resumo financeiro diario (cards + grafico tendencia)
6. `vw_sportsbook_by_sport` — performance por esporte (Futebol, Basquete, Tennis...)
7. `vw_sportsbook_exposure` — apostas abertas / risco por esporte

---

## Estado atual dos dados (populados em 08/04/2026)

| View | Linhas | Periodo disponivel | Atualizado ate |
|------|--------|-------------------|----------------|
| `vw_casino_kpis` | 163 | 28/10/2025 a 08/04/2026 | D-0 (parcial) |
| `vw_sportsbook_kpis` | 162 | 29/10/2025 a 08/04/2026 | D-0 (parcial) |
| `vw_casino_by_provider` | 326 | 28/10/2025 a 08/04/2026 | D-0 (parcial) |
| `vw_casino_by_category` | 325 | 28/10/2025 a 08/04/2026 | D-0 (parcial) |
| `vw_casino_top_games` | 116.803 | 28/10/2025 a 08/04/2026 | D-0 (parcial) |
| `vw_sportsbook_by_sport` | 1.743 | 31/12/2025 a 08/04/2026 | D-0 (parcial) |
| `vw_sportsbook_exposure` | 46 | Snapshot 08/04/2026 | Tempo real (re-run) |

**IMPORTANTE:** D-0 (dia corrente) contem dados parciais. O front deve filtrar `WHERE dt < CURRENT_DATE` para exibir apenas dias completos, ou marcar D-0 como "parcial".

---

## Conexao

- **Host:** `supernova-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com`
- **Port:** 5432
- **Database:** `supernova_db`
- **Schema:** `multibet`
- **Acesso:** via bastion SSH (script `db/supernova.py`)

---

## TELA 1: CASINO

### View 1: `vw_casino_kpis` — Resumo diario

**Uso:** Cards de KPI no topo + grafico de tendencia GGR e GGR/Jogador

| Coluna | Tipo | Unidade | Descricao |
|--------|------|---------|-----------|
| dt | DATE | BRT | Data |
| qty_players | INT | count | Jogadores unicos no dia |
| casino_real_bet | NUMERIC | BRL | Turnover real (apostas dinheiro real) |
| casino_bonus_bet | NUMERIC | BRL | Turnover bonus |
| casino_total_bet | NUMERIC | BRL | Turnover total |
| casino_real_win | NUMERIC | BRL | Ganhos pagos (real) |
| casino_bonus_win | NUMERIC | BRL | Ganhos pagos (bonus) |
| casino_total_win | NUMERIC | BRL | Ganhos pagos (total) |
| casino_real_ggr | NUMERIC | BRL | GGR Real = Bet - Win |
| casino_bonus_ggr | NUMERIC | BRL | GGR Bonus |
| casino_total_ggr | NUMERIC | BRL | GGR Total |
| **ggr_per_player** | NUMERIC | BRL | **GGR Real / Jogador** (pedido do Castrin) |
| hold_rate_pct | NUMERIC | % | GGR Real / Bet Real * 100 |
| refreshed_at | TIMESTAMPTZ | UTC | Ultima atualizacao |

#### Exemplos de query

```sql
-- Cards KPIs (periodo)
SELECT
    SUM(casino_real_ggr) AS ggr,
    SUM(casino_real_bet) AS turnover,
    SUM(qty_players) AS players_soma,
    ROUND(SUM(casino_real_ggr) / NULLIF(SUM(qty_players), 0), 2) AS ggr_per_player,
    ROUND(SUM(casino_real_ggr) / NULLIF(SUM(casino_real_bet), 0) * 100, 2) AS hold_rate
FROM multibet.vw_casino_kpis
WHERE dt >= '2026-03-01' AND dt < '2026-04-01';

-- Grafico de tendencia GGR/Jogador (serie diaria)
SELECT dt, ggr_per_player, casino_real_ggr, qty_players
FROM multibet.vw_casino_kpis
WHERE dt >= CURRENT_DATE - INTERVAL '90 days'
  AND dt < CURRENT_DATE  -- exclui D-0 parcial
ORDER BY dt ASC;
```

---

### View 2: `vw_casino_by_provider` — Performance por provedor

**Uso:** Grafico donut (mix de receita) + tabela de providers

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| dt | DATE | Data |
| provider | VARCHAR | Nome do provedor (Pragmatic, Evolution, PG Soft, Spribe...) |
| qty_games | INT | Quantidade de jogos ativos no dia |
| qty_players | INT | Jogadores que apostaram nesse provider |
| total_rounds | INT | Total de rodadas |
| turnover_real | NUMERIC | Turnover real (BRL) |
| wins_real | NUMERIC | Wins real (BRL) |
| ggr_real | NUMERIC | GGR real (BRL) |
| turnover_total | NUMERIC | Turnover total (real + bonus) |
| ggr_total | NUMERIC | GGR total |
| hold_rate_pct | NUMERIC | Hold Rate (%) |
| rtp_pct | NUMERIC | RTP — Return to Player (%) |
| jackpot_win | NUMERIC | Jackpots pagos (BRL) |
| jackpot_contribution | NUMERIC | Contribuicoes ao pot |
| free_spins_bet | NUMERIC | Apostas de free spins |
| free_spins_win | NUMERIC | Ganhos de free spins |

```sql
-- Ranking de providers por GGR no periodo
SELECT
    provider,
    SUM(ggr_real) AS ggr,
    SUM(turnover_real) AS turnover,
    COUNT(DISTINCT dt) AS dias_ativos,
    SUM(qty_games) AS jogos_ativos,
    ROUND(SUM(ggr_real) / NULLIF(SUM(turnover_real), 0) * 100, 2) AS hold_rate
FROM multibet.vw_casino_by_provider
WHERE dt >= '2026-03-01' AND dt < '2026-04-01'
GROUP BY provider
ORDER BY ggr DESC;
```

---

### View 3: `vw_casino_by_category` — Performance por categoria

**Uso:** Grafico de barras agrupado (Slots vs Live vs Crash)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| dt | DATE | Data |
| category | VARCHAR | Slots, Live, altenar-category, Outros |
| qty_games | INT | Jogos ativos |
| qty_players | INT | Jogadores |
| total_rounds | INT | Rodadas |
| turnover_real | NUMERIC | Turnover real (BRL) |
| wins_real | NUMERIC | Wins (BRL) |
| ggr_real | NUMERIC | GGR real (BRL) |
| ggr_total | NUMERIC | GGR total |
| hold_rate_pct | NUMERIC | Hold Rate (%) |
| rtp_pct | NUMERIC | RTP (%) |
| jackpot_win | NUMERIC | Jackpots pagos |

```sql
-- Mix de categoria no periodo
SELECT
    category,
    SUM(ggr_real) AS ggr,
    ROUND(SUM(ggr_real) * 100.0 / NULLIF(SUM(SUM(ggr_real)) OVER (), 0), 1) AS pct_ggr
FROM multibet.vw_casino_by_category
WHERE dt >= '2026-03-01' AND dt < '2026-04-01'
GROUP BY category
ORDER BY ggr DESC;
```

---

### View 4: `vw_casino_top_games` — Detalhe por jogo

**Uso:** Tabela com busca e ordenacao (drill-down por jogo)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| dt | DATE | Data |
| game_id | VARCHAR | ID unico do jogo |
| game_name | VARCHAR | Nome do jogo (ex: "Fortune Tiger") |
| provider | VARCHAR | Provedor |
| category | VARCHAR | Categoria (Slots, Live, etc.) |
| qty_players | INT | Jogadores |
| total_rounds | INT | Rodadas |
| rounds_per_player | NUMERIC | Rodadas/jogador |
| turnover_real | NUMERIC | Turnover real (BRL) |
| wins_real | NUMERIC | Wins (BRL) |
| ggr_real | NUMERIC | GGR real (BRL) |
| hold_rate_pct | NUMERIC | Hold Rate (%) |
| rtp_pct | NUMERIC | RTP (%) |
| jackpot_win | NUMERIC | Jackpots pagos |
| free_spins_bet | NUMERIC | Apostas free spins |
| free_spins_win | NUMERIC | Ganhos free spins |

```sql
-- Top 20 jogos por GGR no periodo
SELECT
    game_name,
    provider,
    category,
    SUM(ggr_real) AS ggr,
    SUM(turnover_real) AS turnover,
    SUM(qty_players) AS players,
    SUM(total_rounds) AS rounds,
    ROUND(SUM(ggr_real) / NULLIF(SUM(turnover_real), 0) * 100, 2) AS hold_rate,
    ROUND(SUM(wins_real) / NULLIF(SUM(turnover_real), 0) * 100, 2) AS rtp
FROM multibet.vw_casino_top_games
WHERE dt >= '2026-03-01' AND dt < '2026-04-01'
GROUP BY game_name, provider, category
ORDER BY ggr DESC
LIMIT 20;
```

---

## TELA 2: SPORTSBOOK

### View 5: `vw_sportsbook_kpis` — Resumo diario

**Uso:** Cards de KPI + grafico de tendencia (mesma logica do casino)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| dt | DATE | Data |
| qty_players | INT | Apostadores unicos no dia |
| sports_real_bet | NUMERIC | Stake real (BRL) |
| sports_bonus_bet | NUMERIC | Stake bonus |
| sports_total_bet | NUMERIC | Stake total |
| sports_real_win | NUMERIC | Payouts real |
| sports_bonus_win | NUMERIC | Payouts bonus |
| sports_total_win | NUMERIC | Payouts total |
| sports_real_ggr | NUMERIC | GGR Real |
| sports_bonus_ggr | NUMERIC | GGR Bonus |
| sports_total_ggr | NUMERIC | GGR Total |
| **ggr_per_player** | NUMERIC | **GGR Real / Apostador** (pedido do Castrin) |
| margin_pct | NUMERIC | Margem = GGR / Stake * 100 |
| refreshed_at | TIMESTAMPTZ | Ultima atualizacao |

---

### View 6: `vw_sportsbook_by_sport` — Performance por esporte

**Uso:** Grafico de barras (top esportes) + tabela + scatter (margin vs turnover)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| dt | DATE | Data |
| sport_name | VARCHAR | **Futebol, KironFootball, Basquete, Tennis, eSports...** |
| qty_bets | INT | Apostas liquidadas |
| qty_players | INT | Apostadores unicos |
| turnover | NUMERIC | Stake total (BRL real, NAO centavos) |
| total_return | NUMERIC | Payout total |
| ggr | NUMERIC | GGR = Stake - Payout |
| margin_pct | NUMERIC | Margem (%) |
| avg_ticket | NUMERIC | Ticket medio (BRL) |
| avg_odds | NUMERIC | Odds media |
| qty_pre_match | INT | Apostas pre-match |
| qty_live | INT | Apostas ao vivo |
| turnover_pre_match | NUMERIC | Stake pre-match |
| turnover_live | NUMERIC | Stake ao vivo |
| pct_pre_match | NUMERIC | % pre-match |
| pct_live | NUMERIC | % ao vivo |
| ggr_per_player | NUMERIC | GGR / Apostador por esporte |

```sql
-- Ranking de esportes no periodo
SELECT
    sport_name,
    SUM(ggr) AS ggr,
    SUM(turnover) AS turnover,
    SUM(qty_bets) AS bets,
    ROUND(SUM(ggr) / NULLIF(SUM(turnover), 0) * 100, 2) AS margin,
    ROUND(SUM(turnover) / NULLIF(SUM(qty_bets), 0), 2) AS ticket_medio,
    ROUND(SUM(turnover_live) * 100.0 / NULLIF(SUM(turnover), 0), 1) AS pct_live
FROM multibet.vw_sportsbook_by_sport
WHERE dt >= '2026-03-01' AND dt < '2026-04-01'
GROUP BY sport_name
ORDER BY ggr DESC;

-- Evolucao diaria Pre-Match vs Live
SELECT
    dt,
    SUM(turnover_pre_match) AS pre_match,
    SUM(turnover_live) AS live
FROM multibet.vw_sportsbook_by_sport
WHERE dt >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY dt
ORDER BY dt ASC;
```

---

### View 7: `vw_sportsbook_exposure` — Apostas abertas (risco)

**Uso:** Tabela de risco / card de exposure total

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| snapshot_dt | DATE | Data do snapshot |
| sport_name | VARCHAR | Esporte |
| qty_open_bets | INT | Bilhetes abertos |
| total_stake_open | NUMERIC | Stake em aberto (BRL) |
| avg_odds_open | NUMERIC | Odds media das apostas abertas |
| projected_liability | NUMERIC | Liability = stake * (odds - 1) |
| projected_ggr | NUMERIC | GGR projetado |
| pct_stake_total | NUMERIC | % do stake total aberto |

```sql
-- Exposure total atual
SELECT
    SUM(total_stake_open) AS stake_aberto,
    SUM(projected_liability) AS liability_total,
    SUM(projected_ggr) AS ggr_projetado
FROM multibet.vw_sportsbook_exposure;

-- Exposure por esporte
SELECT sport_name, qty_open_bets, total_stake_open, projected_liability, pct_stake_total
FROM multibet.vw_sportsbook_exposure
ORDER BY total_stake_open DESC;
```

---

## Glossario de KPIs

| KPI | Formula | Casino | Sportsbook | Significado |
|-----|---------|--------|------------|-------------|
| **GGR** | Bets - Wins | casino_real_ggr | sports_real_ggr | Receita bruta da casa |
| **GGR/Jogador** | GGR Real / Players | ggr_per_player | ggr_per_player | Monetizacao por jogador ativo |
| **Hold Rate** | GGR / Bet * 100 | hold_rate_pct (2-5%) | margin_pct (8-15%) | % que a casa retém |
| **RTP** | Win / Bet * 100 | rtp_pct (~95-98%) | -- | Retorno ao jogador (casino) |
| **Turnover** | Total apostado | casino_real_bet | turnover | Volume de apostas |
| **Ticket Medio** | Turnover / Qty bets | -- | avg_ticket | Valor por aposta (SB) |
| **Exposure** | Stake em aberto | -- | total_stake_open | Risco pendente (SB) |
| **Liability** | Stake * (Odds - 1) | -- | projected_liability | Perda maxima potencial (SB) |

---

## Observacoes IMPORTANTES para o front-end

### 1. D-0 e parcial — NUNCA mostrar como completo
Dados do dia corrente sao parciais. Filtrar `WHERE dt < CURRENT_DATE` ou marcar visualmente como "parcial".

### 2. qty_players e diario — NAO somar pra ter unicos de periodo
A soma de `qty_players` de 30 dias NAO e o total de jogadores unicos no mes (mesmo jogador aparece em multiplos dias). Para cards de periodo, usar a soma como proxy e colocar tooltip "soma diaria, nao unicos".

**ATENCAO nas views by_provider e by_category:** o `qty_players` nessas views e a soma de player-game-days, nao jogadores unicos por provider. Um jogador que jogou 10 jogos da Pragmatic no mesmo dia aparece 10 vezes. Tratar como "sessoes" (player_game_days), nao como unicos.

### 3. GGR pode ser negativo
Em dias que jogadores ganham mais do que apostam. Normal em dias isolados. Nao tratar como erro.

### 4. Campos NULL
`ggr_per_player` e `hold_rate_pct` sao NULL quando nao ha jogadores/apostas. Tratar como "N/A" no front.

### 5. Fontes dos dados
As views sao alimentadas por silver tables que consultam o Athena (data lake AWS). O refresh acontece via pipelines Python (cron ou manual). As views gold atualizam automaticamente (sao views, nao materialized views).

### 6. Valores em BRL
Todos os valores financeiros ja estao em BRL (nao centavos).

### 7. Filtros sugeridos
- **Periodo:** date range picker (padrao: ultimos 30 dias)
- **Casino:** provider (multi-select), category (Slots/Live/Outros)
- **Sportsbook:** sport_name (multi-select), tipo aposta (Pre-Match/Live/Todos)

---

## Pipelines de refresh

| Pipeline | Fonte | Destino | Tempo aprox |
|----------|-------|---------|-------------|
| `fct_casino_activity.py` | Athena fund_ec2 | fct_casino_activity | ~30s |
| `fct_sports_activity.py` | Athena fund_ec2 | fct_sports_activity | ~30s |
| `fact_casino_rounds.py` | Athena ps_bi | fact_casino_rounds | ~60s |
| `fact_sports_bets_by_sport.py` | Athena vendor_ec2 | fact_sports_bets_by_sport | ~3min |
| `fact_sports_bets.py` | Athena vendor_ec2 | fact_sports_bets + open_bets | ~60s |
| `create_views_casino_sportsbook.py` | -- | 7 views gold | ~5s (DDL only) |

### Ordem de execucao (IMPORTANTE — silver antes de gold)

```bash
# 1. Silver tables (podem rodar em paralelo entre si)
python pipelines/fct_casino_activity.py           # ~30s
python pipelines/fct_sports_activity.py            # ~30s
python pipelines/fact_casino_rounds.py             # ~60s
python pipelines/fact_sports_bets_by_sport.py      # ~2min
python pipelines/fact_sports_bets.py               # ~60s (inclui open_bets)

# 2. Gold views (executar por ULTIMO, apos todas as silver)
python pipelines/create_views_casino_sportsbook.py # ~5s (DDL only)
```

Se qualquer silver falhar, a view correspondente retornara dados desatualizados (nao zero — sao views, nao materialized views). Verificar `refreshed_at` para confirmar freshness

---

## Diagrama de dependencias

```
Athena (fonte)           Silver (SuperNova DB)          Gold (views)
-----------------        ----------------------        ----------------------
fund_ec2 (sub-fund) ---> fct_casino_activity ---------> vw_casino_kpis
fund_ec2 (sub-fund) ---> fct_sports_activity ---------> vw_sportsbook_kpis
ps_bi + dim_game ------> fact_casino_rounds ----------> vw_casino_by_provider
                                                     |-> vw_casino_by_category
                                                     |-> vw_casino_top_games
vendor_ec2 + details --> fact_sports_bets_by_sport ---> vw_sportsbook_by_sport
vendor_ec2 (open) -----> fact_sports_open_bets -------> vw_sportsbook_exposure
```
