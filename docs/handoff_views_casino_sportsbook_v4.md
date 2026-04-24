# Handoff v4.2: Views Gold Casino & Sportsbook — SuperNova DB Web

**Data:** 2026-04-10 (revisao v4.2 — ultima view pedida pelo Gusta)
**Versao:** v4.2 (adiciona vw_player_performance_period)
**De:** Mateus (Squad Intelligence Engine)
**Para:** Gusta (front-end SuperNova DB Web)
**Solicitante:** Castrin (Head de Dados)
**Pipelines:** `pipelines/create_views_casino_sportsbook.py` + silvers + fct_active_players_by_period
**Documento base do feedback:** `gaps_views_gold_v3.md` (Gusta, 10/04/2026)
**Deploy EC2:** `ec2_deploy/views_casino_sportsbook/` (PRONTO, aguardando validacao)

---

## Respostas diretas ao teu feedback de 10/04 (v4.1)

**Ponto 1 — category='Outros' antes de tocar no front:** ✅ RESOLVIDO
Nao removi o chart nem deixei como estava. Fiz o fix completo:
- Troquei a fonte de categoria de `ps_bi.dim_game` (fallback ruim) pra
  `bireports_ec2.tbl_vendor_games_mapping_data` que JA uso no fix #1 e
  ja cobre 97.9% dos jogos (2010 Slots + 641 Live + 58 NULL de 2709).
- Adicionei override por nome na view pra crash/instant games (Aviator,
  Spaceman, Mines, Plinko, High Flyer, Big Bass Crash, JetX, etc) que
  o bireports nao classifica explicitamente — esses sozinhos valem
  R$ 850K de GGR so do Aviator.
- **Resultado em Mar/2026:**
  - Slots: 64.3% (R$ 3.4M GGR)
  - Crash/Instant: 18.8% (R$ 996K GGR) — NOVO bucket
  - Live: 16.2% (R$ 856K GGR)
  - Outros: 0.7% (R$ 37K GGR) — residual minusculo
- Chart de categoria agora funciona limpo. Pode tocar.

**Ponto 2 — Cron do fct_active_players_by_period.py:** ⏳ PRONTO, AGUARDANDO VALIDACAO
Voce tinha razao — sem scheduling nenhuma silver atualiza, nao so a nova.
Mas o problema e MAIOR do que voce pensou: **nenhuma das 7 silver tables**
estava agendada na EC2. Todas as 8 views gold dependiam de mim rodar manualmente.

O que fiz:
- Preparei deploy ISOLADO em `ec2_deploy/views_casino_sportsbook/`:
  - Pasta propria (nao mexe em nada existente na EC2)
  - `run_views_casino_sportsbook.sh` — script que roda os 7 pipelines em
    sequencia (silvers → active_players → create_views), tempo total ~18min
  - `deploy_views_casino_sportsbook.sh` — script de deploy com backup de
    crontab, smoke test, APPEND-ONLY no crontab (zero toque em entries
    existentes)
  - `rollback_views_casino_sportsbook.sh` — rollback limpo
  - `README.md` — documentacao completa
- Horario proposto: **04:30 BRT (07:30 UTC) diario**
  - Depois de grandes_ganhos (00:30 BRT)
  - Depois de sync_all (madrugada ~02:00-04:00 BRT)
  - Antes do expediente (09:00 BRT)
  - Nao conflita com etl_aquisicao_trafego (hourly minuto :10)
- **NAO vou subir isso pra EC2 ate voce validar tudo localmente.** Quando
  der OK, rodo deploy com smoke test (que aborta se qualquer pipeline
  falhar, zero risco de deixar coisa quebrada em producao).

**Ponto 3 — avg_ticket com NULLIF:** ✅ JA PROTEGIDO
Auditei o codigo completo. Todas as divisoes nas 8 views usam
`CASE WHEN qty_bets > 0 THEN ... ELSE NULL END` (equivalente funcional ao
NULLIF). Zero risco de division by zero. Linha por linha no
`create_views_casino_sportsbook.py`:
```sql
CASE WHEN qty_bets > 0 THEN ROUND(sports_real_bet / qty_bets, 2)
     ELSE NULL END AS avg_ticket              -- vw_sportsbook_kpis
CASE WHEN qty_players > 0 THEN ROUND(sports_real_ggr / qty_players, 2)
     ELSE NULL END AS ggr_per_player          -- vw_sportsbook_kpis
CASE WHEN sports_real_bet > 0 THEN ROUND(sports_real_ggr / sports_real_bet * 100, 2)
     ELSE NULL END AS margin_pct              -- vw_sportsbook_kpis
CASE WHEN c.qty_players > 0 THEN ROUND(c.casino_real_ggr / c.qty_players, 2)
     ELSE NULL END AS ggr_per_player          -- vw_casino_kpis
CASE WHEN c.casino_real_bet > 0 THEN ROUND(c.casino_real_ggr / c.casino_real_bet * 100, 2)
     ELSE NULL END AS hold_rate_pct           -- vw_casino_kpis
CASE WHEN SUM(turnover_real) > 0 THEN ... ELSE NULL END  -- by_provider + by_category
```
Dia com qty_bets=0 (feriado/manutencao) → coluna retorna NULL, zero crash.

---

## TL;DR — O que mudou da v3 para a v4.1

**3 bloqueadores corrigidos:**
1. `vw_casino_top_games` saiu de 11 vendors / 98% "Desconhecido" para **28 vendors e 2.749 jogos identificados**. PG Soft (R$ 10,7M GGR), Spribe (R$ 3,1M), Evolution (R$ 1,4M) agora aparecem.
2. `vw_sportsbook_exposure` saiu de R$ 1,59 bilhao de liability para **R$ 309,3M** (ratio 17,2x → **3,29x**). Cap de odds individual de 50x aplicado.
3. `sport_name` mojibake foi **FALSO POSITIVO** — dados sempre estiveram em UTF-8 correto. Problema era display do client. Ver secao "Falso positivo sobre encoding" abaixo.

**4 gaps menores resolvidos:**
4. `vw_sportsbook_kpis` agora expoe `qty_bets` + `avg_ticket` (bate 1-pra-1 com `sports_real_bet`)
5. `vw_sportsbook_by_sport` tem nova coluna `sport_category` canonica (46 esportes → 24 categorias)
6. "Projecao GGR por Settled Date" redesenhada — usar `vw_sportsbook_exposure` (sem dimensao temporal)
7. **Nova view** `vw_active_players_period` para COUNT DISTINCT por periodo × produto

**5 decisoes semanticas alinhadas** (secao dedicada abaixo)

**Historico curto de `vw_sportsbook_by_sport` explicado:** nao e bug, e limitacao da fonte Athena (`vendor_ec2.tbl_sports_book_bets_info` so existe a partir de 31/12/2025 — go-live do provedor).

**Pendente (v5):** Aba Players (Winners/Losers/Churn/LTV) e integracao com fixtures do Altenar (se/quando quiserem "Projecao GGR por Settled Date" temporal de verdade).

---

## Estado atual dos dados (atualizado 10/04/2026 14:18)

| View | Linhas | Periodo disponivel | Fonte |
|------|--------|-------------------|-------|
| `vw_casino_kpis` | 164 | 28/10/2025 → 10/04/2026 | fct_casino_activity (fund_ec2) |
| `vw_sportsbook_kpis` | 164 | 29/10/2025 → 10/04/2026 | fct_sports_activity (fund_ec2) |
| `vw_casino_by_provider` | ~340 | 28/10/2025 → 10/04/2026 | fact_casino_rounds (ps_bi + bireports) |
| `vw_casino_by_category` | ~340 | 28/10/2025 → 10/04/2026 | fact_casino_rounds |
| `vw_casino_top_games` | 117.296 | 28/10/2025 → 10/04/2026 | fact_casino_rounds |
| `vw_sportsbook_by_sport` | 1.783 | **31/12/2025** → 10/04/2026 | fact_sports_bets_by_sport (vendor_ec2) |
| `vw_sportsbook_exposure` | 46 | Snapshot 10/04/2026 | fact_sports_open_bets |
| **`vw_active_players_period`** (NOVO v4) | **18** | 6 periodos × 3 produtos | fct_active_players_by_period |
| **`vw_player_performance_period`** (NOVO v4.2) | **596.104** | 6 periodos × 3 verticals × N players | fct_player_performance_by_period |

**Regra operacional obrigatoria:** D-0 (dia corrente) contem dados parciais. Filtrar `WHERE dt < CURRENT_DATE` em series diarias OU marcar D-0 como "parcial" visualmente.

---

## TELA 1: CASINO

### View 1: `vw_casino_kpis` — Resumo diario (sem mudanca na v4)

**Uso:** Cards de KPI + grafico de tendencia GGR e GGR/Jogador

| Coluna | Tipo | Unidade | Descricao |
|--------|------|---------|-----------|
| dt | DATE | BRT | Data |
| qty_players | INT | count | Jogadores unicos no dia |
| total_rounds | INT | count | Total de rodadas (sessoes) |
| casino_real_bet | NUMERIC | BRL | Turnover real |
| casino_bonus_bet | NUMERIC | BRL | Turnover bonus |
| casino_total_bet | NUMERIC | BRL | Turnover total |
| casino_real_win | NUMERIC | BRL | Ganhos pagos (real) |
| casino_bonus_win | NUMERIC | BRL | Ganhos pagos (bonus) |
| casino_total_win | NUMERIC | BRL | Ganhos pagos (total) |
| casino_real_ggr | NUMERIC | BRL | GGR Real = Bet - Win |
| casino_bonus_ggr | NUMERIC | BRL | GGR Bonus |
| casino_total_ggr | NUMERIC | BRL | GGR Total |
| **ggr_per_player** | NUMERIC | BRL | **GGR Real / Jogador** (pedido Castrin) |
| hold_rate_pct | NUMERIC | % | GGR Real / Bet Real × 100 |
| refreshed_at | TIMESTAMPTZ | UTC | Ultima atualizacao |

---

### View 2: `vw_casino_by_provider` — Performance por provedor

**O QUE MUDOU NA v4:** fonte trocada de `ps_bi.dim_game` (cobria 0,2% do turnover, so 11 vendors)
para `bireports_ec2.tbl_vendor_games_mapping_data` (catalogo completo, 28 vendors). Agora:
- alea_pgsoft: R$ 10,7M GGR (antes nao aparecia)
- pragmaticplay: R$ 9,8M GGR
- alea_spribe: R$ 3,1M GGR (antes nao aparecia)
- alea_playtech: R$ 1,85M GGR
- alea_evolution: R$ 1,4M GGR (antes nao aparecia)

Mesmo schema da v3, so o JOIN foi corrigido.

---

### View 3: `vw_casino_by_category` — Performance por categoria

**FIX Gusta v4.1 (10/04/2026) — bucket de categorias agora funciona:**

Categoria agora vem direto de `bireports_ec2.tbl_vendor_games_mapping_data`
(coluna `c_game_category`, cobertura 97.9%), com override explicito por nome
pra crash/instant games que o bireports nao classifica.

**Distribuicao Mar/2026 (validado empiricamente):**

| Categoria | GGR | % | Jogos |
|---|---:|---:|---:|
| **Slots** | R$ 3.405.533 | 64.3% | 1.390 |
| **Crash/Instant** | R$ 996.215 | 18.8% | 60 |
| **Live** | R$ 855.746 | 16.2% | 533 |
| Outros | R$ 36.806 | 0.7% | 48 |

**Lista de nomes que disparam o override 'Crash/Instant':**
Aviator, Spaceman, High Flyer, Mines (todos), Plinko (todos), Go Rush,
Big Bass Crash, Penalty (Shootout/Champion), JetX, Rocket, Balloon,
Dice, Limbo, Wheel (via ILIKE).

Se aparecer crash game novo, e so adicionar no CASE WHEN da view
(alteracao em 1 arquivo, sem reprocessamento de silver table).

Mesmo schema de colunas da v3/v4.0.

---

### View 4: `vw_casino_top_games` — Detalhe por jogo

**O QUE MUDOU NA v4:** fix do bloqueador #1.
- Antes: 115.519 de 116.803 linhas com `game_name = 'Desconhecido (game_id)'` (98%)
- Agora: 2.749 jogos nominais identificados, incluindo game_ids compostos como `7617_164515` (tratados via `SPLIT_PART`)

Mesmo schema da v3.

**Query de validacao (critério: >= 80% do GGR em "nominal"):**
```sql
SELECT
  CASE WHEN game_name LIKE 'Desconhecido%' THEN 'desconhecido' ELSE 'nominal' END AS tipo,
  COUNT(DISTINCT game_id) AS game_ids,
  SUM(ggr_real)::numeric(18,2) AS ggr
FROM multibet.vw_casino_top_games
WHERE dt >= '2026-03-01' AND dt < '2026-04-01'
GROUP BY 1;
```

---

## TELA 2: SPORTSBOOK

### View 5: `vw_sportsbook_kpis` — Resumo diario

**O QUE MUDOU NA v4:** agora tem `qty_bets` e `avg_ticket`.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| dt | DATE | Data |
| qty_players | INT | Apostadores unicos no dia |
| **qty_bets** | INT | **Bilhetes colocados no dia** (SB_BUYIN SUCCESS, fund_ec2) |
| sports_real_bet | NUMERIC | Stake real (BRL) |
| sports_bonus_bet | NUMERIC | Stake bonus |
| sports_total_bet | NUMERIC | Stake total |
| sports_real_win | NUMERIC | Payouts real |
| sports_bonus_win | NUMERIC | Payouts bonus |
| sports_total_win | NUMERIC | Payouts total |
| sports_real_ggr | NUMERIC | GGR Real |
| sports_bonus_ggr | NUMERIC | GGR Bonus |
| sports_total_ggr | NUMERIC | GGR Total |
| **ggr_per_player** | NUMERIC | **GGR Real / Apostador** (pedido Castrin) |
| **avg_ticket** | NUMERIC | **Ticket medio** = sports_real_bet / qty_bets |
| margin_pct | NUMERIC | GGR / Stake × 100 |
| refreshed_at | TIMESTAMPTZ | Ultima atualizacao |

**Decisao tecnica importante sobre qty_bets:**
Contei via `c_txn_type = 59` (SB_BUYIN) direto em `fct_sports_activity`. Isso significa
que `qty_bets` e `sports_real_bet` vem da **mesma fonte (fund_ec2)** e **bate 1-pra-1**
— zero divergencia bet-date vs settle-date, zero reconciliacao com back-office. E
bilhete PLACED no dia (independente de ja ter settled), que e a metrica mais acionavel
pra operacao.

Se voce quiser "bilhetes LIQUIDADOS no dia" (settle-date), usa `vw_sportsbook_by_sport.qty_bets`
(que vem do vendor_ec2 filtrando `c_transaction_type = 'P'`). Duas metricas pra duas
perguntas diferentes — qty_bets do kpis e place-date, qty_bets do by_sport e settle-date.

**Resultado empirico (164 dias ate 10/04):** 1.729.174 bilhetes totais, ~10.5K bilhetes/dia,
R$ 8,45M de Real GGR, R$ 80,28 de ticket medio global.

---

### View 6: `vw_sportsbook_by_sport` — Performance por esporte

**O QUE MUDOU NA v4:** agora tem `sport_category` canonica para grafico agregado.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| dt | DATE | Data |
| sport_name | VARCHAR | Nome original do esporte (drill-down) |
| **sport_category** | VARCHAR | **Categoria canonica normalizada** (NOVO v4) |
| qty_bets | INT | Apostas liquidadas |
| qty_players | INT | Apostadores unicos |
| turnover | NUMERIC | Stake total (BRL) |
| total_return | NUMERIC | Payout total |
| ggr | NUMERIC | GGR = Stake - Payout |
| margin_pct | NUMERIC | Margem (%) |
| avg_ticket | NUMERIC | Ticket medio |
| avg_odds | NUMERIC | Odds media |
| qty_pre_match | INT | Apostas pre-match |
| qty_live | INT | Apostas ao vivo |
| turnover_pre_match | NUMERIC | Stake pre-match |
| turnover_live | NUMERIC | Stake ao vivo |
| pct_pre_match | NUMERIC | % pre-match |
| pct_live | NUMERIC | % ao vivo |
| ggr_per_player | NUMERIC | GGR / Apostador |
| refreshed_at | TIMESTAMPTZ | Ultima atualizacao |

**Mapa de `sport_category` (hardcoded na view):**

| sport_category | Agrupa (sport_name original) |
|---|---|
| **Futebol** | Futebol, Football, Soccer, Football Cup - World |
| **Futebol Virtual** | KironFootball, Virtual Football Cup, Virtual Football League |
| **Futebol Americano** | Futebol Americano, American Football, AmericanFootballH2H |
| **Basquete** | Basquete, Basketball, Baloncesto |
| **Tenis** | Tenis, Tênis, Tennis |
| **Tenis de Mesa** | Tenis de mesa, Tênis de mesa, Table Tennis |
| **Volei** | Volei, Vôlei, Volleyball |
| **Volei de Praia** | Volei de Praia, Vôlei de Praia |
| **Beisebol** | Beisebol, Baseball |
| **Hoquei no Gelo** | Hoquei no Gelo, Hóquei no Gelo, Ice Hockey |
| **Hoquei em Campo** | Hoquei em campo, Hóquei em campo |
| **Handebol** | Handebol, Handball |
| **Boxe** | Boxe, Boxing |
| **Dardos** | Dardos, Darts |
| **Rugby** | Rugby, Rugby League, Rugby Union |
| **MMA, Futsal, Cricket, Ciclismo, Badminton, Floorball, E-sports +, Especiais, Esportes Motorizados, Sinuca internacional** | (mantem nome original — sem variantes) |
| **Outros** | Qualquer novo esporte nao mapeado (fallback) |

**Resultado empirico (Mar/2026):** 46 esportes fragmentados → **24 categorias canonicas**.
Futebol unificou 4 variantes (R$ 28,69M de turnover), Basquete 2 variantes (R$ 4,55M), etc.

**Recomendacao pro front:**
- Usar `sport_category` nos graficos agregados (Top Sports, tabela Detalhamento)
- Usar `sport_name` no drill-down (quando usuario clica num esporte e quer ver variantes)

```sql
-- Top categorias no periodo (usa sport_category)
SELECT sport_category,
       SUM(ggr) AS ggr,
       SUM(turnover) AS turnover,
       SUM(qty_bets) AS bets,
       ROUND(SUM(ggr) / NULLIF(SUM(turnover), 0) * 100, 2) AS margin
FROM multibet.vw_sportsbook_by_sport
WHERE dt >= '2026-03-01' AND dt < '2026-04-01'
GROUP BY sport_category
ORDER BY ggr DESC;
```

**Historico curto explicado:** essa view comeca em **31/12/2025**, enquanto `vw_sportsbook_kpis`
comeca em 29/10/2025. Motivo: a fonte `vendor_ec2.tbl_sports_book_bets_info` so tem dados
a partir de 31/12/2025 (validado empiricamente no Athena, 10/04/2026). E limitacao da
fonte, nao bug do pipeline. Antes disso, sportsbook estava em outra tabela/provedor.
Use `vw_sportsbook_kpis` pra series diarias longas e `vw_sportsbook_by_sport` pra
breakdown a partir de 31/12/2025.

---

### View 7: `vw_sportsbook_exposure` — Apostas abertas (risco)

**O QUE MUDOU NA v4:** fix do bloqueador #2 (overflow).
- Antes: liability R$ 1,59 BILHAO, ratio 17,2x (impossivel)
- Agora: liability R$ 309,3M, ratio **3,29x** (range 1-4x esperado)

**Causa:** `c_total_odds` do Altenar tinha outliers de 702x sem cap. Apliquei
`LEAST(odds, 50.0)` por bilhete ANTES de agregar. 50x e folga conservadora (cobre
pre-match exotico e corta lixo).

Mesmo schema da v3.

**IMPORTANTE: Esta view substitui o chart "Projecao GGR por Settled Date":**
"Projecao por Settled Date" era inviavel — validei empiricamente nas 3 tabelas de sportsbook
do `vendor_ec2` (~625K legs amostradas) que **NAO EXISTE nenhum campo de expected_settle_date**,
`c_event_start_time`, `scheduled_time`, etc. E o feed de fixtures do Altenar nao esta
ingerido em nenhum database Athena.

**Redesign proposto:** usar `vw_sportsbook_exposure` para grafico "Exposicao por Esporte":
- Card: Liability Total = SUM(projected_liability)
- Barras horizontais: top 10 esportes por liability
- Nota no front: "Snapshot atual — exposicao de bilhetes abertos"

Se no futuro o trading team precisar de dimensao temporal de verdade, vira projeto v5 com
ingestao do feed de fixtures do Altenar. Por ora, snapshot por esporte entrega 80% do valor.

---

## TELA 3: OVERVIEW (cross-vertical)

### View 9 (NOVA v4.2): `vw_player_performance_period` — Performance por jogador × periodo × vertical

**Por que essa view existe:** Gusta pediu uma view pra alimentar a aba Players
(Top Winners, Top Losers, high rollers, etc) com o mesmo padrao pre-agregado
da `vw_active_players_period`. Tabela fisica + refresh diario, indices pros
queries ORDER BY LIMIT 10 do front.

**Grao:** user_id × period × vertical (~596K linhas atuais, cresce com YTD)

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| user_id | BIGINT | ecr_id do jogador |
| period | VARCHAR | `yesterday`, `last_7d`, `last_30d`, `last_90d`, `mtd`, `ytd` |
| period_label | VARCHAR | Label human-readable em pt |
| period_start | DATE | Inicio do range (inclusive) |
| period_end | DATE | Fim do range (inclusive, sempre D-1 BRT) |
| vertical | VARCHAR | `casino`, `sports`, `both` |
| **player_result** | NUMERIC(18,2) | **NGR do jogador: positivo = player ganhou, negativo = casa ganhou** |
| turnover | NUMERIC(18,2) | Total apostado no periodo × vertical (BRL) |
| deposit_total | NUMERIC(18,2) | Depositos confirmados no periodo (global, nao split por vertical) |
| qty_sessions | INTEGER | Dias distintos ativos (player-days) |
| refreshed_at | TIMESTAMPTZ | Ultima atualizacao |

**Convencao importante de `player_result`:**
`player_result = -ggr_house`, ou seja, positivo quer dizer que o JOGADOR ganhou
dinheiro (a casa pagou mais em premios do que recebeu em stakes desse jogador).
Negativo = casa ganhou = player perdeu.

- **Top Winners:** `ORDER BY player_result DESC LIMIT 10`
- **Top Losers:** `ORDER BY player_result ASC LIMIT 10` (mais negativos primeiro)
- **High rollers por turnover:** `ORDER BY turnover DESC LIMIT 10`

**Definicao de `qty_sessions`:**
Atualmente = **player-days** (dias distintos em que o jogador teve atividade de
gaming no periodo). Opcao mais estavel, zero heuristica, consistente com
`vw_active_players_period`. Se precisar de "sessao de login real" (janela
contigua com heuristica de timeout), me avisa que troco a logica — e uma
mudanca de ~5 linhas no pipeline.

**Row `both`:** jogador que apostou nas DUAS verticals no mesmo periodo aparece
em **3 linhas** naquele periodo:
- 1 row `casino` com metricas so de casino
- 1 row `sports` com metricas so de sports
- 1 row `both` com metricas combinadas (`casino + sports`)

Isso facilita queries como "top winners omnichannel" (filtra `vertical='both'`
direto sem precisar agregar).

**`deposit_total` NAO e split por vertical:** deposito vai pra wallet unica
do jogador no momento da confirmacao — nao da pra alocar 1:1 pra casino ou
sports (dinheiro e fungivel). Entao as 3 rows do mesmo player+period tem o
**MESMO** `deposit_total`. Se front quiser mostrar deposito total do jogador
no cabecalho, pega de qualquer uma das rows.

**Snapshot atual (10/04/2026, D-1 = 09/04):**

| Periodo | Vertical | Players | Winners | Losers | Turnover |
|---|---|---:|---:|---:|---:|
| yesterday | casino | 4.414 | 946 | 3.465 | R$ 6.44M |
| yesterday | sports | 2.628 | 379 | 2.224 | R$ 713K |
| yesterday | both | 429 | 116 | 312 | R$ 1.36M |
| last_7d | casino | 18.588 | 3.898 | 14.665 | R$ 37.1M |
| last_7d | sports | 11.413 | 3.115 | 7.999 | R$ 7.71M |
| last_7d | both | 3.206 | 849 | 2.334 | R$ 10.97M |
| last_30d | casino | 58.705 | 16.662 | 41.826 | R$ 158.6M |
| last_30d | sports | 25.879 | 7.466 | 17.956 | R$ 30.4M |
| last_30d | both | 9.648 | 2.866 | 6.732 | R$ 50.2M |
| last_90d | casino | 136.004 | 43.203 | 89.778 | R$ 415.5M |
| last_90d | sports | 47.122 | 12.697 | 33.714 | R$ 88.3M |
| last_90d | both | 19.614 | 6.078 | 13.336 | R$ 152.6M |
| mtd | casino | 22.706 | 5.068 | 17.603 | R$ 47.0M |
| mtd | sports | 12.786 | 3.652 | 8.807 | R$ 9.37M |
| mtd | both | 3.790 | 1.029 | 2.736 | R$ 13.3M |
| ytd | casino | 148.406 | 49.394 | 95.600 | R$ 456.2M |
| ytd | sports | 49.668 | 13.117 | 35.823 | R$ 93.6M |
| ytd | both | 21.098 | 6.795 | 14.019 | R$ 167.3M |

Ratio winners vs losers fica em ~28-35% winners / 65-72% losers em casino
(house edge fazendo o trabalho), ~30% winners / 70% losers em sportsbook.

**Indices pro front (criados):**
- `(period, vertical, player_result DESC)` — Top Winners
- `(period, vertical, player_result ASC)` — Top Losers
- `(period, vertical, turnover DESC)` — High Rollers por turnover

**Fonte e filtros:**
- `fund_ec2.tbl_real_fund_txn` + `tbl_real_fund_txn_type_mst` (gaming txns SUCCESS, is_gaming_txn=Y)
- `cashier_ec2.tbl_cashier_deposit` (status `txn_confirmed_success`, valores em centavos /100)
- `ecr_ec2.tbl_ecr_flags` (filtro `c_test_user = false`)
- Timezone UTC → BRT antes de truncar pra data
- Scope: `c_start_time >= 2026-01-01 03:00 UTC` (cobre YTD, reduz scan Athena)

**Pipeline:** `pipelines/fct_player_performance_by_period.py` (NOVO v4.2).
Refresh diario no cron EC2, rodar APOS fct_casino_activity, fct_sports_activity
e fct_active_players_by_period. Tempo ~4min.

**Exemplos de query pro front:**

```sql
-- Top 10 Winners casino ultimos 30 dias
SELECT user_id, player_result, turnover, deposit_total, qty_sessions
FROM multibet.vw_player_performance_period
WHERE period = 'last_30d' AND vertical = 'casino'
ORDER BY player_result DESC
LIMIT 10;

-- Top 10 Losers omnichannel MTD (jogadores em ambas as verticals)
SELECT user_id, player_result, turnover, deposit_total, qty_sessions
FROM multibet.vw_player_performance_period
WHERE period = 'mtd' AND vertical = 'both'
ORDER BY player_result ASC
LIMIT 10;

-- High rollers por turnover sportsbook YTD
SELECT user_id, player_result, turnover, deposit_total, qty_sessions
FROM multibet.vw_player_performance_period
WHERE period = 'ytd' AND vertical = 'sports'
ORDER BY turnover DESC
LIMIT 10;

-- Contagem geral: quantos winners vs losers em cada vertical no periodo
SELECT vertical,
       COUNT(*) FILTER (WHERE player_result > 0) AS winners,
       COUNT(*) FILTER (WHERE player_result < 0) AS losers,
       COUNT(*) FILTER (WHERE player_result = 0) AS break_even
FROM multibet.vw_player_performance_period
WHERE period = 'last_30d'
GROUP BY vertical;
```

---

### View 8 (NOVA v4): `vw_active_players_period` — Jogadores unicos por periodo × produto

**Por que essa view existe:** somar `qty_players` diario de `vw_casino_kpis` ou
`vw_sportsbook_kpis` vira player-days (double count — um jogador que jogou 30 dias
conta 30 vezes). Pra pies/cards de Overview, o correto e COUNT(DISTINCT player_id).
Como nao da pra fazer isso em view simples (precisa do detalhe por jogador),
criei tabela pre-agregada com refresh diario.

**Grao:** 18 linhas fixas = 6 periodos × 3 produtos.

| Coluna | Tipo | Descricao |
|--------|------|-----------|
| period | VARCHAR | `yesterday`, `last_7d`, `last_30d`, `last_90d`, `mtd`, `ytd` |
| period_label | VARCHAR | Label human-readable em pt |
| period_start | DATE | Inicio do range (inclusive) |
| period_end | DATE | Fim do range (inclusive, sempre D-1 ancorado em BRT) |
| product | VARCHAR | `casino`, `sportsbook`, `both` |
| unique_players | INT | COUNT(DISTINCT ecr_id) no periodo × produto |
| refreshed_at | TIMESTAMPTZ | Ultima atualizacao |

**`both` = jogadores omnichannel** = jogou em CASINO **e** em SPORTS_BOOK no mesmo periodo
(intersecao, nao uniao). KPI estrategico pro CGO/Castrin — cross-sell rate e LTV multiplicador.

**Snapshot atual (ancorado em D-1 = 09/04/2026):**

| Periodo | Casino | Sportsbook | Both (omnichannel) |
|---|---:|---:|---:|
| Yesterday | 4.462 | 3.031 | 452 |
| last_7d | 18.741 | 11.530 | 3.234 |
| last_30d | 60.486 | 26.157 | 10.249 |
| last_90d | 148.168 | 47.413 | 22.347 |
| MTD | 23.026 | 12.985 | 3.848 |
| YTD | 161.323 | 49.967 | 24.033 |

**Fonte:** `fund_ec2.tbl_real_fund_txn` JOIN `tbl_real_fund_txn_type_mst` com `c_is_gaming_txn = 'Y'`,
filtro test users via `ecr_ec2.tbl_ecr_flags`, conversao UTC→BRT antes de truncar pra data.

**Periodo end sempre = D-1 BRT** — conforme regra `feedback_sempre_usar_d_menos_1`.

**Pipeline:** `pipelines/fct_active_players_by_period.py` (NOVO). Refresh diario,
rodar APOS `fct_casino_activity` e `fct_sports_activity`.

**Como o front consome:**
```sql
-- Pie "Jogadores por Vertical" (periodo selecionado)
SELECT product, unique_players
FROM multibet.vw_active_players_period
WHERE period = 'last_30d'
  AND product IN ('casino', 'sportsbook');

-- Card "Jogadores Ativos" + "Omnichannel"
SELECT
    SUM(CASE WHEN product IN ('casino','sportsbook') THEN unique_players END) AS total_ativos,
    SUM(CASE WHEN product = 'both' THEN unique_players END) AS omnichannel
FROM multibet.vw_active_players_period
WHERE period = 'last_30d';
```

ATENCAO: `casino + sportsbook` NAO bate com "total jogadores" porque "both" ta contado
nos dois. Se voce quiser total unique:
`casino_only + sportsbook_only + both = total`, onde:
- `casino_only = casino - both`
- `sportsbook_only = sportsbook - both`

---

## Decisoes semanticas (alinhadas 10/04/2026)

### 1. RTP Casino (card "RTP Medio Realizado")
**Decisao:** usar `100 - hold_rate_pct` derivado de `vw_casino_kpis` (metodo canonico).

**Por que:** `hold_rate_pct` no `vw_casino_kpis` ja e calculado como `SUM(ggr_real) / SUM(bet_real)`
ponderado corretamente pela massa de turnover. `100 - hold_rate = rtp` por definicao.
Usar a ponderacao de `by_provider` da o mesmo numero (dentro de 0,01% — diferenca de rounding).

**Formula pro front:**
```sql
SELECT ROUND(100 - SUM(casino_real_ggr) / NULLIF(SUM(casino_real_bet), 0) * 100, 2) AS rtp_pct
FROM multibet.vw_casino_kpis
WHERE dt BETWEEN ... AND ...;
```

### 2. "Composicao de Receita" (chart Overview)
**Decisao:** grafico de barras empilhadas com 4 combinacoes:
- `casino_real_ggr` (barra 1, cor A)
- `casino_bonus_ggr` (barra 1, cor B, empilhada)
- `sports_real_ggr` (barra 2, cor C)
- `sports_bonus_ggr` (barra 2, cor D, empilhada)

**Por que:** mostra SIMULTANEAMENTE o mix real vs bonus (eficiencia da politica de bonus)
E a composicao casino vs sports (distribuicao vertical). Responde 2 perguntas de negocio
num chart so.

### 3. Hold Rate Geral (KPI Overview)
**Decisao:** sempre ponderado pelo turnover, NUNCA media simples.

**Formula:**
```sql
SELECT ROUND(
    (SUM(casino_real_ggr) + SUM(sports_real_ggr)) /
    NULLIF(SUM(casino_real_bet) + SUM(sports_real_bet), 0) * 100,
    2
) AS hold_rate_geral_pct
FROM multibet.vw_casino_kpis c
FULL JOIN multibet.vw_sportsbook_kpis s ON c.dt = s.dt
WHERE COALESCE(c.dt, s.dt) BETWEEN ... AND ...;
```

**Por que:** media simples de "hold_casino + hold_sports / 2" esta matematicamente
errada — o turnover de casino e tipicamente 5x maior que o de sports, entao o hold
medio ponderado e dominado pelo casino (2-5%). Media simples daria algo como
(3% + 10%) / 2 = 6,5%, que nao representa a realidade.

### 4. D-0 parcial
**Decisao:** **duas regras simultaneas:**
- (a) Filtrar `WHERE dt < CURRENT_DATE` em **series historicas** (charts de tendencia)
- (b) **Mostrar** D-0 em cards "Hoje" com badge visual "parcial" ou tooltip explicando

**Por que:** series historicas com D-0 parcial criam degrau falso no grafico que da
impressao de queda. Mas o usuario tambem quer ver "como estamos hoje ate agora" nos
cards de acompanhamento operacional — entao exibir D-0 separado e OK, desde que
visualmente marcado.

### 5. GGR negativo no Sportsbook
**Decisao:** tooltip/ícone **permanente** explicando.

**Texto sugerido para o tooltip:**
> "Sportsbook tem volatilidade natural. GGR negativo em dias isolados e comum
> (32% dos dias historicos nessa vertical) — significa que a casa pagou mais
> em premios do que recebeu em stakes naquele dia. Em horizontes maiores
> (semana/mes) a margem converge para 5-10% positivo."

**Por que:** Sportsbook tem 52 dias negativos em 162 (32%) versus casino com 15/162 (9%).
E comportamento esperado, nao bug. Sem contexto visual, usuario vai abrir ticket achando
que e erro.

---

## Falso positivo sobre encoding (bloqueador #3)

**Diagnostico original do Gusta:** `sport_name` voltando com caracteres `?` ou U+FFFD
no lugar de acentos (Tênis → T?nis, Vôlei → V?lei, Hóquei → H?quei).

**Investigacao empirica (10/04/2026, hex dump direto no RDS):**

```
'Tênis'           no banco: 54 c3 aa 6e 69 73        (UTF-8 valido: c3 aa = ê)
'Tênis de mesa'   no banco: 54 c3 aa 6e 69 73 20 64 65 20 6d 65 73 61
'Vôlei'           no banco: 56 c3 b4 6c 65 69        (UTF-8 valido: c3 b4 = ô)
'Vôlei de Praia'  no banco: 56 c3 b4 6c 65 69 20 64 65 20 50 72 61 69 61
'Hóquei no Gelo'  no banco: 48 c3 b3 71 75 65 69 20 6e 6f 20 47 65 6c 6f  (c3 b3 = ó)
'Jogos Olímpicos' no banco: 4a 6f 67 6f 73 20 4f 6c c3 ad 6d 70 69 63 6f 73  (c3 ad = í)
```

**Os dados estao em UTF-8 perfeito** — sempre estiveram, desde a v1. O que aconteceu:

1. **A query de validacao do Gusta tem bug semantico:**
   ```sql
   WHERE OCTET_LENGTH(sport_name) > LENGTH(sport_name)
   ```
   Essa condicao e **TRUE para QUALQUER caractere multi-byte UTF-8**. Qualquer `é`, `ç`,
   `á`, `ô`, `í` faz ela disparar. Nao indica encoding quebrado — indica que tem acento.
   A query retorna vazio SO se nenhum sport_name tiver acentos (que e o oposto do que queremos).

2. **O `T?nis` que o Gusta viu e problema de DISPLAY do client psycopg2:**
   - `psycopg2` sem `client_encoding = 'UTF8'` tenta decodificar como Windows-1252
   - Terminal do Windows com code page cp1252 mostra bytes UTF-8 como `Ã¢`, `Ã©`, ou `?`
   - DBeaver/pgAdmin com connection encoding errado fazem o mesmo

3. **Como confirmar que os dados estao OK (Gusta rodar no client dele):**
   ```sql
   SET client_encoding = 'UTF8';
   SELECT sport_name, encode(sport_name::bytea, 'hex') AS hex_bytes
   FROM multibet.vw_sportsbook_by_sport
   WHERE sport_name LIKE '%nis%' OR sport_name LIKE '%lei%'
   GROUP BY sport_name
   LIMIT 10;
   ```
   Voce vai ver `Tênis` com bytes `54 c3 aa 6e 69 73` — isso e UTF-8 correto.

4. **Acao pro front:**
   - Verificar que o driver Node/Python/Java/etc ta usando `UTF8` na connection string
   - No Postgres conn: `client_encoding=UTF8` ou `options=-c client_encoding=UTF8`
   - Se for Flask/SQLAlchemy: `create_engine('postgresql://...', client_encoding='utf8')`
   - Se for servido por API: garantir `Content-Type: application/json; charset=utf-8` na resposta

**Bonus:** incluindo esse handoff, TODAS as tabelas/views em `multibet.*` estao em UTF-8.
Se aparecer `?` em QUALQUER lugar, o problema e client, nao dado.

---

## Glossario de KPIs (atualizado v4)

| KPI | Formula | View | Uso |
|-----|---------|------|-----|
| **GGR** | Bets - Wins | * | Receita bruta da casa |
| **GGR/Jogador** | GGR Real / Players | vw_*_kpis.ggr_per_player | Monetizacao por jogador ativo |
| **Hold Rate (Casino)** | GGR / Bet × 100 | vw_casino_kpis.hold_rate_pct (2-5%) | % que a casa retem |
| **Margin (Sportsbook)** | GGR / Stake × 100 | vw_sportsbook_kpis.margin_pct (5-12%) | % que a casa retem (SB) |
| **RTP (Casino)** | 100 - Hold Rate | derivado (95-98%) | Retorno ao jogador |
| **Turnover** | Total apostado | * | Volume de apostas |
| **Ticket Medio** | Turnover / Qty bets | avg_ticket | Valor por aposta |
| **qty_bets** | Bilhetes placed no dia | vw_sportsbook_kpis.qty_bets | Volume transacional (place-date) |
| **Exposure** | Stake em aberto | vw_sportsbook_exposure.total_stake_open | Risco pendente |
| **Liability** | Stake × (Odds - 1), cap 50x | vw_sportsbook_exposure.projected_liability | Perda maxima potencial |
| **Jogadores Ativos** | COUNT(DISTINCT player_id) | **vw_active_players_period.unique_players** | Alcance por periodo |
| **Omnichannel** | jogadores em ambos produtos | vw_active_players_period WHERE product='both' | Cross-sell rate |

---

## Pipelines de refresh (ordem obrigatoria no cron)

```bash
# Bloco 1 — silvers independentes (podem rodar em paralelo)
python pipelines/fct_casino_activity.py              # ~30s
python pipelines/fct_sports_activity.py              # ~60s  (qty_bets v4)
python pipelines/fact_casino_rounds.py               # ~5min (catalogo bireports v4)
python pipelines/fact_sports_bets_by_sport.py        # ~3min
python pipelines/fact_sports_bets.py                 # ~6min (open bets v4)

# Bloco 2 — depende de casino+sports activity completarem
python pipelines/fct_active_players_by_period.py     # ~2min (NOVO v4)

# Bloco 3 — gold views (DDL only, ~10s)
python pipelines/create_views_casino_sportsbook.py   # recria DDLs v4
```

**Se algum silver falhar:** a view correspondente retorna dados desatualizados (views
gold nao sao materialized). Verificar `refreshed_at` em cada silver pra confirmar freshness.

---

## Sumario executivo para o Castrin (uma tela)

### O que entregamos em v4:
- **7 views operacionais** cobrindo Casino (4) + Sportsbook (3)
- **1 view nova** para Overview (`vw_active_players_period` com 6 periodos × 3 produtos)
- **Omnichannel exposto** como KPI estrategico (24K jogadores YTD jogaram casino E sportsbook)
- **Catalogo de jogos completo** (2.749 jogos, 28 vendors) — PG Soft, Spribe, Evolution corretos
- **Exposicao de risco confiavel** (liability 3,3x — antes 17,2x falso positivo)

### O que nao e v4 (fica pra v5):
- Aba Players (Top Winners/Losers, Churn, LTV por segmento)
- Projecao GGR por Settled Date **temporal** (requer ingestao de fixtures Altenar)
- `casino_ec2.tbl_casino_game_category_mst` integration (pra corrigir "category=Outros" dos 2.349 jogos novos)

### O que o front precisa alinhar com a gente:
- Como marcar D-0 parcial visualmente nos cards "Hoje"
- Como expressar "Omnichannel" nos cards (valor absoluto + % do total, ou so valor?)
- Qual icone/cor pra GGR negativo em Sportsbook

---

**Fim do handoff v4.**

Duvidas ou pedidos de ajuste → me chama direto.
