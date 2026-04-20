# Schema `play4` — Super Nova DB (foreign tables para Play4Tune)

**Schema:** `play4`
**Banco host:** Super Nova DB (`supernova_db`)
**Banco remoto:** `supernova-bet-db.c8r8mcwe6zq9.us-east-1.rds.amazonaws.com` / `supernova_bet`
**Mecanismo:** foreign data wrapper (`postgres_fdw` via server `supernova_bet_server`)
**Moeda:** PKR (Paquistao) | **Provider:** 2J Games | 100% Casino
**Versao:** 2.0
**Data:** 2026-04-19 (atualizado com `vw_ggr_player_game_daily`)

---

## Resumo

| Tipo | Quantidade |
|------|-----------|
| Foreign tables | 10 |

## Observacoes

- Foreign tables sao **somente leitura** — escritas devem ir direto no DB remoto (usar `db/supernova_bet.py`).
- Dados **agregados** (hora/dia). Para granularidade completa (transacoes, apostas, jogadores) usar acesso direto ao `supernova_bet`.
- **Nova tabela em 2026-04:** `vw_ggr_player_game_daily` (31 colunas) — GGR granular por jogador/jogo/dia com flags de outlier.
- Criacao do schema e foreign tables: ver projeto [project_play4tune.md](../../../.claude/projects/c--Users-NITRO-OneDrive---PGX-MultiBet/memory/project_play4tune.md).

---

## Foreign tables

## Foreign tables

### `heatmap_hour`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 9

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `data_nome` | text |  | Y | — |
| 3 | `hour` | integer |  | Y | — |
| 4 | `dep_amount` | double precision |  | Y | — |
| 5 | `withdrawal_amount` | double precision |  | Y | — |
| 6 | `net_deposit` | double precision |  | Y | — |
| 7 | `ggr_cassino` | double precision |  | Y | — |
| 8 | `ggr_sport` | double precision |  | Y | — |
| 9 | `ngr` | double precision |  | Y | — |

### `matriz_aquisicao`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 14

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data_registro` | date |  | Y | — |
| 2 | `nome` | character varying |  | Y | — |
| 3 | `users` | bigint |  | Y | — |
| 4 | `ftd` | bigint |  | Y | — |
| 5 | `ftd_amount` | numeric |  | Y | — |
| 6 | `std` | bigint |  | Y | — |
| 7 | `std_amount` | numeric |  | Y | — |
| 8 | `ttd` | bigint |  | Y | — |
| 9 | `ttd_amount` | numeric |  | Y | — |
| 10 | `qtd` | bigint |  | Y | — |
| 11 | `qtd_amount` | numeric |  | Y | — |
| 12 | `dep_amount` | numeric |  | Y | — |
| 13 | `withdrawal_amount` | numeric |  | Y | — |
| 14 | `net_deposit` | numeric |  | Y | — |

### `matriz_financeiro`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 26

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `deposit` | double precision |  | Y | — |
| 3 | `adpu` | double precision |  | Y | — |
| 4 | `avg_dep` | double precision |  | Y | — |
| 5 | `withdrawal` | double precision |  | Y | — |
| 6 | `net_deposit` | double precision |  | Y | — |
| 7 | `users` | integer |  | Y | — |
| 8 | `ftd` | integer |  | Y | — |
| 9 | `conversion` | double precision |  | Y | — |
| 10 | `ftd_amount` | double precision |  | Y | — |
| 11 | `avg_ftd_amount` | double precision |  | Y | — |
| 12 | `turnover_cassino` | double precision |  | Y | — |
| 13 | `win_cassino` | double precision |  | Y | — |
| 14 | `ggr_cassino` | double precision |  | Y | — |
| 15 | `turnover_sports` | double precision |  | Y | — |
| 16 | `win_sports` | double precision |  | Y | — |
| 17 | `ggr_sport` | double precision |  | Y | — |
| 18 | `ggr_total` | double precision |  | Y | — |
| 19 | `ngr` | double precision |  | Y | — |
| 20 | `retencao` | integer |  | Y | — |
| 21 | `arpu` | numeric |  | Y | — |
| 22 | `ativos` | integer |  | Y | — |
| 23 | `hold_cassino` | numeric |  | Y | — |
| 24 | `hold_sport` | numeric |  | Y | — |
| 25 | `btr_ggr` | numeric |  | Y | — |
| 26 | `hold_total` | numeric |  | Y | — |

### `matriz_financeiro_hora`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 22

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `hour` | integer |  | Y | — |
| 3 | `deposit` | double precision |  | Y | — |
| 4 | `adpu` | double precision |  | Y | — |
| 5 | `avg_dep` | double precision |  | Y | — |
| 6 | `withdrawal` | double precision |  | Y | — |
| 7 | `net_deposit` | double precision |  | Y | — |
| 8 | `users` | integer |  | Y | — |
| 9 | `ftd` | integer |  | Y | — |
| 10 | `conversion` | double precision |  | Y | — |
| 11 | `ftd_amount` | double precision |  | Y | — |
| 12 | `avg_ftd_amount` | double precision |  | Y | — |
| 13 | `turnover_cassino` | double precision |  | Y | — |
| 14 | `win_cassino` | double precision |  | Y | — |
| 15 | `ggr_cassino` | double precision |  | Y | — |
| 16 | `turnover_sports` | double precision |  | Y | — |
| 17 | `win_sports` | double precision |  | Y | — |
| 18 | `ggr_sports` | double precision |  | Y | — |
| 19 | `ggr_total` | double precision |  | Y | — |
| 20 | `ngr` | double precision |  | Y | — |
| 21 | `arpu` | numeric |  | Y | — |
| 22 | `ativos` | integer |  | Y | — |

### `mv_aquisicao`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 14

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data_registro` | date |  | Y | — |
| 2 | `nome` | character varying |  | Y | — |
| 3 | `users` | bigint |  | Y | — |
| 4 | `ftd` | bigint |  | Y | — |
| 5 | `ftd_amount` | numeric |  | Y | — |
| 6 | `std` | bigint |  | Y | — |
| 7 | `std_amount` | numeric |  | Y | — |
| 8 | `ttd` | bigint |  | Y | — |
| 9 | `ttd_amount` | numeric |  | Y | — |
| 10 | `qtd` | bigint |  | Y | — |
| 11 | `qtd_amount` | numeric |  | Y | — |
| 12 | `dep_amount` | numeric |  | Y | — |
| 13 | `withdrawal_amount` | numeric |  | Y | — |
| 14 | `net_deposit` | numeric |  | Y | — |

### `mv_cohort_aquisicao`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 6

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data_registro` | date |  | Y | — |
| 2 | `nome` | character varying |  | Y | — |
| 3 | `data_dif` | integer |  | Y | — |
| 4 | `qtd_depositantes` | numeric |  | Y | — |
| 5 | `qtd_jogadores` | numeric |  | Y | — |
| 6 | `total_users` | integer |  | Y | — |

### `tab_cassino`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 2

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `casino_bonus_bet_amount_inhouse` | double precision |  | Y | — |

### `tab_sports`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 2

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `sportsbook_bonus_bet` | double precision |  | Y | — |

### `vw_active_player_retention_weekly`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 8

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `semana` | date |  | Y | — |
| 2 | `semana_label` | character varying |  | Y | — |
| 3 | `depositantes_semana_atual` | integer |  | Y | — |
| 4 | `depositantes_semana_anterior` | integer |  | Y | — |
| 5 | `retidos_da_semana_anterior` | integer |  | Y | — |
| 6 | `repeat_depositors` | integer |  | Y | — |
| 7 | `retention_pct` | numeric |  | Y | — |
| 8 | `repeat_depositor_pct` | numeric |  | Y | — |

### `vw_ggr_player_game_daily`
**Tipo:** foreign_table &nbsp;&nbsp; **Linhas (est.):** -1 &nbsp;&nbsp; **Tamanho:** — &nbsp;&nbsp; **Colunas:** 31

| # | Coluna | Tipo | PK | Null | Default |
|---|--------|------|----|------|---------|
| 1 | `data` | date |  | Y | — |
| 2 | `username` | character varying(50) |  | Y | — |
| 3 | `public_id` | character varying(9) |  | Y | — |
| 4 | `phone` | character varying(20) |  | Y | — |
| 5 | `data_cadastro` | date |  | Y | — |
| 6 | `dias_conta` | integer |  | Y | — |
| 7 | `is_affiliate` | boolean |  | Y | — |
| 8 | `jogo` | character varying |  | Y | — |
| 9 | `jogo_slug` | character varying |  | Y | — |
| 10 | `rtp_configurado` | numeric(8,2) |  | Y | — |
| 11 | `provider` | character varying |  | Y | — |
| 12 | `rodadas` | integer |  | Y | — |
| 13 | `sessoes` | integer |  | Y | — |
| 14 | `apostado` | numeric |  | Y | — |
| 15 | `ganho` | numeric |  | Y | — |
| 16 | `ggr` | numeric |  | Y | — |
| 17 | `payout_pct` | numeric |  | Y | — |
| 18 | `apostado_total_jogador` | numeric |  | Y | — |
| 19 | `ganho_total_jogador` | numeric |  | Y | — |
| 20 | `ggr_total_jogador` | numeric |  | Y | — |
| 21 | `rodadas_total_jogador` | integer |  | Y | — |
| 22 | `jogos_jogados` | bigint |  | Y | — |
| 23 | `pct_turnover_dia` | numeric |  | Y | — |
| 24 | `pct_ggr_dia` | numeric |  | Y | — |
| 25 | `total_apostado_dia` | numeric |  | Y | — |
| 26 | `total_ganho_dia` | numeric |  | Y | — |
| 27 | `total_ggr_dia` | numeric |  | Y | — |
| 28 | `jogadores_ativos_dia` | bigint |  | Y | — |
| 29 | `flag_risco` | character varying |  | Y | — |
| 30 | `casa_perdeu` | boolean |  | Y | — |
| 31 | `flag_fraude` | character varying |  | Y | — |
