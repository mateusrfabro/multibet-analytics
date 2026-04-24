"""
Investigacao: Sharp Bettors no Live 1.01-2.00
==============================================
Contexto: Hold rate Live nessa faixa = 0.98% (alerta vermelho)
Objetivo: Identificar jogadores responsaveis, medir concentracao de risco
Fonte: vendor_ec2.tbl_sports_book_bets_info (Athena)
"""

import sys, os, logging
import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# =============================================================================
# Query 1: Top jogadores no Live 1.01-2.00 (perspectiva da casa)
# =============================================================================
QUERY_TOP_PLAYERS = """
WITH
valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

raw_bets AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        c_total_return,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 0), 9999) AS odds,
        c_bet_state,
        c_created_time,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2025-10-01'
      AND (c_bet_type = 'Live' OR c_is_live = true)
),

bets AS (
    SELECT * FROM raw_bets
    WHERE rn = 1
      AND odds >= 1.01 AND odds <= 2.00
)

SELECT
    b.c_customer_id AS player_id,
    COUNT(*) AS total_bets,

    -- Win/Loss count (perspectiva do PLAYER)
    SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) AS player_wins,
    SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) AS player_losses,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS player_win_rate,

    -- Financeiro
    ROUND(SUM(b.c_total_stake), 2) AS total_stake,
    ROUND(SUM(COALESCE(b.c_total_return, 0)), 2) AS total_payout,
    ROUND(SUM(b.c_total_stake) - SUM(COALESCE(b.c_total_return, 0)), 2) AS ggr_casa,

    -- Hold individual
    ROUND(
        (SUM(b.c_total_stake) - SUM(COALESCE(b.c_total_return, 0)))
        * 100.0 / NULLIF(SUM(b.c_total_stake), 0), 2
    ) AS hold_rate_pct,

    -- P&L do player (inverso do GGR da casa)
    ROUND(SUM(COALESCE(b.c_total_return, 0)) - SUM(b.c_total_stake), 2) AS player_pnl,

    -- Odds e ticket
    ROUND(AVG(b.odds), 3) AS avg_odds,
    ROUND(AVG(b.c_total_stake), 2) AS avg_ticket,
    ROUND(MAX(b.c_total_stake), 2) AS max_ticket,

    -- Periodo de atividade
    CAST(MIN(b.c_created_time) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS first_bet,
    CAST(MAX(b.c_created_time) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS last_bet,
    DATE_DIFF('day',
        CAST(MIN(b.c_created_time) AS DATE),
        CAST(MAX(b.c_created_time) AS DATE)
    ) AS dias_ativo

FROM bets b
JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
GROUP BY b.c_customer_id
HAVING COUNT(*) >= 10
ORDER BY player_pnl DESC
"""


# =============================================================================
# Query 2: Concentracao — top N jogadores vs total
# =============================================================================
QUERY_CONCENTRACAO = """
WITH
valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

raw_bets AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        c_total_return,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 0), 9999) AS odds,
        c_created_time,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2025-10-01'
      AND (c_bet_type = 'Live' OR c_is_live = true)
),

bets AS (
    SELECT * FROM raw_bets WHERE rn = 1 AND odds >= 1.01 AND odds <= 2.00
),

player_stats AS (
    SELECT
        b.c_customer_id,
        SUM(b.c_total_stake) AS stake,
        SUM(COALESCE(b.c_total_return, 0)) AS payout,
        SUM(COALESCE(b.c_total_return, 0)) - SUM(b.c_total_stake) AS player_pnl,
        COUNT(*) AS bets
    FROM bets b
    JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
    GROUP BY b.c_customer_id
)

SELECT
    COUNT(*) AS total_players,
    SUM(bets) AS total_bets,
    ROUND(SUM(stake), 2) AS total_stake,
    ROUND(SUM(payout), 2) AS total_payout,
    ROUND(SUM(stake) - SUM(payout), 2) AS ggr_total,

    -- Top 5 winners (player PnL > 0, sorted by PnL desc)
    SUM(CASE WHEN player_pnl > 0 THEN 1 ELSE 0 END) AS players_no_lucro,
    ROUND(SUM(CASE WHEN player_pnl > 0 THEN player_pnl ELSE 0 END), 2) AS pnl_total_winners,
    ROUND(SUM(CASE WHEN player_pnl > 0 THEN stake ELSE 0 END), 2) AS stake_winners,

    -- Losers
    SUM(CASE WHEN player_pnl <= 0 THEN 1 ELSE 0 END) AS players_no_prejuizo,
    ROUND(SUM(CASE WHEN player_pnl <= 0 THEN player_pnl ELSE 0 END), 2) AS pnl_total_losers,
    ROUND(SUM(CASE WHEN player_pnl <= 0 THEN stake ELSE 0 END), 2) AS stake_losers

FROM player_stats
"""


# =============================================================================
# Query 3: Evolucao mensal dos top 20 sharp bettors
# =============================================================================
QUERY_MONTHLY_SHARP = """
WITH
valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

raw_bets AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        c_total_return,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 0), 9999) AS odds,
        c_created_time,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2025-10-01'
      AND (c_bet_type = 'Live' OR c_is_live = true)
),

bets AS (
    SELECT * FROM raw_bets WHERE rn = 1 AND odds >= 1.01 AND odds <= 2.00
),

-- Identificar top 20 winners
top_winners AS (
    SELECT c_customer_id,
           SUM(COALESCE(c_total_return, 0)) - SUM(c_total_stake) AS total_pnl
    FROM bets b
    JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
    GROUP BY c_customer_id
    ORDER BY total_pnl DESC
    LIMIT 20
)

SELECT
    DATE_FORMAT(
        b.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
        '%Y-%m'
    ) AS mes,
    b.c_customer_id AS player_id,
    COUNT(*) AS bets,
    ROUND(SUM(b.c_total_stake), 2) AS stake,
    ROUND(SUM(COALESCE(b.c_total_return, 0)) - SUM(b.c_total_stake), 2) AS pnl,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) AS win_rate,
    ROUND(AVG(b.odds), 3) AS avg_odds
FROM bets b
JOIN top_winners tw ON b.c_customer_id = tw.c_customer_id
JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
GROUP BY 1, b.c_customer_id
ORDER BY b.c_customer_id, 1
"""


def main():
    log.info("=" * 70)
    log.info("INVESTIGACAO: Sharp Bettors no Live 1.01-2.00")
    log.info("=" * 70)

    os.makedirs("reports", exist_ok=True)

    # --- Query 1: Top jogadores ---
    log.info("\n--- Query 1: Top jogadores (min 10 bets) ---")
    df_top = query_athena(QUERY_TOP_PLAYERS, database="vendor_ec2")

    if df_top.empty:
        log.error("Nenhum dado retornado!")
        return

    # Separar winners e losers
    winners = df_top[df_top["player_pnl"] > 0].copy()
    losers = df_top[df_top["player_pnl"] <= 0].copy()

    print("\n" + "=" * 120)
    print("TOP 30 JOGADORES COM LUCRO — LIVE 1.01-2.00 (suspeitos de sharp betting)")
    print("=" * 120)
    cols_show = ["player_id", "total_bets", "player_win_rate", "total_stake",
                 "player_pnl", "hold_rate_pct", "avg_odds", "avg_ticket",
                 "max_ticket", "first_bet", "last_bet", "dias_ativo"]
    print(winners.head(30)[cols_show].to_string(index=False))

    print(f"\n--- Resumo Winners ---")
    print(f"Jogadores no lucro (min 10 bets): {len(winners)}")
    print(f"PnL total dos winners: R$ {winners['player_pnl'].sum():,.2f}")
    print(f"Stake total dos winners: R$ {winners['total_stake'].sum():,.2f}")
    print(f"Win rate medio: {winners['player_win_rate'].mean():.1f}%")

    print(f"\n--- Top 5 Winners ---")
    for i, (_, row) in enumerate(winners.head(5).iterrows()):
        print(f"  #{i+1} Player {int(row['player_id'])}: "
              f"PnL R$ {row['player_pnl']:,.2f} | "
              f"{int(row['total_bets'])} bets | "
              f"Win rate {row['player_win_rate']:.1f}% | "
              f"Ticket medio R$ {row['avg_ticket']:.2f} | "
              f"Max ticket R$ {row['max_ticket']:.2f} | "
              f"Ativo {int(row['dias_ativo'])}d")

    # Top 5 concentration
    if len(winners) >= 5:
        top5_pnl = winners.head(5)["player_pnl"].sum()
        total_pnl = winners["player_pnl"].sum()
        pct = top5_pnl / total_pnl * 100 if total_pnl > 0 else 0
        print(f"\n  CONCENTRACAO: Top 5 respondem por R$ {top5_pnl:,.2f} "
              f"({pct:.1f}% do PnL total dos winners)")

    df_top.to_csv("reports/sharp_bettors_live_1_2.csv", index=False)
    log.info("Salvo: reports/sharp_bettors_live_1_2.csv")

    # --- Query 2: Concentracao geral ---
    log.info("\n--- Query 2: Concentracao geral ---")
    df_conc = query_athena(QUERY_CONCENTRACAO, database="vendor_ec2")

    print("\n" + "=" * 120)
    print("CONCENTRACAO: WINNERS vs LOSERS — LIVE 1.01-2.00")
    print("=" * 120)
    for _, row in df_conc.iterrows():
        print(f"Total de jogadores: {int(row['total_players'])}")
        print(f"Total de bets: {int(row['total_bets']):,}")
        print(f"Stake total: R$ {row['total_stake']:,.2f}")
        print(f"GGR da casa: R$ {row['ggr_total']:,.2f}")
        print(f"\n  WINNERS (player PnL > 0):")
        print(f"    Jogadores: {int(row['players_no_lucro'])}")
        print(f"    PnL total extraido: R$ {row['pnl_total_winners']:,.2f}")
        print(f"    Stake movimentado: R$ {row['stake_winners']:,.2f}")
        print(f"\n  LOSERS (player PnL <= 0):")
        print(f"    Jogadores: {int(row['players_no_prejuizo'])}")
        print(f"    Prejuizo total: R$ {abs(row['pnl_total_losers']):,.2f}")
        print(f"    Stake movimentado: R$ {row['stake_losers']:,.2f}")

        # % de winners
        pct_winners = row['players_no_lucro'] / row['total_players'] * 100
        print(f"\n  {pct_winners:.1f}% dos jogadores estao no lucro nesta faixa")

    df_conc.to_csv("reports/sharp_concentracao_live.csv", index=False)
    log.info("Salvo: reports/sharp_concentracao_live.csv")

    # --- Query 3: Evolucao mensal top 20 ---
    log.info("\n--- Query 3: Evolucao mensal dos top 20 ---")
    df_monthly = query_athena(QUERY_MONTHLY_SHARP, database="vendor_ec2")

    print("\n" + "=" * 120)
    print("EVOLUCAO MENSAL DOS TOP 20 SHARP BETTORS")
    print("=" * 120)

    # Pivot: mostrar PnL por player por mes
    if not df_monthly.empty:
        pivot = df_monthly.pivot_table(
            index="player_id", columns="mes",
            values="pnl", aggfunc="sum", fill_value=0
        )
        pivot["total_pnl"] = pivot.sum(axis=1)
        pivot = pivot.sort_values("total_pnl", ascending=False)
        print(pivot.to_string())

        # Consistencia: quantos meses cada top player lucrou?
        print("\n--- Consistencia (meses com lucro / meses ativos) ---")
        for pid in pivot.index[:10]:
            player_data = df_monthly[df_monthly["player_id"] == pid]
            meses_ativos = len(player_data)
            meses_lucro = len(player_data[player_data["pnl"] > 0])
            total = pivot.loc[pid, "total_pnl"]
            print(f"  Player {int(pid)}: {meses_lucro}/{meses_ativos} meses no lucro "
                  f"| PnL total R$ {total:,.2f}")

    df_monthly.to_csv("reports/sharp_monthly_top20.csv", index=False)
    log.info("Salvo: reports/sharp_monthly_top20.csv")

    print("\n" + "=" * 120)
    print("VEREDICTO")
    print("=" * 120)
    print("Analise completa salva em reports/sharp_bettors_live_*.csv")
    print("Proximo passo: cruzar estes player_ids com a matriz de risco (multibet.matriz_risco)")


if __name__ == "__main__":
    main()
