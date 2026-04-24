"""
Cria view multibet.vw_odds_performance_summary
==============================================
View agregada sobre multibet.fact_sports_odds_performance.
Soma todos os dias por (odds_range, bet_mode), pronta para dashboards.

Uso:
    python scripts/create_view_odds_performance_summary.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db.supernova import execute_supernova

DROP_SQL = "DROP VIEW IF EXISTS multibet.vw_odds_performance_summary;"

VIEW_SQL = """
CREATE VIEW multibet.vw_odds_performance_summary AS
SELECT
    odds_range,
    odds_order,
    bet_mode,
    SUM(total_bets)         AS total_bets,
    SUM(unique_players)     AS unique_players_sum,  -- nao distinct, soma simples
    SUM(bets_casa_ganha)    AS bets_casa_ganha,
    SUM(bets_casa_perde)    AS bets_casa_perde,
    ROUND(SUM(bets_casa_ganha)::NUMERIC * 100.0 / NULLIF(SUM(total_bets), 0), 2) AS pct_casa_ganha,
    SUM(total_stake)        AS total_stake,
    SUM(total_payout)       AS total_payout,
    SUM(ggr)                AS ggr,
    ROUND(SUM(ggr)::NUMERIC * 100.0 / NULLIF(SUM(total_stake), 0), 2) AS hold_rate_pct,
    ROUND(SUM(total_stake)::NUMERIC / NULLIF(SUM(total_bets), 0), 2) AS avg_ticket,
    -- Periodo coberto
    MIN(dt) AS first_dt,
    MAX(dt) AS last_dt,
    MAX(dt) - MIN(dt) + 1 AS dias_cobertos,
    MAX(refreshed_at) AS last_refresh
FROM multibet.fact_sports_odds_performance
GROUP BY odds_range, odds_order, bet_mode
ORDER BY odds_order, bet_mode;
"""

# View geral (sem split Live/PreMatch) — agregado por faixa
DROP_SQL_RANGE = "DROP VIEW IF EXISTS multibet.vw_odds_performance_by_range;"

VIEW_SQL_RANGE = """
CREATE VIEW multibet.vw_odds_performance_by_range AS
SELECT
    odds_range,
    odds_order,
    SUM(total_bets)         AS total_bets,
    SUM(bets_casa_ganha)    AS bets_casa_ganha,
    SUM(bets_casa_perde)    AS bets_casa_perde,
    ROUND(SUM(bets_casa_ganha)::NUMERIC * 100.0 / NULLIF(SUM(total_bets), 0), 2) AS pct_casa_ganha,
    SUM(total_stake)        AS total_stake,
    SUM(total_payout)       AS total_payout,
    SUM(ggr)                AS ggr,
    ROUND(SUM(ggr)::NUMERIC * 100.0 / NULLIF(SUM(total_stake), 0), 2) AS hold_rate_pct,
    ROUND(SUM(total_stake)::NUMERIC / NULLIF(SUM(total_bets), 0), 2) AS avg_ticket,
    -- % do GGR total
    ROUND(SUM(ggr)::NUMERIC * 100.0 / NULLIF((
        SELECT SUM(ggr) FROM multibet.fact_sports_odds_performance
    ), 0), 2) AS pct_ggr_total,
    MIN(dt) AS first_dt,
    MAX(dt) AS last_dt,
    MAX(refreshed_at) AS last_refresh
FROM multibet.fact_sports_odds_performance
GROUP BY odds_range, odds_order
ORDER BY odds_order;
"""

VERIFY_SQL = """
SELECT odds_range, bet_mode,
       total_bets, hold_rate_pct,
       ggr, avg_ticket,
       first_dt, last_dt, dias_cobertos
FROM multibet.vw_odds_performance_summary
ORDER BY odds_order, bet_mode;
"""

VERIFY_SQL_RANGE = """
SELECT odds_range, total_bets, pct_casa_ganha, hold_rate_pct,
       ggr, pct_ggr_total
FROM multibet.vw_odds_performance_by_range
ORDER BY odds_order;
"""


if __name__ == "__main__":
    print("Dropando views antigas...")
    execute_supernova(DROP_SQL)
    execute_supernova(DROP_SQL_RANGE)

    print("Criando vw_odds_performance_summary (split Live/PreMatch)...")
    execute_supernova(VIEW_SQL)

    print("Criando vw_odds_performance_by_range (agregado por faixa)...")
    execute_supernova(VIEW_SQL_RANGE)

    print("Views criadas.\n")

    print("=" * 90)
    print("vw_odds_performance_summary (Live x PreMatch):")
    print("=" * 90)
    rows = execute_supernova(VERIFY_SQL, fetch=True)
    for row in rows:
        print(f"  {row[0]:<14} {row[1]:<10} bets={row[2]:>8} hold={row[3]:>6}% "
              f"GGR=R$ {float(row[4] or 0):>12,.2f} ticket=R$ {float(row[5] or 0):>8,.2f} "
              f"({row[6]} a {row[7]} = {row[8]}d)")

    print("\n" + "=" * 90)
    print("vw_odds_performance_by_range (agregado):")
    print("=" * 90)
    rows = execute_supernova(VERIFY_SQL_RANGE, fetch=True)
    for row in rows:
        print(f"  {row[0]:<14} bets={row[1]:>8} casa_ganha={row[2]:>5}% hold={row[3]:>6}% "
              f"GGR=R$ {float(row[4] or 0):>12,.2f} ({row[5]}% do GGR total)")

    print("\nDone.")