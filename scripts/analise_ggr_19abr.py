"""
Analise GGR 19/04/2026 (domingo) — investigacao de queda no GGR.

Pergunta do usuario:
- GGR ficou abaixo do normal?
- Sportsbook puxou pra baixo?
- Quais jogos/players impactaram?

Fontes:
- bireports_ec2.tbl_ecr_wise_daily_bi_summary (BRT, centavos) — baseline diario + top players
- bireports_ec2.tbl_ecr (filtro test users)
- ps_bi.fct_casino_activity_daily (BRL real) — top jogos casino
- vendor_ec2.tbl_sports_book_bets_info (BRL real) — top eventos sportsbook
"""
import sys
import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from db.athena import query_athena

pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 240)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# ============================================================
# 1) BASELINE 14 DIAS — GGR diario Casino vs Sportsbook (BRT)
# ============================================================
sql_baseline = """
SELECT
    s.c_created_date AS data,
    CASE day_of_week(s.c_created_date)
        WHEN 1 THEN 'SEG' WHEN 2 THEN 'TER' WHEN 3 THEN 'QUA'
        WHEN 4 THEN 'QUI' WHEN 5 THEN 'SEX' WHEN 6 THEN 'SAB'
        WHEN 7 THEN 'DOM' END AS dia,
    ROUND(SUM(s.c_casino_realcash_bet_amount) / 100.0, 2) AS casino_bet,
    ROUND(SUM(s.c_casino_realcash_win_amount) / 100.0, 2) AS casino_win,
    ROUND(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount) / 100.0, 2) AS casino_ggr,
    ROUND(SUM(s.c_sb_realcash_bet_amount) / 100.0, 2) AS sb_bet,
    ROUND(SUM(s.c_sb_realcash_win_amount) / 100.0, 2) AS sb_win,
    ROUND(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount) / 100.0, 2) AS sb_ggr,
    ROUND(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount
              + s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount) / 100.0, 2) AS ggr_total,
    COUNT(DISTINCT s.c_ecr_id) AS players_ativos
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
WHERE s.c_created_date BETWEEN DATE '2026-04-06' AND DATE '2026-04-19'
  AND e.c_test_user = false
GROUP BY 1, 2
ORDER BY 1
"""

# ============================================================
# 2) TOP 20 PLAYERS PERDA/LUCRO DA CASA EM 19/04
# ============================================================
sql_top_players = """
SELECT
    s.c_ecr_id,
    e.c_external_id,
    e.c_category,
    ROUND(SUM(s.c_casino_realcash_bet_amount) / 100.0, 2) AS casino_bet,
    ROUND(SUM(s.c_casino_realcash_win_amount) / 100.0, 2) AS casino_win,
    ROUND(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount) / 100.0, 2) AS casino_ggr,
    ROUND(SUM(s.c_sb_realcash_bet_amount) / 100.0, 2) AS sb_bet,
    ROUND(SUM(s.c_sb_realcash_win_amount) / 100.0, 2) AS sb_win,
    ROUND(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount) / 100.0, 2) AS sb_ggr,
    ROUND(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount
              + s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount) / 100.0, 2) AS ggr_total,
    ROUND(SUM(s.c_deposit_success_amount) / 100.0, 2) AS depositos,
    ROUND(SUM(s.c_co_success_amount) / 100.0, 2) AS saques
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
WHERE s.c_created_date = DATE '2026-04-19'
  AND e.c_test_user = false
GROUP BY 1, 2, 3
ORDER BY ggr_total ASC
LIMIT 20
"""

# ============================================================
# 3) TOP 15 JOGOS CASINO COM MAIOR PERDA DA CASA 19/04
# ============================================================
sql_top_casino_games = """
SELECT
    c.sub_vendor_id,
    c.game_id,
    g.c_game_desc AS game_name,
    COUNT(DISTINCT c.player_id) AS players,
    ROUND(SUM(c.real_bet_amount_local), 2) AS bet,
    ROUND(SUM(c.real_win_amount_local), 2) AS win,
    ROUND(SUM(c.real_bet_amount_local) - SUM(c.real_win_amount_local), 2) AS ggr_casino,
    SUM(c.real_bet_count) AS apostas
FROM ps_bi.fct_casino_activity_daily c
LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data g
    ON c.game_id = g.c_game_id
WHERE c.activity_date = DATE '2026-04-19'
  AND c.real_bet_amount_local > 0
GROUP BY 1, 2, 3
ORDER BY ggr_casino ASC
LIMIT 15
"""

# ============================================================
# 4) TOP 15 EVENTOS SPORTSBOOK COM MAIOR PAYOUT 19/04
# ============================================================
# Nota: SB vendor_ec2 ja esta em BRL real. c_created_time em UTC.
# 19/04 BRT = 2026-04-19 03:00 UTC ate 2026-04-20 03:00 UTC
sql_sb_events = """
WITH bets_19abr AS (
    SELECT
        b.c_bet_slip_id,
        b.c_customer_id,
        b.c_total_stake,
        b.c_total_return,
        b.c_total_return - b.c_total_stake AS player_profit,
        b.c_bet_type,
        b.c_bet_state,
        b.c_is_live
    FROM vendor_ec2.tbl_sports_book_bets_info b
    WHERE b.c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
      AND b.c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
      AND b.c_bet_state = 'C'
      AND b.c_transaction_type = 'M'
)
SELECT
    COUNT(*) AS bilhetes,
    COUNT(DISTINCT c_customer_id) AS apostadores,
    ROUND(SUM(c_total_stake), 2) AS stake_total,
    ROUND(SUM(c_total_return), 2) AS payout_total,
    ROUND(SUM(c_total_stake) - SUM(c_total_return), 2) AS ggr_sb,
    ROUND(SUM(CASE WHEN c_is_live THEN c_total_stake ELSE 0 END), 2) AS stake_live,
    ROUND(SUM(CASE WHEN c_is_live THEN c_total_return ELSE 0 END), 2) AS payout_live,
    ROUND(SUM(CASE WHEN NOT c_is_live THEN c_total_stake ELSE 0 END), 2) AS stake_prelive,
    ROUND(SUM(CASE WHEN NOT c_is_live THEN c_total_return ELSE 0 END), 2) AS payout_prelive
FROM bets_19abr
"""

# Top 15 maiores payouts individuais SB 19/04
sql_sb_top_bets = """
SELECT
    b.c_customer_id,
    b.c_bet_slip_id,
    b.c_total_stake,
    b.c_total_return,
    ROUND(b.c_total_return - b.c_total_stake, 2) AS player_profit,
    b.c_total_odds,
    b.c_bet_type,
    b.c_is_live,
    b.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt
FROM vendor_ec2.tbl_sports_book_bets_info b
WHERE b.c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
  AND b.c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
  AND b.c_bet_state = 'C'
  AND b.c_transaction_type = 'M'
  AND b.c_total_return > b.c_total_stake
ORDER BY player_profit DESC
LIMIT 15
"""

# Top esportes/torneios 19/04 (legs vencidas)
sql_sb_sports = """
WITH legs_19 AS (
    SELECT DISTINCT
        d.c_bet_slip_id,
        d.c_sport_type_name,
        d.c_tournament_name
    FROM vendor_ec2.tbl_sports_book_bet_details d
    WHERE d.c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
      AND d.c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
)
SELECT
    l.c_sport_type_name AS esporte,
    COUNT(DISTINCT b.c_bet_slip_id) AS bilhetes,
    ROUND(SUM(b.c_total_stake), 2) AS stake,
    ROUND(SUM(b.c_total_return), 2) AS payout,
    ROUND(SUM(b.c_total_stake) - SUM(b.c_total_return), 2) AS ggr_sb
FROM legs_19 l
JOIN vendor_ec2.tbl_sports_book_bets_info b
    ON l.c_bet_slip_id = b.c_bet_slip_id
WHERE b.c_bet_state = 'C'
  AND b.c_transaction_type = 'M'
GROUP BY 1
ORDER BY ggr_sb ASC
LIMIT 15
"""


def run(label: str, sql: str, db: str = "default"):
    print(f"\n{'='*80}")
    print(f"[{label}]")
    print(f"{'='*80}")
    try:
        df = query_athena(sql, database=db)
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"ERRO: {e}")
        return None


if __name__ == "__main__":
    print("ANALISE GGR — 19/04/2026 (DOMINGO)")
    print("=" * 80)

    df_baseline = run("1) BASELINE 14 DIAS (06/04 a 19/04) — GGR por dia", sql_baseline)
    df_players = run("2) TOP 20 PLAYERS — maior PERDA da casa em 19/04", sql_top_players)
    df_games = run("3) TOP 15 JOGOS CASINO — maior perda da casa em 19/04", sql_top_casino_games)
    df_sb_agg = run("4a) SPORTSBOOK 19/04 — agregado (Live vs PreLive)", sql_sb_events)
    df_sb_top = run("4b) SPORTSBOOK 19/04 — top 15 maiores payouts individuais", sql_sb_top_bets)
    df_sb_sports = run("4c) SPORTSBOOK 19/04 — GGR por esporte", sql_sb_sports)

    # Exportar para CSV (referencia)
    outdir = ROOT / "reports" / "ggr_19abr"
    outdir.mkdir(parents=True, exist_ok=True)
    if df_baseline is not None: df_baseline.to_csv(outdir / "01_baseline_14d.csv", index=False)
    if df_players is not None: df_players.to_csv(outdir / "02_top_players.csv", index=False)
    if df_games is not None: df_games.to_csv(outdir / "03_top_games.csv", index=False)
    if df_sb_agg is not None: df_sb_agg.to_csv(outdir / "04a_sb_agg.csv", index=False)
    if df_sb_top is not None: df_sb_top.to_csv(outdir / "04b_sb_top_bets.csv", index=False)
    if df_sb_sports is not None: df_sb_sports.to_csv(outdir / "04c_sb_sports.csv", index=False)
    print(f"\nCSVs salvos em: {outdir}")
