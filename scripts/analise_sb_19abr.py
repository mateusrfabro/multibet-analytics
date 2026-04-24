"""
Analise SPORTSBOOK 19/04/2026 — GGR SB foi -R$214K.
Foco: top 20 players que trouxeram SB GGR para baixo + bilhetes/eventos/odds/stakes.
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db.athena import query_athena
import pandas as pd
pd.set_option("display.max_columns", 80)
pd.set_option("display.width", 320)
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

def show(label, sql, db="default"):
    print(f"\n{'='*100}\n[{label}]\n{'='*100}")
    try:
        df = query_athena(sql, database=db); print(df.to_string(index=False)); return df
    except Exception as e:
        print(f"ERRO: {e}"); return None

# 1) TOP 20 players com pior SB GGR em 19/04 (bireports — atividade do dia)
sql_top20 = """
SELECT
    s.c_ecr_id,
    e.c_external_id,
    e.c_category,
    ROUND(SUM(s.c_sb_realcash_bet_amount) / 100.0, 2) AS sb_stake,
    ROUND(SUM(s.c_sb_realcash_win_amount) / 100.0, 2) AS sb_payout,
    ROUND(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount) / 100.0, 2) AS sb_ggr,
    SUM(s.c_sb_realcash_bet_count) AS sb_apostas,
    ROUND(SUM(s.c_deposit_success_amount) / 100.0, 2) AS depositos,
    ROUND(SUM(s.c_co_success_amount) / 100.0, 2) AS saques
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
JOIN bireports_ec2.tbl_ecr e ON s.c_ecr_id = e.c_ecr_id
WHERE s.c_created_date = DATE '2026-04-19'
  AND e.c_test_user = false
  AND s.c_sb_realcash_bet_amount > 0
GROUP BY 1, 2, 3
ORDER BY sb_ggr ASC
LIMIT 20
"""
df_top20 = show("1) TOP 20 PLAYERS SPORTSBOOK — maior perda da casa em 19/04", sql_top20)

# Extrair customer_ids (= external_id numerico)
if df_top20 is not None and not df_top20.empty:
    customer_ids = df_top20['c_external_id'].astype(str).tolist()
    ids_list = ",".join([f"'{x}'" for x in customer_ids])

    # 2) Bilhetes fechados desses 20 players com payout > stake (bilhetes vencedores)
    sql_winning_bets = f"""
    WITH bets AS (
        SELECT
            CAST(c_customer_id AS VARCHAR) AS customer_id,
            c_bet_slip_id,
            MAX(c_total_stake) AS stake,
            MAX(c_total_return) AS payout,
            MAX(c_total_odds) AS odds,
            MAX(c_bet_type) AS bet_type,
            MAX(CAST(c_is_live AS INTEGER)) AS is_live,
            MAX(c_bet_state) AS bet_state,
            MAX(c_created_time) AS created_at
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-17 00:00:00'
          AND c_created_time <  TIMESTAMP '2026-04-20 23:59:59'
          AND CAST(c_customer_id AS VARCHAR) IN ({ids_list})
        GROUP BY CAST(c_customer_id AS VARCHAR), c_bet_slip_id
    )
    SELECT customer_id, c_bet_slip_id,
           ROUND(stake, 2) AS stake, ROUND(payout, 2) AS payout,
           ROUND(payout - stake, 2) AS profit_jogador,
           odds, bet_type,
           CASE WHEN is_live=1 THEN 'LIVE' ELSE 'PRELIVE' END AS tipo,
           bet_state,
           created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt
    FROM bets
    WHERE payout > stake
    ORDER BY profit_jogador DESC
    LIMIT 50
    """
    df_bets = show("2) Bilhetes VENCEDORES dos top 20 players (17-20/04, todos os settled)", sql_winning_bets)

    # 3) Para os bilhetes principais — legs (eventos/mercados/odds)
    if df_bets is not None and not df_bets.empty:
        top_slips = df_bets['c_bet_slip_id'].head(25).tolist()
        slips_list = ",".join([f"'{x}'" for x in top_slips])
        sql_legs = f"""
        SELECT
            d.c_bet_slip_id,
            d.c_sport_type_name AS esporte,
            d.c_tournament_name AS torneio,
            d.c_event_name AS evento,
            d.c_market_name AS mercado,
            d.c_selection_name AS selecao,
            d.c_odds AS odd_leg,
            d.c_leg_status AS status_leg,
            d.c_is_live AS is_live_leg,
            d.c_vs_participant_home AS home,
            d.c_vs_participant_away AS away,
            d.c_ts_realstart AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS evento_brt
        FROM vendor_ec2.tbl_sports_book_bet_details d
        WHERE d.c_bet_slip_id IN ({slips_list})
        ORDER BY d.c_bet_slip_id, d.c_leg_settlement_date
        """
        show("3) LEGS dos 25 maiores bilhetes (evento/mercado/odd/selecao/status)", sql_legs)

# 4) Extra — GGR por esporte 19/04 (recap)
sql_sport_recap = """
WITH slip_sport AS (
    SELECT c_bet_slip_id, MIN(c_sport_type_name) AS esporte, MIN(c_tournament_name) AS torneio
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
      AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
    GROUP BY c_bet_slip_id
),
bets AS (
    SELECT c_bet_slip_id, MAX(c_total_stake) AS stake, MAX(c_total_return) AS payout
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
      AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
    GROUP BY c_bet_slip_id
)
SELECT s.esporte, s.torneio,
       COUNT(*) AS bilhetes,
       ROUND(SUM(b.stake), 2) AS stake,
       ROUND(SUM(b.payout), 2) AS payout,
       ROUND(SUM(b.stake) - SUM(b.payout), 2) AS ggr_sb
FROM slip_sport s
JOIN bets b ON s.c_bet_slip_id = b.c_bet_slip_id
WHERE s.esporte IS NOT NULL
GROUP BY 1, 2
HAVING SUM(b.stake) > 2000
ORDER BY ggr_sb ASC
LIMIT 20
"""
show("4) Top 20 TORNEIOS com maior perda da casa 19/04", sql_sport_recap)

# Exportar csvs
outdir = ROOT / "reports" / "ggr_19abr"
outdir.mkdir(parents=True, exist_ok=True)
if df_top20 is not None:
    df_top20.to_csv(outdir / "sb_top20_players.csv", index=False)
    print(f"\nCSV top20 SB: {outdir / 'sb_top20_players.csv'}")
