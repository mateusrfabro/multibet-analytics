"""
Analise GGR 19/04 v3 — queries ajustadas.

Ajustes:
- Casino games via bireports_ec2 (ps_bi.fct_casino_activity_daily nao tem 19/04 ainda)
- SB: usar c_bet_state distribution + sem filtro transaction_type='M'
"""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db.athena import query_athena
import pandas as pd
pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 260)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")


def show(label, sql, db="default"):
    print(f"\n{'='*80}\n[{label}]\n{'='*80}")
    try:
        df = query_athena(sql, database=db)
        print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"ERRO: {e}")
        return None


# 1) SB — distribuição bet_state e transaction_type 19/04 (janela BRT)
show("SB 19/04 — distribuicao bet_state x transaction_type",
     """SELECT c_bet_state, c_transaction_type, COUNT(*) AS n,
               ROUND(SUM(c_total_stake), 2) AS stake_total,
               ROUND(SUM(c_total_return), 2) AS payout_total
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
          AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
        GROUP BY 1, 2
        ORDER BY n DESC""")

# 2) SB — agregado com filtro correto (unique bet_slip_id)
show("SB 19/04 — agregado live vs prelive (distinct bet_slip)",
     """WITH bets AS (
         SELECT c_bet_slip_id,
                MAX(c_total_stake) AS stake,
                MAX(c_total_return) AS payout,
                MAX(CAST(c_is_live AS INTEGER)) AS is_live,
                MAX(c_bet_type) AS bet_type,
                MAX(c_bet_state) AS bet_state
         FROM vendor_ec2.tbl_sports_book_bets_info
         WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
           AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
         GROUP BY c_bet_slip_id
     )
     SELECT bet_state,
            COUNT(*) AS bilhetes,
            ROUND(SUM(stake), 2) AS stake,
            ROUND(SUM(payout), 2) AS payout,
            ROUND(SUM(stake) - SUM(payout), 2) AS ggr_sb,
            ROUND(SUM(CASE WHEN is_live=1 THEN stake ELSE 0 END), 2) AS stake_live,
            ROUND(SUM(CASE WHEN is_live=1 THEN payout ELSE 0 END), 2) AS payout_live,
            ROUND(SUM(CASE WHEN is_live=0 THEN stake ELSE 0 END), 2) AS stake_prelive,
            ROUND(SUM(CASE WHEN is_live=0 THEN payout ELSE 0 END), 2) AS payout_prelive
     FROM bets
     GROUP BY bet_state""")

# 3) SB — top 15 maiores lucros de jogadores (payout > stake)
show("SB 19/04 — top 15 bilhetes mais pagos ao jogador",
     """WITH bets AS (
         SELECT c_bet_slip_id, c_customer_id,
                MAX(c_total_stake) AS stake,
                MAX(c_total_return) AS payout,
                MAX(c_total_odds) AS odds,
                MAX(c_bet_type) AS bet_type,
                MAX(CAST(c_is_live AS INTEGER)) AS is_live,
                MAX(c_created_time) AS created_at
         FROM vendor_ec2.tbl_sports_book_bets_info
         WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
           AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
         GROUP BY c_bet_slip_id, c_customer_id
     )
     SELECT c_customer_id, c_bet_slip_id,
            ROUND(stake, 2) AS stake,
            ROUND(payout, 2) AS payout,
            ROUND(payout - stake, 2) AS profit_jogador,
            odds, bet_type,
            CASE WHEN is_live=1 THEN 'LIVE' ELSE 'PRELIVE' END AS tipo,
            created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt
     FROM bets
     WHERE payout > stake
     ORDER BY profit_jogador DESC
     LIMIT 15""")

# 4) SB — GGR por esporte 19/04
show("SB 19/04 — GGR por esporte",
     """WITH slip_first_sport AS (
         SELECT c_bet_slip_id, MIN(c_sport_type_name) AS esporte
         FROM vendor_ec2.tbl_sports_book_bet_details
         WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
           AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
         GROUP BY c_bet_slip_id
     ),
     bets AS (
         SELECT c_bet_slip_id,
                MAX(c_total_stake) AS stake,
                MAX(c_total_return) AS payout
         FROM vendor_ec2.tbl_sports_book_bets_info
         WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
           AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
         GROUP BY c_bet_slip_id
     )
     SELECT s.esporte,
            COUNT(*) AS bilhetes,
            ROUND(SUM(b.stake), 2) AS stake,
            ROUND(SUM(b.payout), 2) AS payout,
            ROUND(SUM(b.stake) - SUM(b.payout), 2) AS ggr_sb
     FROM slip_first_sport s
     JOIN bets b ON s.c_bet_slip_id = b.c_bet_slip_id
     GROUP BY s.esporte
     ORDER BY ggr_sb ASC""")

# 5) CASINO — top jogos via bireports (tem vendor)
show("Casino 19/04 — top 15 jogos por vendor x game_type (bireports)",
     """SELECT t.c_vendor_id,
               t.c_game_type_id,
               g.c_game_desc AS game_name,
               COUNT(DISTINCT t.c_ecr_id) AS players,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN (27,47) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS bet,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN (45,48) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS win,
               ROUND(
                   (SUM(CASE WHEN t.c_txn_type_key IN (27,47) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                  - SUM(CASE WHEN t.c_txn_type_key IN (45,48) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                  - SUM(CASE WHEN t.c_txn_type_key IN (72) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                   ) / 100.0, 2) AS ggr
        FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary t
        LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data g
            ON t.c_vendor_id = g.c_vendor_id AND CAST(t.c_game_type_id AS VARCHAR) = g.c_game_id
        WHERE t.c_created_date = DATE '2026-04-19'
          AND t.c_product_id = 'CASINO'
        GROUP BY 1, 2, 3
        ORDER BY ggr ASC
        LIMIT 15""")

# 6) CASINO — top vendors 19/04
show("Casino 19/04 — GGR por vendor",
     """SELECT t.c_vendor_id,
               COUNT(DISTINCT t.c_ecr_id) AS players,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN (27,47) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS bet,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN (45,48) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS win,
               ROUND(
                   (SUM(CASE WHEN t.c_txn_type_key IN (27,47) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                  - SUM(CASE WHEN t.c_txn_type_key IN (45,48) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                  - SUM(CASE WHEN t.c_txn_type_key IN (72) THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                   ) / 100.0, 2) AS ggr
        FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary t
        WHERE t.c_created_date = DATE '2026-04-19'
          AND t.c_product_id = 'CASINO'
        GROUP BY 1
        ORDER BY ggr ASC""")

# Exportar csvs
outdir = ROOT / "reports" / "ggr_19abr"
outdir.mkdir(parents=True, exist_ok=True)
print(f"\nSalvando CSVs em {outdir}")
