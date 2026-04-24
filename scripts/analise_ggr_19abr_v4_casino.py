"""v4 — Casino top vendors/games com c_txn_type_key como varchar."""
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
        df = query_athena(sql, database=db); print(df.to_string(index=False)); return df
    except Exception as e:
        print(f"ERRO: {e}"); return None

# 1) Valores distintos de c_txn_type_key
show("c_txn_type_key distintos 19/04",
     """SELECT c_txn_type_key, COUNT(*) n
        FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary
        WHERE c_created_date = DATE '2026-04-19'
        GROUP BY 1 ORDER BY n DESC""")

# 2) Casino por vendor (txn_type_key varchar)
show("Casino 19/04 — GGR por vendor",
     """SELECT t.c_vendor_id,
               COUNT(DISTINCT t.c_ecr_id) AS players,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN ('27','47') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS bet,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN ('45','48') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS win,
               ROUND(SUM(CASE WHEN t.c_txn_type_key = '72' THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS rollback,
               ROUND(
                 (SUM(CASE WHEN t.c_txn_type_key IN ('27','47') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                - SUM(CASE WHEN t.c_txn_type_key IN ('45','48') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                - SUM(CASE WHEN t.c_txn_type_key = '72' THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                 ) / 100.0, 2) AS ggr
        FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary t
        WHERE t.c_created_date = DATE '2026-04-19'
          AND t.c_product_id = 'CASINO'
        GROUP BY 1
        ORDER BY ggr ASC""")

# 3) Casino por vendor + game_type (top 15 worst)
show("Casino 19/04 — top 15 vendor+game com maior perda da casa",
     """SELECT t.c_vendor_id,
               t.c_game_type_id,
               COUNT(DISTINCT t.c_ecr_id) AS players,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN ('27','47') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS bet,
               ROUND(SUM(CASE WHEN t.c_txn_type_key IN ('45','48') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) / 100.0, 2) AS win,
               ROUND(
                 (SUM(CASE WHEN t.c_txn_type_key IN ('27','47') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                - SUM(CASE WHEN t.c_txn_type_key IN ('45','48') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                - SUM(CASE WHEN t.c_txn_type_key = '72' THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END)
                 ) / 100.0, 2) AS ggr
        FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary t
        WHERE t.c_created_date = DATE '2026-04-19'
          AND t.c_product_id = 'CASINO'
        GROUP BY 1, 2
        HAVING SUM(CASE WHEN t.c_txn_type_key IN ('27','47') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) > 100000
        ORDER BY ggr ASC
        LIMIT 15""")

# 4) Enriquecer top vendor+game_type com nome do jogo
show("Casino 19/04 — top 15 jogos (com nome) maior perda",
     """WITH base AS (
         SELECT t.c_vendor_id,
                CAST(t.c_game_type_id AS VARCHAR) AS c_game_id,
                COUNT(DISTINCT t.c_ecr_id) AS players,
                SUM(CASE WHEN t.c_txn_type_key IN ('27','47') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) AS bet_cents,
                SUM(CASE WHEN t.c_txn_type_key IN ('45','48') THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) AS win_cents,
                SUM(CASE WHEN t.c_txn_type_key = '72' THEN t.c_txn_real_cash_amount_ecr_crncy ELSE 0 END) AS rb_cents
         FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary t
         WHERE t.c_created_date = DATE '2026-04-19'
           AND t.c_product_id = 'CASINO'
         GROUP BY 1, 2
     )
     SELECT b.c_vendor_id AS vendor,
            b.c_game_id AS game_id,
            COALESCE(g.c_game_desc, '(sem mapeamento)') AS game_name,
            b.players,
            ROUND(b.bet_cents / 100.0, 2) AS bet,
            ROUND(b.win_cents / 100.0, 2) AS win,
            ROUND((b.bet_cents - b.win_cents - b.rb_cents) / 100.0, 2) AS ggr
     FROM base b
     LEFT JOIN bireports_ec2.tbl_vendor_games_mapping_data g
         ON b.c_vendor_id = g.c_vendor_id AND b.c_game_id = g.c_game_id
     WHERE b.bet_cents > 100000
     ORDER BY ggr ASC
     LIMIT 15""")
