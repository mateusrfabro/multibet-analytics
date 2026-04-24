"""v5 — Investigar schema txn_type_wise_daily_game_play_summary e alternativas."""
import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from db.athena import query_athena
import pandas as pd
pd.set_option("display.max_columns", 80)
pd.set_option("display.width", 280)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

def show(label, sql, db="default"):
    print(f"\n{'='*80}\n[{label}]\n{'='*80}")
    try:
        df = query_athena(sql, database=db); print(df.to_string(index=False)); return df
    except Exception as e:
        print(f"ERRO: {e}"); return None

# Colunas da tabela
show("SCHEMA tbl_ecr_txn_type_wise_daily_game_play_summary",
     """SHOW COLUMNS FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary""")

# Amostra de 10 linhas 19/04 (ver o que tem de fato)
show("Amostra 10 linhas 19/04",
     """SELECT * FROM bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary
        WHERE c_created_date = DATE '2026-04-19' AND c_product_id = 'CASINO'
        LIMIT 10""")
