"""
Analise GGR 19/04 v2 — valida schemas reais antes de rodar queries de jogos/SB.
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


# 1) Validar colunas fct_casino_activity_daily
show("SCHEMA fct_casino_activity_daily (filtrando 19/04)",
     """SHOW COLUMNS FROM ps_bi.fct_casino_activity_daily""")

# 2) Testar: datas disponiveis nas ultimas entries
show("ps_bi.fct_casino_activity_daily — MAX(activity_date)",
     """SELECT MAX(activity_date) AS max_date, MIN(activity_date) AS min_date,
               COUNT(*) AS rows_19abr
        FROM ps_bi.fct_casino_activity_daily
        WHERE activity_date = DATE '2026-04-19'""")

# 3) Checar schema sportsbook bets_info
show("SCHEMA vendor_ec2.tbl_sports_book_bets_info",
     """SHOW COLUMNS FROM vendor_ec2.tbl_sports_book_bets_info""")

# 4) Qualquer bilhete 19/04? (qualquer janela UTC)
show("vendor_ec2.tbl_sports_book_bets_info — contagem 19/04 UTC e janela BRT",
     """SELECT 'janela_utc_19abr' AS janela, COUNT(*) AS n
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-19 00:00:00'
          AND c_created_time <  TIMESTAMP '2026-04-20 00:00:00'
        UNION ALL
        SELECT 'janela_brt_19abr' AS janela, COUNT(*) AS n
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-19 03:00:00'
          AND c_created_time <  TIMESTAMP '2026-04-20 03:00:00'
        UNION ALL
        SELECT 'max_data' AS janela, CAST(EXTRACT(DAY FROM MAX(c_created_time)) AS BIGINT)
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-15 00:00:00'""")
