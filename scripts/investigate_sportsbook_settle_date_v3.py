"""
STEP 10+ — Aprofundamento apos achados do v2.

Achados criticos do v2:
  - c_ts_off e c_leg_settlement_date sao VARCHAR (nao TIMESTAMP)
  - c_bet_closure_time (header): 100% NULL em bets abertos
  - c_ts_off: 100% NULL em legs de bets abertos
  - c_leg_settlement_date: 50.35% NULL
  - c_bet_slip_closure_time (detail): 100% populado
  - c_is_live: 0 true em bets com c_bet_state='O' (inconsistente)
  - c_leg_status: O, L, W, V

Hipotese 1: "NULL" esta sendo contado pelo COUNT mas o valor eh string vazia ou 'null'.
Hipotese 2: c_ts_off so eh populado quando o evento ja aconteceu (post-settle).
Hipotese 3: c_bet_slip_closure_time tem valores "placeholder" ('0000-00-00') em bets abertos.
Hipotese 4: c_leg_settlement_date pode ser o EVENT START ou o SETTLE TIME real.

Agora: samples BRUTAS (sem cast) + stats de valores distintos.
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_colwidth", 35)


def banner(title):
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


def run(sql, label):
    print(f"\n>>> {label}")
    try:
        df = query_athena(sql, database="vendor_ec2")
        print(f"[{len(df)} linhas]")
        if len(df) > 0:
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"[ERRO] {e}")
        return None


# ============================================================================
# STEP 10 — Amostras BRUTAS (varchar, sem cast) de legs de bets abertos
# ============================================================================
banner("STEP 10 — Amostras BRUTAS dos varchar (c_ts_off, c_leg_settlement_date)")

run(
    """
    WITH open_bets AS (
        SELECT DISTINCT c_bet_slip_id
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'O'
          AND c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
        LIMIT 20
    )
    SELECT
        d.c_bet_slip_id,
        d.c_leg_status,
        d.c_sport_type_name,
        substr(d.c_event_name, 1, 30) AS event_short,
        d.c_ts_off,
        d.c_leg_settlement_date,
        d.c_bet_slip_closure_time,
        d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt
    FROM vendor_ec2.tbl_sports_book_bet_details d
    INNER JOIN open_bets o ON d.c_bet_slip_id = o.c_bet_slip_id
    WHERE d.c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
    ORDER BY d.c_bet_slip_id
    LIMIT 40
    """,
    "Raw varchar values dos campos candidatos (bets ABERTOS)",
)

# ============================================================================
# STEP 11 — Amostra de legs de bets FECHADOS (para ver se tem valor no varchar)
# ============================================================================
banner("STEP 11 — Amostras BRUTAS em bets FECHADOS (c_bet_state='C')")

run(
    """
    WITH closed_bets AS (
        SELECT DISTINCT c_bet_slip_id
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'C'
          AND c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
        LIMIT 20
    )
    SELECT
        d.c_bet_slip_id,
        d.c_leg_status,
        d.c_sport_type_name,
        substr(d.c_event_name, 1, 30) AS event_short,
        d.c_ts_off,
        d.c_leg_settlement_date,
        d.c_bet_slip_closure_time
    FROM vendor_ec2.tbl_sports_book_bet_details d
    INNER JOIN closed_bets o ON d.c_bet_slip_id = o.c_bet_slip_id
    WHERE d.c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
    ORDER BY d.c_bet_slip_id
    LIMIT 40
    """,
    "Raw varchar values em bets FECHADOS",
)

# ============================================================================
# STEP 12 — Contagem por PATTERN do valor (detectar empty string, 'null', '0000-00-00')
# ============================================================================
banner("STEP 12 — Padroes de valor em c_ts_off e c_leg_settlement_date")

run(
    """
    SELECT
        CASE
            WHEN c_ts_off IS NULL THEN 'NULL'
            WHEN c_ts_off = '' THEN 'EMPTY_STR'
            WHEN c_ts_off = 'null' THEN 'STR_null'
            WHEN c_ts_off LIKE '0000%' THEN 'ZERO_DATE'
            WHEN c_ts_off LIKE '2026%' THEN 'DATE_2026'
            WHEN c_ts_off LIKE '2025%' THEN 'DATE_2025'
            ELSE 'OTHER'
        END AS ts_off_pattern,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
    GROUP BY 1
    ORDER BY n DESC
    """,
    "Padroes c_ts_off",
)

run(
    """
    SELECT
        CASE
            WHEN c_leg_settlement_date IS NULL THEN 'NULL'
            WHEN c_leg_settlement_date = '' THEN 'EMPTY_STR'
            WHEN c_leg_settlement_date = 'null' THEN 'STR_null'
            WHEN c_leg_settlement_date LIKE '0000%' THEN 'ZERO_DATE'
            WHEN c_leg_settlement_date LIKE '2026%' THEN 'DATE_2026'
            WHEN c_leg_settlement_date LIKE '2025%' THEN 'DATE_2025'
            ELSE 'OTHER'
        END AS leg_settle_pattern,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
    GROUP BY 1
    ORDER BY n DESC
    """,
    "Padroes c_leg_settlement_date",
)

run(
    """
    SELECT
        CASE
            WHEN c_bet_slip_closure_time IS NULL THEN 'NULL'
            WHEN c_bet_slip_closure_time = '' THEN 'EMPTY_STR'
            WHEN c_bet_slip_closure_time = 'null' THEN 'STR_null'
            WHEN c_bet_slip_closure_time LIKE '0000%' THEN 'ZERO_DATE'
            WHEN c_bet_slip_closure_time LIKE '2026%' THEN 'DATE_2026'
            WHEN c_bet_slip_closure_time LIKE '2025%' THEN 'DATE_2025'
            ELSE 'OTHER'
        END AS slip_closure_pattern,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
    GROUP BY 1
    ORDER BY n DESC
    """,
    "Padroes c_bet_slip_closure_time",
)

# ============================================================================
# STEP 13 — Cruzamento: padrao x leg_status (descobrir quando c_ts_off populado)
# ============================================================================
banner("STEP 13 — c_ts_off x c_leg_status (quando esta populado?)")

run(
    """
    SELECT
        c_leg_status,
        CASE
            WHEN c_ts_off IS NULL THEN 'NULL'
            WHEN c_ts_off = '' THEN 'EMPTY_STR'
            WHEN c_ts_off = 'null' THEN 'STR_null'
            WHEN c_ts_off LIKE '0000%' THEN 'ZERO_DATE'
            ELSE 'HAS_DATE'
        END AS ts_off_pattern,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
    GROUP BY 1, 2
    ORDER BY c_leg_status, n DESC
    """,
    "c_ts_off populacao por leg_status",
)

run(
    """
    SELECT
        c_leg_status,
        CASE
            WHEN c_leg_settlement_date IS NULL THEN 'NULL'
            WHEN c_leg_settlement_date = '' THEN 'EMPTY_STR'
            WHEN c_leg_settlement_date LIKE '0000%' THEN 'ZERO_DATE'
            ELSE 'HAS_DATE'
        END AS settle_pattern,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
    GROUP BY 1, 2
    ORDER BY c_leg_status, n DESC
    """,
    "c_leg_settlement_date populacao por leg_status",
)

print("\n\n[FIM STEP 10-13]")
