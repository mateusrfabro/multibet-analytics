"""
STEP 5+ — Investigacao empirica profunda dos campos de settle/event date.

Campos candidatos identificados no STEP 1-3:
  tbl_sports_book_bets_info:
    - c_bet_closure_time        (header - bet slip)
    - c_created_time
    - c_updated_time
    - c_bet_state               (filtro: 'O' = open)

  tbl_sports_book_bet_details:
    - c_ts_off                  (provavel "take off" = event start time)
    - c_leg_settlement_date     (settle por leg!)
    - c_bet_slip_closure_time   (replicado do header)
    - c_leg_status
    - c_created_time
    - c_is_live

  tbl_sports_book_info:
    - c_time_stamp
    - c_created_time
    (nao parece relevante para settle date)

Objetivo STEP 5+:
  A) Samples de 10 bets ABERTOS (c_bet_state = 'O')
  B) Analise de populacao (null rate) dos campos candidatos
  C) Analise de bilhetes multiplos (multi-leg): qual leg define settle?
  D) Analise pre-match vs live (c_is_live)
  E) Distribuicao temporal de c_ts_off vs hoje (horizonte de projecao)
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_colwidth", 40)


def banner(title):
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


def run(sql, label):
    print(f"\n>>> {label}")
    print(f"SQL:\n{sql}\n")
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
# STEP 5 — Contagem de bets por estado (ultimos 30 dias)
# Descobrir volumes para calibrar amostras
# ============================================================================
banner("STEP 5 — Distribuicao c_bet_state ultimos 30 dias")

run(
    """
    SELECT
        c_bet_state,
        COUNT(*) AS n_bets,
        COUNT(DISTINCT c_bet_slip_id) AS n_slips
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_created_time >= TIMESTAMP '2026-03-10 00:00:00'
    GROUP BY c_bet_state
    ORDER BY n_bets DESC
    """,
    "Distribuicao c_bet_state",
)

# ============================================================================
# STEP 6 — Amostra de 10 bets ABERTOS (c_bet_state = 'O') — HEADER
# ============================================================================
banner("STEP 6 — Amostra 10 bets ABERTOS (header bets_info)")

run(
    """
    SELECT
        c_bet_slip_id,
        c_bet_id,
        c_bet_type,
        c_bet_state,
        c_is_live,
        c_total_stake,
        c_total_return,
        c_total_odds,
        c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt,
        c_bet_closure_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS closure_brt,
        c_updated_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS updated_brt
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'O'
      AND c_created_time >= TIMESTAMP '2026-03-25 00:00:00'
    ORDER BY c_created_time DESC
    LIMIT 10
    """,
    "10 bets ABERTOS com timestamps",
)

# ============================================================================
# STEP 7 — Amostra dos DETAILS (legs) desses bets abertos
# ============================================================================
banner("STEP 7 — Legs dos bets abertos (c_ts_off, c_leg_settlement_date)")

run(
    """
    WITH open_bets AS (
        SELECT c_bet_slip_id, c_bet_id
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'O'
          AND c_created_time >= TIMESTAMP '2026-03-25 00:00:00'
        LIMIT 10
    )
    SELECT
        d.c_bet_slip_id,
        d.c_bet_id,
        d.c_leg_status,
        d.c_is_live,
        d.c_sport_type_name,
        d.c_tournament_name,
        substr(d.c_event_name, 1, 30) AS event_short,
        d.c_ts_off AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_off_brt,
        d.c_leg_settlement_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS leg_settle_brt,
        d.c_bet_slip_closure_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS slip_closure_brt,
        d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt
    FROM vendor_ec2.tbl_sports_book_bet_details d
    INNER JOIN open_bets o
        ON d.c_bet_slip_id = o.c_bet_slip_id
       AND d.c_bet_id = o.c_bet_id
    WHERE d.c_created_time >= TIMESTAMP '2026-03-25 00:00:00'
    ORDER BY d.c_bet_slip_id, d.c_ts_off
    """,
    "Legs dos 10 bets abertos",
)

# ============================================================================
# STEP 8 — Null rate dos campos candidatos em bets ABERTOS
# ============================================================================
banner("STEP 8 — Null rate dos campos candidatos (c_bet_state='O', ultimos 15d)")

run(
    """
    SELECT
        COUNT(*) AS total_bets_abertos,
        COUNT(c_bet_closure_time) AS nn_bet_closure_time,
        COUNT(*) - COUNT(c_bet_closure_time) AS null_bet_closure_time,
        CAST(100.0 * (COUNT(*) - COUNT(c_bet_closure_time)) / COUNT(*) AS DECIMAL(5,2)) AS pct_null_closure,
        SUM(CASE WHEN c_is_live = true THEN 1 ELSE 0 END) AS live_bets,
        SUM(CASE WHEN c_is_live = false THEN 1 ELSE 0 END) AS prematch_bets
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'O'
      AND c_created_time >= TIMESTAMP '2026-03-25 00:00:00'
    """,
    "Null rate c_bet_closure_time (header) em bets ABERTOS",
)

run(
    """
    SELECT
        COUNT(*) AS total_legs_abertas,
        COUNT(c_ts_off) AS nn_ts_off,
        CAST(100.0 * (COUNT(*) - COUNT(c_ts_off)) / COUNT(*) AS DECIMAL(5,2)) AS pct_null_ts_off,
        COUNT(c_leg_settlement_date) AS nn_leg_settle,
        CAST(100.0 * (COUNT(*) - COUNT(c_leg_settlement_date)) / COUNT(*) AS DECIMAL(5,2)) AS pct_null_leg_settle,
        COUNT(c_bet_slip_closure_time) AS nn_slip_closure,
        CAST(100.0 * (COUNT(*) - COUNT(c_bet_slip_closure_time)) / COUNT(*) AS DECIMAL(5,2)) AS pct_null_slip_closure,
        SUM(CASE WHEN c_leg_status = 'PENDING' THEN 1 ELSE 0 END) AS legs_pending,
        SUM(CASE WHEN c_leg_status = 'OPEN' THEN 1 ELSE 0 END) AS legs_open
    FROM vendor_ec2.tbl_sports_book_bet_details d
    WHERE d.c_created_time >= TIMESTAMP '2026-03-25 00:00:00'
      AND EXISTS (
          SELECT 1
          FROM vendor_ec2.tbl_sports_book_bets_info h
          WHERE h.c_bet_slip_id = d.c_bet_slip_id
            AND h.c_bet_state = 'O'
            AND h.c_created_time >= TIMESTAMP '2026-03-25 00:00:00'
      )
    """,
    "Null rate detalhes (c_ts_off, c_leg_settlement_date) em legs de bets ABERTOS",
)

# ============================================================================
# STEP 9 — Distribuicao de c_leg_status (entender o vocabulario)
# ============================================================================
banner("STEP 9 — Distribuicao c_leg_status")

run(
    """
    SELECT
        c_leg_status,
        COUNT(*) AS n_legs
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-03-25 00:00:00'
    GROUP BY c_leg_status
    ORDER BY n_legs DESC
    """,
    "Distribuicao c_leg_status",
)

print("\n\n[FIM STEP 5-9]")
