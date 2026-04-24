"""
STEP 14+ — Entender semantica real de c_leg_settlement_date e c_bet_slip_closure_time.

Achados do v3:
  - c_leg_settlement_date, c_bet_slip_closure_time sao TIMESTAMP (nao VARCHAR)
  - c_ts_off: 100% NULL (abandonado)
  - Cada leg aparece 2x: uma 'O' (open, settle NULL) + uma L/W/V (post-settle)
  - c_bet_slip_closure_time (detail) tambem aparece em bets 'O', mas pode ser lixo:
      * '1975-01-01' (placeholder)
      * ou valor ja populado (bet foi settled apos snapshot do header)

Objetivo v4:
  A) Quando c_leg_settlement_date fica populado, e ANTES ou DEPOIS do event start?
     (comparar com c_created_time e verificar gap)
  B) Existe algum bet com c_bet_state='O' e c_leg_settlement_date populado que seja
     claramente FUTURO (pre-match nao settled)?
  C) c_bet_slip_closure_time serve como proxy de "evento esperado"?
  D) Horizonte temporal: distribuicao de (settlement_date - created_time)
     para saber se e "evento comeca em X horas" ou "settle apos evento terminar"
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
import pandas as pd

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 240)
pd.set_option("display.max_colwidth", 40)


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


def banner(title):
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


# ============================================================================
# STEP 14 — Populacao c_leg_settlement_date x c_leg_status (tipos corretos)
# ============================================================================
banner("STEP 14 — Cobertura c_leg_settlement_date x c_leg_status")

run(
    """
    SELECT
        c_leg_status,
        COUNT(*) AS n_legs,
        COUNT(c_leg_settlement_date) AS nn_settle,
        CAST(100.0 * COUNT(c_leg_settlement_date) / COUNT(*) AS DECIMAL(5,2)) AS pct_settle_populated,
        COUNT(c_bet_slip_closure_time) AS nn_closure,
        CAST(100.0 * COUNT(c_bet_slip_closure_time) / COUNT(*) AS DECIMAL(5,2)) AS pct_closure_populated
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
    GROUP BY c_leg_status
    ORDER BY n_legs DESC
    """,
    "Cobertura timestamps por leg_status",
)

# ============================================================================
# STEP 15 — Bets com header c_bet_state='O' que tem c_leg_settlement_date populado
# Descobrir se eh pre-match futuro ou settle ja ocorrido (race condition)
# ============================================================================
banner("STEP 15 — Bets ABERTOS com settle_date populado (futuro ou passado?)")

run(
    """
    WITH open_headers AS (
        SELECT c_bet_slip_id, c_bet_id
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'O'
          AND c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
    )
    SELECT
        d.c_leg_status,
        COUNT(*) AS n_legs,
        SUM(CASE WHEN d.c_leg_settlement_date IS NULL THEN 1 ELSE 0 END) AS settle_null,
        SUM(CASE WHEN d.c_leg_settlement_date > current_timestamp THEN 1 ELSE 0 END) AS settle_future,
        SUM(CASE WHEN d.c_leg_settlement_date <= current_timestamp THEN 1 ELSE 0 END) AS settle_past
    FROM vendor_ec2.tbl_sports_book_bet_details d
    INNER JOIN open_headers o
        ON d.c_bet_slip_id = o.c_bet_slip_id
       AND d.c_bet_id = o.c_bet_id
    WHERE d.c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
    GROUP BY d.c_leg_status
    """,
    "Distribuicao temporal settle_date em bets ABERTOS",
)

# ============================================================================
# STEP 16 — Mesma analise com c_bet_slip_closure_time (detail)
# ============================================================================
banner("STEP 16 — Distribuicao c_bet_slip_closure_time em bets ABERTOS")

run(
    """
    WITH open_headers AS (
        SELECT c_bet_slip_id, c_bet_id
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'O'
          AND c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
    )
    SELECT
        d.c_leg_status,
        COUNT(*) AS n_legs,
        SUM(CASE WHEN d.c_bet_slip_closure_time IS NULL THEN 1 ELSE 0 END) AS cl_null,
        SUM(CASE WHEN d.c_bet_slip_closure_time < TIMESTAMP '2000-01-01' THEN 1 ELSE 0 END) AS cl_placeholder,
        SUM(CASE WHEN d.c_bet_slip_closure_time > current_timestamp THEN 1 ELSE 0 END) AS cl_future,
        SUM(CASE
            WHEN d.c_bet_slip_closure_time >= TIMESTAMP '2000-01-01'
             AND d.c_bet_slip_closure_time <= current_timestamp
            THEN 1 ELSE 0 END) AS cl_past_valid
    FROM vendor_ec2.tbl_sports_book_bet_details d
    INNER JOIN open_headers o
        ON d.c_bet_slip_id = o.c_bet_slip_id
       AND d.c_bet_id = o.c_bet_id
    WHERE d.c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
    GROUP BY d.c_leg_status
    """,
    "Distribuicao temporal c_bet_slip_closure_time em bets ABERTOS",
)

# ============================================================================
# STEP 17 — Horizonte temporal: settle - created (quando evento ocorre apos bet?)
# ============================================================================
banner("STEP 17 — Horizonte (settle - created) em legs settled (L/W/V)")

run(
    """
    SELECT
        c_leg_status,
        approx_percentile(
            date_diff('minute', c_created_time, c_leg_settlement_date),
            0.05
        ) AS p05_min,
        approx_percentile(
            date_diff('minute', c_created_time, c_leg_settlement_date),
            0.50
        ) AS p50_min,
        approx_percentile(
            date_diff('minute', c_created_time, c_leg_settlement_date),
            0.95
        ) AS p95_min,
        MAX(date_diff('minute', c_created_time, c_leg_settlement_date)) AS max_min,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
      AND c_leg_status IN ('L', 'W', 'V')
      AND c_leg_settlement_date IS NOT NULL
    GROUP BY c_leg_status
    """,
    "Horizonte settle vs created (minutos)",
)

# ============================================================================
# STEP 18 — Tem bets com settle_date FUTURO (pre-match real)?
# Amostra para inspecao visual
# ============================================================================
banner("STEP 18 — Bets com settle_date > hoje (pre-match real)")

run(
    """
    SELECT
        d.c_bet_slip_id,
        d.c_leg_status,
        d.c_sport_type_name,
        substr(d.c_event_name, 1, 35) AS event_short,
        d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt,
        d.c_leg_settlement_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS settle_brt,
        d.c_bet_slip_closure_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS closure_brt,
        date_diff('hour', d.c_created_time, d.c_leg_settlement_date) AS hours_until_settle
    FROM vendor_ec2.tbl_sports_book_bet_details d
    WHERE d.c_created_time >= TIMESTAMP '2026-04-08 00:00:00'
      AND d.c_leg_settlement_date > current_timestamp
    ORDER BY d.c_leg_settlement_date
    LIMIT 20
    """,
    "Bets com settle_date no futuro (se houver)",
)

# ============================================================================
# STEP 19 — Outro angulo: c_bet_slip_closure_time em bets com c_leg_status='O'
# Entender se eh placeholder ou real
# ============================================================================
banner("STEP 19 — c_bet_slip_closure_time em legs OPEN (apenas)")

run(
    """
    SELECT
        CASE
            WHEN c_bet_slip_closure_time IS NULL THEN 'NULL'
            WHEN c_bet_slip_closure_time < TIMESTAMP '2000-01-01' THEN 'PLACEHOLDER_1975'
            WHEN c_bet_slip_closure_time > current_timestamp THEN 'FUTURE'
            ELSE 'PAST_VALID'
        END AS closure_pattern,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_bet_details
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
      AND c_leg_status = 'O'
    GROUP BY 1
    ORDER BY n DESC
    """,
    "c_bet_slip_closure_time patterns em legs OPEN",
)

# ============================================================================
# STEP 20 — E nos detalhes para legs OPEN que tem closure no futuro?
# ============================================================================
banner("STEP 20 — Amostra legs OPEN com c_bet_slip_closure_time futuro")

run(
    """
    SELECT
        d.c_bet_slip_id,
        d.c_leg_status,
        d.c_sport_type_name,
        substr(d.c_event_name, 1, 35) AS event_short,
        d.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt,
        d.c_bet_slip_closure_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS closure_brt,
        date_diff('hour', d.c_created_time, d.c_bet_slip_closure_time) AS hours_until_closure
    FROM vendor_ec2.tbl_sports_book_bet_details d
    WHERE d.c_created_time >= TIMESTAMP '2026-04-08 00:00:00'
      AND d.c_leg_status = 'O'
      AND d.c_bet_slip_closure_time > current_timestamp
    ORDER BY d.c_bet_slip_closure_time
    LIMIT 20
    """,
    "Legs OPEN com closure futuro",
)

print("\n[FIM STEP 14-20]")
