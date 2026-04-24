"""
Auditoria: divergencia entre bet_date (vendor_ec2) e txn_date (fund_ec2)
========================================================================
Objetivo: validar se e seguro adicionar qty_bets na view vw_sportsbook_kpis
via LEFT JOIN em fact_sports_bets_by_sport, considerando que as fontes usam
bases de data diferentes:

  - fct_sports_activity:         c_start_time do fund_ec2 (data da txn financeira)
  - fact_sports_bets_by_sport:   c_created_time do vendor_ec2 (linha de payout)

Perguntas:
  1. Quais colunas *time existem em vendor_ec2.tbl_sports_book_bets_info?
  2. Quais valores de c_transaction_type e multiplicidade por slip?
  3. Para slips liquidados nos ultimos 7d:
       - delta entre data da criacao da aposta vs data do payout
       - delta entre c_created_time (vendor) vs c_start_time (fund)
  4. Qual e a taxa de mismatch diario (qty_bets do dia X nao fecha com
     sports_real_ggr do mesmo dia X)?
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena


def banner(title):
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


def run(sql, db="vendor_ec2", label=""):
    try:
        df = query_athena(sql, database=db)
        print(f"\n[OK] {label} - {len(df)} linha(s)")
        if len(df) > 0:
            with_ = 500 if len(df.columns) <= 8 else 200
            print(df.to_string(index=False, max_colwidth=with_))
        return df
    except Exception as e:
        print(f"\n[ERRO] {label}: {e}")
        return None


# ============================================================================
# STEP 1 — Colunas de tempo disponiveis em tbl_sports_book_bets_info
# ============================================================================
banner("STEP 1 - Colunas *time em tbl_sports_book_bets_info")

run(
    """
    SHOW COLUMNS IN vendor_ec2.tbl_sports_book_bets_info
    """,
    db="vendor_ec2",
    label="Todas as colunas",
)


# ============================================================================
# STEP 2 — Multiplicidade por c_bet_slip_id e por c_transaction_type
# ============================================================================
banner("STEP 2 - c_transaction_type: distribuicao e multiplicidade")

run(
    """
    SELECT
        c_transaction_type,
        COUNT(*) AS n_rows,
        COUNT(DISTINCT c_bet_slip_id) AS n_slips
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_created_time >= TIMESTAMP '2026-04-03'
      AND c_created_time <  TIMESTAMP '2026-04-10'
    GROUP BY 1
    ORDER BY 2 DESC
    """,
    db="vendor_ec2",
    label="Distribuicao c_transaction_type (7d)",
)

run(
    """
    WITH rows_per_slip AS (
        SELECT
            c_bet_slip_id,
            COUNT(*) AS n_rows,
            COUNT(DISTINCT c_transaction_type) AS n_types,
            MIN(c_created_time) AS first_ct,
            MAX(c_created_time) AS last_ct,
            ARRAY_AGG(DISTINCT c_transaction_type) AS types
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-03'
          AND c_created_time <  TIMESTAMP '2026-04-10'
        GROUP BY 1
    )
    SELECT
        n_rows,
        COUNT(*) AS n_slips,
        AVG(date_diff('second', first_ct, last_ct)) AS avg_delta_seconds,
        APPROX_PERCENTILE(date_diff('second', first_ct, last_ct), 0.5) AS p50_delta_s,
        APPROX_PERCENTILE(date_diff('second', first_ct, last_ct), 0.95) AS p95_delta_s,
        MAX(date_diff('second', first_ct, last_ct)) AS max_delta_s
    FROM rows_per_slip
    GROUP BY n_rows
    ORDER BY n_rows
    """,
    db="vendor_ec2",
    label="Multiplicidade linhas/slip + delta temporal",
)


# ============================================================================
# STEP 3 — Delta entre data do payout ('P') vs data da criacao da aposta
# Hipotese: para 'P' rows, c_created_time pode ser o instante do pagamento,
# nao da criacao do bilhete. A data real da criacao seria um 'L' ou 'M' ou
# row inicial com c_created_time mais antigo.
# ============================================================================
banner("STEP 3 - Delta date_payout vs date_bet_creation por slip")

run(
    """
    WITH slip_history AS (
        SELECT
            c_bet_slip_id,
            MIN(c_created_time) AS first_event_ct,
            MAX(CASE WHEN c_transaction_type = 'P' THEN c_created_time END) AS payout_ct
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-03-25'
          AND c_created_time <  TIMESTAMP '2026-04-10'
        GROUP BY 1
    ),
    deltas AS (
        SELECT
            CAST(first_event_ct   AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS bet_date_brt,
            CAST(payout_ct        AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS payout_date_brt,
            date_diff('hour',   first_event_ct, payout_ct) AS delta_hours,
            date_diff('day',    first_event_ct, payout_ct) AS delta_days
        FROM slip_history
        WHERE payout_ct IS NOT NULL
    )
    SELECT
        COUNT(*)                                  AS n_slips_settled,
        COUNT_IF(bet_date_brt = payout_date_brt)  AS n_same_day,
        COUNT_IF(bet_date_brt <> payout_date_brt) AS n_diff_day,
        ROUND(100.0 * COUNT_IF(bet_date_brt <> payout_date_brt) / COUNT(*), 2) AS pct_diff_day,
        ROUND(AVG(delta_hours), 2)              AS avg_hours,
        APPROX_PERCENTILE(delta_hours, 0.5)     AS p50_hours,
        APPROX_PERCENTILE(delta_hours, 0.95)    AS p95_hours,
        APPROX_PERCENTILE(delta_hours, 0.99)    AS p99_hours,
        MAX(delta_hours)                        AS max_hours,
        MAX(delta_days)                         AS max_days
    FROM deltas
    """,
    db="vendor_ec2",
    label="Delta bet_creation -> payout (slips liquidados)",
)


# ============================================================================
# STEP 4 — Impacto diario: mismatch entre qty_bets (payout_date) e
# qty_bets real (bet_creation_date) agrupado pelo dia
# ============================================================================
banner("STEP 4 - Mismatch diario: qty_bets por payout_date vs bet_creation_date")

run(
    """
    WITH slip_dates AS (
        SELECT
            c_bet_slip_id,
            CAST(MIN(c_created_time) AT TIME ZONE 'UTC'
                 AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS bet_creation_date,
            CAST(MAX(CASE WHEN c_transaction_type = 'P' THEN c_created_time END)
                 AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS payout_date
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-03-25'
          AND c_created_time <  TIMESTAMP '2026-04-10'
        GROUP BY 1
    ),
    by_payout AS (
        SELECT payout_date AS dt, COUNT(DISTINCT c_bet_slip_id) AS qty_by_payout
        FROM slip_dates
        WHERE payout_date IS NOT NULL
        GROUP BY 1
    ),
    by_creation AS (
        SELECT bet_creation_date AS dt, COUNT(DISTINCT c_bet_slip_id) AS qty_by_creation
        FROM slip_dates
        WHERE payout_date IS NOT NULL   -- so slips liquidados, para comparacao apples-to-apples
        GROUP BY 1
    )
    SELECT
        COALESCE(p.dt, c.dt) AS dt,
        p.qty_by_payout,
        c.qty_by_creation,
        (p.qty_by_payout - c.qty_by_creation) AS delta_abs,
        CASE WHEN c.qty_by_creation > 0
             THEN ROUND(100.0 * (p.qty_by_payout - c.qty_by_creation) / c.qty_by_creation, 2)
             ELSE NULL END AS delta_pct
    FROM by_payout p
    FULL OUTER JOIN by_creation c ON p.dt = c.dt
    WHERE COALESCE(p.dt, c.dt) >= DATE '2026-03-27'
    ORDER BY dt DESC
    """,
    db="vendor_ec2",
    label="qty_bets por payout_date vs bet_creation_date",
)


# ============================================================================
# STEP 5 — Delta entre c_created_time (vendor_ec2) e c_start_time (fund_ec2)
# para SB_BUYIN (txn_type = 59) correspondente (mesmo slip / ecr).
# Amostra de 100 slips liquidados nos ultimos 7 dias.
# ============================================================================
banner("STEP 5 - vendor.c_created_time vs fund.c_start_time (SB_BUYIN)")

run(
    """
    WITH sample_slips AS (
        SELECT
            c_bet_slip_id,
            c_customer_id,
            MIN(c_created_time) AS first_event_ct
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-03'
          AND c_created_time <  TIMESTAMP '2026-04-10'
          AND c_transaction_type = 'P'
        GROUP BY 1, 2
        LIMIT 200
    ),
    fund_buyins AS (
        SELECT
            t.c_ecr_id,
            t.c_start_time,
            t.c_amount_in_ecr_ccy
        FROM fund_ec2.tbl_real_fund_txn t
        WHERE t.c_start_time >= TIMESTAMP '2026-04-03'
          AND t.c_start_time <  TIMESTAMP '2026-04-11'
          AND t.c_product_id = 'SPORTS_BOOK'
          AND t.c_txn_status = 'SUCCESS'
          AND t.c_txn_type   = 59   -- SB_BUYIN
    ),
    ecr_mapping AS (
        SELECT c_ecr_id, c_external_id
        FROM ecr_ec2.tbl_ecr
    ),
    joined AS (
        SELECT
            s.c_bet_slip_id,
            s.first_event_ct  AS vendor_ct,
            fb.c_start_time   AS fund_st,
            date_diff('second', fb.c_start_time, s.first_event_ct) AS delta_s
        FROM sample_slips s
        JOIN ecr_mapping e ON CAST(s.c_customer_id AS VARCHAR) = CAST(e.c_external_id AS VARCHAR)
        JOIN fund_buyins fb ON fb.c_ecr_id = e.c_ecr_id
        -- Match pelo tempo mais proximo: diferenca em segundos < 600 (10 min)
        WHERE ABS(date_diff('second', fb.c_start_time, s.first_event_ct)) < 600
    )
    SELECT
        COUNT(*)                              AS n_matched,
        AVG(ABS(delta_s))                     AS avg_abs_delta_s,
        APPROX_PERCENTILE(ABS(delta_s), 0.5)  AS p50_abs_s,
        APPROX_PERCENTILE(ABS(delta_s), 0.95) AS p95_abs_s,
        MAX(ABS(delta_s))                     AS max_abs_s
    FROM joined
    """,
    db="vendor_ec2",
    label="Delta vendor.c_created_time vs fund.c_start_time",
)

print("\n[FIM] Auditoria concluida.")
