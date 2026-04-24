"""
Auditoria: decomposicao do sports_real_ggr diario em place-side vs settle-side
=============================================================================
Objetivo: validar quanto do GGR diario de fct_sports_activity vem de
transacoes de PLACE (SB_BUYIN/SB_LOCK_CANCEL) vs SETTLE (SB_WIN/SB_SETTLEMENT)
e se a contagem de bilhetes place-side bate com bet-creation-date do vendor.
"""

import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena


def banner(t):
    print("\n" + "=" * 90 + f"\n  {t}\n" + "=" * 90)


def run(sql, db="fund_ec2", label=""):
    try:
        df = query_athena(sql, database=db)
        print(f"\n[OK] {label} - {len(df)} linha(s)")
        if len(df) > 0:
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"\n[ERRO] {label}: {e}")


banner("STEP A - Distribuicao diaria c_txn_type SPORTS_BOOK")

run(
    """
    WITH sp AS (
        SELECT
            CAST(t.c_start_time AT TIME ZONE 'UTC'
                 AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
            t.c_txn_type,
            t.c_op_type,
            t.c_amount_in_ecr_ccy / 100.0 AS amt_brl
        FROM fund_ec2.tbl_real_fund_txn t
        WHERE t.c_start_time >= TIMESTAMP '2026-04-03'
          AND t.c_start_time <  TIMESTAMP '2026-04-10'
          AND t.c_product_id = 'SPORTS_BOOK'
          AND t.c_txn_status = 'SUCCESS'
    )
    SELECT
        c_txn_type,
        c_op_type,
        COUNT(*)     AS n,
        ROUND(SUM(amt_brl), 2) AS total_brl
    FROM sp
    GROUP BY 1, 2
    ORDER BY n DESC
    """,
    label="Tipos SB por volume (7d)",
)


banner("STEP B - qty_bets no fund pelo c_start_time vs qty_bets settled no vendor")

run(
    """
    WITH fund_place AS (
        -- Cada SB_BUYIN (type 59) representa UMA aposta colocada
        SELECT
            CAST(t.c_start_time AT TIME ZONE 'UTC'
                 AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
            COUNT(*) AS qty_buyins_fund
        FROM fund_ec2.tbl_real_fund_txn t
        JOIN ecr_ec2.tbl_ecr_flags ef ON t.c_ecr_id = ef.c_ecr_id
        WHERE t.c_start_time >= TIMESTAMP '2026-04-03'
          AND t.c_start_time <  TIMESTAMP '2026-04-10'
          AND t.c_product_id = 'SPORTS_BOOK'
          AND t.c_txn_status = 'SUCCESS'
          AND t.c_txn_type = 59  -- SB_BUYIN
          AND ef.c_test_user = false
        GROUP BY 1
    ),
    vendor_place AS (
        -- Rows M (place) no vendor
        SELECT
            CAST(c_created_time AT TIME ZONE 'UTC'
                 AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
            COUNT(DISTINCT c_bet_slip_id) AS qty_place_vendor
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-03'
          AND c_created_time <  TIMESTAMP '2026-04-10'
          AND c_transaction_type = 'M'
        GROUP BY 1
    ),
    vendor_settle AS (
        -- Rows P (settle)
        SELECT
            CAST(c_created_time AT TIME ZONE 'UTC'
                 AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dt,
            COUNT(DISTINCT c_bet_slip_id) AS qty_settle_vendor
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '2026-04-03'
          AND c_created_time <  TIMESTAMP '2026-04-10'
          AND c_transaction_type = 'P'
        GROUP BY 1
    )
    SELECT
        COALESCE(fp.dt, vp.dt, vs.dt) AS dt,
        fp.qty_buyins_fund     AS fund_place,
        vp.qty_place_vendor    AS vendor_place,
        vs.qty_settle_vendor   AS vendor_settle,
        -- delta_abs entre fund_place e vendor_place (deveriam bater)
        (fp.qty_buyins_fund - vp.qty_place_vendor) AS delta_fund_vs_vendor_place,
        -- delta_abs entre vendor_place e vendor_settle (hoje usado no pipeline)
        (vs.qty_settle_vendor - vp.qty_place_vendor) AS delta_settle_vs_place
    FROM fund_place fp
    FULL OUTER JOIN vendor_place  vp ON fp.dt = vp.dt
    FULL OUTER JOIN vendor_settle vs ON COALESCE(fp.dt, vp.dt) = vs.dt
    ORDER BY 1 DESC
    """,
    db="fund_ec2",
    label="Fund place vs Vendor place vs Vendor settle (diario)",
)

print("\n[FIM]")
