"""
Investigacao: Divergencia ps_bi vs vendor_ec2 (35% stake, 46% GGR)
===================================================================
Hipoteses:
  H1: ps_bi conta bets abertas (O) + settled (C), vendor_ec2 so settled
  H2: ps_bi conta por leg, vendor_ec2 por slip
  H3: product_id inclui virtuais/Kiron
  H4: Diferenca de dedup (vendor_ec2 dedup por slip, ps_bi nao)
  H5: Refunds (R) nao estao no vendor_ec2 filtrado
"""

import sys, os, logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    log.info("=" * 70)
    log.info("INVESTIGACAO: Divergencia ps_bi vs vendor_ec2 (35%)")
    log.info("=" * 70)

    # --- H1: vendor_ec2 TODOS os estados (nao so C) ---
    log.info("\n=== H1: Vendor_ec2 por c_bet_state (incluindo Open) ===")
    q1 = """
    SELECT
        c_bet_state,
        COUNT(DISTINCT c_bet_slip_id) AS slips,
        COUNT(*) AS registros,
        ROUND(SUM(c_total_stake), 2) AS stake,
        ROUND(SUM(COALESCE(c_total_return, 0)), 2) AS payout
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_created_time >= TIMESTAMP '2025-10-01'
    GROUP BY c_bet_state
    ORDER BY slips DESC
    """
    df1 = query_athena(q1, database="vendor_ec2")
    print("\nvendor_ec2 por c_bet_state:")
    print(df1.to_string(index=False))
    total_stake_all = df1['stake'].sum()
    total_slips_all = df1['slips'].sum()
    print(f"\n  TOTAL stake (todos estados): R$ {total_stake_all:,.2f}")
    print(f"  TOTAL slips (todos estados): {total_slips_all:,.0f}")
    print(f"  ps_bi stake: R$ 150,603,047.80")
    div = abs(total_stake_all - 150603047.80) / 150603047.80 * 100
    print(f"  Divergencia: {div:.1f}%")

    # --- H2: vendor_ec2 por c_transaction_type TODOS estados ---
    log.info("\n=== H2: Todos transaction_types (incluindo Open) ===")
    q2 = """
    SELECT
        c_transaction_type,
        c_bet_state,
        COUNT(*) AS registros,
        COUNT(DISTINCT c_bet_slip_id) AS slips,
        ROUND(SUM(c_total_stake), 2) AS stake
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_created_time >= TIMESTAMP '2025-10-01'
    GROUP BY c_transaction_type, c_bet_state
    ORDER BY registros DESC
    """
    df2 = query_athena(q2, database="vendor_ec2")
    print("\nvendor_ec2 por transaction_type x bet_state:")
    print(df2.to_string(index=False))

    # --- H3: ps_bi product_id breakdown ---
    log.info("\n=== H3: ps_bi product_id breakdown (sports%) ===")
    q3 = """
    SELECT
        product_id,
        COUNT(DISTINCT player_id) AS players,
        SUM(bet_count) AS bets,
        ROUND(SUM(bet_amount_local), 2) AS stake,
        ROUND(SUM(ggr_local), 2) AS ggr
    FROM ps_bi.fct_casino_activity_daily
    WHERE activity_date >= DATE '2025-10-01'
      AND LOWER(product_id) LIKE '%sport%'
    GROUP BY product_id
    ORDER BY stake DESC
    """
    df3 = query_athena(q3, database="ps_bi")
    print("\nps_bi product_ids com 'sport':")
    print(df3.to_string(index=False))

    # --- H4: ps_bi mensal vs vendor_ec2 mensal ---
    log.info("\n=== H4: Comparacao mensal ps_bi vs vendor_ec2 ===")
    q4a = """
    SELECT
        DATE_FORMAT(activity_date, '%Y-%m') AS mes,
        SUM(bet_count) AS bets,
        ROUND(SUM(bet_amount_local), 2) AS stake,
        ROUND(SUM(ggr_local), 2) AS ggr
    FROM ps_bi.fct_casino_activity_daily
    WHERE LOWER(product_id) = 'sports_book'
      AND activity_date >= DATE '2025-10-01'
    GROUP BY 1 ORDER BY 1
    """
    df4a = query_athena(q4a, database="ps_bi")
    print("\nps_bi mensal (sports_book):")
    print(df4a.to_string(index=False))

    q4b = """
    WITH dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'C'
          AND c_created_time >= TIMESTAMP '2025-10-01'
    )
    SELECT
        DATE_FORMAT(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo', '%Y-%m') AS mes,
        COUNT(*) AS bets,
        ROUND(SUM(c_total_stake), 2) AS stake,
        ROUND(SUM(c_total_stake) - SUM(COALESCE(c_total_return, 0)), 2) AS ggr
    FROM dedup WHERE rn = 1
    GROUP BY 1 ORDER BY 1
    """
    df4b = query_athena(q4b, database="vendor_ec2")
    print("\nvendor_ec2 mensal (settled, dedup):")
    print(df4b.to_string(index=False))

    # --- Veredicto ---
    print("\n" + "=" * 70)
    print("INVESTIGACAO COMPLETA")
    print("=" * 70)


if __name__ == "__main__":
    main()
