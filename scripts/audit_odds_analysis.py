"""
Auditoria: Validacao da analise de odds Win/Loss
=================================================
5 queries de validacao:
  1. Cross-check com ps_bi (GGR/Stake total sports_book)
  2. Deduplicacao (registros vs slips unicos)
  3. Distribuicao c_transaction_type (M vs P vs outros)
  4. Distribuicao c_total_return (NULL, NaN, ZERO, PARCIAL, WIN)
  5. Impacto de free bets (c_is_free = true)
  6. Filtro test users (com vs sem)
"""

import sys, os, logging
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


def main():
    log.info("=" * 70)
    log.info("AUDITORIA: Validacao da analise Win/Loss por faixa de odds")
    log.info("=" * 70)

    # --- 1. Cross-check com ps_bi ---
    log.info("\n=== VALIDACAO 1: Cross-check com ps_bi ===")
    q1 = """
    SELECT
        SUM(bet_amount_local) AS total_stake_psbi,
        SUM(win_amount_local) AS total_payout_psbi,
        SUM(ggr_local) AS ggr_psbi,
        COUNT(DISTINCT player_id) AS players_psbi,
        SUM(bet_count) AS total_bets_psbi
    FROM ps_bi.fct_casino_activity_daily
    WHERE LOWER(product_id) = 'sports_book'
      AND activity_date >= DATE '2025-10-01'
    """
    df1 = query_athena(q1, database="ps_bi")
    print("\nps_bi.fct_casino_activity_daily (sports_book desde 2025-10-01):")
    for _, row in df1.iterrows():
        print(f"  Stake:   R$ {row['total_stake_psbi']:,.2f}")
        print(f"  Payout:  R$ {row['total_payout_psbi']:,.2f}")
        print(f"  GGR:     R$ {row['ggr_psbi']:,.2f}")
        print(f"  Players: {int(row['players_psbi']):,}")
        print(f"  Bets:    {int(row['total_bets_psbi']):,}")

    print("\n  COMPARACAO com analise vendor_ec2:")
    print(f"  Analise: Stake R$ 97,830,861 | GGR R$ 5,424,218 | Bets ~1,186,978")
    stake_psbi = float(df1.iloc[0]['total_stake_psbi'])
    ggr_psbi = float(df1.iloc[0]['ggr_psbi'])
    bets_psbi = int(df1.iloc[0]['total_bets_psbi'])
    div_stake = abs(97830861 - stake_psbi) / stake_psbi * 100 if stake_psbi else 0
    div_ggr = abs(5424218 - ggr_psbi) / abs(ggr_psbi) * 100 if ggr_psbi else 0
    div_bets = abs(1186978 - bets_psbi) / bets_psbi * 100 if bets_psbi else 0
    print(f"  Divergencia Stake: {div_stake:.1f}%")
    print(f"  Divergencia GGR:   {div_ggr:.1f}%")
    print(f"  Divergencia Bets:  {div_bets:.1f}%")

    # --- 2. Deduplicacao ---
    log.info("\n=== VALIDACAO 2: Deduplicacao ===")
    q2 = """
    SELECT
        COUNT(*) AS total_registros,
        COUNT(DISTINCT c_bet_slip_id) AS slips_unicos,
        COUNT(*) - COUNT(DISTINCT c_bet_slip_id) AS duplicados
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2025-10-01'
    """
    df2 = query_athena(q2, database="vendor_ec2")
    print("\nDeduplicacao tbl_sports_book_bets_info (c_bet_state='C'):")
    for _, row in df2.iterrows():
        print(f"  Total registros: {int(row['total_registros']):,}")
        print(f"  Slips unicos:    {int(row['slips_unicos']):,}")
        print(f"  Duplicados:      {int(row['duplicados']):,}")
        dup_pct = row['duplicados'] / row['total_registros'] * 100
        print(f"  Taxa duplicacao: {dup_pct:.1f}%")

    # --- 3. Distribuicao c_transaction_type ---
    log.info("\n=== VALIDACAO 3: Distribuicao c_transaction_type ===")
    q3 = """
    SELECT
        c_transaction_type,
        COUNT(*) AS qty,
        COUNT(DISTINCT c_bet_slip_id) AS slips,
        ROUND(SUM(c_total_stake), 2) AS stake,
        ROUND(SUM(COALESCE(c_total_return, 0)), 2) AS payout
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2025-10-01'
    GROUP BY c_transaction_type
    ORDER BY qty DESC
    """
    df3 = query_athena(q3, database="vendor_ec2")
    print("\nDistribuicao c_transaction_type:")
    print(df3.to_string(index=False))

    # --- 4. Distribuicao c_total_return ---
    log.info("\n=== VALIDACAO 4: Distribuicao c_total_return (apos dedup rn=1) ===")
    q4 = """
    WITH dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'C'
          AND c_created_time >= TIMESTAMP '2025-10-01'
    )
    SELECT
        CASE
            WHEN c_total_return IS NULL THEN '1_NULL'
            WHEN c_total_return != c_total_return THEN '2_NaN'
            WHEN c_total_return = 0 THEN '3_ZERO (casa ganha)'
            WHEN c_total_return > 0 AND c_total_return < c_total_stake THEN '4_PARCIAL (return < stake)'
            WHEN c_total_return > 0 AND c_total_return = c_total_stake THEN '5_EMPATE (return = stake)'
            WHEN c_total_return > c_total_stake THEN '6_PLAYER_WIN (return > stake)'
            ELSE '7_NEGATIVO'
        END AS return_type,
        COUNT(*) AS qty,
        ROUND(SUM(c_total_stake), 2) AS stake,
        ROUND(SUM(COALESCE(c_total_return, 0)), 2) AS payout
    FROM dedup
    WHERE rn = 1
    GROUP BY 1
    ORDER BY 1
    """
    df4 = query_athena(q4, database="vendor_ec2")
    print("\nDistribuicao c_total_return (classificado):")
    print(df4.to_string(index=False))

    # --- 5. Impacto de free bets ---
    log.info("\n=== VALIDACAO 5: Free bets ===")
    q5 = """
    WITH dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'C'
          AND c_created_time >= TIMESTAMP '2025-10-01'
    )
    SELECT
        COALESCE(CAST(c_is_free AS VARCHAR), 'NULL') AS is_free,
        COUNT(*) AS qty,
        ROUND(SUM(c_total_stake), 2) AS stake,
        ROUND(SUM(COALESCE(c_total_return, 0)), 2) AS payout,
        ROUND(SUM(c_total_stake) - SUM(COALESCE(c_total_return, 0)), 2) AS ggr,
        ROUND(SUM(COALESCE(c_bonus_amount, 0)), 2) AS bonus_total
    FROM dedup
    WHERE rn = 1
    GROUP BY 1
    ORDER BY qty DESC
    """
    df5 = query_athena(q5, database="vendor_ec2")
    print("\nImpacto de free bets:")
    print(df5.to_string(index=False))

    # --- 6. Filtro test users ---
    log.info("\n=== VALIDACAO 6: Filtro test users ===")
    q6 = """
    WITH dedup AS (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_bet_state = 'C'
          AND c_created_time >= TIMESTAMP '2025-10-01'
    ),
    valid_players AS (
        SELECT CAST(c_external_id AS VARCHAR) AS ext_id
        FROM bireports_ec2.tbl_ecr
        WHERE c_test_user = false
    )
    SELECT
        COUNT(*) AS total_sem_filtro,
        SUM(CASE WHEN vp.ext_id IS NOT NULL THEN 1 ELSE 0 END) AS com_filtro,
        SUM(CASE WHEN vp.ext_id IS NULL THEN 1 ELSE 0 END) AS excluidos,
        ROUND(SUM(CASE WHEN vp.ext_id IS NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_excluidos
    FROM dedup d
    LEFT JOIN valid_players vp ON CAST(d.c_customer_id AS VARCHAR) = vp.ext_id
    WHERE d.rn = 1
    """
    df6 = query_athena(q6, database="vendor_ec2")
    print("\nFiltro test users:")
    print(df6.to_string(index=False))

    # --- Veredicto ---
    print("\n" + "=" * 70)
    print("AUDITORIA COMPLETA — verificar resultados acima")
    print("=" * 70)


if __name__ == "__main__":
    main()
