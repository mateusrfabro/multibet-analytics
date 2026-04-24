"""
Analise: Distribuicao Win/Loss por Faixa de Odds (perspectiva da casa)
======================================================================
Objetivo: Quantas bets em cada faixa de odds a casa ganha vs perde
Fonte: vendor_ec2.tbl_sports_book_bets_info (Athena)
Periodo: desde 2026-01-01
Saida: reports/odds_win_loss_*.csv
"""

import sys, os, logging
import pandas as pd
from datetime import datetime

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# =============================================================================
# Query 1: Distribuicao geral por faixa de odds
# =============================================================================
# Logica:
#   - Deduplicar por c_bet_slip_id (ultimo registro = estado final)
#   - Filtrar bets liquidadas (c_bet_state = 'C')
#   - Filtrar test users via bireports_ec2.tbl_ecr
#   - Classificar por faixa de odds
#   - Casa ganha = player perde = c_total_return <= 0 (ou NaN/NULL)
#   - Casa perde = player ganha = c_total_return > 0
#   - Valores em BRL real (vendor_ec2 NAO divide por 100)

QUERY_ODDS_RANGES = """
WITH
valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

-- Deduplicar: ultimo registro por bet_slip_id (estado settled)
raw_bets AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        c_total_return,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 0), 9999) AS odds,
        c_bet_state,
        c_bet_type,
        c_is_live,
        c_created_time,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2026-01-01'
),

bets AS (
    SELECT rb.*,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN '1.01 - 2.00'
            WHEN odds > 2.00  AND odds <= 5.00 THEN '2.01 - 5.00'
            WHEN odds > 5.00  AND odds <= 20.00 THEN '5.01 - 20.00'
            WHEN odds > 20.00 THEN '20.00+'
            ELSE 'Invalido'
        END AS odds_range,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN 1
            WHEN odds > 2.00  AND odds <= 5.00 THEN 2
            WHEN odds > 5.00  AND odds <= 20.00 THEN 3
            WHEN odds > 20.00 THEN 4
            ELSE 0
        END AS odds_order
    FROM raw_bets rb
    WHERE rn = 1
      AND odds >= 1.01
)

SELECT
    b.odds_range,
    b.odds_order,

    -- Volume
    COUNT(*) AS total_bets,
    COUNT(DISTINCT b.c_customer_id) AS unique_players,

    -- Resultado da CASA
    SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) AS bets_casa_ganha,
    SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) AS bets_casa_perde,

    -- Win rate da casa
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) * 100.0 / COUNT(*), 2) AS pct_casa_ganha,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS pct_casa_perde,

    -- Financeiro (BRL)
    ROUND(SUM(b.c_total_stake), 2) AS total_stake_brl,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END), 2) AS total_payout_brl,
    ROUND(SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END), 2) AS ggr_brl,

    -- Hold rate
    ROUND(
        (SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END))
        * 100.0 / NULLIF(SUM(b.c_total_stake), 0), 2
    ) AS hold_rate_pct,

    -- Odds e ticket
    ROUND(AVG(b.odds), 2) AS avg_odds,
    ROUND(APPROX_PERCENTILE(b.odds, 0.5), 2) AS median_odds,
    ROUND(AVG(b.c_total_stake), 2) AS avg_ticket_brl,

    -- GGR medio por bet
    ROUND(
        (SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END))
        / NULLIF(COUNT(*), 0), 2
    ) AS ggr_medio_por_bet

FROM bets b
JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
GROUP BY b.odds_range, b.odds_order
ORDER BY b.odds_order
"""


# =============================================================================
# Query 2: Tendencia mensal por faixa
# =============================================================================
QUERY_MONTHLY = """
WITH
valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

raw_bets AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        c_total_return,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 0), 9999) AS odds,
        c_bet_state,
        c_created_time,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2026-01-01'
),

bets AS (
    SELECT rb.*,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN '1.01 - 2.00'
            WHEN odds > 2.00  AND odds <= 5.00 THEN '2.01 - 5.00'
            WHEN odds > 5.00  AND odds <= 20.00 THEN '5.01 - 20.00'
            WHEN odds > 20.00 THEN '20.00+'
            ELSE 'Invalido'
        END AS odds_range,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN 1
            WHEN odds > 2.00  AND odds <= 5.00 THEN 2
            WHEN odds > 5.00  AND odds <= 20.00 THEN 3
            WHEN odds > 20.00 THEN 4
            ELSE 0
        END AS odds_order
    FROM raw_bets rb
    WHERE rn = 1
      AND odds >= 1.01
)

SELECT
    DATE_FORMAT(
        b.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo',
        '%Y-%m'
    ) AS mes,
    b.odds_range,
    b.odds_order,
    COUNT(*) AS total_bets,
    SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) AS bets_casa_ganha,
    SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) AS bets_casa_perde,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) * 100.0 / COUNT(*), 2) AS pct_casa_ganha,
    ROUND(SUM(b.c_total_stake), 2) AS total_stake_brl,
    ROUND(SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END), 2) AS ggr_brl,
    ROUND(
        (SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END))
        * 100.0 / NULLIF(SUM(b.c_total_stake), 0), 2
    ) AS hold_rate_pct
FROM bets b
JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
GROUP BY 1, b.odds_range, b.odds_order
ORDER BY 1, b.odds_order
"""


# =============================================================================
# Query 3: Live vs PreMatch por faixa
# =============================================================================
QUERY_LIVE_SPLIT = """
WITH
valid_players AS (
    SELECT CAST(c_external_id AS VARCHAR) AS ext_id
    FROM bireports_ec2.tbl_ecr
    WHERE c_test_user = false
),

raw_bets AS (
    SELECT
        c_customer_id,
        c_bet_slip_id,
        c_total_stake,
        c_total_return,
        LEAST(COALESCE(TRY_CAST(c_total_odds AS DOUBLE), 0), 9999) AS odds,
        c_bet_state,
        CASE WHEN c_bet_type = 'Live' OR c_is_live = true
             THEN 'Live' ELSE 'PreMatch' END AS bet_mode,
        ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id ORDER BY c_updated_time DESC) AS rn
    FROM vendor_ec2.tbl_sports_book_bets_info
    WHERE c_bet_state = 'C'
      AND c_created_time >= TIMESTAMP '2026-01-01'
),

bets AS (
    SELECT rb.*,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN '1.01 - 2.00'
            WHEN odds > 2.00  AND odds <= 5.00 THEN '2.01 - 5.00'
            WHEN odds > 5.00  AND odds <= 20.00 THEN '5.01 - 20.00'
            WHEN odds > 20.00 THEN '20.00+'
            ELSE 'Invalido'
        END AS odds_range,
        CASE
            WHEN odds >= 1.01 AND odds <= 2.00 THEN 1
            WHEN odds > 2.00  AND odds <= 5.00 THEN 2
            WHEN odds > 5.00  AND odds <= 20.00 THEN 3
            WHEN odds > 20.00 THEN 4
            ELSE 0
        END AS odds_order
    FROM raw_bets rb
    WHERE rn = 1
      AND odds >= 1.01
)

SELECT
    b.odds_range,
    b.odds_order,
    b.bet_mode,
    COUNT(*) AS total_bets,
    SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) AS bets_casa_ganha,
    SUM(CASE WHEN b.c_total_return > 0 THEN 1 ELSE 0 END) AS bets_casa_perde,
    ROUND(SUM(CASE WHEN b.c_total_return > 0 THEN 0 ELSE 1 END) * 100.0 / COUNT(*), 2) AS pct_casa_ganha,
    ROUND(SUM(b.c_total_stake), 2) AS total_stake_brl,
    ROUND(SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END), 2) AS ggr_brl,
    ROUND(
        (SUM(b.c_total_stake) - SUM(CASE WHEN b.c_total_return > 0 THEN b.c_total_return ELSE 0 END))
        * 100.0 / NULLIF(SUM(b.c_total_stake), 0), 2
    ) AS hold_rate_pct
FROM bets b
JOIN valid_players vp ON CAST(b.c_customer_id AS VARCHAR) = vp.ext_id
GROUP BY b.odds_range, b.odds_order, b.bet_mode
ORDER BY b.odds_order, b.bet_mode
"""


def main():
    log.info("=" * 70)
    log.info("ANALISE: Win/Loss por Faixa de Odds (perspectiva da casa)")
    log.info("Fonte: vendor_ec2.tbl_sports_book_bets_info")
    log.info("Periodo: desde 2026-01-01")
    log.info("=" * 70)

    os.makedirs("reports", exist_ok=True)

    # ---- Query 1: Distribuicao geral ----
    log.info("\n--- Query 1: Distribuicao geral por faixa de odds ---")
    df_geral = query_athena(QUERY_ODDS_RANGES, database="vendor_ec2")

    if df_geral.empty:
        log.error("Nenhum dado retornado na query geral!")
        return

    print("\n" + "=" * 100)
    print("DISTRIBUICAO WIN/LOSS POR FAIXA DE ODDS  —  PERSPECTIVA DA CASA")
    print("=" * 100)
    print(df_geral.to_string(index=False))

    df_geral.to_csv("reports/odds_win_loss_geral.csv", index=False)
    log.info("Salvo: reports/odds_win_loss_geral.csv")

    # ---- Query 2: Tendencia mensal ----
    log.info("\n--- Query 2: Tendencia mensal por faixa ---")
    df_mensal = query_athena(QUERY_MONTHLY, database="vendor_ec2")

    print("\n" + "=" * 100)
    print("TENDENCIA MENSAL POR FAIXA DE ODDS")
    print("=" * 100)
    print(df_mensal.to_string(index=False))

    df_mensal.to_csv("reports/odds_win_loss_mensal.csv", index=False)
    log.info("Salvo: reports/odds_win_loss_mensal.csv")

    # ---- Query 3: Live vs PreMatch ----
    log.info("\n--- Query 3: Live vs PreMatch por faixa ---")
    df_live = query_athena(QUERY_LIVE_SPLIT, database="vendor_ec2")

    print("\n" + "=" * 100)
    print("LIVE vs PREMATCH POR FAIXA DE ODDS")
    print("=" * 100)
    print(df_live.to_string(index=False))

    df_live.to_csv("reports/odds_win_loss_live_prematch.csv", index=False)
    log.info("Salvo: reports/odds_win_loss_live_prematch.csv")

    # ---- Resumo executivo ----
    print("\n" + "=" * 100)
    print("RESUMO EXECUTIVO")
    print("=" * 100)

    total_bets = df_geral["total_bets"].sum()
    total_stake = df_geral["total_stake_brl"].sum()
    total_ggr = df_geral["ggr_brl"].sum()
    total_payout = df_geral["total_payout_brl"].sum()
    overall_hold = (total_ggr / total_stake * 100) if total_stake > 0 else 0

    print(f"Total de apostas liquidadas: {total_bets:,.0f}")
    print(f"Volume apostado (Stake):     R$ {total_stake:,.2f}")
    print(f"Total pago (Payout):         R$ {total_payout:,.2f}")
    print(f"GGR total:                   R$ {total_ggr:,.2f}")
    print(f"Hold rate geral:             {overall_hold:.2f}%")

    for _, row in df_geral.iterrows():
        pct_vol = row["total_bets"] / total_bets * 100 if total_bets > 0 else 0
        pct_ggr = row["ggr_brl"] / total_ggr * 100 if total_ggr != 0 else 0
        print(f"\n  {row['odds_range']}:")
        print(f"    Bets: {row['total_bets']:,.0f} ({pct_vol:.1f}% do volume)")
        print(f"    Casa ganha: {row['bets_casa_ganha']:,.0f} ({row['pct_casa_ganha']:.1f}%)")
        print(f"    Casa perde: {row['bets_casa_perde']:,.0f} ({row['pct_casa_perde']:.1f}%)")
        print(f"    GGR: R$ {row['ggr_brl']:,.2f} ({pct_ggr:.1f}% do GGR)")
        print(f"    Hold rate: {row['hold_rate_pct']:.2f}%")
        print(f"    Ticket medio: R$ {row['avg_ticket_brl']:.2f}")


if __name__ == "__main__":
    main()
