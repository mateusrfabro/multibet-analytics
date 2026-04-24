"""
Reconciliacao BTR (convertido) vs Bonus Emitido — 20/04/2026 MultiBet
======================================================================
Objetivo: entender de onde vem o "47k" se a Athena BTR mostra R$ 26k.

Hipoteses a testar:
  H1: Valor reportado e bonus EMITIDO (bonus_ec2), nao BTR (convertido)
  H2: Valor e BTR acumulado (dia+noite anterior, D-1 + D-0)
  H3: Valor inclui bonus ativo nao convertido
  H4: Outra fonte/calculo diferente

Fontes:
  - fund_ec2.tbl_realcash_sub_fund_txn -> BTR (type 20, CR) = dinheiro real creditado
  - bonus_ec2.tbl_bonus_summary_details -> bonus emitido (c_actual_issued_amount)
  - bonus_ec2.tbl_bonus_status_change   -> eventos de status (se existir)

Uso:
  python scripts/reconciliar_btr_vs_bonus_emitido_20abr.py
"""

import sys
import os
import traceback
from datetime import datetime

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena

pd.set_option("display.max_columns", 30)
pd.set_option("display.width", 240)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

HOJE_UTC_START = "2026-04-20 03:00:00"
HOJE_UTC_END   = "2026-04-21 03:00:00"
ONTEM_UTC_START = "2026-04-19 03:00:00"
ONTEM_UTC_END   = "2026-04-20 03:00:00"
D7_UTC_START   = "2026-04-13 03:00:00"

SEP = "=" * 90


def fmt_brl(v):
    if pd.isna(v):
        return "R$ 0,00"
    return f"R$ {v:,.2f}"


def run_query(desc, sql, database="fund_ec2"):
    print(f"\n{SEP}")
    print(f"  {desc}")
    print(f"{SEP}")
    try:
        df = query_athena(sql, database=database)
        if df.empty:
            print("  [VAZIO]")
        else:
            print(f"  {len(df)} linhas.\n")
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"  [ERRO] {e}")
        traceback.print_exc()
        return pd.DataFrame()


print(f"\n{'#' * 90}")
print(f"  RECONCILIACAO BTR vs BONUS EMITIDO -- 20/04/2026 (MultiBet BR)")
print(f"  Executado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'#' * 90}")


# =======================================================================
# H1: BONUS EMITIDO hoje -- bonus_ec2.tbl_bonus_summary_details
# Campo validado: c_actual_issued_amount (feedback_bonus_ec2_colunas_validadas)
# =======================================================================
sql_h1_schema = """
SHOW COLUMNS FROM bonus_ec2.tbl_bonus_summary_details
"""
df_schema = run_query("H1-schema: colunas de tbl_bonus_summary_details", sql_h1_schema, database="bonus_ec2")


# Tentativa com c_start_time (fund_ec2 usa c_start_time; bonus pode ser c_issued_time ou similar)
sql_h1 = f"""
SELECT
    date(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(*) AS qtd_bonus,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_emitido_brl,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    COUNT(DISTINCT c_ecr_id) AS jogadores
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_start_time >= TIMESTAMP '{D7_UTC_START}'
  AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
  AND c_actual_issued_amount > 0
GROUP BY 1
ORDER BY 1
"""
df_h1 = run_query("H1: Bonus EMITIDO diario (7d + hoje)", sql_h1, database="bonus_ec2")


# =======================================================================
# H2: BTR acumulado hoje + ontem (se Head viu total D-1+D-0)
# =======================================================================
sql_h2 = f"""
SELECT
    'Ontem (19/04)' AS periodo,
    COUNT(*) AS qtd,
    ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_brl
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{ONTEM_UTC_START}'
  AND c_start_time <  TIMESTAMP '{ONTEM_UTC_END}'
  AND c_txn_type = 20 AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0

UNION ALL

SELECT
    'Hoje (20/04 parcial)' AS periodo,
    COUNT(*) AS qtd,
    ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_brl
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
  AND c_txn_type = 20 AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0

UNION ALL

SELECT
    'Acumulado D-1 + D-0' AS periodo,
    COUNT(*) AS qtd,
    ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_brl
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{ONTEM_UTC_START}'
  AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
  AND c_txn_type = 20 AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0
"""
df_h2 = run_query("H2: BTR acumulado D-1 + D-0", sql_h2, database="fund_ec2")


# =======================================================================
# H3: Comparar em bireports (resumo BI) se tem campo de bonus
# =======================================================================
sql_h3 = f"""
SELECT
    date(c_ecr_start_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    ROUND(SUM(c_btr) / 100.0, 2) AS btr_bireports,
    ROUND(SUM(c_bonus_issued) / 100.0, 2) AS bonus_issued_bireports,
    ROUND(SUM(c_bonus_wagered) / 100.0, 2) AS bonus_wagered_bireports
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
WHERE c_ecr_start_date >= TIMESTAMP '{D7_UTC_START}'
  AND c_ecr_start_date <  TIMESTAMP '{HOJE_UTC_END}'
GROUP BY 1
ORDER BY 1
"""
df_h3 = run_query("H3: Bireports resumo BI (btr, issued, wagered)", sql_h3, database="bireports_ec2")


print(f"\n{SEP}")
print("  FIM RECONCILIACAO")
print(f"{SEP}")
