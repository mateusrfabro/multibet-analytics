"""
Debug: recencia do player 305245081792208985 reportada pelo Victor (VIPs).
Caso: PCR/segmentacao mostra recencia=28d mas Victor diz que joga todo dia.

Investigacao:
  1) Freshness atual de ps_bi.fct_player_activity_daily (fonte do PCR)
  2) Atividade do player nessa fato (ultimos 60 dias)
  3) Atividade do player na fonte raw alternativa (bireports_ec2.tbl_ecr_wise_daily_bi_summary)
  4) Ultima aposta/deposito real direto da raw (fund_ec2 + cashier_ec2)
  5) O que esta em multibet.pcr_ratings / pcr_atual / segmentacao_sa_diaria

ID recebido: 305245081792208985 (18 digitos -> provavelmente ecr_id/player_id)
"""
from __future__ import annotations
import sys
sys.path.insert(0, ".")

import pandas as pd
from db.athena import query_athena
from db.supernova import execute_supernova

PLAYER = "305245081792208985"

print("=" * 70)
print(f"DEBUG RECENCIA — player {PLAYER}")
print("=" * 70)

# ----------------------------------------------------------------------
# 1) Freshness das fontes
# ----------------------------------------------------------------------
print("\n[1] Freshness das fontes (max date)")
sql = """
SELECT 'ps_bi.fct_player_activity_daily' AS fonte,
       CAST(MAX(activity_date) AS VARCHAR) AS ultima
FROM ps_bi.fct_player_activity_daily
UNION ALL
SELECT 'bireports_ec2.tbl_ecr_wise_daily_bi_summary',
       CAST(MAX(c_created_date) AS VARCHAR)
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
UNION ALL
SELECT 'fund_ec2.tbl_real_fund_txn (BRT)',
       CAST(MAX(CAST(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)) AS VARCHAR)
FROM fund_ec2.tbl_real_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-04-01'
"""
df = query_athena(sql)
print(df.to_string(index=False))

# ----------------------------------------------------------------------
# 2) Existe o player? (descobre se 18 digitos e ecr_id/player_id mesmo)
# ----------------------------------------------------------------------
print("\n[2] Localizando o player em dim_user")
sql = f"""
SELECT ecr_id, external_id, registration_date, is_test, affiliate_id
FROM ps_bi.dim_user
WHERE CAST(ecr_id AS VARCHAR) = '{PLAYER}'
   OR CAST(external_id AS VARCHAR) = '{PLAYER}'
LIMIT 5
"""
df_user = query_athena(sql)
print(df_user.to_string(index=False))
if df_user.empty:
    print("Player NAO encontrado. Abortando.")
    sys.exit(1)
ecr_id = str(df_user.iloc[0]["ecr_id"])
print(f"ecr_id resolvido: {ecr_id}")

# ----------------------------------------------------------------------
# 3) Atividade na FATO ps_bi (o que o PCR enxerga)
# ----------------------------------------------------------------------
print("\n[3] Atividade em ps_bi.fct_player_activity_daily — ultimos 60 dias")
sql = f"""
SELECT activity_date,
       casino_realbet_count,
       sb_realbet_count,
       deposit_success_count,
       ROUND(casino_realbet_base, 2) AS casino_bet_brl,
       ROUND(sb_realbet_base, 2) AS sb_bet_brl,
       ROUND(deposit_success_base, 2) AS dep_brl
FROM ps_bi.fct_player_activity_daily
WHERE player_id = {ecr_id}
  AND activity_date >= CURRENT_DATE - INTERVAL '60' DAY
ORDER BY activity_date DESC
"""
df_fct = query_athena(sql)
print(f"Linhas em fct_player_activity_daily (60d): {len(df_fct)}")
if not df_fct.empty:
    print(df_fct.head(20).to_string(index=False))
    print(f"\nUltima activity_date no fct: {df_fct['activity_date'].max()}")

# ----------------------------------------------------------------------
# 4) Atividade na RAW alternativa
# ----------------------------------------------------------------------
print("\n[4] Atividade em bireports_ec2.tbl_ecr_wise_daily_bi_summary — ultimos 60 dias")
sql = f"""
SELECT s.c_created_date AS dia,
       s.c_casino_realcash_bet_amount/100.0 AS casino_bet_brl,
       s.c_sb_realcash_bet_amount/100.0     AS sb_bet_brl,
       s.c_deposit_success_amount/100.0     AS dep_brl
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
WHERE s.c_ecr_id = {ecr_id}
  AND s.c_created_date >= CURRENT_DATE - INTERVAL '60' DAY
  AND (s.c_casino_realcash_bet_amount > 0
       OR s.c_sb_realcash_bet_amount > 0
       OR s.c_deposit_success_amount > 0)
ORDER BY s.c_created_date DESC
"""
df_raw = query_athena(sql)
print(f"Linhas em bireports raw (60d): {len(df_raw)}")
if not df_raw.empty:
    print(df_raw.head(20).to_string(index=False))
    print(f"\nUltima atividade real (raw): {df_raw['dia'].max()}")

# ----------------------------------------------------------------------
# 5) O que esta gravado em multibet.pcr_ratings / segmentacao_sa
# ----------------------------------------------------------------------
print("\n[5] Estado atual em multibet (Super Nova DB)")
try:
    rows = execute_supernova(
        """
        SELECT snapshot_date, rating, recency_days
        FROM multibet.pcr_ratings
        WHERE player_id = %s
        ORDER BY snapshot_date DESC
        LIMIT 10
        """,
        params=(int(ecr_id),), fetch=True,
    )
    print("\nmultibet.pcr_ratings (ultimas 10 rodadas):")
    for r in rows:
        print(f"  {r}")
except Exception as e:
    print(f"  Erro: {e}")

try:
    rows = execute_supernova(
        """
        SELECT snapshot_date, rating, recency_days, "LIFECYCLE_STATUS"
        FROM multibet.segmentacao_sa_diaria
        WHERE player_id = %s
        ORDER BY snapshot_date DESC
        LIMIT 10
        """,
        params=(int(ecr_id),), fetch=True,
    )
    print("\nmultibet.segmentacao_sa_diaria (ultimas 10 rodadas):")
    for r in rows:
        print(f"  {r}")
except Exception as e:
    print(f"  Erro: {e}")

print("\n" + "=" * 70)
print("FIM DEBUG")
print("=" * 70)
