"""
Calibracao das 2 novas tags aprovadas pelo Castrin em 15/05:

Tag A - CANCEL_HEAVY_DAILY: >=10 c_txn_type=72 em UM dia -> -10
Tag B - CRASH_FARMER (soft): 2+ jogos simultaneos sendo 1 crash -> -5 ou -10

Calibracao na base de 30 dias.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

# ======================================================================
# Tag A - CANCEL_HEAVY_DAILY
# ======================================================================
print("=" * 80)
print("TAG A - CANCEL_HEAVY_DAILY: >=N cancels (c_txn_type=72) em UM dia")
print("=" * 80)

sql_a = """
WITH cancels_por_dia AS (
  SELECT
    c_ecr_id,
    CAST(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dia_brt,
    COUNT(*) AS n_cancels
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_txn_type = 72
    AND c_txn_status = 'SUCCESS'
    AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  GROUP BY c_ecr_id, CAST(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
),
players_max_dia AS (
  SELECT
    c_ecr_id,
    MAX(n_cancels) AS max_cancels_dia,
    COUNT(*) AS dias_com_cancel
  FROM cancels_por_dia
  GROUP BY c_ecr_id
)
SELECT
  CASE
    WHEN max_cancels_dia = 1 THEN '01 - 1/dia max'
    WHEN max_cancels_dia BETWEEN 2 AND 4 THEN '02 - 2-4/dia max'
    WHEN max_cancels_dia BETWEEN 5 AND 9 THEN '03 - 5-9/dia max'
    WHEN max_cancels_dia BETWEEN 10 AND 19 THEN '04 - 10-19/dia (THRESHOLD CASTRIN)'
    WHEN max_cancels_dia BETWEEN 20 AND 49 THEN '05 - 20-49/dia'
    WHEN max_cancels_dia BETWEEN 50 AND 99 THEN '06 - 50-99/dia'
    ELSE '07 - >=100/dia'
  END AS faixa,
  COUNT(*) AS players,
  ROUND(AVG(dias_com_cancel), 1) AS media_dias_com_cancel
FROM players_max_dia
GROUP BY 1
ORDER BY 1
"""
df_a = query_athena(sql_a, database="fund_ec2")
print(df_a.to_string(index=False))


# Spot check 30311442
print("\n--- 30311442 nesse perfil ---")
sql_a_case = """
SELECT
  CAST(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dia_brt,
  COUNT(*) AS n_cancels
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = 440908751792034786
  AND c_txn_type = 72
  AND c_txn_status = 'SUCCESS'
  AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
GROUP BY CAST(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
ORDER BY dia_brt
"""
print(query_athena(sql_a_case, database="fund_ec2").to_string(index=False))


# ======================================================================
# Tag B - CRASH_FARMER (soft): 2+ jogos simultaneos, 1 crash
# ======================================================================
print("\n\n" + "=" * 80)
print("TAG B - CRASH_FARMER (soft): 2+ jogos simultaneos em janela curta, 1 crash")
print("=" * 80)
print("Premissa: c_game_category=158 = MINES (crash). Vou usar isso como proxy.")
print()

# Hipotese: player tem bet em jogo crash (158) e bet em outro c_game_id no mesmo periodo curto
# Vou contar quantos players tem >=N "sobreposicoes"
sql_b = """
WITH bets_15min_bins AS (
  SELECT
    c_ecr_id,
    -- bin de 15min
    CAST(date_trunc('hour', c_start_time) AS TIMESTAMP)
      + (CAST(floor(EXTRACT(MINUTE FROM c_start_time) / 15) AS INTEGER) * INTERVAL '15' MINUTE) AS bin15,
    c_game_id,
    c_game_category,
    COUNT(*) AS n_bets
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_txn_type = 27
    AND c_txn_status = 'SUCCESS'
    AND c_game_id IS NOT NULL
    AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  GROUP BY c_ecr_id,
    CAST(date_trunc('hour', c_start_time) AS TIMESTAMP)
      + (CAST(floor(EXTRACT(MINUTE FROM c_start_time) / 15) AS INTEGER) * INTERVAL '15' MINUTE),
    c_game_id,
    c_game_category
),
overlaps AS (
  -- bins onde player joga >=2 c_game_id distintos E pelo menos um e crash (158)
  SELECT
    c_ecr_id,
    bin15,
    COUNT(DISTINCT c_game_id) AS jogos_distintos,
    MAX(CASE WHEN c_game_category = 158 THEN 1 ELSE 0 END) AS tem_crash
  FROM bets_15min_bins
  GROUP BY c_ecr_id, bin15
  HAVING COUNT(DISTINCT c_game_id) >= 2
     AND MAX(CASE WHEN c_game_category = 158 THEN 1 ELSE 0 END) = 1
),
players AS (
  SELECT
    c_ecr_id,
    COUNT(*) AS n_overlaps,
    MAX(jogos_distintos) AS max_jogos_simultaneo
  FROM overlaps
  GROUP BY c_ecr_id
)
SELECT
  CASE
    WHEN n_overlaps = 1 THEN '01 - 1x overlap (pode ser acidente)'
    WHEN n_overlaps BETWEEN 2 AND 5 THEN '02 - 2-5x'
    WHEN n_overlaps BETWEEN 6 AND 20 THEN '03 - 6-20x'
    WHEN n_overlaps BETWEEN 21 AND 100 THEN '04 - 21-100x (multitasker)'
    ELSE '05 - >100x (poweruser/farmer)'
  END AS faixa,
  COUNT(*) AS players,
  ROUND(AVG(max_jogos_simultaneo), 1) AS avg_max_jogos
FROM players
GROUP BY 1
ORDER BY 1
"""
df_b = query_athena(sql_b, database="fund_ec2")
print(df_b.to_string(index=False))


# Spot check 30311442
print("\n--- 30311442 nesse perfil ---")
sql_b_case = """
WITH bets_15min_bins AS (
  SELECT
    CAST(date_trunc('hour', c_start_time) AS TIMESTAMP)
      + (CAST(floor(EXTRACT(MINUTE FROM c_start_time) / 15) AS INTEGER) * INTERVAL '15' MINUTE) AS bin15,
    c_game_id,
    c_game_category,
    COUNT(*) AS n_bets
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_ecr_id = 440908751792034786
    AND c_txn_type = 27
    AND c_txn_status = 'SUCCESS'
    AND c_game_id IS NOT NULL
    AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  GROUP BY 1, 2, 3
)
SELECT
  bin15,
  COUNT(DISTINCT c_game_id) AS jogos_distintos,
  MAX(CASE WHEN c_game_category = 158 THEN 1 ELSE 0 END) AS tem_crash,
  array_agg(DISTINCT c_game_id) AS lista_game_ids
FROM bets_15min_bins
GROUP BY bin15
HAVING COUNT(DISTINCT c_game_id) >= 2
ORDER BY bin15
"""
print(query_athena(sql_b_case, database="fund_ec2").to_string(index=False))
