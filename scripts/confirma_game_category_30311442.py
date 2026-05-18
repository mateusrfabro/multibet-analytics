"""
Confirma c_game_category do exploit do 30311442 (bet R$400 + rollback R$400)
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

sql = """
SELECT
  c_txn_id,
  c_txn_type,
  c_game_id,
  c_game_category,
  c_game_type,
  c_sub_vendor_id,
  c_amount_in_ecr_ccy/100.0 AS amount_brl,
  c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_brt
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = 440908751792034786
  AND c_start_time >= TIMESTAMP '2026-05-13 21:40:00'
  AND c_start_time <  TIMESTAMP '2026-05-13 23:00:00'
  AND c_txn_status = 'SUCCESS'
ORDER BY c_start_time
"""
df = query_athena(sql, database="fund_ec2")
print(df.to_string(index=False))

print("\n\n--- Distribuicao por c_game_category ---")
sql2 = """
SELECT
  c_game_category,
  c_txn_type,
  COUNT(*) AS qtd
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = 440908751792034786
  AND c_start_time >= TIMESTAMP '2026-05-13 00:00:00'
  AND c_start_time <  TIMESTAMP '2026-05-14 00:00:00'
  AND c_txn_status = 'SUCCESS'
GROUP BY c_game_category, c_txn_type
ORDER BY c_game_category, c_txn_type
"""
df2 = query_athena(sql2, database="fund_ec2")
print(df2.to_string(index=False))
