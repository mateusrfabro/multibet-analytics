"""
Diagnostico: por que 30311442 nao caiu na regra MINES_PENDING_ABUSER?

Vou desenhar a sequencia minuto-a-minuto do exploit dele.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

CASE_ECR_ID = 440908751792034786

print("=" * 80)
print("Resumo MINUTO-A-MINUTO do 30311442 em 13/05/2026")
print("=" * 80)

sql = f"""
SELECT
  date_trunc('minute', c_start_time) AS minuto_utc,
  c_txn_type,
  COUNT(*) AS qtd,
  ROUND(SUM(c_amount_in_ecr_ccy) / 100.0, 2) AS soma_brl,
  ROUND(MIN(c_amount_in_ecr_ccy) / 100.0, 2) AS min_brl,
  ROUND(MAX(c_amount_in_ecr_ccy) / 100.0, 2) AS max_brl
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_start_time >= TIMESTAMP '2026-05-13 18:00:00'
  AND c_start_time <  TIMESTAMP '2026-05-14 06:00:00'
  AND c_txn_status = 'SUCCESS'
GROUP BY date_trunc('minute', c_start_time), c_txn_type
ORDER BY minuto_utc, c_txn_type
"""
df = query_athena(sql, database="fund_ec2")
print(df.to_string(index=False))


print("\n\n" + "=" * 80)
print("Sao os rollbacks 72 SOMENTE no horario do burst?")
print("=" * 80)
sql2 = f"""
SELECT
  c_txn_id,
  c_txn_type,
  c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_brt,
  date_trunc('minute', c_start_time) AS minuto_utc,
  ROUND(c_amount_in_ecr_ccy / 100.0, 2) AS amount_brl,
  c_session_id
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_start_time >= TIMESTAMP '2026-05-13 22:00:00'
  AND c_start_time <  TIMESTAMP '2026-05-14 02:00:00'
  AND c_txn_status = 'SUCCESS'
  AND c_txn_type IN (72, 76, 91, 77, 113, 86, 114)  -- todos rollback/cancel
ORDER BY c_start_time
"""
df2 = query_athena(sql2, database="fund_ec2")
print(df2.to_string(index=False))


print("\n\n" + "=" * 80)
print("Quantos bets PEQUENOS por minuto na sessao de burst?")
print("=" * 80)
sql3 = f"""
SELECT
  date_trunc('minute', c_start_time) AS minuto_utc,
  COUNT(*) AS n_bets_pequenos,
  COUNT(DISTINCT c_session_id) AS n_sessoes_distintas
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_start_time >= TIMESTAMP '2026-05-13 22:00:00'
  AND c_start_time <  TIMESTAMP '2026-05-14 02:00:00'
  AND c_txn_status = 'SUCCESS'
  AND c_txn_type = 27
  AND c_amount_in_ecr_ccy BETWEEN 40 AND 200
GROUP BY date_trunc('minute', c_start_time)
ORDER BY minuto_utc
"""
df3 = query_athena(sql3, database="fund_ec2")
print(df3.to_string(index=False))
