"""
Amostra exploratoria para entender:
1. Distribuicao de c_bonus_status, c_issue_type, c_criteria_type
2. Flags c_is_freebet / c_is_sportsbooktoken
3. Join com tbl_bonus_summary_details (c_actual_issued_amount)
4. Join com sub_fund_txn para BTR (type 20)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.athena import query_athena

# 1) Distribuicao de status + issue_type nos inativos de marco/2026
sql_status = """
SELECT
    c_bonus_status,
    c_issue_type,
    c_criteria_type,
    c_is_freebet,
    c_is_sportsbooktoken,
    COUNT(*) AS qtd
FROM bonus_ec2.tbl_ecr_bonus_details_inactive
WHERE c_created_time >= TIMESTAMP '2026-03-01'
  AND c_created_time <  TIMESTAMP '2026-04-01'
GROUP BY 1,2,3,4,5
ORDER BY qtd DESC
LIMIT 30
"""

# 2) Amostra de bonus com summary (valor)
sql_summary = """
SELECT
    b.c_ecr_bonus_id,
    b.c_bonus_id,
    b.c_ecr_id,
    b.c_bonus_status,
    b.c_issue_type,
    b.c_is_freebet,
    b.c_is_sportsbooktoken,
    b.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS created_brt,
    s.c_issue_date   AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS issue_brt,
    s.c_offered_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS offered_brt,
    s.c_actual_issued_amount / 100.0 AS valor_brl,
    s.c_bonus_status AS sum_status
FROM bonus_ec2.tbl_ecr_bonus_details_inactive b
LEFT JOIN bonus_ec2.tbl_bonus_summary_details s
  ON b.c_ecr_bonus_id = s.c_ecr_bonus_id
WHERE b.c_created_time >= TIMESTAMP '2026-03-20'
  AND b.c_created_time <  TIMESTAMP '2026-03-21'
  AND b.c_bonus_status = 'BONUS_ISSUED_OFFER'
LIMIT 10
"""

# 3) Verifica tipos de txn em sub_fund_txn no periodo + join com bonus
sql_subfund = """
SELECT
    c_txn_type,
    c_op_type,
    COUNT(*) AS qtd,
    SUM(c_amount_in_ecr_ccy)/100.0 AS total_brl
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-03-20'
  AND c_start_time <  TIMESTAMP '2026-03-21'
  AND c_op_type = 'CR'
GROUP BY 1,2
ORDER BY qtd DESC
LIMIT 20
"""

# 4) Campanhas ativas - quantos c_bonus_id distintos existem no periodo
sql_campaigns = """
SELECT
    b.c_bonus_id,
    COUNT(DISTINCT b.c_ecr_id) AS jogadores,
    COUNT(*) AS total_bonus,
    MIN(b.c_created_time) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS primeiro,
    MAX(b.c_created_time) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ultimo,
    SUM(CASE WHEN b.c_bonus_status = 'BONUS_ISSUED_OFFER' THEN 1 ELSE 0 END) AS resgatados,
    SUM(CASE WHEN b.c_bonus_status = 'EXPIRED' THEN 1 ELSE 0 END) AS expirados,
    SUM(CASE WHEN b.c_bonus_status = 'DROPPED' THEN 1 ELSE 0 END) AS dropped
FROM bonus_ec2.tbl_ecr_bonus_details_inactive b
WHERE b.c_created_time >= TIMESTAMP '2026-01-01'
  AND b.c_created_time <  TIMESTAMP '2026-04-18'
GROUP BY 1
ORDER BY total_bonus DESC
LIMIT 15
"""


def run(name, sql):
    print(f"\n{'=' * 70}")
    print(f"  {name}")
    print(f"{'=' * 70}")
    try:
        df = query_athena(sql)
        print(df.to_string(index=False, max_colwidth=40))
        print(f"\nLinhas: {len(df)}")
    except Exception as e:
        print(f"ERRO: {e}")


if __name__ == "__main__":
    run("1) Distribuicao status/issue_type (marco/2026)", sql_status)
    run("2) Amostra bonus + summary (bonus resgatados 20/03)", sql_summary)
    run("3) Tipos de txn em sub_fund_txn (CR em 20/03)", sql_subfund)
    run("4) Top 15 campanhas (c_bonus_id) jan-abr/2026", sql_campaigns)
