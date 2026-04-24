"""
Investiga qual timestamp realmente representa a conversao BTR.

Hipoteses:
- c_created_time    = criacao do registro bonus
- c_offered_date    = oferta feita ao player
- c_issue_date      = emissao / credit na carteira bonus
- c_claimed_date    = player fez claim (opt-in) OU wagering batido
- c_win_issue_timestamp = wagering completo, bonus virou real cash (BTR real)
- c_updated_time (inactive) = momento que mudou de ativo pra inativo

Comparar com sub_fund_txn type 20 (BTR efetivo)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.athena import query_athena


sql_detalhado = """
SELECT
    b.c_ecr_bonus_id,
    b.c_bonus_status,
    (b.c_created_time         AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS created,
    (b.c_win_issue_timestamp  AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS win_issue_ts,
    (b.c_claimed_date         AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS claimed,
    (b.c_claim_last_date      AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS claim_last,
    (b.c_updated_time         AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS updated,
    (s.c_offered_date         AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS offered,
    (s.c_issue_date           AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS issued,
    (s.c_win_issue_timestamp  AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS s_win_issue_ts,
    s.c_actual_issued_amount / 100.0 AS valor_brl
FROM bonus_ec2.tbl_ecr_bonus_details_inactive b
LEFT JOIN bonus_ec2.tbl_bonus_summary_details s
  ON b.c_ecr_bonus_id = s.c_ecr_bonus_id
WHERE b.c_created_time >= TIMESTAMP '2026-03-20'
  AND b.c_created_time <  TIMESTAMP '2026-03-21'
  AND b.c_bonus_status = 'BONUS_ISSUED_OFFER'
LIMIT 15
"""

# Compara quantidade de bonus com BONUS_ISSUED_OFFER vs
# quantidade de transacoes BTR (sub_fund type 20) no mesmo periodo
sql_comparacao = """
SELECT
    'BONUS_ISSUED_OFFER (criados no dia 20/03)' AS fonte,
    COUNT(*) AS qtd
FROM bonus_ec2.tbl_ecr_bonus_details_inactive
WHERE c_created_time >= TIMESTAMP '2026-03-20'
  AND c_created_time <  TIMESTAMP '2026-03-21'
  AND c_bonus_status = 'BONUS_ISSUED_OFFER'
UNION ALL
SELECT
    'BTR sub_fund type 20 CR (em 20/03)' AS fonte,
    COUNT(*) AS qtd
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-03-20'
  AND c_start_time <  TIMESTAMP '2026-03-21'
  AND c_txn_type = 20
  AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0
"""

# Bônus resgatados com wagering real: teriam c_win_issue_timestamp preenchido
sql_win_issue = """
SELECT
    c_bonus_status,
    CASE WHEN c_win_issue_timestamp IS NULL THEN 'NULL' ELSE 'POPULATED' END AS win_issue,
    COUNT(*) AS qtd
FROM bonus_ec2.tbl_ecr_bonus_details_inactive
WHERE c_created_time >= TIMESTAMP '2026-03-20'
  AND c_created_time <  TIMESTAMP '2026-03-21'
GROUP BY 1, 2
ORDER BY 1, 2
"""


def run(name, sql):
    print(f"\n{'=' * 80}")
    print(f"  {name}")
    print(f"{'=' * 80}")
    df = query_athena(sql)
    print(df.to_string(index=False, max_colwidth=30))


if __name__ == "__main__":
    run("1) Timestamps lado a lado (BONUS_ISSUED_OFFER, 20/03)", sql_detalhado)
    run("2) Bonus marcados 'resgatados' vs BTR em sub_fund (20/03)", sql_comparacao)
    run("3) c_win_issue_timestamp por status (20/03)", sql_win_issue)
