"""
Checa o schema de bireports_ec2.tbl_ecr_wise_daily_bi_summary e valida
se da pra calcular last_active_date (recencia) a partir dela.
"""
import sys
sys.path.insert(0, ".")
from db.athena import query_athena

print("[1] Colunas da tabela bireports_ec2.tbl_ecr_wise_daily_bi_summary")
df = query_athena("SHOW COLUMNS FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary")
print(df.to_string(index=False))

print("\n[2] Conferindo last_active_date pelos 2 paths para o player do Victor")
PLAYER = 305245081792208985
sql = f"""
WITH atividade_bireports AS (
    SELECT
        c_ecr_id AS player_id,
        MAX(c_created_date) AS last_active_date_bireports,
        COUNT(DISTINCT c_created_date) AS days_active_bireports
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
    WHERE c_ecr_id = {PLAYER}
      AND c_created_date >= CURRENT_DATE - INTERVAL '90' DAY
      AND c_created_date < CURRENT_DATE
      AND (c_casino_realcash_bet_amount > 0
           OR c_sb_realcash_bet_amount  > 0
           OR c_deposit_success_amount  > 0)
    GROUP BY c_ecr_id
),
atividade_fct AS (
    SELECT
        player_id,
        MAX(activity_date) AS last_active_date_fct,
        COUNT(DISTINCT activity_date) AS days_active_fct
    FROM ps_bi.fct_player_activity_daily
    WHERE player_id = {PLAYER}
      AND activity_date >= CURRENT_DATE - INTERVAL '90' DAY
      AND activity_date < CURRENT_DATE
      AND (casino_realbet_count > 0
           OR sb_realbet_count > 0
           OR deposit_success_count > 0)
    GROUP BY player_id
)
SELECT
    DATE_DIFF('day', b.last_active_date_bireports, CURRENT_DATE) AS recencia_bireports,
    b.last_active_date_bireports,
    b.days_active_bireports,
    DATE_DIFF('day', f.last_active_date_fct, CURRENT_DATE) AS recencia_fct,
    f.last_active_date_fct,
    f.days_active_fct
FROM atividade_bireports b
CROSS JOIN atividade_fct f
"""
df = query_athena(sql)
print(df.to_string(index=False))
