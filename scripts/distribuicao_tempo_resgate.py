"""
Investiga distribuicao real do tempo de resgate.

Hipotese: a maioria dos bonus do MultiBet sao cashback/auto-credit que
convertem instantaneamente (delta ~0). Se for verdade, os buckets propostos
(0-1d/1-3d/...) ja nao fazem sentido — precisa-se de buckets sub-diarios.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from db.athena import query_athena


sql = """
WITH resgatados AS (
    SELECT
        b.c_ecr_bonus_id,
        b.c_bonus_id,
        b.c_is_freebet,
        date_diff('second', b.c_created_time, b.c_updated_time) AS segundos_ate_resgate
    FROM bonus_ec2.tbl_ecr_bonus_details_inactive b
    LEFT JOIN ps_bi.dim_user du ON b.c_ecr_id = du.ecr_id
    WHERE b.c_created_time >= TIMESTAMP '2026-01-01'
      AND b.c_created_time <  TIMESTAMP '2026-04-18'
      AND b.c_bonus_status = 'BONUS_ISSUED_OFFER'
      AND COALESCE(du.is_test, false) = false
)
SELECT
    CASE
        WHEN segundos_ate_resgate <  60        THEN '< 1 min (instantaneo)'
        WHEN segundos_ate_resgate <  300       THEN '1-5 min'
        WHEN segundos_ate_resgate <  3600      THEN '5-60 min'
        WHEN segundos_ate_resgate <  21600     THEN '1-6 h'
        WHEN segundos_ate_resgate <  86400     THEN '6-24 h'
        WHEN segundos_ate_resgate <  259200    THEN '1-3 d'
        WHEN segundos_ate_resgate <  604800    THEN '3-7 d'
        WHEN segundos_ate_resgate < 1209600    THEN '7-14 d'
        WHEN segundos_ate_resgate < 2592000    THEN '14-30 d'
        ELSE '30+ d'
    END AS bucket_real,
    CAST(c_is_freebet AS VARCHAR) AS freebet,
    COUNT(*) AS qtd,
    MIN(segundos_ate_resgate) AS min_seg,
    MAX(segundos_ate_resgate) AS max_seg,
    AVG(segundos_ate_resgate) AS avg_seg
FROM resgatados
GROUP BY 1, 2
ORDER BY 1, 2
"""

sql_top = """
-- top bonus_id com tempos >> 0 (indica wagering real)
SELECT
    b.c_bonus_id,
    COUNT(*) AS qtd_resgates,
    APPROX_PERCENTILE(date_diff('second', b.c_created_time, b.c_updated_time), 0.5)/3600.0 AS p50_horas,
    APPROX_PERCENTILE(date_diff('second', b.c_created_time, b.c_updated_time), 0.9)/3600.0 AS p90_horas,
    APPROX_PERCENTILE(date_diff('second', b.c_created_time, b.c_updated_time), 0.99)/3600.0 AS p99_horas
FROM bonus_ec2.tbl_ecr_bonus_details_inactive b
LEFT JOIN ps_bi.dim_user du ON b.c_ecr_id = du.ecr_id
WHERE b.c_created_time >= TIMESTAMP '2026-01-01'
  AND b.c_created_time <  TIMESTAMP '2026-04-18'
  AND b.c_bonus_status = 'BONUS_ISSUED_OFFER'
  AND COALESCE(du.is_test, false) = false
GROUP BY 1
HAVING COUNT(*) >= 500
ORDER BY p50_horas DESC
LIMIT 20
"""


df = query_athena(sql)
print("\nDistribuicao granular (jan-abr/2026, excluindo test users):\n")
print(df.to_string(index=False))

total = df['qtd'].sum()
print(f"\nTotal resgatados: {total:,}")
for bucket in df['bucket_real'].unique():
    sub = df[df['bucket_real'] == bucket]['qtd'].sum()
    print(f"  {bucket:30s}  {sub:>8,}  ({sub/total*100:5.1f}%)")

print("\n\nTop campanhas por mediana de tempo (≥500 resgates):\n")
df2 = query_athena(sql_top)
print(df2.to_string(index=False))
