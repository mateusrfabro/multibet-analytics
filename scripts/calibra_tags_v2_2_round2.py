"""
Calibracao v2 das tags. Assinatura corrigida:

  MINES_PENDING_ABUSER = bet(27) >= R$ X + rollback(72) MESMO VALOR
                        em janela <= 15min, NN+ ocorrencias em 30d.

Testa varios thresholds de valor e janela.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

CASE_ECR_ID = 440908751792034786

print("=" * 80)
print("RE-CALIBRACAO MINES_PENDING_ABUSER — assinatura: bet27 = rollback72 mesmo valor")
print("=" * 80)


# Threshold de valor (R$ 100, R$ 200, R$ 500)
for valor_min in [10000, 20000, 50000]:  # em centavos
    print(f"\n--- Threshold valor >= R$ {valor_min/100:.0f} ---")
    sql = f"""
    WITH pares AS (
      SELECT
        b.c_ecr_id,
        b.c_start_time AS t_bet,
        b.c_amount_in_ecr_ccy AS valor_centavos,
        r.c_start_time AS t_rollback
      FROM fund_ec2.tbl_real_fund_txn b
      JOIN fund_ec2.tbl_real_fund_txn r
        ON b.c_ecr_id = r.c_ecr_id
       AND r.c_txn_type = 72
       AND r.c_txn_status = 'SUCCESS'
       AND r.c_amount_in_ecr_ccy = b.c_amount_in_ecr_ccy
       AND r.c_start_time BETWEEN b.c_start_time AND b.c_start_time + INTERVAL '15' MINUTE
      WHERE b.c_txn_type = 27
        AND b.c_txn_status = 'SUCCESS'
        AND b.c_amount_in_ecr_ccy >= {valor_min}
        AND b.c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    ),
    players_agg AS (
      SELECT c_ecr_id, COUNT(*) AS n_ocorrencias,
             ROUND(MAX(valor_centavos)/100.0, 2) AS max_valor_brl
      FROM pares
      GROUP BY c_ecr_id
    )
    SELECT
      CASE
        WHEN n_ocorrencias = 1 THEN '01 - 1x'
        WHEN n_ocorrencias = 2 THEN '02 - 2x'
        WHEN n_ocorrencias BETWEEN 3 AND 5 THEN '03 - 3-5x'
        WHEN n_ocorrencias BETWEEN 6 AND 10 THEN '04 - 6-10x'
        ELSE '05 - >10x'
      END AS faixa,
      COUNT(*) AS players,
      ROUND(AVG(max_valor_brl), 2) AS avg_max_valor_brl
    FROM players_agg
    GROUP BY 1 ORDER BY 1
    """
    df = query_athena(sql, database="fund_ec2")
    print(df.to_string(index=False))


# Spot-check 30311442 com novo critério (valor >= R$ 100, qualquer ocorrência)
print("\n\n" + "=" * 80)
print("SPOT-CHECK 30311442 — ele cai agora?")
print("=" * 80)

sql_case = f"""
SELECT
  b.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS bet_ts_brt,
  r.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS rollback_ts_brt,
  ROUND(b.c_amount_in_ecr_ccy / 100.0, 2) AS valor_brl,
  date_diff('second', b.c_start_time, r.c_start_time) AS gap_segundos
FROM fund_ec2.tbl_real_fund_txn b
JOIN fund_ec2.tbl_real_fund_txn r
  ON b.c_ecr_id = r.c_ecr_id
 AND r.c_txn_type = 72
 AND r.c_txn_status = 'SUCCESS'
 AND r.c_amount_in_ecr_ccy = b.c_amount_in_ecr_ccy
 AND r.c_start_time BETWEEN b.c_start_time AND b.c_start_time + INTERVAL '15' MINUTE
WHERE b.c_ecr_id = {CASE_ECR_ID}
  AND b.c_txn_type = 27
  AND b.c_txn_status = 'SUCCESS'
  AND b.c_amount_in_ecr_ccy >= 10000
  AND b.c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
ORDER BY b.c_start_time
"""
df_case = query_athena(sql_case, database="fund_ec2")
print(df_case.to_string(index=False))


# Sinal composto: pares + freespin win na mesma janela
print("\n\n" + "=" * 80)
print("ASSINATURA COMPOSTA: bet=rollback + freespin win co-ocorrente em 15min")
print("=" * 80)

sql_composto = """
WITH pares AS (
  SELECT
    b.c_ecr_id,
    b.c_start_time AS t_bet,
    r.c_start_time AS t_rollback,
    b.c_amount_in_ecr_ccy
  FROM fund_ec2.tbl_real_fund_txn b
  JOIN fund_ec2.tbl_real_fund_txn r
    ON b.c_ecr_id = r.c_ecr_id
   AND r.c_txn_type = 72
   AND r.c_txn_status = 'SUCCESS'
   AND r.c_amount_in_ecr_ccy = b.c_amount_in_ecr_ccy
   AND r.c_start_time BETWEEN b.c_start_time AND b.c_start_time + INTERVAL '15' MINUTE
  WHERE b.c_txn_type = 27
    AND b.c_txn_status = 'SUCCESS'
    AND b.c_amount_in_ecr_ccy >= 10000  -- bet >= R$ 100
    AND b.c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
),
freespins AS (
  SELECT
    c_ecr_id,
    c_start_time AS t_fs,
    c_amount_in_ecr_ccy
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_txn_type IN (45, 80)
    AND c_txn_status = 'SUCCESS'
    AND c_amount_in_ecr_ccy BETWEEN 40 AND 5000  -- R$ 0.40 a R$ 50
    AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
),
sinal_composto AS (
  SELECT DISTINCT p.c_ecr_id, p.t_bet
  FROM pares p
  JOIN freespins f
    ON p.c_ecr_id = f.c_ecr_id
   AND f.t_fs BETWEEN p.t_bet - INTERVAL '15' MINUTE AND p.t_rollback + INTERVAL '15' MINUTE
)
SELECT c_ecr_id, COUNT(*) AS ocorrencias
FROM sinal_composto
GROUP BY c_ecr_id
ORDER BY ocorrencias DESC
LIMIT 30
"""
df_comp = query_athena(sql_composto, database="fund_ec2")
print(f"Players com sinal composto: {len(df_comp)}")
print(df_comp.to_string(index=False))


# 30311442 caiu no top?
print(f"\n--- 30311442 (ecr_id={CASE_ECR_ID}) esta no top? ---")
print(f"Esta na lista: {'SIM' if CASE_ECR_ID in df_comp['c_ecr_id'].astype(int).values else 'NAO'}")
