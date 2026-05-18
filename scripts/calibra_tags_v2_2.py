"""
Calibracao empirica das 3 tags propostas para v2.2 da Matriz de Risco.

Roda na base TODA (ultimos 30d) pra medir:
  1) Quantos players caem em cada threshold
  2) Distribuicao de intensidade (histograma)
  3) Co-ocorrencia entre as 3 regras
  4) Confirma que o player 30311442 (caso real) cai nas tags

NAO altera nada. Apenas leitura Athena.

Tags testadas:
  T1 - BET_CANCEL_ABUSER: % cancel ATIVO (72,61,76,133) sobre total > X em 30d
  T2 - CASHOUT_CANCELLER: N saques co_reversed em janela X
  T3 - MINES_PENDING_ABUSER: burst 27 pequenos + rollback 72 grande em <5min
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from db.athena import query_athena

CASE_EXT_ID = 30311442
CASE_ECR_ID = 440908751792034786


def fmt(df, n=30):
    if df is None or df.empty:
        return "(vazio)"
    return df.head(n).to_string(index=False)


# ========================================================================
# TAG 1 — BET_CANCEL_ABUSER: % cancel ATIVO sobre total de bets em 30d
# ========================================================================
print("=" * 80)
print("TAG 1 - BET_CANCEL_ABUSER")
print("Hipotese: player com >= X% de c_txn_type IN (72,61,76,133) sobre")
print("          total de transacoes em 30d e abuser de cancel")
print("=" * 80)

sql_t1 = """
WITH base AS (
  SELECT
    c_ecr_id,
    COUNT(*) AS total_txns,
    SUM(CASE WHEN c_txn_type IN (27, 28, 41, 43, 59, 127) THEN 1 ELSE 0 END) AS total_bets,
    SUM(CASE WHEN c_txn_type IN (72, 61, 76, 133) THEN 1 ELSE 0 END) AS total_cancels,
    SUM(CASE WHEN c_txn_type IN (72, 61, 76, 133)
             THEN c_amount_in_ecr_ccy ELSE 0 END) / 100.0 AS cancels_brl
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND c_txn_status = 'SUCCESS'
  GROUP BY c_ecr_id
)
SELECT
  CASE
    WHEN total_bets = 0 THEN '00 - sem bets'
    WHEN total_cancels = 0 THEN '01 - 0% (nunca cancela)'
    WHEN CAST(total_cancels AS DOUBLE) / total_bets < 0.02 THEN '02 - <2%'
    WHEN CAST(total_cancels AS DOUBLE) / total_bets < 0.05 THEN '03 - 2-5%'
    WHEN CAST(total_cancels AS DOUBLE) / total_bets < 0.08 THEN '04 - 5-8%'
    WHEN CAST(total_cancels AS DOUBLE) / total_bets < 0.15 THEN '05 - 8-15% (proposto)'
    WHEN CAST(total_cancels AS DOUBLE) / total_bets < 0.30 THEN '06 - 15-30%'
    ELSE '07 - >30% (extremo)'
  END AS faixa,
  COUNT(*) AS players,
  ROUND(AVG(total_cancels), 1) AS avg_cancels,
  ROUND(AVG(cancels_brl), 2) AS avg_brl_cancelado
FROM base
WHERE total_bets >= 5  -- minimo de bets pra qualificar
GROUP BY 1
ORDER BY 1
"""
df_t1 = query_athena(sql_t1, database="fund_ec2")
print(fmt(df_t1))

# Spot-check: 30311442 cai onde?
print("\n--- 30311442 nesse perfil ---")
sql_t1_case = f"""
SELECT
  COUNT(*) AS total_txns,
  SUM(CASE WHEN c_txn_type IN (27, 28, 41, 43, 59, 127) THEN 1 ELSE 0 END) AS total_bets,
  SUM(CASE WHEN c_txn_type IN (72, 61, 76, 133) THEN 1 ELSE 0 END) AS total_cancels,
  ROUND(SUM(CASE WHEN c_txn_type IN (72, 61, 76, 133) THEN 1 ELSE 0 END) * 100.0 /
        NULLIF(SUM(CASE WHEN c_txn_type IN (27, 28, 41, 43, 59, 127) THEN 1 ELSE 0 END), 0), 2) AS pct_cancel
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  AND c_txn_status = 'SUCCESS'
"""
print(fmt(query_athena(sql_t1_case, database="fund_ec2")))


# ========================================================================
# TAG 2 — CASHOUT_CANCELLER: N saques co_reversed em janela
# ========================================================================
print("\n\n" + "=" * 80)
print("TAG 2 - CASHOUT_CANCELLER")
print("Hipotese: player com N+ saques co_reversed em janela apertada e suspeito")
print("=" * 80)

sql_t2 = """
WITH co_reversed AS (
  SELECT
    c_ecr_id,
    c_created_time,
    c_initial_amount / 100.0 AS amount_brl
  FROM cashier_ec2.tbl_cashier_cashout
  WHERE c_created_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND c_txn_status = 'co_reversed'
),
agg AS (
  SELECT
    c_ecr_id,
    COUNT(*) AS reversed_30d,
    SUM(CASE WHEN c_created_time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR THEN 1 ELSE 0 END) AS reversed_24h,
    SUM(CASE WHEN c_created_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY THEN 1 ELSE 0 END) AS reversed_7d
  FROM co_reversed
  GROUP BY c_ecr_id
)
SELECT
  CASE
    WHEN reversed_30d = 1 THEN '01 - 1 em 30d (provavel acidente)'
    WHEN reversed_30d = 2 THEN '02 - 2 em 30d (incomodo)'
    WHEN reversed_30d BETWEEN 3 AND 5 THEN '03 - 3-5 em 30d (proposto)'
    WHEN reversed_30d BETWEEN 6 AND 10 THEN '04 - 6-10 em 30d'
    WHEN reversed_30d BETWEEN 11 AND 20 THEN '05 - 11-20 em 30d'
    ELSE '06 - >20 em 30d (caos)'
  END AS faixa_30d,
  COUNT(*) AS players,
  SUM(CASE WHEN reversed_24h >= 3 THEN 1 ELSE 0 END) AS tambem_3_em_24h
FROM agg
GROUP BY 1
ORDER BY 1
"""
df_t2 = query_athena(sql_t2, database="cashier_ec2")
print(fmt(df_t2))

# Spot-check 30311442
print("\n--- 30311442 nesse perfil ---")
sql_t2_case = f"""
SELECT
  COUNT(*) AS total_reversed_30d,
  COUNT(CASE WHEN c_created_time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY THEN 1 END) AS reversed_7d,
  COUNT(CASE WHEN c_created_time >= CURRENT_TIMESTAMP - INTERVAL '24' HOUR THEN 1 END) AS reversed_24h
FROM cashier_ec2.tbl_cashier_cashout
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_created_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
  AND c_txn_status = 'co_reversed'
"""
print(fmt(query_athena(sql_t2_case, database="cashier_ec2")))


# ========================================================================
# TAG 3 — MINES_PENDING_ABUSER: assinatura do exploit JELLY+MINES
# ========================================================================
print("\n\n" + "=" * 80)
print("TAG 3 - MINES_PENDING_ABUSER")
print("Hipotese: player com burst de >= N bets PEQUENOS (R$0.40-1.20) em <5min")
print("          AND rollback (72) >= R$200 na mesma janela de 5min = abuser")
print("=" * 80)

# Detecta: para cada minuto, conta quantos bet 27 pequenos teve e qual o maior rollback 72.
# Se o player tem >=1 ocorrencia onde "burst pequeno" e "rollback grande" coexistem em
# janela de 5min, conta como ocorrencia.
sql_t3 = """
WITH bets_pequenos AS (
  SELECT
    c_ecr_id,
    date_trunc('minute', c_start_time) AS minuto,
    COUNT(*) AS n_bets_pequenos
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND c_txn_status = 'SUCCESS'
    AND c_txn_type = 27
    AND c_amount_in_ecr_ccy BETWEEN 40 AND 200  -- R$ 0.40 a R$ 2.00
  GROUP BY c_ecr_id, date_trunc('minute', c_start_time)
  HAVING COUNT(*) >= 20  -- burst minimo de 20 bets/min
),
rollback_grande AS (
  SELECT
    c_ecr_id,
    date_trunc('minute', c_start_time) AS minuto,
    c_amount_in_ecr_ccy / 100.0 AS amount_brl
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND c_txn_status = 'SUCCESS'
    AND c_txn_type = 72
    AND c_amount_in_ecr_ccy >= 20000  -- >= R$ 200
),
coocorrencias AS (
  -- player com burst+rollback no MESMO ecr_id em janela de 5min
  SELECT DISTINCT
    b.c_ecr_id,
    b.minuto AS minuto_burst,
    r.amount_brl AS rollback_brl,
    b.n_bets_pequenos
  FROM bets_pequenos b
  JOIN rollback_grande r
    ON b.c_ecr_id = r.c_ecr_id
   AND r.minuto BETWEEN b.minuto - INTERVAL '5' MINUTE AND b.minuto + INTERVAL '5' MINUTE
),
players_agg AS (
  SELECT
    c_ecr_id,
    COUNT(*) AS n_ocorrencias,
    MAX(rollback_brl) AS max_rollback_brl,
    MAX(n_bets_pequenos) AS max_burst
  FROM coocorrencias
  GROUP BY c_ecr_id
)
SELECT
  CASE
    WHEN n_ocorrencias = 1 THEN '01 - 1 ocorrencia (provavel coincidencia)'
    WHEN n_ocorrencias = 2 THEN '02 - 2 ocorrencias (proposto threshold)'
    WHEN n_ocorrencias BETWEEN 3 AND 5 THEN '03 - 3-5 ocorrencias'
    ELSE '04 - >5 ocorrencias (abuso claro)'
  END AS faixa,
  COUNT(*) AS players,
  ROUND(AVG(max_rollback_brl), 2) AS avg_max_rollback,
  ROUND(AVG(max_burst), 1) AS avg_max_burst
FROM players_agg
GROUP BY 1
ORDER BY 1
"""
df_t3 = query_athena(sql_t3, database="fund_ec2")
print(fmt(df_t3))

# Spot-check 30311442
print("\n--- 30311442 nesse perfil ---")
sql_t3_case = f"""
WITH bets_pequenos AS (
  SELECT
    date_trunc('minute', c_start_time) AS minuto,
    COUNT(*) AS n_bets_pequenos
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_ecr_id = {CASE_ECR_ID}
    AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND c_txn_status = 'SUCCESS'
    AND c_txn_type = 27
    AND c_amount_in_ecr_ccy BETWEEN 40 AND 200
  GROUP BY date_trunc('minute', c_start_time)
  HAVING COUNT(*) >= 20
),
rollback_grande AS (
  SELECT
    date_trunc('minute', c_start_time) AS minuto,
    c_amount_in_ecr_ccy / 100.0 AS amount_brl
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_ecr_id = {CASE_ECR_ID}
    AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '30' DAY
    AND c_txn_status = 'SUCCESS'
    AND c_txn_type = 72
    AND c_amount_in_ecr_ccy >= 20000
)
SELECT
  b.minuto AS minuto_burst_utc,
  b.n_bets_pequenos,
  r.amount_brl AS rollback_brl
FROM bets_pequenos b
JOIN rollback_grande r
  ON r.minuto BETWEEN b.minuto - INTERVAL '5' MINUTE AND b.minuto + INTERVAL '5' MINUTE
"""
print(fmt(query_athena(sql_t3_case, database="fund_ec2")))


print("\n\n" + "=" * 80)
print("CALIBRACAO CONCLUIDA")
print("=" * 80)
