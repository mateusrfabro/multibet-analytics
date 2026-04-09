-- ================================================================
-- Tag: ZERO_RISK_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: 0 | Categoria: Saques e Depositos | Tipo: Neutro
-- Descricao: Valor medio de saque ≈ valor medio do deposito
--            Jogador que evita risco
-- ================================================================
WITH params AS (
  SELECT
    CURRENT_TIMESTAMP - INTERVAL '90' DAY AS start_ts,
    CURRENT_TIMESTAMP AS end_ts
),
users AS (
  SELECT u.c_ecr_id AS user_id, u.c_partner_id AS crm_brand_id
  FROM ecr_ec2.tbl_ecr u
  JOIN ecr_ec2.tbl_ecr_flags f ON u.c_ecr_id = f.c_ecr_id
  WHERE f.c_test_user = false
),
brand AS (
  SELECT c_partner_id AS crm_brand_id, c_partner_id AS label_id
  FROM ecr_ec2.tbl_ecr
  WHERE c_partner_id IS NOT NULL
  GROUP BY c_partner_id
),

-- Media de depositos por jogador
avg_deposits AS (
  SELECT
    d.c_ecr_id AS user_id,
    AVG(CAST(d.c_initial_amount AS DOUBLE)) AS avg_deposit
  FROM cashier_ec2.tbl_cashier_deposit d
  WHERE d.c_created_time >= (SELECT start_ts FROM params)
    AND d.c_created_time <  (SELECT end_ts FROM params)
    AND d.c_txn_status = 'txn_confirmed_success'
    AND d.c_initial_amount > 0
  GROUP BY d.c_ecr_id
),

-- Media de saques por jogador
avg_cashouts AS (
  SELECT
    c.c_ecr_id AS user_id,
    AVG(CAST(c.c_paid_amount AS DOUBLE)) AS avg_cashout
  FROM cashier_ec2.tbl_cashier_cashout c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time <  (SELECT end_ts FROM params)
    AND c.c_txn_status = 'co_success'
    AND c.c_paid_amount > 0
  GROUP BY c.c_ecr_id
),

-- Qualifica: avg_cashout dentro de 30% do avg_deposit (equilibrado)
qualifying AS (
  SELECT d.user_id
  FROM avg_deposits d
  JOIN avg_cashouts c ON d.user_id = c.user_id
  WHERE d.avg_deposit > 0
    AND ABS(c.avg_cashout - d.avg_deposit) / d.avg_deposit <= 0.30
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'ZERO_RISK_PLAYER'            AS tag,
  0                              AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
