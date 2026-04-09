-- ================================================================
-- Tag: NON_BONUS_DEPOSITOR
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +10 | Categoria: Depositos | Tipo: Positivo
-- Descricao: Depositos sem bonus ativo — engajamento organico
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

-- Jogadores com depositos
deposits AS (
  SELECT
    d.c_ecr_id AS user_id,
    COUNT(*) AS total_deposits,
    SUM(d.c_initial_amount) AS total_amount
  FROM cashier_ec2.tbl_cashier_deposit d
  WHERE d.c_created_time >= (SELECT start_ts FROM params)
    AND d.c_created_time <  (SELECT end_ts FROM params)
    AND d.c_txn_status = 'txn_confirmed_success'
    AND d.c_initial_amount > 0
  GROUP BY d.c_ecr_id
),

-- Jogadores que usaram bonus no periodo
bonus_users AS (
  SELECT DISTINCT b.c_ecr_id AS user_id
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time <  (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status = 'SUCCESS'
),

-- Qualifica: tem depositos mas NAO usou bonus
qualifying AS (
  SELECT d.user_id
  FROM deposits d
  WHERE d.total_deposits >= 3
    AND NOT EXISTS (SELECT 1 FROM bonus_users b WHERE b.user_id = d.user_id)
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'NON_BONUS_DEPOSITOR'         AS tag,
  10                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
