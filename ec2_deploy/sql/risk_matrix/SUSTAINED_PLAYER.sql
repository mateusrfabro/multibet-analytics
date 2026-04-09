-- ================================================================
-- Tag: SUSTAINED_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +15 | Categoria: Comportamental | Tipo: Positivo
-- Descricao: Mantem saldo apos sacar e continua jogando
--            Engajamento real
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

-- Saques do jogador
cashouts AS (
  SELECT
    c.c_ecr_id AS user_id,
    MAX(c.c_created_time) AS last_cashout_time
  FROM cashier_ec2.tbl_cashier_cashout c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time <  (SELECT end_ts FROM params)
    AND c.c_txn_status = 'co_success'
  GROUP BY c.c_ecr_id
),

-- Atividade pos-saque: depositos ou apostas DEPOIS do ultimo saque
post_cashout_activity AS (
  SELECT DISTINCT t.c_ecr_id AS user_id
  FROM fund_ec2.tbl_real_fund_txn t
  JOIN cashouts co ON t.c_ecr_id = co.user_id
  WHERE t.c_start_time > co.last_cashout_time
    AND t.c_start_time < (SELECT end_ts FROM params)
    AND t.c_txn_status = 'SUCCESS'
    AND t.c_txn_type IN (27, 28, 41, 43, 59, 127) -- apostas
),

-- Qualifica: sacou E continuou jogando depois
qualifying AS (
  SELECT co.user_id
  FROM cashouts co
  JOIN post_cashout_activity pa ON co.user_id = pa.user_id
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(u.user_id AS VARCHAR)   AS user_id,
  'SUSTAINED_PLAYER'            AS tag,
  15                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
