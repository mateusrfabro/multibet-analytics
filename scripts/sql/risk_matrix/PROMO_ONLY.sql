-- ================================================================
-- Tag: PROMO_ONLY
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -15 | Categoria: Depositos | Tipo: Negativo
-- Descricao: Depositos apenas durante promocoes (>=80%)
--            Indicio de exploracao de bonus
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

-- Dias em que o jogador recebeu bonus
promotion_dates AS (
  SELECT DISTINCT
    b.c_ecr_id AS user_id,
    CAST(b.c_created_time AS DATE) AS promo_date
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time <  (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status = 'SUCCESS'
),

-- Todos os depositos do jogador
deposits AS (
  SELECT
    c.c_ecr_id AS user_id,
    CAST(c.c_created_time AS DATE) AS deposit_date
  FROM cashier_ec2.tbl_cashier_deposit c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time <  (SELECT end_ts FROM params)
    AND c.c_txn_status = 'txn_confirmed_success'
    AND c.c_initial_amount > 0
),

-- Conta depositos totais e depositos em dia de promo
deposit_analysis AS (
  SELECT
    d.user_id,
    COUNT(*) AS total_deposits,
    COUNT(p.promo_date) AS deposits_on_promo_day
  FROM deposits d
  LEFT JOIN promotion_dates p
    ON d.user_id = p.user_id
   AND d.deposit_date = p.promo_date
  GROUP BY d.user_id
),

-- Qualifica: >= 80% dos depositos sao em dia de promo
qualifying AS (
  SELECT user_id
  FROM deposit_analysis
  WHERE total_deposits >= 3
    AND CAST(deposits_on_promo_day AS DOUBLE) / total_deposits >= 0.80
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'PROMO_ONLY'                  AS tag,
  -15                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
