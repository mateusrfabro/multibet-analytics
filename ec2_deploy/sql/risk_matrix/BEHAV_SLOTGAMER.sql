-- ================================================================
-- Tag: BEHAV_SLOTGAMER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +5 | Categoria: Preferencia | Tipo: Positivo
-- Descricao: Jogador focado em Slots e tem pelo menos um deposito
--            Preferencia por jogos de casino
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

-- Apostas por produto
bet_by_product AS (
  SELECT
    t.c_ecr_id AS user_id,
    COUNT(*) AS total_bets,
    COUNT_IF(t.c_product_id = 'CASINO') AS casino_bets
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type IN (27, 28, 41, 43) -- apostas casino (exclui SB 59/127)
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Tem pelo menos 1 deposito
has_deposit AS (
  SELECT DISTINCT d.c_ecr_id AS user_id
  FROM cashier_ec2.tbl_cashier_deposit d
  WHERE d.c_created_time >= (SELECT start_ts FROM params)
    AND d.c_created_time <  (SELECT end_ts FROM params)
    AND d.c_txn_status = 'txn_confirmed_success'
    AND d.c_initial_amount > 0
),

-- Qualifica: >= 70% das apostas sao casino (slots) + tem deposito
qualifying AS (
  SELECT bp.user_id
  FROM bet_by_product bp
  JOIN has_deposit hd ON bp.user_id = hd.user_id
  WHERE bp.total_bets >= 10
    AND CAST(bp.casino_bets AS DOUBLE) / bp.total_bets >= 0.70
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'BEHAV_SLOTGAMER'             AS tag,
  5                              AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
