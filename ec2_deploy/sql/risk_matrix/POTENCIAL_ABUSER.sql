-- ================================================================
-- Tag: POTENCIAL_ABUSER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -5 | Categoria: Comportamental | Tipo: Negativo
-- Descricao: Contas criadas com < 2 dias
--            Peso nas margens negativas — conta muito nova
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

-- Primeiro deposito de cada jogador como proxy de data de criacao da conta
first_deposit AS (
  SELECT
    d.c_ecr_id AS user_id,
    MIN(d.c_created_time) AS first_deposit_time
  FROM cashier_ec2.tbl_cashier_deposit d
  WHERE d.c_txn_status = 'txn_confirmed_success'
    AND d.c_initial_amount > 0
  GROUP BY d.c_ecr_id
),

-- Qualifica: primeiro deposito nos ultimos 2 dias
qualifying AS (
  SELECT user_id
  FROM first_deposit
  WHERE first_deposit_time >= CURRENT_TIMESTAMP - INTERVAL '2' DAY
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'POTENCIAL_ABUSER'            AS tag,
  -5                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
