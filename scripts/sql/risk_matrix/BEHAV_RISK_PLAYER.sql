-- ================================================================
-- Tag: BEHAV_RISK_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -10 | Categoria: Saques e Comportamental | Tipo: Negativo
-- Descricao: Cashouts incomuns + horarios extremos
--            Alto risco de ser fraudador — horario e valor divergentes
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

-- Saques por jogador com hora BRT
cashout_analysis AS (
  SELECT
    c.c_ecr_id AS user_id,
    c.c_paid_amount,
    EXTRACT(HOUR FROM c.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hour_brt,
    COUNT(*) OVER (PARTITION BY c.c_ecr_id) AS total_cashouts,
    -- Conta saques em horario extremo (2-6 AM BRT)
    SUM(CASE
      WHEN EXTRACT(HOUR FROM c.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
           BETWEEN 2 AND 5
      THEN 1 ELSE 0
    END) OVER (PARTITION BY c.c_ecr_id) AS extreme_hour_cashouts
  FROM cashier_ec2.tbl_cashier_cashout c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time <  (SELECT end_ts FROM params)
    AND c.c_txn_status = 'co_success'
    AND c.c_paid_amount > 0
),

-- Agregado por jogador
user_cashout_stats AS (
  SELECT
    user_id,
    MAX(total_cashouts) AS total_cashouts,
    MAX(extreme_hour_cashouts) AS extreme_hour_cashouts,
    STDDEV(CAST(c_paid_amount AS DOUBLE)) AS amount_stddev,
    AVG(CAST(c_paid_amount AS DOUBLE)) AS amount_avg
  FROM cashout_analysis
  GROUP BY user_id
),

-- Qualifica: >= 30% dos saques em horario extremo OU desvio padrao > 2x media
qualifying AS (
  SELECT user_id
  FROM user_cashout_stats
  WHERE total_cashouts >= 3
    AND (
      CAST(extreme_hour_cashouts AS DOUBLE) / total_cashouts >= 0.30
      OR (amount_avg > 0 AND amount_stddev / amount_avg > 2.0)
    )
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'BEHAV_RISK_PLAYER'           AS tag,
  -10                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
