-- ================================================================
-- Tag: ENGAGED_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +10 | Categoria: Comportamental | Tipo: Positivo
-- Descricao: Possui de 3 a 10 sessoes por dia
--            Jogador ativo e engajado
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

-- Sessoes por dia por jogador
daily_sessions AS (
  SELECT
    t.c_ecr_id AS user_id,
    CAST(t.c_start_time AS DATE) AS game_date,
    COUNT(DISTINCT t.c_session_id) AS sessions_count
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_status = 'SUCCESS'
    AND t.c_session_id IS NOT NULL
  GROUP BY t.c_ecr_id, CAST(t.c_start_time AS DATE)
),

-- Media de sessoes por dia
avg_sessions AS (
  SELECT
    user_id,
    AVG(CAST(sessions_count AS DOUBLE)) AS avg_daily_sessions
  FROM daily_sessions
  GROUP BY user_id
),

-- Qualifica: entre 3 e 10 sessoes/dia em media
qualifying AS (
  SELECT user_id
  FROM avg_sessions
  WHERE avg_daily_sessions >= 3.0
    AND avg_daily_sessions <= 10.0
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'ENGAGED_PLAYER'              AS tag,
  10                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
