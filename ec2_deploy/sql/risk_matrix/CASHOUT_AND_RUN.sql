-- ================================================================
-- Tag: CASHOUT_AND_RUN
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -25 | Categoria: Comportamental | Tipo: Negativo
-- Descricao: Usa bonus, saca e fica inativo > 48h
--            Abusador classico
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
    AND u.c_partner_id IS NOT NULL
),
brand AS (
  SELECT c_partner_id AS crm_brand_id, c_partner_id AS label_id
  FROM ecr_ec2.tbl_ecr
  WHERE c_partner_id IS NOT NULL
  GROUP BY c_partner_id
),

-- FIX auditoria 20/04/2026 (critico #2 / BO4):
--   Antes comparava DATEs (UTC truncado) via BETWEEN, o que gerava drift
--   de +/-1 dia na virada de dia BRT e permitia casar cashout ANTES do
--   bonus se ambos cairam no mesmo dia calendario. Agora compara
--   TIMESTAMPS crus com janela de 24h apos o bonus.

-- Uso de bonus (timestamp cru pra comparacao precisa)
bonus_usage AS (
  SELECT DISTINCT
    b.c_ecr_id AS user_id,
    b.c_created_time AS bonus_ts
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time <  (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status = 'SUCCESS'
),

-- Saques (timestamp cru)
cashouts AS (
  SELECT
    c.c_ecr_id AS user_id,
    c.c_created_time AS cashout_ts
  FROM cashier_ec2.tbl_cashier_cashout c
  WHERE c.c_created_time >= (SELECT start_ts FROM params)
    AND c.c_created_time <  (SELECT end_ts FROM params)
    AND c.c_txn_status = 'co_success'
    AND c.c_paid_amount > 0
),

-- Ultima atividade por jogador (timestamp cru)
recent_activity AS (
  SELECT
    t.c_ecr_id AS user_id,
    MAX(t.c_start_time) AS last_activity_ts
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id
),

-- Padrao: bonus -> saque em ate 24h -> sem atividade por 48h
qualifying AS (
  SELECT DISTINCT bu.user_id
  FROM bonus_usage bu
  JOIN cashouts co
    ON bu.user_id = co.user_id
   AND co.cashout_ts >  bu.bonus_ts
   AND co.cashout_ts <= bu.bonus_ts + INTERVAL '24' HOUR
  JOIN recent_activity ra
    ON bu.user_id = ra.user_id
  WHERE ra.last_activity_ts <= co.cashout_ts + INTERVAL '48' HOUR
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'CASHOUT_AND_RUN'             AS tag,
  -25                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
