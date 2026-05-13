-- ================================================================
-- Tag: REINVEST_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: +15 | Categoria: Depositos e Comportamental | Tipo: Positivo
-- Descricao: Participa de promocoes, volta a jogar com saldo proprio
--            e faz pelo menos um deposito — jogador saudavel
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

-- FIX 13/05/2026: regra antes era "qualquer ciclo saque->deposito em 7d" -> 19.3%
--   da base (35.436 jogadores). Captura qualquer player com ciclo normal.
--   Agora exige >=2 ciclos distintos (saque + deposito-em-ate-7d, em dois saques
--   diferentes) — padrao real de reinvestimento, nao evento unico.

-- Saques no periodo
cashouts AS (
  SELECT
    co.c_ecr_id AS user_id,
    co.c_created_time AS cashout_ts
  FROM cashier_ec2.tbl_cashier_cashout co
  WHERE co.c_created_time >= (SELECT start_ts FROM params)
    AND co.c_created_time <  (SELECT end_ts FROM params)
    AND co.c_txn_status = 'co_success'
    AND co.c_paid_amount > 0
),

-- Saques que tiveram deposito subsequente em <= 7 dias
reinvest_cycles AS (
  SELECT
    co.user_id,
    COUNT(DISTINCT co.cashout_ts) AS n_cycles
  FROM cashouts co
  WHERE EXISTS (
    SELECT 1
    FROM cashier_ec2.tbl_cashier_deposit d
    WHERE d.c_ecr_id = co.user_id
      AND d.c_txn_status = 'txn_confirmed_success'
      AND d.c_initial_amount > 0
      AND d.c_created_time >  co.cashout_ts
      AND d.c_created_time <= co.cashout_ts + INTERVAL '7' DAY
  )
  GROUP BY co.user_id
),

qualifying AS (
  SELECT user_id FROM reinvest_cycles WHERE n_cycles >= 2
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'REINVEST_PLAYER'             AS tag,
  15                             AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
