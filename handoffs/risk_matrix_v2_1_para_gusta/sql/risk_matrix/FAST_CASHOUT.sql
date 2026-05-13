-- ================================================================
-- Tag: FAST_CASHOUT
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -25 | Categoria: Saques | Tipo: Negativo
-- Descricao: Tempo medio entre deposito e saque < 1h
--            Padrao classico de abuso
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

-- FIX 13/05/2026: regra antes (qualquer par dep->saque <1h, com JOIN cartesiano
--   user_id x user_id) gerou 29.2% cobertura (53.731 jogadores). Whales legitimos
--   caiam aqui por terem volume alto. Mudanca:
--     1) Conta OCORRENCIAS distintas, nao apenas existencia.
--     2) Exige >= 3 pares para ser flagado (captura padrao, nao evento isolado).
--     3) Pre-filtra com EXISTS p/ reduzir o universo antes do count.

deposits AS (
  SELECT
    d.c_ecr_id AS user_id,
    d.c_created_time AS deposit_time
  FROM cashier_ec2.tbl_cashier_deposit d
  WHERE d.c_created_time >= (SELECT start_ts FROM params)
    AND d.c_created_time <  (SELECT end_ts FROM params)
    AND d.c_txn_status = 'txn_confirmed_success'
    AND d.c_initial_amount > 0
),

-- Para cada deposito, conta se houve saque em ate 1h apos
deposits_with_fast_cashout AS (
  SELECT
    d.user_id,
    d.deposit_time
  FROM deposits d
  WHERE EXISTS (
    SELECT 1
    FROM cashier_ec2.tbl_cashier_cashout c
    WHERE c.c_ecr_id = d.user_id
      AND c.c_txn_status = 'co_success'
      AND c.c_paid_amount > 0
      AND c.c_created_time >  d.deposit_time
      AND c.c_created_time <= d.deposit_time + INTERVAL '1' HOUR
  )
),

-- Qualifica: 3+ ocorrencias de deposito-com-saque-rapido
qualifying AS (
  SELECT user_id
  FROM deposits_with_fast_cashout
  GROUP BY user_id
  HAVING COUNT(*) >= 3
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'FAST_CASHOUT'                AS tag,
  -25                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
