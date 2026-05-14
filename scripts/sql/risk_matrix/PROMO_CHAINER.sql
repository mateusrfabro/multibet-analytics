-- ================================================================
-- Tag: PROMO_CHAINER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -10 | Categoria: Comportamental | Tipo: Negativo
-- Descricao: Participa de 3+ promocoes consecutivas sem jogar
--            fora delas — bonus grinder
--
-- FIX 14/05/2026 (auditoria v2.2):
--   1) Timezone BRT em todos os CAST de datas (regra ouro CLAUDE.md).
--   2) Logica do ratio refeita. Antes: bonus_day_count / active_day_count
--      em CTEs INDEPENDENTES — podia dar ratio > 1 (dia com bonus sem aposta)
--      e sempre flagar. Agora: faz UNION ALL para ter (user_id, day, has_bonus,
--      has_bet) por dia, conta os 3 cruzados, e exige "dos dias ATIVOS, >=80%
--      tem bonus".
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

-- Dias com bonus (em BRT)
bonus_days_raw AS (
  SELECT DISTINCT
    b.c_ecr_id AS user_id,
    CAST(b.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS day
  FROM bonus_ec2.tbl_bonus_pocket_txn b
  WHERE b.c_created_time >= (SELECT start_ts FROM params)
    AND b.c_created_time <  (SELECT end_ts FROM params)
    AND b.c_bonus_txn_status = 'SUCCESS'
),

-- Dias com aposta (em BRT)
activity_days_raw AS (
  SELECT DISTINCT
    t.c_ecr_id AS user_id,
    CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS day
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type IN (27, 28, 41, 43, 59, 127) -- apostas
    AND t.c_txn_status = 'SUCCESS'
),

-- Combina os dois em (user_id, day, has_bonus, has_bet) com 0/1
day_flags AS (
  SELECT
    user_id,
    day,
    MAX(has_bonus) AS has_bonus,
    MAX(has_bet)   AS has_bet
  FROM (
    SELECT user_id, day, 1 AS has_bonus, 0 AS has_bet FROM bonus_days_raw
    UNION ALL
    SELECT user_id, day, 0 AS has_bonus, 1 AS has_bet FROM activity_days_raw
  )
  GROUP BY user_id, day
),

-- Agrega por jogador: total de dias com bonus, total de dias com aposta,
-- e total de dias com AMBOS (intersecao)
agg AS (
  SELECT
    user_id,
    SUM(has_bonus)               AS bonus_days,
    SUM(has_bet)                 AS active_days,
    SUM(has_bonus * has_bet)     AS bonus_and_bet_days
  FROM day_flags
  GROUP BY user_id
),

-- Qualifica: 3+ dias de bonus E (zero atividade real OU >=80% dos dias
-- ATIVOS coincidem com bonus). Ratio sempre <=1 por construcao.
qualifying AS (
  SELECT user_id
  FROM agg
  WHERE bonus_days >= 3
    AND (
      active_days = 0
      OR CAST(bonus_and_bet_days AS DOUBLE) / active_days >= 0.80
    )
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'PROMO_CHAINER'               AS tag,
  -10                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
