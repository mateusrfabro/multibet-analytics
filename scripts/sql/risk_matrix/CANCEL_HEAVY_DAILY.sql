-- ================================================================
-- Tag: CANCEL_HEAVY_DAILY
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -10 | Categoria: Comportamental | Tipo: Negativo
-- Descricao: Jogador com 10+ cancelamentos de buyin (c_txn_type=72)
--            em UM unico dia (BRT) na janela de 90d.
--            Pega exploit MINES (refresh travando saldo) e bot.
--
-- Aprovado por Castrin em 15/05/2026 apos investigacao do exploit
-- do player 30311442 (caso 13/05). Castrin confirmou:
--   "mais de 10 cancelamentos no dia ja da -10 pro maluco"
--
-- Calibracao 13/05/2026 sobre base de abusers do Castrin
-- (jelly_cenarios_bonus_1305_v3_SELECTED.csv, 2.102 players):
--   596 players caem nessa regra (proximo dos 591 confirmados
--   por Castrin como "resgataram bonus sem turnover").
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

-- Conta cancels (c_txn_type=72 / Casino Cancel Buyin) por jogador POR DIA BRT
cancels_por_dia AS (
  SELECT
    t.c_ecr_id AS user_id,
    CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS dia_brt,
    COUNT(*) AS n_cancels
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type = 72
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id, CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE)
),

-- Qualifica: pelo menos 1 dia com >= 10 cancels na janela 90d
qualifying AS (
  SELECT DISTINCT user_id
  FROM cancels_por_dia
  WHERE n_cancels >= 10
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'CANCEL_HEAVY_DAILY'          AS tag,
  -10                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
