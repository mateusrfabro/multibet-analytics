-- ================================================================
-- Tag: MULTI_GAME_PLAYER
-- Matriz de Risco MultiBet — Athena/Trino
-- Score: -10 | Categoria: Comportamental | Tipo: Negativo
-- Descricao: Jogador com multiplas sessoes simultaneas na mesma hora
--            Pode indicar uso de bots ou multi-accounting
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

-- Sessoes concorrentes: agrupa por user + dia + hora
-- FIX auditoria 20/04/2026 (P4): hora agora em BRT, nao UTC (conformidade CLAUDE.md).
-- Antes usava EXTRACT(HOUR FROM c_start_time) direto = hora UTC.
-- Impacto pratico pequeno (regra e sobre sessoes simultaneas, invariante por TZ),
-- mas BEHAV_RISK_PLAYER ja usa BRT -> consistencia entre SQLs do mesmo projeto.
concurrent_sessions AS (
  SELECT
    t.c_ecr_id AS user_id,
    CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS game_date,
    EXTRACT(HOUR FROM t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS game_hour,
    COUNT(DISTINCT t.c_session_id) AS concurrent_games
  FROM fund_ec2.tbl_real_fund_txn t
  WHERE t.c_start_time >= (SELECT start_ts FROM params)
    AND t.c_start_time <  (SELECT end_ts FROM params)
    AND t.c_txn_type IN (27, 28, 41, 43, 59, 127)  -- apostas
    AND t.c_session_id IS NOT NULL
    AND t.c_txn_status = 'SUCCESS'
  GROUP BY t.c_ecr_id,
           CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE),
           EXTRACT(HOUR FROM t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
  HAVING COUNT(DISTINCT t.c_session_id) >= 3  -- 3+ sessoes na mesma hora (Notion spec)
),

-- Qualifica: ocorreu 10+ vezes com 3+ sessoes simultaneas
qualifying AS (
  SELECT user_id
  FROM concurrent_sessions
  GROUP BY user_id
  HAVING COUNT(*) >= 10
)

SELECT
  CAST(br.label_id AS VARCHAR) AS label_id,
  CAST(q.user_id AS VARCHAR)   AS user_id,
  'MULTI_GAME_PLAYER'           AS tag,
  -10                            AS score,
  CURRENT_DATE                   AS snapshot_date,
  CURRENT_TIMESTAMP              AS computed_at
FROM qualifying q
JOIN users u ON q.user_id = u.user_id
LEFT JOIN brand br ON u.crm_brand_id = br.crm_brand_id
WHERE br.label_id IS NOT NULL
