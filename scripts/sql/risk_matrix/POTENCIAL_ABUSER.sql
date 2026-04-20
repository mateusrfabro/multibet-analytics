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

-- FIX auditoria 20/04/2026 (critico #3): trocado proxy first_deposit pela data
-- real de signup (ecr_ec2.tbl_ecr.c_created_time). 2 ganhos:
--   1. Semantica correta: conta criada !== primeiro deposito (podem diferir por dias).
--   2. Custo: antes escaneava TODO cashier_deposit historico (sem filtro temporal
--      na CTE) so pra filtrar ultimos 2 dias no qualifying. Agora query enxuta.
qualifying AS (
  SELECT u.c_ecr_id AS user_id
  FROM ecr_ec2.tbl_ecr u
  WHERE u.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '2' DAY
    AND u.c_partner_id IS NOT NULL
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
