-- ============================================================================
-- FRAUDE "RECARREGUE E GANHE FORTUNE TIGER" — Cohort + Gameplay + Rollback
-- Data: 21/04/2026 (D-0, near-real-time risco/fraude — autorizado)
-- Autor: extractor (MultiBet / PGX)
--
-- Contexto:
--   Denuncia de abuso: jogador deposita pra qualificar, pega giros/saldo
--   do Fortune Tiger e joga Mines em vez disso. Suspeita de rollback coordenado.
--
-- Regras validadas (memory/feedback_athena_sql_rules.md + docs/athena_pragmatic_guide.md):
--   * Valor fund_ec2: c_amount_in_ecr_ccy / 100.0 (centavos BRL). NAO existe c_confirmed_amount_in_inhouse_ccy.
--   * Status fund_ec2: c_txn_status = 'SUCCESS' (NAO confundir com cashier 'txn_confirmed_success')
--   * Partição `dt` nao existe visivel em fund_ec2 — filtrar por c_start_time >= TIMESTAMP '2026-04-21' AND < '2026-04-22'
--   * Timezone: sempre AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' na saida
--   * Test users: is_test = false (ps_bi.dim_user) | c_test_user = false (ecr_ec2.tbl_ecr_flags)
--   * bonus_ec2: usar c_actual_issued_amount (c_total_bonus_offered NAO EXISTE — validado Mauro 20/03/2026)
--   * c_product_id = 'CASINO' obrigatorio em fund_ec2 (memory/feedback_fund_ec2_product_id_obrigatorio.md)
--   * Rollback aposta cassino: c_txn_type = 72 (CASINO_BUYIN_CANCEL)
--   * Aposta cassino: c_txn_type = 27 (CASINO_BUYIN)
--
-- Janela:
--   BRT 2026-04-21 00:00 → 2026-04-22 00:00
--   = UTC 2026-04-21 03:00 → 2026-04-22 03:00
--
-- Premissa:
--   Nome exato da campanha NAO confirmado. Filtro por LIKE em multiplos termos
--   ("recarregue", "fortune tiger", "FT", "reload"). Validar IDs retornados
--   na Parte 1 manualmente antes de expandir cohort (Parte 2-3).
-- ============================================================================

-- ----------------------------------------------------------------------------
-- PARTE 1 — IDENTIFICAR CAMPANHA "Recarregue e Ganhe FT"
-- Objetivo: listar bonus emitidos hoje cujo nome/descricao casa com a campanha.
-- Resultado esperado: 1 ou mais c_bonus_id candidatos.
-- Ação do auditor: confirmar qual ID corresponde A campanha real.
-- ----------------------------------------------------------------------------
-- Performance: bonus_ec2.tbl_bonus_summary_details ~10-50MB/dia
-- Nao temos coluna visivel de nome do bonus em summary_details.
-- Precisamos cruzar com tbl_bonus_profile (config master).
WITH bonus_hoje AS (
    -- Bonus emitidos hoje (BRT 21/04)
    SELECT
        bsd.c_bonus_id,
        bsd.c_ecr_bonus_id,
        bsd.c_ecr_id,
        bsd.c_bonus_status,
        bsd.c_actual_issued_amount / 100.0 AS bonus_issued_brl,
        bsd.c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS bonus_issued_at_brt
    FROM bonus_ec2.tbl_bonus_summary_details bsd
    WHERE bsd.c_issue_date >= TIMESTAMP '2026-04-21 03:00:00'  -- 21/04 00h BRT
      AND bsd.c_issue_date <  TIMESTAMP '2026-04-22 03:00:00'  -- 22/04 00h BRT
),
campanhas_candidatas AS (
    -- Cruzar com tbl_bonus_profile pra achar nome/descricao da campanha
    -- tbl_bonus_profile tem a config master (nome, regras, segmento)
    -- OBS: estrutura exata da tbl_bonus_profile nao esta 100% documentada.
    -- Usar SELECT * no script Python pra validar colunas reais
    -- (ver pipeline Q3b em scripts/investigacao_campanha_bonus_13abr.py).
    SELECT DISTINCT
        bh.c_bonus_id,
        bh.c_bonus_status,
        COUNT(*) OVER (PARTITION BY bh.c_bonus_id) AS qtd_emissoes_hoje,
        SUM(bh.bonus_issued_brl) OVER (PARTITION BY bh.c_bonus_id) AS total_emitido_brl_hoje
    FROM bonus_hoje bh
)
SELECT
    c_bonus_id,
    c_bonus_status,
    qtd_emissoes_hoje,
    ROUND(total_emitido_brl_hoje, 2) AS total_emitido_brl_hoje
FROM campanhas_candidatas
ORDER BY total_emitido_brl_hoje DESC
LIMIT 50;
-- ACAO: rodar em paralelo "SELECT * FROM bonus_ec2.tbl_bonus_profile WHERE c_bonus_id IN (...)"
-- pra obter nome/descricao e filtrar por termos "recarregue/fortune tiger/FT/reload".

-- ============================================================================
-- PARTE 2 + 3 — QUERY CONSOLIDADA (rodar apos confirmar o c_bonus_id)
-- Substituir <BONUS_IDS_CONFIRMADOS> pela lista retornada na Parte 1.
-- ============================================================================

WITH
-- Cohort: quem recebeu o bonus alvo hoje
cohort_bonus AS (
    SELECT
        bsd.c_ecr_id,
        bsd.c_bonus_id,
        bsd.c_ecr_bonus_id,
        MIN(bsd.c_actual_issued_amount / 100.0) AS bonus_issued_amount_brl,
        MIN(bsd.c_issue_date) AS bonus_issued_at_utc
    FROM bonus_ec2.tbl_bonus_summary_details bsd
    WHERE bsd.c_issue_date >= TIMESTAMP '2026-04-21 03:00:00'
      AND bsd.c_issue_date <  TIMESTAMP '2026-04-22 03:00:00'
      AND bsd.c_bonus_id IN (<BONUS_IDS_CONFIRMADOS>)  -- ex: (20260421083000, 20260421084500)
    GROUP BY bsd.c_ecr_id, bsd.c_bonus_id, bsd.c_ecr_bonus_id
),

-- Perfil (PII + is_test) via ps_bi.dim_user + ecr_profile (mobile)
cohort_perfil AS (
    SELECT
        du.ecr_id,
        du.external_id,
        du.c_email_id AS email,
        ep.c_mobile_number AS mobile_number,
        du.is_test
    FROM ps_bi.dim_user du
    LEFT JOIN ecr_ec2.tbl_ecr_profile ep
        ON ep.c_ecr_id = du.ecr_id
    WHERE du.ecr_id IN (SELECT c_ecr_id FROM cohort_bonus)
      AND du.is_test = false  -- filtro test users (ps_bi)
),

-- Transacoes cassino hoje (bets + rollbacks)
-- Filtro obrigatorio: c_product_id = 'CASINO' + c_txn_status = 'SUCCESS'
txn_casino_hoje AS (
    SELECT
        f.c_ecr_id,
        f.c_game_id,
        f.c_txn_type,
        f.c_amount_in_ecr_ccy / 100.0 AS valor_brl,
        f.c_start_time
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_start_time >= TIMESTAMP '2026-04-21 03:00:00'  -- 21/04 00h BRT em UTC
      AND f.c_start_time <  TIMESTAMP '2026-04-22 03:00:00'  -- 22/04 00h BRT em UTC
      AND f.c_product_id = 'CASINO'
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_txn_type IN (27, 72)  -- 27=BUYIN (aposta), 72=BUYIN_CANCEL (rollback)
      AND f.c_ecr_id IN (SELECT c_ecr_id FROM cohort_bonus)
),

-- Mapeamento jogo -> nome/vendor (bireports_ec2 tem catalogo mais completo que ps_bi.dim_game)
-- Memory: dim_game ps_bi so cobre ~0.2%, PG Soft ausente
jogos AS (
    SELECT
        CAST(c_game_id AS VARCHAR) AS c_game_id,
        c_game_desc,
        c_sub_vendor_id
    FROM bireports_ec2.tbl_vendor_games_mapping_data
),

-- Classificacao: Fortune Tiger, Mines ou Outros
-- Fortune Tiger: c_game_id = '45838245' (memory/feedback_smartico_game_ids.md)
-- Mines: identificar via nome do jogo (Spribe, SmartSoft, Turbogames tem variantes)
-- ATENCAO: game_id Smartico != c_game_id Pragmatic em alguns casos.
-- Estrategia: matching por c_game_desc LIKE '%mines%' (case-insensitive)
-- + fallback por c_sub_vendor_id IN ('spribe', 'smartsoft')
txn_classificada AS (
    SELECT
        t.c_ecr_id,
        t.c_txn_type,
        t.valor_brl,
        CASE
            WHEN t.c_game_id = '45838245'
              OR LOWER(COALESCE(j.c_game_desc, '')) LIKE '%fortune tiger%'
                THEN 'FORTUNE_TIGER'
            WHEN LOWER(COALESCE(j.c_game_desc, '')) LIKE '%mines%'
                THEN 'MINES'
            ELSE 'OUTROS'
        END AS bucket_jogo
    FROM txn_casino_hoje t
    LEFT JOIN jogos j ON j.c_game_id = t.c_game_id
),

-- Agregado por jogador
agg_jogador AS (
    SELECT
        c_ecr_id,
        -- Fortune Tiger
        COUNT_IF(c_txn_type = 27 AND bucket_jogo = 'FORTUNE_TIGER') AS rounds_fortune_tiger,
        COALESCE(SUM(CASE WHEN c_txn_type = 27 AND bucket_jogo = 'FORTUNE_TIGER' THEN valor_brl END), 0) AS stake_ft_brl,
        -- Mines
        COUNT_IF(c_txn_type = 27 AND bucket_jogo = 'MINES') AS rounds_mines,
        COALESCE(SUM(CASE WHEN c_txn_type = 27 AND bucket_jogo = 'MINES' THEN valor_brl END), 0) AS stake_mines_brl,
        -- Outros
        COUNT_IF(c_txn_type = 27 AND bucket_jogo = 'OUTROS') AS rounds_outros,
        COALESCE(SUM(CASE WHEN c_txn_type = 27 AND bucket_jogo = 'OUTROS' THEN valor_brl END), 0) AS stake_outros_brl,
        -- Rollback
        COUNT_IF(c_txn_type = 72) AS rollback_count_24h,
        COALESCE(SUM(CASE WHEN c_txn_type = 72 THEN valor_brl END), 0) AS rollback_amount_brl,
        -- Rollback concentrado em Mines (sinal de fraude)
        COUNT_IF(c_txn_type = 72 AND bucket_jogo = 'MINES') AS rollback_count_mines
    FROM txn_classificada
    GROUP BY c_ecr_id
)

-- OUTPUT FINAL: cohort + metricas + flag_suspeito
SELECT
    cp.ecr_id,
    cp.external_id,
    cp.email,
    cp.mobile_number,
    CAST(cb.c_bonus_id AS VARCHAR) AS bonus_template_id,
    ROUND(cb.bonus_issued_amount_brl, 2) AS bonus_issued_amount_brl,
    cb.bonus_issued_at_utc AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS bonus_issued_at_brt,

    COALESCE(aj.rounds_fortune_tiger, 0) AS rounds_fortune_tiger,
    ROUND(COALESCE(aj.stake_ft_brl, 0), 2) AS stake_ft_brl,

    COALESCE(aj.rounds_mines, 0) AS rounds_mines,
    ROUND(COALESCE(aj.stake_mines_brl, 0), 2) AS stake_mines_brl,

    COALESCE(aj.rounds_outros, 0) AS rounds_outros,
    ROUND(COALESCE(aj.stake_outros_brl, 0), 2) AS stake_outros_brl,

    COALESCE(aj.rollback_count_24h, 0) AS rollback_count_24h,
    ROUND(COALESCE(aj.rollback_amount_brl, 0), 2) AS rollback_amount_brl,
    COALESCE(aj.rollback_count_mines, 0) AS rollback_count_mines,

    -- Flag suspeito: stake_mines > stake_ft OU rollback_count > 0
    CASE
        WHEN COALESCE(aj.stake_mines_brl, 0) > COALESCE(aj.stake_ft_brl, 0)
          OR COALESCE(aj.rollback_count_24h, 0) > 0
            THEN TRUE
        ELSE FALSE
    END AS flag_suspeito,

    -- Prefixo mobile (p/ deteccao de cluster sequencial)
    SUBSTR(cp.mobile_number, 1, 7) AS mobile_prefix_7
FROM cohort_bonus cb
JOIN cohort_perfil cp ON cp.ecr_id = cb.c_ecr_id
LEFT JOIN agg_jogador aj ON aj.c_ecr_id = cb.c_ecr_id
ORDER BY flag_suspeito DESC, stake_mines_brl DESC;

-- ============================================================================
-- PARTE 4 — DETECCAO DE CLUSTER (executar em pos-processamento no Python)
-- Logica: agrupar cohort por SUBSTR(mobile_number, 1, 7); se >= 5 contas
-- no mesmo prefixo, sinal forte de farming coordenado
-- (ver memory/project_bonus_farming_p4t_18abr.md — caso batch +92341374xxx).
-- ============================================================================
