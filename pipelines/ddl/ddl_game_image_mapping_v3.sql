-- ============================================================
-- DDL v3: multibet.game_image_mapping + vw_front_api_games
-- Data: 2026-04-17
-- Demanda: CTO (via Castrin) — alinhar com shape esperado pela
--          categories-api (NestJS) para consumo direto no front
--
-- Adicoes em v3 (sobre v2):
--   - total_bet_24h   (valor total apostado ultimas 24h em R$)
--   - total_wins_24h  (valor total ganho ultimas 24h em R$)
--   - view vw_front_api_games (aliases CamelCase do GameResponseDto)
-- ============================================================

CREATE SCHEMA IF NOT EXISTS multibet;

ALTER TABLE multibet.game_image_mapping
    ADD COLUMN IF NOT EXISTS total_bet_24h   NUMERIC(18,2) DEFAULT 0,
    ADD COLUMN IF NOT EXISTS total_wins_24h  NUMERIC(18,2) DEFAULT 0;

COMMENT ON COLUMN multibet.game_image_mapping.total_bet_24h  IS 'Valor total apostado (R$) nas ultimas 24h rolantes';
COMMENT ON COLUMN multibet.game_image_mapping.total_wins_24h IS 'Valor total ganho (R$) nas ultimas 24h rolantes';

-- ============================================================
-- VIEW: vw_front_api_games (shape do GameResponseDto da API)
-- Consumida diretamente pelo GameCachedRepository (NestJS)
-- Retorna CamelCase para simetria 1:1 com o DTO TypeScript
-- ============================================================
CREATE OR REPLACE VIEW multibet.vw_front_api_games AS
SELECT
    provider_game_id        AS "gameId",
    game_name               AS "name",
    game_slug               AS "gameSlug",
    game_slug               AS "gamePath",
    game_image_url          AS "image",
    vendor_id               AS "provider",
    game_category           AS "category",
    game_category_desc      AS "categoryDescription",
    COALESCE(rounds_24h, 0)    AS "totalBets",
    COALESCE(players_24h, 0)   AS "uniquePlayers",
    COALESCE(total_bet_24h, 0) AS "totalBet",
    COALESCE(total_wins_24h, 0) AS "totalWins",

    -- Campos extras uteis para o backend (nao no DTO mas no repositorio)
    popularity_rank_24h     AS "rank",
    live_subtype,
    has_jackpot,
    popularity_window_end   AS "windowEndUtc"
FROM multibet.game_image_mapping
WHERE is_active = TRUE
  AND game_image_url IS NOT NULL
  AND game_image_url <> '';

COMMENT ON VIEW multibet.vw_front_api_games IS
'Shape 1:1 com GameResponseDto da categories-api. Front consome via /games/*.
 Refresh: pipeline game_image_mapper a cada 4h (cron EC2 ETL).
 Fonte catalogo: bireports_ec2 + vendor_ec2. Fonte metricas: silver_game_15min (24h rolantes).';
