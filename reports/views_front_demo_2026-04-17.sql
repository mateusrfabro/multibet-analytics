-- ============================================================
-- VIEWS vw_front_* — Demo para CTO/Castrin
-- Gerado em: 17/04/2026 14:41 BRT
-- Banco: Super Nova DB (PostgreSQL) — schema multibet.*
-- ============================================================
-- Cole/rode bloco a bloco no DBeaver para conferir cada view.
-- ============================================================

-- 0. STATS GLOBAIS (sanity check do refresh)
SELECT
    COUNT(*) AS total_jogos,
    SUM(CASE WHEN game_image_url IS NOT NULL THEN 1 ELSE 0 END) AS com_imagem,
    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS ativos,
    SUM(CASE WHEN popularity_rank_24h IS NOT NULL THEN 1 ELSE 0 END) AS com_atividade_24h,
    MAX(popularity_window_end) AS ultima_janela_24h
FROM multibet.game_image_mapping;

-- ============================================================
-- 1. Mais jogados nas ultimas 24h
--    View: multibet.vw_front_top_24h
--    Para que: Carrossel principal 'Mais jogados' (rank 1 = jogo mais jogado nas ultimas 24h rolantes)
--    Como front consome: Mostra os top 50 com posicionamento ja calculado (rank 1, 2, 3...)
-- ============================================================
SELECT rank, game_name, vendor, category, live_subtype,
                   rounds_24h, players_24h,
                   TO_CHAR(window_end_utc AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI') AS atualizado_em_brt
            FROM multibet.vw_front_top_24h
            ORDER BY rank
            LIMIT 15;

-- ============================================================
-- 2. Cassino ao Vivo (com subtipo)
--    View: multibet.vw_front_live_casino
--    Para que: Filtros por Roleta / Blackjack / Baccarat / GameShow no Cassino ao Vivo
--    Como front consome: Front passa filtro: WHERE live_subtype = 'Roleta' → mostra so roletas
-- ============================================================
SELECT live_subtype, COUNT(*) AS qtd_jogos
            FROM multibet.vw_front_live_casino
            GROUP BY live_subtype
            ORDER BY qtd_jogos DESC;

-- Top 10 roletas
            SELECT game_name, vendor, rounds_24h, rank
            FROM multibet.vw_front_live_casino
            WHERE live_subtype = 'Roleta'
            ORDER BY COALESCE(rank, 999999), game_name
            LIMIT 10;

-- ============================================================
-- 3. Por Provedor (Pragmatic, PG Soft, etc)
--    View: multibet.vw_front_by_vendor
--    Para que: Carrossel 'Jogos Pragmatic', 'Jogos PG Soft' — vendor_id agrupa por marca
--    Como front consome: Front passa: WHERE vendor = 'pragmaticplay' → carrossel Pragmatic
-- ============================================================
SELECT vendor, COUNT(*) AS qtd_jogos
            FROM multibet.vw_front_by_vendor
            GROUP BY vendor
            ORDER BY qtd_jogos DESC
            LIMIT 8;

-- Top 10 jogos da Pragmatic
            SELECT game_name, category, live_subtype, rounds_24h, rank
            FROM multibet.vw_front_by_vendor
            WHERE vendor = 'pragmaticplay'
            ORDER BY COALESCE(rank, 999999)
            LIMIT 10;

-- ============================================================
-- 4. Por Categoria (Slots / Live)
--    View: multibet.vw_front_by_category
--    Para que: Filtros macro: Slots vs Live. Backup do front se nao quiser usar live_subtype
--    Como front consome: Front passa: WHERE category = 'live' → todos jogos ao vivo
-- ============================================================
SELECT category, category_desc, COUNT(*) AS qtd_jogos
            FROM multibet.vw_front_by_category
            GROUP BY category, category_desc
            ORDER BY qtd_jogos DESC;

-- ============================================================
-- 5. Jogos com Jackpot
--    View: multibet.vw_front_jackpot
--    Para que: Carrossel 'Jackpots' — todos jogos onde has_jackpot = TRUE
--    Como front consome: Front consome direto, ja vem filtrado
--    ⚠️ ATENCAO: atualmente retorna 0 linhas. Fonte vendor_ec2.tbl_vendor_games_mapping_mst esta vazia/sem acesso. Em investigacao com Mauro/Gusta.
-- ============================================================
SELECT game_name, vendor, category, rank
            FROM multibet.vw_front_jackpot
            LIMIT 10;

-- ============================================================
-- LEGENDA
-- ============================================================
-- rank             1 = jogo mais jogado nas ultimas 24h rolantes
-- rounds_24h       total de rodadas/apostas nas ultimas 24h
-- players_24h      jogadores unicos nas ultimas 24h
-- category         slots | live | (NULL=DrawGames/outros)
-- live_subtype     Roleta | Blackjack | Baccarat | GameShow | Outros
-- vendor           pragmaticplay, alea_redtiger, alea_pgsoft, etc
-- image_url        URL CDN multi.bet pronta para o front
-- slug             path para abrir o jogo (ex: /pb/gameplay/fortune_ox/real-game)
-- window_end_utc   timestamp UTC do fim da janela 24h (=hora do refresh)

-- Refresh planejado: 00, 04, 08, 12, 16, 20 BRT (cron 4h EC2 ETL)
-- Apos deploy EC2, a tabela atualiza sozinha — front nao precisa fazer nada.