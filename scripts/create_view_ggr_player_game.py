"""
Cria view vw_ggr_player_game_daily no supernova_bet e foreign table no supernova_db.play4.

Objetivo: permitir que o Head visualize GGR por jogador, por jogo, por dia,
identificando outliers e big winners que derrubam o GGR da casa.

Demanda: Castrin pediu apos GGR negativo em -R$20K no dia 08/04/2026.
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection
from db.supernova import get_supernova_connection


# ============================================================
# PASSO 1: Criar VIEW no supernova_bet
# ============================================================

VIEW_SQL = """
CREATE OR REPLACE VIEW public.vw_ggr_player_game_daily AS
WITH daily_totals AS (
    SELECT
        m.date,
        SUM(m.total_bet_amount)  AS day_bet,
        SUM(m.total_win_amount)  AS day_win,
        SUM(m.net_revenue)       AS day_ggr,
        COUNT(DISTINCT m.user_id) AS day_players
    FROM casino_user_game_metrics m
    GROUP BY m.date
),
player_daily AS (
    SELECT
        m.date,
        m.user_id,
        SUM(m.total_bet_amount)  AS player_bet,
        SUM(m.total_win_amount)  AS player_win,
        SUM(m.net_revenue)       AS player_ggr,
        SUM(m.played_rounds)     AS player_rounds,
        COUNT(DISTINCT m.game_id) AS player_games
    FROM casino_user_game_metrics m
    GROUP BY m.date, m.user_id
)
SELECT
    m.date                                          AS data,
    -- Jogador
    u.username,
    u.public_id,
    u.phone,
    u.created_at::date                              AS data_cadastro,
    (m.date - u.created_at::date)                   AS dias_conta,
    u.is_affiliate,
    -- Jogo
    g.name                                          AS jogo,
    g.slug                                          AS jogo_slug,
    g.rtp                                           AS rtp_configurado,
    -- Provider
    COALESCE(pv.name, 'Unknown')                    AS provider,
    -- Metricas do jogador NESTE jogo NESTE dia
    m.played_rounds                                 AS rodadas,
    m.played_sessions                               AS sessoes,
    ROUND(m.total_bet_amount::numeric, 2)           AS apostado,
    ROUND(m.total_win_amount::numeric, 2)           AS ganho,
    ROUND(m.net_revenue::numeric, 2)                AS ggr,
    -- Payout % (win/bet) deste jogador neste jogo
    CASE WHEN m.total_bet_amount > 0
        THEN ROUND((m.total_win_amount / m.total_bet_amount * 100)::numeric, 1)
        ELSE 0
    END                                             AS payout_pct,
    -- Metricas do jogador NO DIA (todos jogos)
    ROUND(pd.player_bet::numeric, 2)                AS apostado_total_jogador,
    ROUND(pd.player_win::numeric, 2)                AS ganho_total_jogador,
    ROUND(pd.player_ggr::numeric, 2)                AS ggr_total_jogador,
    pd.player_rounds                                AS rodadas_total_jogador,
    pd.player_games                                 AS jogos_jogados,
    -- % do impacto no dia
    CASE WHEN dt.day_bet > 0
        THEN ROUND((pd.player_bet / dt.day_bet * 100)::numeric, 1)
        ELSE 0
    END                                             AS pct_turnover_dia,
    CASE WHEN dt.day_ggr != 0
        THEN ROUND((pd.player_ggr / dt.day_ggr * 100)::numeric, 1)
        ELSE 0
    END                                             AS pct_ggr_dia,
    -- Totais do dia (contexto)
    ROUND(dt.day_bet::numeric, 2)                   AS total_apostado_dia,
    ROUND(dt.day_win::numeric, 2)                   AS total_ganho_dia,
    ROUND(dt.day_ggr::numeric, 2)                   AS total_ggr_dia,
    dt.day_players                                  AS jogadores_ativos_dia,
    -- Flags de alerta
    CASE
        WHEN pd.player_ggr < -10000 THEN 'CRITICO'
        WHEN pd.player_ggr < -5000  THEN 'ALERTA'
        WHEN pd.player_ggr < -1000  THEN 'ATENCAO'
        WHEN pd.player_ggr > 5000   THEN 'BOM_PRA_CASA'
        ELSE 'NORMAL'
    END                                             AS flag_risco,
    -- Perda da casa (TRUE = casa perdeu dinheiro com este jogador)
    (pd.player_ggr < 0)                             AS casa_perdeu,
    -- Conta nova (< 3 dias) + volume alto = suspeito
    CASE
        WHEN (m.date - u.created_at::date) <= 1
             AND pd.player_bet > 50000              THEN 'CONTA_NOVA_ALTO_VOLUME'
        WHEN (m.date - u.created_at::date) <= 3
             AND pd.player_ggr < -5000              THEN 'CONTA_NOVA_BIG_WINNER'
        ELSE NULL
    END                                             AS flag_fraude
FROM casino_user_game_metrics m
JOIN users u             ON u.id = m.user_id
JOIN casino_games g      ON g.id = m.game_id
LEFT JOIN casino_providers pv ON pv.id = g.provider_id
JOIN daily_totals dt     ON dt.date = m.date
JOIN player_daily pd     ON pd.date = m.date AND pd.user_id = m.user_id
"""


# ============================================================
# PASSO 2: Foreign table no supernova_db.play4
# ============================================================

FOREIGN_TABLE_SQL = """
CREATE FOREIGN TABLE IF NOT EXISTS play4.vw_ggr_player_game_daily (
    data                      date,
    username                  varchar(50),
    public_id                 varchar(9),
    phone                     varchar(20),
    data_cadastro             date,
    dias_conta                integer,
    is_affiliate              boolean,
    jogo                      varchar,
    jogo_slug                 varchar,
    rtp_configurado           numeric(8,2),
    provider                  varchar,
    rodadas                   integer,
    sessoes                   integer,
    apostado                  numeric,
    ganho                     numeric,
    ggr                       numeric,
    payout_pct                numeric,
    apostado_total_jogador    numeric,
    ganho_total_jogador       numeric,
    ggr_total_jogador         numeric,
    rodadas_total_jogador     integer,
    jogos_jogados             bigint,
    pct_turnover_dia          numeric,
    pct_ggr_dia               numeric,
    total_apostado_dia        numeric,
    total_ganho_dia           numeric,
    total_ggr_dia             numeric,
    jogadores_ativos_dia      bigint,
    flag_risco                varchar,
    casa_perdeu               boolean,
    flag_fraude               varchar
)
SERVER supernova_bet_server
OPTIONS (schema_name 'public', table_name 'vw_ggr_player_game_daily')
"""


def step1_create_view():
    """Cria a VIEW no supernova_bet."""
    print("=" * 60)
    print("PASSO 1: Criando VIEW no supernova_bet...")
    print("=" * 60)

    tunnel, conn = get_supernova_bet_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(VIEW_SQL)
            conn.commit()
            print("  VIEW vw_ggr_player_game_daily criada com sucesso!")

            # Testar
            cur.execute("""
                SELECT data, username, jogo, ggr, flag_risco, casa_perdeu
                FROM vw_ggr_player_game_daily
                WHERE data = '2026-04-08'
                ORDER BY ggr ASC
                LIMIT 5
            """)
            rows = cur.fetchall()
            print(f"\n  Top 5 maiores perdas da casa em 08/04:")
            for r in rows:
                flag = f" [{r[4]}]" if r[4] != 'NORMAL' else ""
                print(f"    {r[1]:<20} | {r[2]:<25} | GGR: {r[3]:>10}{flag}")
    finally:
        conn.close()
        tunnel.stop()


def step2_create_foreign_table():
    """Cria FOREIGN TABLE no supernova_db.play4."""
    print("\n" + "=" * 60)
    print("PASSO 2: Criando FOREIGN TABLE no supernova_db.play4...")
    print("=" * 60)

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            # Dropar se existir (para recriar limpo)
            cur.execute("DROP FOREIGN TABLE IF EXISTS play4.vw_ggr_player_game_daily")
            conn.commit()

            cur.execute(FOREIGN_TABLE_SQL)
            conn.commit()
            print("  FOREIGN TABLE play4.vw_ggr_player_game_daily criada!")

            # Testar
            cur.execute("""
                SELECT data, username, jogo, ggr, flag_risco
                FROM play4.vw_ggr_player_game_daily
                WHERE data = '2026-04-08'
                ORDER BY ggr ASC
                LIMIT 3
            """)
            rows = cur.fetchall()
            print(f"\n  Teste via supernova_db.play4 (top 3 perdas 08/04):")
            for r in rows:
                print(f"    {r[1]:<20} | {r[2]:<25} | GGR: {r[3]:>10} | {r[4]}")

            print("\n  A view esta disponivel no DBeaver em:")
            print("  supernova_db > play4 > vw_ggr_player_game_daily")
    except Exception as e:
        print(f"\n  ERRO ao criar foreign table: {e}")
        print("  Pode ser necessario pedir ao Gusta/DBA para criar.")
        print("  A VIEW ja esta disponivel direto no supernova_bet.")
        conn.rollback()
    finally:
        conn.close()
        tunnel.stop()


def step3_validate():
    """Valida a view com o caso real do dia 08/04."""
    print("\n" + "=" * 60)
    print("VALIDACAO: Caso 08/04 (GGR negativo reportado)")
    print("=" * 60)

    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    try:
        with conn.cursor() as cur:
            # Resumo do dia
            cur.execute("""
                SELECT data, total_ggr_dia, jogadores_ativos_dia
                FROM vw_ggr_player_game_daily
                WHERE data = '2026-04-08'
                LIMIT 1
            """)
            row = cur.fetchone()
            if row:
                print(f"\n  Dia: {row[0]} | GGR total: {row[1]} | Ativos: {row[2]}")

            # Jogadores que mais impactaram (GGR do jogador no dia)
            cur.execute("""
                SELECT DISTINCT ON (username)
                    username, public_id, dias_conta,
                    ggr_total_jogador, pct_turnover_dia, pct_ggr_dia,
                    jogos_jogados, rodadas_total_jogador,
                    flag_risco, flag_fraude
                FROM vw_ggr_player_game_daily
                WHERE data = '2026-04-08'
                ORDER BY username, ggr_total_jogador ASC
            """)
            players = cur.fetchall()

            # Ordenar por GGR do jogador
            players = sorted(players, key=lambda x: x[3])

            print(f"\n  Jogadores no dia ({len(players)}):")
            print(f"  {'Username':<20} {'PID':<12} {'Dias':<6} {'GGR Jogador':>12} {'%Turn':>6} {'%GGR':>6} {'Jogos':>6} {'Rodadas':>8} {'Flag':>12}")
            print(f"  {'-'*100}")
            for p in players:
                flag = p[8]
                fraud = f" !! {p[9]}" if p[9] else ""
                print(f"  {p[0]:<20} {p[1]:<12} {p[2]:<6} {p[3]:>12} {p[4]:>6}% {p[5]:>6}% {p[6]:>6} {p[7]:>8} {flag:>12}{fraud}")

            # Detalhe do maior outlier
            cur.execute("""
                SELECT username, jogo, rodadas, apostado, ganho, ggr, payout_pct
                FROM vw_ggr_player_game_daily
                WHERE data = '2026-04-08'
                ORDER BY ggr ASC
                LIMIT 10
            """)
            details = cur.fetchall()
            print(f"\n  Top 10 perdas da casa por jogo (08/04):")
            print(f"  {'Username':<20} {'Jogo':<25} {'Rodadas':>8} {'Apostado':>10} {'Ganho':>10} {'GGR':>10} {'Payout':>8}")
            print(f"  {'-'*95}")
            for d in details:
                print(f"  {d[0]:<20} {d[1]:<25} {d[2]:>8} {d[3]:>10} {d[4]:>10} {d[5]:>10} {d[6]:>7}%")

    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    step1_create_view()
    step2_create_foreign_table()
    step3_validate()
    print("\n" + "=" * 60)
    print("CONCLUIDO!")
    print("=" * 60)
