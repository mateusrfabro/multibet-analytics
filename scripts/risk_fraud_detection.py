"""
Agente de Riscos — Deteccao de Fraude MultiBet
===============================================
Executa 8 regras de fraude contra dados Athena e gera relatorio consolidado.

Regras:
  R1 — Pico em jogo anormal (bets > 3x desvio padrao da media do jogador)
  R2 — Abuso de bonus (multiplos bonus no mesmo dia, acima do P95)
  R3a — Saque sem deposito real (zero deposito + cashout > R$50)
  R3b — Saque desproporcional (cashout > 5x depositos reais + cashout > R$200)
  R4 — Rollbacks excessivos (c_txn_type = 72 acima do P99)
  R5 — Multiplas sessoes simultaneas (sessoes com timestamps sobrepostos)
  R6 — Velocity check (muitas transacoes em curto periodo)
  R7 — Saque rapido pos-registro (cashout < 24h apos registro)
  R8 — Free Spin Abuser (revenue negativo + bonus alto + freespin wins)

Uso:
    python scripts/risk_fraud_detection.py --days 7
    python scripts/risk_fraud_detection.py --days 30 --rule R3
    python scripts/risk_fraud_detection.py --days 7 --all

Saida:
    output/risk_fraud_alerts_YYYY-MM-DD.csv
    output/risk_fraud_alerts_YYYY-MM-DD_legenda.txt

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-03
"""

import sys
import os
import argparse
import logging
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# ===========================================================================
# Mapeamento de c_txn_type (validado empiricamente 03/04/2026)
# ===========================================================================
TXN_DEPOSIT = 1       # REAL_CASH_DEPOSIT (CR)
TXN_WITHDRAW = 2      # REAL_CASH_WITHDRAW (DB)
TXN_CASINO_BET = 27   # CASINO_BUYIN (DB)
TXN_CASINO_WIN = 45   # CASINO_WIN (CR)
TXN_SB_BET = 59       # SB_BUYIN (DB)
TXN_JACKPOT_WIN = 65  # JACKPOT_WIN (CR)
TXN_ROLLBACK = 72     # CASINO_BUYIN_CANCEL (CR)
TXN_FREESPIN_WIN = 80 # CASINO_FREESPIN_WIN (CR)
TXN_SB_WIN = 112      # SB_WIN (CR)


def get_date_range(days: int) -> tuple[str, str]:
    """Retorna (data_inicio, data_fim) para filtro Athena. Usa D-1 como fim (dados completos)."""
    end = datetime.now().date() - timedelta(days=1)   # D-1 (dados completos)
    start = end - timedelta(days=days - 1)
    return str(start), str(end + timedelta(days=1))   # end+1 pra < funcionar


# ===========================================================================
# Filtro de test users — CTE reutilizavel
# ps_bi.dim_user.is_test = true => 715 contas de teste (validado 03/04/2026)
# fund_ec2 NAO tem coluna test, precisa JOIN via ecr_id
# ===========================================================================
TEST_USERS_CTE = """
    test_users AS (
        SELECT ecr_id FROM ps_bi.dim_user WHERE is_test = true
    )
"""

TEST_USERS_FILTER = "AND f.c_ecr_id NOT IN (SELECT ecr_id FROM test_users)"


# ===========================================================================
# R3a — Saque sem deposito real (REGRA CRITICA)
# R3b — Saque desproporcional (REGRA ALTA)
# ===========================================================================
def _query_player_flows(date_start: str, date_end: str) -> pd.DataFrame:
    """Query base compartilhada entre R3a e R3b — fluxo financeiro por jogador."""
    sql = f"""
    -- Base R3: Fluxo financeiro por jogador
    -- Fonte: fund_ec2.tbl_real_fund_txn (centavos /100)
    -- Filtro: c_txn_status = 'SUCCESS', excluindo test users
    WITH {TEST_USERS_CTE},
    player_flows AS (
        SELECT
            f.c_ecr_id,
            -- Depositos reais
            SUM(CASE WHEN f.c_txn_type = {TXN_DEPOSIT}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_deposits_brl,
            COUNT_IF(f.c_txn_type = {TXN_DEPOSIT}) AS qty_deposits,
            -- Saques
            SUM(CASE WHEN f.c_txn_type = {TXN_WITHDRAW}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_cashouts_brl,
            COUNT_IF(f.c_txn_type = {TXN_WITHDRAW}) AS qty_cashouts,
            -- Bets e wins pra contexto
            SUM(CASE WHEN f.c_txn_type = {TXN_CASINO_BET}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_bets_brl,
            SUM(CASE WHEN f.c_txn_type IN ({TXN_CASINO_WIN}, {TXN_FREESPIN_WIN}, {TXN_JACKPOT_WIN})
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_wins_brl,
            -- Rollbacks pra contexto
            COUNT_IF(f.c_txn_type = {TXN_ROLLBACK}) AS qty_rollbacks,
            SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_rollbacks_brl
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_status = 'SUCCESS'
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id
    )
    SELECT
        pf.*,
        CASE WHEN pf.total_deposits_brl > 0
             THEN ROUND(pf.total_cashouts_brl / pf.total_deposits_brl, 2)
             ELSE 999.99 END AS cashout_deposit_ratio,
        ROUND(pf.total_bets_brl - pf.total_wins_brl, 2) AS player_ggr_brl,
        ROUND(pf.total_cashouts_brl - pf.total_deposits_brl, 2) AS player_profit_brl
    FROM player_flows pf
    WHERE pf.total_cashouts_brl > 0
    """
    return query_athena(sql, database="fund_ec2")


def rule_r3a_zero_deposit(date_start: str, date_end: str, flows_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    R3a: Jogador sacou dinheiro SEM NUNCA ter depositado (zero deposito LIFETIME).

    IMPORTANTE: Olha depositos LIFETIME (toda a vida), NAO apenas no periodo.
    Isso evita falsos positivos com jogadores que depositaram semanas atras e
    sacaram agora (comportamento normal de jogador que volta).

    Criterios:
      - Sacou > R$50 no periodo analisado
      - NUNCA depositou dinheiro real na vida inteira (lifetime deposits = 0)

    Severidade: CRITICA (peso 35)
    """
    log.info(f"R3a — Zero deposito LIFETIME com saque no periodo [{date_start} a {date_end}]")

    sql = f"""
    -- R3a: Jogadores que sacaram no periodo mas NUNCA depositaram na vida
    -- 1) Pega quem sacou no periodo
    -- 2) Cruza com lifetime deposits (sem filtro de data)
    -- 3) Flag se lifetime deposits = 0
    WITH {TEST_USERS_CTE},
    -- Saques no periodo analisado
    saques_periodo AS (
        SELECT
            f.c_ecr_id,
            SUM(f.c_amount_in_ecr_ccy / 100.0) AS total_cashouts_brl,
            COUNT(*) AS qty_cashouts
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_type = {TXN_WITHDRAW}
          AND f.c_txn_status = 'SUCCESS'
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id
        HAVING SUM(f.c_amount_in_ecr_ccy / 100.0) > 50
    ),
    -- Depositos LIFETIME (toda a vida do jogador, sem filtro de data)
    lifetime_deposits AS (
        SELECT
            f.c_ecr_id,
            SUM(f.c_amount_in_ecr_ccy / 100.0) AS lifetime_dep_brl,
            COUNT(*) AS lifetime_dep_qty
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_txn_type = {TXN_DEPOSIT}
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_ecr_id IN (SELECT c_ecr_id FROM saques_periodo)
        GROUP BY f.c_ecr_id
    ),
    -- Contexto: bets/wins no periodo pra evidencia
    contexto AS (
        SELECT
            f.c_ecr_id,
            SUM(CASE WHEN f.c_txn_type = {TXN_CASINO_BET}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_bets_brl,
            SUM(CASE WHEN f.c_txn_type IN ({TXN_CASINO_WIN}, {TXN_FREESPIN_WIN}, {TXN_JACKPOT_WIN})
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_wins_brl,
            COUNT_IF(f.c_txn_type = {TXN_ROLLBACK}) AS qty_rollbacks
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_ecr_id IN (SELECT c_ecr_id FROM saques_periodo)
        GROUP BY f.c_ecr_id
    )
    SELECT
        sp.c_ecr_id,
        0.0 AS total_deposits_brl,
        0 AS qty_deposits,
        sp.total_cashouts_brl,
        sp.qty_cashouts,
        COALESCE(ctx.total_bets_brl, 0) AS total_bets_brl,
        COALESCE(ctx.total_wins_brl, 0) AS total_wins_brl,
        COALESCE(ctx.qty_rollbacks, 0) AS qty_rollbacks,
        0.0 AS total_rollbacks_brl,
        999.99 AS cashout_deposit_ratio,
        ROUND(COALESCE(ctx.total_bets_brl, 0) - COALESCE(ctx.total_wins_brl, 0), 2) AS player_ggr_brl,
        sp.total_cashouts_brl AS player_profit_brl,
        COALESCE(ld.lifetime_dep_brl, 0) AS lifetime_dep_brl,
        COALESCE(ld.lifetime_dep_qty, 0) AS lifetime_dep_qty
    FROM saques_periodo sp
    LEFT JOIN lifetime_deposits ld ON sp.c_ecr_id = ld.c_ecr_id
    LEFT JOIN contexto ctx ON sp.c_ecr_id = ctx.c_ecr_id
    -- FILTRO CHAVE: somente quem NUNCA depositou na vida inteira
    WHERE ld.c_ecr_id IS NULL
    ORDER BY sp.total_cashouts_brl DESC
    """

    df = query_athena(sql, database="fund_ec2")

    if df.empty:
        df["regra"] = []
        df["severidade"] = []
        df["descricao"] = []
        log.info("R3a — 0 jogadores flagados")
        return df

    # Enriquecer com origem provavel do saldo
    # Query separada: pra cada jogador R3a, ver que tipos de txn tem no lifetime
    r3a_ids_str = ",".join([str(x) for x in df["c_ecr_id"].tolist()])
    df_origins = query_athena(f"""
        SELECT
            c_ecr_id,
            -- Sportsbook: txn 59 (bet) ou 112 (win)
            COUNT_IF(c_txn_type IN (59, 112)) AS sb_txns,
            -- Credito manual CS: txn 3
            COUNT_IF(c_txn_type = 3) AS manual_credit_txns,
            -- Estorno saque: txn 36
            COUNT_IF(c_txn_type = 36) AS cashout_reversal_txns,
            -- Bonus: txn 19, 20, 30
            COUNT_IF(c_txn_type IN (19, 20)) AS bonus_txns,
            -- Casino: txn 27, 45
            COUNT_IF(c_txn_type IN (27, 45)) AS casino_txns
        FROM fund_ec2.tbl_real_fund_txn
        WHERE c_ecr_id IN ({r3a_ids_str})
          AND c_txn_status = 'SUCCESS'
        GROUP BY c_ecr_id
    """, database="fund_ec2")

    # Merge e classificar origem
    df = df.merge(df_origins, on="c_ecr_id", how="left")

    def classify_origin(row):
        if row.get("sb_txns", 0) > 0:
            return "SPORTSBOOK"
        elif row.get("cashout_reversal_txns", 0) > 0:
            return "ESTORNO_SAQUE"
        elif row.get("manual_credit_txns", 0) > 0:
            return "CREDITO_MANUAL"
        elif row.get("bonus_txns", 0) > 0:
            return "BONUS"
        elif row.get("casino_txns", 0) > 0:
            return "CASINO_SEM_DEPOSITO"
        else:
            return "ORIGEM_DESCONHECIDA"

    df["origem_saldo"] = df.apply(classify_origin, axis=1)

    df["regra"] = "R3a"
    df["severidade"] = "CRITICA"
    df["descricao"] = df.apply(
        lambda r: f"ZERO deposito LIFETIME, sacou R${r['total_cashouts_brl']:,.2f} | "
                  f"Origem: {r['origem_saldo']} | "
                  f"bets R${r['total_bets_brl']:,.2f}, wins R${r['total_wins_brl']:,.2f}",
        axis=1
    )
    log.info(f"R3a — {len(df)} jogadores flagados (zero deposito LIFETIME)")
    origin_counts = df["origem_saldo"].value_counts()
    for origin, count in origin_counts.items():
        log.info(f"  R3a origem: {origin} = {count} jogadores")
    return df


def rule_r3b_ratio_desproporcional(date_start: str, date_end: str, flows_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    R3b: Jogador sacou muito mais do que depositou (ratio >= 5x).

    Criterios:
      - total_deposits_brl > 0 (tem deposito, mas pouco)
      - total_cashouts_brl >= 5x total_deposits_brl
      - total_cashouts_brl > R$200 (excluir valores baixos)

    Severidade: ALTA (peso 25)
    """
    log.info(f"R3b — Saque desproporcional 5x+ [{date_start} a {date_end}]")

    if flows_df is None:
        flows_df = _query_player_flows(date_start, date_end)

    df = flows_df[
        (flows_df["total_deposits_brl"] > 0) &
        (flows_df["total_cashouts_brl"] >= 5.0 * flows_df["total_deposits_brl"]) &
        (flows_df["total_cashouts_brl"] > 200)
    ].copy()

    df["regra"] = "R3b"
    df["severidade"] = "ALTA"
    df["descricao"] = df.apply(
        lambda r: f"Saque R${r['total_cashouts_brl']:,.2f} vs deposito R${r['total_deposits_brl']:,.2f} "
                  f"(ratio {r['cashout_deposit_ratio']}x)",
        axis=1
    )
    log.info(f"R3b — {len(df)} jogadores flagados")
    return df


# ===========================================================================
# R4 — Rollbacks excessivos
# ===========================================================================
def rule_r4_rollbacks(date_start: str, date_end: str) -> pd.DataFrame:
    """
    Identifica jogadores com volume anormal de rollbacks.

    Logica:
      - Conta rollbacks (c_txn_type = 72) por jogador
      - Flag se > P99 da distribuicao geral (calculado dinamicamente)
      - Ou se > 50 rollbacks no periodo (threshold fixo de seguranca)

    Severidade: ALTA (peso 15)
    """
    log.info(f"R4 — Rollbacks excessivos [{date_start} a {date_end}]")

    sql = f"""
    -- R4: Rollbacks excessivos por jogador
    -- c_txn_type = 72 = CASINO_BUYIN_CANCEL
    WITH {TEST_USERS_CTE},
    rollback_counts AS (
        SELECT
            f.c_ecr_id,
            COUNT(*) AS qty_rollbacks,
            SUM(f.c_amount_in_ecr_ccy / 100.0) AS total_rollback_brl,
            MIN(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS first_rollback_brt,
            MAX(f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS last_rollback_brt
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_type = {TXN_ROLLBACK}
          AND f.c_txn_status = 'SUCCESS'
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id
    ),
    -- Calcula P99 da distribuicao
    threshold AS (
        SELECT APPROX_PERCENTILE(qty_rollbacks, 0.99) AS p99_rollbacks
        FROM rollback_counts
    )
    SELECT
        rc.c_ecr_id,
        rc.qty_rollbacks,
        rc.total_rollback_brl,
        rc.first_rollback_brt,
        rc.last_rollback_brt,
        t.p99_rollbacks
    FROM rollback_counts rc
    CROSS JOIN threshold t
    WHERE rc.qty_rollbacks > GREATEST(t.p99_rollbacks, 50)
    ORDER BY rc.qty_rollbacks DESC
    """

    df = query_athena(sql, database="fund_ec2")
    df["regra"] = "R4"
    df["severidade"] = "ALTA"
    df["descricao"] = df.apply(
        lambda r: f"{r['qty_rollbacks']} rollbacks (R${r['total_rollback_brl']:,.2f}) — "
                  f"P99={r.get('p99_rollbacks', 'N/A')}",
        axis=1
    )
    log.info(f"R4 — {len(df)} jogadores flagados")
    return df


# ===========================================================================
# R5 — Multiplas sessoes simultaneas
# ===========================================================================
def rule_r5_sessoes_simultaneas(date_start: str, date_end: str) -> pd.DataFrame:
    """
    Identifica jogadores com sessoes de jogo sobrepostas no tempo.

    Logica:
      - Para cada jogador, busca sessoes com timestamps sobrepostos
      - Flag se teve 3+ sessoes simultaneas em algum momento

    Severidade: ALTA (peso 20)
    """
    log.info(f"R5 — Sessoes simultaneas [{date_start} a {date_end}]")

    sql = f"""
    -- R5: Sessoes simultaneas — self-join para encontrar overlaps
    -- Fonte: bireports_ec2.tbl_ecr_gaming_sessions
    WITH {TEST_USERS_CTE},
    active_sessions AS (
        SELECT
            s.c_ecr_id,
            s.c_game_session_id,
            s.c_game_id,
            s.c_game_desc,
            s.c_session_start_time,
            -- Se sessao ainda ativa, usar timestamp atual como proxy
            COALESCE(s.c_session_end_time, CURRENT_TIMESTAMP) AS c_session_end_time,
            s.c_session_length_in_sec
        FROM bireports_ec2.tbl_ecr_gaming_sessions s
        WHERE s.c_session_start_time >= TIMESTAMP '{date_start}'
          AND s.c_session_start_time < TIMESTAMP '{date_end}'
          AND s.c_session_active IS NOT NULL
          AND s.c_ecr_id NOT IN (SELECT ecr_id FROM test_users)
    ),
    -- Conta sessoes sobrepostas por jogador
    overlaps AS (
        SELECT
            a.c_ecr_id,
            a.c_game_session_id AS session_a,
            b.c_game_session_id AS session_b,
            a.c_game_desc AS game_a,
            b.c_game_desc AS game_b,
            a.c_session_start_time AS start_a,
            b.c_session_start_time AS start_b
        FROM active_sessions a
        JOIN active_sessions b
          ON a.c_ecr_id = b.c_ecr_id
         AND a.c_game_session_id < b.c_game_session_id
         -- Overlap: sessao A comeca antes de B terminar E sessao B comeca antes de A terminar
         AND a.c_session_start_time < b.c_session_end_time
         AND b.c_session_start_time < a.c_session_end_time
    ),
    -- Agregar por jogador
    player_overlaps AS (
        SELECT
            c_ecr_id,
            COUNT(*) AS qty_overlaps,
            COUNT(DISTINCT session_a) + COUNT(DISTINCT session_b) AS unique_sessions,
            ARRAY_AGG(DISTINCT game_a) AS games_played,
            MIN(start_a) AS first_overlap,
            MAX(start_b) AS last_overlap
        FROM overlaps
        GROUP BY c_ecr_id
        HAVING COUNT(*) >= 3
    )
    SELECT
        c_ecr_id,
        qty_overlaps,
        unique_sessions,
        games_played,
        first_overlap AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS first_overlap_brt,
        last_overlap AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS last_overlap_brt
    FROM player_overlaps
    ORDER BY qty_overlaps DESC
    """

    df = query_athena(sql, database="bireports_ec2")
    df["regra"] = "R5"
    df["severidade"] = "ALTA"
    df["descricao"] = df.apply(
        lambda r: f"{r['qty_overlaps']} sessoes sobrepostas em {r['unique_sessions']} sessoes unicas",
        axis=1
    )
    log.info(f"R5 — {len(df)} jogadores flagados")
    return df


# ===========================================================================
# R6 — Velocity check (depositos/saques rapidos)
# ===========================================================================
def rule_r6_velocity(date_start: str, date_end: str) -> pd.DataFrame:
    """
    Identifica jogadores com muitas transacoes financeiras em curto periodo.

    Logica:
      - Conta depositos + saques por jogador em janelas de 1 hora
      - Flag se > 5 transacoes em qualquer janela de 1h

    Severidade: MEDIA-ALTA (peso 15)
    """
    log.info(f"R6 — Velocity check [{date_start} a {date_end}]")

    sql = f"""
    -- R6: Velocity — muitas transacoes financeiras em janelas de 1 hora
    -- Usa date_trunc para agrupar por hora
    WITH {TEST_USERS_CTE},
    hourly_txns AS (
        SELECT
            f.c_ecr_id,
            date_trunc('hour', f.c_start_time) AS hora,
            COUNT(*) AS txns_na_hora,
            COUNT_IF(f.c_txn_type = {TXN_DEPOSIT}) AS deposits_na_hora,
            COUNT_IF(f.c_txn_type = {TXN_WITHDRAW}) AS cashouts_na_hora,
            SUM(f.c_amount_in_ecr_ccy / 100.0) AS total_brl_na_hora
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_type IN ({TXN_DEPOSIT}, {TXN_WITHDRAW})
          AND f.c_txn_status = 'SUCCESS'
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id, date_trunc('hour', f.c_start_time)
        HAVING COUNT(*) >= 5
    )
    SELECT
        h.c_ecr_id,
        COUNT(*) AS horas_com_pico,
        MAX(h.txns_na_hora) AS max_txns_em_1h,
        SUM(h.deposits_na_hora) AS total_deposits_pico,
        SUM(h.cashouts_na_hora) AS total_cashouts_pico,
        ROUND(SUM(h.total_brl_na_hora), 2) AS total_brl_em_picos,
        MIN(h.hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_pico_brt,
        MAX(h.hora AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_pico_brt
    FROM hourly_txns h
    GROUP BY h.c_ecr_id
    ORDER BY MAX(h.txns_na_hora) DESC
    """

    df = query_athena(sql, database="fund_ec2")
    df["regra"] = "R6"
    df["severidade"] = "MEDIA-ALTA"
    df["descricao"] = df.apply(
        lambda r: f"Max {r['max_txns_em_1h']} txns/hora em {r['horas_com_pico']} hora(s) de pico "
                  f"(R${r['total_brl_em_picos']:,.2f})",
        axis=1
    )
    log.info(f"R6 — {len(df)} jogadores flagados")
    return df


# ===========================================================================
# R1 — Pico em jogo anormal
# ===========================================================================
def rule_r1_pico_jogo(date_start: str, date_end: str) -> pd.DataFrame:
    """
    Identifica jogadores com volume de apostas muito acima do seu proprio padrao.

    Logica:
      - Calcula media e desvio padrao de bets diarios por jogador nos ultimos 30d
      - Flag se algum dia no periodo teve bets > media + 3*stddev
      - Minimo de 5 dias de historico (evitar falso positivo em jogadores novos)
      - Minimo de R$500 em bets no dia de pico (excluir micro-jogadores)

    Severidade: MEDIA (peso 10) — pode ser whale legitimo
    """
    log.info(f"R1 — Pico em jogo anormal [{date_start} a {date_end}]")

    sql = f"""
    -- R1: Pico em jogo — compara atividade diaria vs media historica do jogador
    -- Fonte: fund_ec2.tbl_real_fund_txn (centavos /100)
    -- Janela: 30 dias de historico para baseline estatistico
    WITH {TEST_USERS_CTE},
    daily_bets AS (
        -- Atividade diaria por jogador nos ultimos 30 dias (janela mais ampla)
        SELECT
            f.c_ecr_id,
            date_trunc('day', f.c_start_time) AS dia,
            SUM(CASE WHEN f.c_txn_type = {TXN_CASINO_BET}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bets_dia_brl,
            COUNT_IF(f.c_txn_type = {TXN_CASINO_BET}) AS qty_bets_dia
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= date_add('day', -30, TIMESTAMP '{date_start}')
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_status = 'SUCCESS'
          AND f.c_txn_type = {TXN_CASINO_BET}
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id, date_trunc('day', f.c_start_time)
    ),
    -- Estatisticas por jogador (media e desvio padrao)
    player_stats AS (
        SELECT
            c_ecr_id,
            COUNT(DISTINCT dia) AS dias_ativos,
            AVG(bets_dia_brl) AS avg_bets_dia,
            STDDEV(bets_dia_brl) AS stddev_bets_dia,
            MAX(bets_dia_brl) AS max_bets_dia
        FROM daily_bets
        GROUP BY c_ecr_id
        -- Minimo 5 dias de historico pra ter baseline confiavel
        HAVING COUNT(DISTINCT dia) >= 5
    ),
    -- Dias de pico no periodo analisado
    picos AS (
        SELECT
            db.c_ecr_id,
            db.dia,
            db.bets_dia_brl,
            db.qty_bets_dia,
            ps.avg_bets_dia,
            ps.stddev_bets_dia,
            ps.dias_ativos,
            -- Quantos desvios padroes acima da media
            CASE WHEN ps.stddev_bets_dia > 0
                 THEN ROUND((db.bets_dia_brl - ps.avg_bets_dia) / ps.stddev_bets_dia, 2)
                 ELSE 0 END AS z_score
        FROM daily_bets db
        JOIN player_stats ps ON db.c_ecr_id = ps.c_ecr_id
        WHERE db.dia >= TIMESTAMP '{date_start}'
          AND db.dia < TIMESTAMP '{date_end}'
          -- Pico: > media + 3 desvios E valor minimo R$500
          AND db.bets_dia_brl > (ps.avg_bets_dia + 3 * ps.stddev_bets_dia)
          AND db.bets_dia_brl > 500
    )
    SELECT
        c_ecr_id,
        COUNT(*) AS dias_com_pico,
        MAX(bets_dia_brl) AS max_pico_brl,
        MAX(z_score) AS max_z_score,
        ROUND(MAX(avg_bets_dia), 2) AS media_normal_brl,
        MAX(dias_ativos) AS dias_historico,
        MAX(dia AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_pico_brt
    FROM picos
    GROUP BY c_ecr_id
    ORDER BY MAX(z_score) DESC
    """

    df = query_athena(sql, database="fund_ec2")
    df["regra"] = "R1"
    df["severidade"] = "MEDIA"
    df["descricao"] = df.apply(
        lambda r: f"Pico R${r['max_pico_brl']:,.2f}/dia vs media R${r['media_normal_brl']:,.2f} "
                  f"({r['max_z_score']}x desvio, {r['dias_com_pico']} dia(s) de pico)",
        axis=1
    )
    log.info(f"R1 — {len(df)} jogadores flagados")
    return df


# ===========================================================================
# R2 — Abuso de bonus
# ===========================================================================
def rule_r2_bonus_abuse(date_start: str, date_end: str) -> pd.DataFrame:
    """
    Identifica jogadores com padrao de abuso de bonus.

    Logica combina 3 sinais:
      a) Muitos bonus ativos/recebidos no periodo (> P95 da distribuicao)
      b) Jogador com bonus_status indicando padrao de claim rapido
      c) Cruzamento: jogador com muitos bonus E saque > deposito

    Fonte principal: bonus_ec2.tbl_bonus_summary_details
      - c_ecr_id: jogador
      - c_bonus_id: qual bonus
      - c_bonus_status: status do bonus
      - c_issue_date: quando foi emitido
      - c_actual_issued_amount: valor real emitido (centavos)

    Severidade: ALTA (peso 25)
    """
    log.info(f"R2 — Abuso de bonus [{date_start} a {date_end}]")

    sql = f"""
    -- R2: Abuso de bonus — jogadores com volume anormal de bonus
    -- Fonte: bonus_ec2.tbl_bonus_summary_details
    WITH {TEST_USERS_CTE},
    bonus_por_jogador AS (
        SELECT
            bs.c_ecr_id,
            COUNT(DISTINCT bs.c_bonus_id) AS qty_bonus_distintos,
            COUNT(*) AS qty_bonus_total,
            -- Bonus emitidos (issued) no periodo
            COUNT_IF(bs.c_issue_date >= TIMESTAMP '{date_start}'
                     AND bs.c_issue_date < TIMESTAMP '{date_end}') AS qty_bonus_periodo,
            -- Valor total de bonus emitidos (c_actual_issued_amount = centavos, validado 06/04)
            SUM(CASE WHEN bs.c_issue_date >= TIMESTAMP '{date_start}'
                          AND bs.c_issue_date < TIMESTAMP '{date_end}'
                     THEN COALESCE(bs.c_actual_issued_amount, 0) END) AS total_bonus_issued,
            -- Quantos dias distintos recebeu bonus no periodo
            COUNT(DISTINCT CASE WHEN bs.c_issue_date >= TIMESTAMP '{date_start}'
                                     AND bs.c_issue_date < TIMESTAMP '{date_end}'
                           THEN date_trunc('day', bs.c_issue_date) END) AS dias_com_bonus,
            -- Max bonus em 1 dia
            MAX(bs.c_actual_issued_amount) AS max_bonus_single
        FROM bonus_ec2.tbl_bonus_summary_details bs
        WHERE bs.c_ecr_id IS NOT NULL
          AND bs.c_ecr_id NOT IN (SELECT ecr_id FROM test_users)
        GROUP BY bs.c_ecr_id
    ),
    -- Calcula P95 de bonus no periodo
    threshold AS (
        SELECT
            APPROX_PERCENTILE(qty_bonus_periodo, 0.95) AS p95_bonus_periodo
        FROM bonus_por_jogador
        WHERE qty_bonus_periodo > 0
    )
    SELECT
        bj.c_ecr_id,
        bj.qty_bonus_distintos,
        bj.qty_bonus_periodo,
        bj.total_bonus_issued,
        bj.dias_com_bonus,
        bj.max_bonus_single,
        t.p95_bonus_periodo
    FROM bonus_por_jogador bj
    CROSS JOIN threshold t
    WHERE
        -- Caso 1: Recebeu mais bonus no periodo que P95 da distribuicao
        bj.qty_bonus_periodo > GREATEST(t.p95_bonus_periodo, 5)
    ORDER BY bj.qty_bonus_periodo DESC
    """

    df = query_athena(sql, database="bonus_ec2")
    df["regra"] = "R2"
    df["severidade"] = "ALTA"
    df["descricao"] = df.apply(
        lambda r: f"{int(r['qty_bonus_periodo'])} bonus no periodo "
                  f"(P95={r.get('p95_bonus_periodo', 'N/A')}, "
                  f"{int(r['dias_com_bonus'])} dias com bonus)",
        axis=1
    )
    log.info(f"R2 — {len(df)} jogadores flagados")
    return df


# ===========================================================================
# R7 — Saque rapido pos-registro
# ===========================================================================
def rule_r7_saque_rapido(date_start: str, date_end: str) -> pd.DataFrame:
    """
    Identifica jogadores que sacaram muito rapido apos o registro.

    Logica:
      - Cruza data de registro (ps_bi.dim_user) com primeiro saque (fund_ec2)
      - Flag se sacou em menos de 24h apos o registro
      - Minimo R$50 de saque (excluir micro-valores)

    Contexto: Contas criadas exclusivamente pra explorar bonus de boas-vindas
    e sacar imediatamente sao um padrao classico de fraude em iGaming.

    Severidade: ALTA (peso 30)
    """
    log.info(f"R7 — Saque rapido pos-registro [{date_start} a {date_end}]")

    sql = f"""
    -- R7: Saque rapido — jogadores que sacaram < 24h apos registro
    -- Cruza ps_bi.dim_user (registro) com fund_ec2 (primeiro saque)
    WITH {TEST_USERS_CTE},
    -- Jogadores registrados recentemente (ultimos 30 dias antes do periodo)
    novos_jogadores AS (
        SELECT
            u.ecr_id,
            u.registration_date
        FROM ps_bi.dim_user u
        WHERE u.is_test = false
          AND u.registration_date >= date_add('day', -30, DATE '{date_start}')
    ),
    -- Primeiro saque de cada jogador novo no periodo
    primeiro_saque AS (
        SELECT
            f.c_ecr_id,
            MIN(f.c_start_time) AS primeiro_saque_time,
            SUM(f.c_amount_in_ecr_ccy / 100.0) AS total_saques_brl,
            COUNT(*) AS qty_saques
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_type = {TXN_WITHDRAW}
          AND f.c_txn_status = 'SUCCESS'
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id
        HAVING SUM(f.c_amount_in_ecr_ccy / 100.0) > 50
    ),
    -- Contexto: depositos e bets
    contexto AS (
        SELECT
            f.c_ecr_id,
            SUM(CASE WHEN f.c_txn_type = {TXN_DEPOSIT}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_dep_brl,
            SUM(CASE WHEN f.c_txn_type = {TXN_CASINO_BET}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_bets_brl,
            SUM(CASE WHEN f.c_txn_type IN ({TXN_CASINO_WIN}, {TXN_FREESPIN_WIN})
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_wins_brl
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_ecr_id IN (SELECT c_ecr_id FROM primeiro_saque)
          AND f.c_txn_status = 'SUCCESS'
        GROUP BY f.c_ecr_id
    )
    SELECT
        ps.c_ecr_id,
        nj.registration_date,
        ps.primeiro_saque_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS primeiro_saque_brt,
        -- Horas entre registro e primeiro saque
        date_diff('hour',
            CAST(nj.registration_date AS TIMESTAMP),
            ps.primeiro_saque_time
        ) AS horas_ate_saque,
        ps.total_saques_brl,
        ps.qty_saques,
        COALESCE(ctx.total_dep_brl, 0) AS total_dep_brl,
        COALESCE(ctx.total_bets_brl, 0) AS total_bets_brl,
        COALESCE(ctx.total_wins_brl, 0) AS total_wins_brl
    FROM primeiro_saque ps
    JOIN novos_jogadores nj ON ps.c_ecr_id = nj.ecr_id
    LEFT JOIN contexto ctx ON ps.c_ecr_id = ctx.c_ecr_id
    -- Flag: sacou em menos de 24 horas apos registro
    WHERE date_diff('hour',
        CAST(nj.registration_date AS TIMESTAMP),
        ps.primeiro_saque_time
    ) < 24
    ORDER BY ps.total_saques_brl DESC
    """

    df = query_athena(sql, database="fund_ec2")
    df["regra"] = "R7"
    df["severidade"] = "ALTA"
    df["descricao"] = df.apply(
        lambda r: f"Sacou R${r['total_saques_brl']:,.2f} em {int(r['horas_ate_saque'])}h apos registro "
                  f"(dep R${r['total_dep_brl']:,.2f}, bets R${r['total_bets_brl']:,.2f})",
        axis=1
    )
    log.info(f"R7 — {len(df)} jogadores flagados (saque <24h apos registro)")
    return df


# ===========================================================================
# R8 — Free Spin Abuser (Revenue negativo + Bonus alto)
# ===========================================================================
def rule_r8_freespin_abuser(date_start: str, date_end: str) -> pd.DataFrame:
    """
    Identifica jogadores com padrao de abuso de free spins:
    - Recebem volume alto de FREESPIN wins (txn_type 80)
    - Revenue fortemente negativo (casa perdendo dinheiro)
    - Bonus emitido desproporcional aos depositos

    Criterios (AND):
      - freespin_wins > R$500 no periodo
      - revenue negativo (bets - wins < 0)
      - bonus_emitido / deposito_real > 0.5 (ou deposito = 0)

    Contexto: Jogadores que exploram campanhas de free spin para extrair valor
    sem contribuir com depositos proporcionais. Padrao classico de bonus abuse.
    Caso Murilo/Fabiano (06/04/2026): R$636k negativo em 30d com R$89k depositados.

    Severidade: CRITICA (peso 30)
    """
    log.info(f"R8 — Free Spin Abuser [{date_start} a {date_end}]")

    sql = f"""
    -- R8: Free Spin Abuser — revenue negativo + bonus alto + freespin wins
    -- Cruza fund_ec2 (financeiro) com bonus_ec2 (bonus emitidos)
    WITH {TEST_USERS_CTE},
    -- Financeiro do periodo
    player_fin AS (
        SELECT
            f.c_ecr_id,
            -- Depositos reais
            SUM(CASE WHEN f.c_txn_type = {TXN_DEPOSIT}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS dep_brl,
            COUNT_IF(f.c_txn_type = {TXN_DEPOSIT}) AS dep_qty,
            -- Casino bets
            SUM(CASE WHEN f.c_txn_type = {TXN_CASINO_BET}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bets_brl,
            -- Total wins (casino + freespin + jackpot)
            SUM(CASE WHEN f.c_txn_type IN ({TXN_CASINO_WIN}, {TXN_FREESPIN_WIN}, {TXN_JACKPOT_WIN})
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS wins_brl,
            -- Free Spin wins separado (sinal principal)
            SUM(CASE WHEN f.c_txn_type = {TXN_FREESPIN_WIN}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS freespin_wins_brl,
            COUNT_IF(f.c_txn_type = {TXN_FREESPIN_WIN}) AS freespin_wins_qty,
            -- Saques
            SUM(CASE WHEN f.c_txn_type = {TXN_WITHDRAW}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS saque_brl
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_status = 'SUCCESS'
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id
        -- Filtro: pelo menos algum freespin win significativo
        HAVING SUM(CASE WHEN f.c_txn_type = {TXN_FREESPIN_WIN}
                        THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) > 500
    ),
    -- Bonus emitidos no periodo (bonus_ec2)
    player_bonus AS (
        SELECT
            bs.c_ecr_id,
            COUNT(*) AS bonus_qty,
            SUM(COALESCE(bs.c_actual_issued_amount, 0)) / 100.0 AS bonus_emitido_brl,
            COUNT(DISTINCT date_trunc('day', bs.c_issue_date)) AS dias_com_bonus
        FROM bonus_ec2.tbl_bonus_summary_details bs
        WHERE bs.c_issue_date >= TIMESTAMP '{date_start}'
          AND bs.c_issue_date < TIMESTAMP '{date_end}'
          AND bs.c_ecr_id IN (SELECT c_ecr_id FROM player_fin)
        GROUP BY bs.c_ecr_id
    )
    SELECT
        pf.c_ecr_id,
        pf.dep_brl,
        pf.dep_qty,
        pf.bets_brl,
        pf.wins_brl,
        pf.freespin_wins_brl,
        pf.freespin_wins_qty,
        pf.saque_brl,
        -- Revenue do periodo (perspectiva casa)
        ROUND(pf.bets_brl - pf.wins_brl, 2) AS revenue_brl,
        -- Bonus
        COALESCE(pb.bonus_qty, 0) AS bonus_qty,
        COALESCE(pb.bonus_emitido_brl, 0) AS bonus_emitido_brl,
        COALESCE(pb.dias_com_bonus, 0) AS dias_com_bonus,
        -- Ratios
        CASE WHEN pf.dep_brl > 0
             THEN ROUND(COALESCE(pb.bonus_emitido_brl, 0) / pf.dep_brl, 2)
             ELSE 999.99 END AS bonus_dep_ratio,
        CASE WHEN pf.dep_brl > 0
             THEN ROUND(pf.freespin_wins_brl / pf.dep_brl, 2)
             ELSE 999.99 END AS freespin_dep_ratio
    FROM player_fin pf
    LEFT JOIN player_bonus pb ON pf.c_ecr_id = pb.c_ecr_id
    WHERE
        -- Revenue negativo (casa perdendo)
        (pf.bets_brl - pf.wins_brl) < 0
        -- E bonus/deposito desproporcional OU zero deposito
        AND (
            pf.dep_brl = 0
            OR COALESCE(pb.bonus_emitido_brl, 0) / NULLIF(pf.dep_brl, 0) > 0.5
            OR pf.freespin_wins_brl / NULLIF(pf.dep_brl, 0) > 1.0
        )
    ORDER BY (pf.bets_brl - pf.wins_brl) ASC
    """

    df = query_athena(sql, database="fund_ec2")

    if df.empty:
        df["regra"] = []
        df["severidade"] = []
        df["descricao"] = []
        log.info("R8 — 0 jogadores flagados")
        return df

    df["regra"] = "R8"
    df["severidade"] = "CRITICA"
    df["descricao"] = df.apply(
        lambda r: f"FS Abuser: {int(r['freespin_wins_qty'])} FS wins (R${r['freespin_wins_brl']:,.0f}) | "
                  f"Revenue R${r['revenue_brl']:,.0f} | "
                  f"Bonus R${r['bonus_emitido_brl']:,.0f} vs Dep R${r['dep_brl']:,.0f} "
                  f"(ratio {r['bonus_dep_ratio']}x) | "
                  f"{int(r['bonus_qty'])} bonus em {int(r['dias_com_bonus'])} dias",
        axis=1
    )
    log.info(f"R8 — {len(df)} jogadores flagados (Free Spin Abuser)")
    return df


# ===========================================================================
# SCORING CONSOLIDADO
# ===========================================================================
RULE_WEIGHTS = {
    "R1": 10,   # Pico jogo (pode ser whale)
    "R2": 25,   # Bonus abuse
    "R3a": 35,  # Zero deposito + saque (fraude direta)
    "R3b": 25,  # Saque desproporcional 5x+ (suspeito)
    "R4": 15,   # Rollbacks
    "R5": 20,   # Sessoes simultaneas
    "R6": 15,   # Velocity
    "R7": 30,   # Saque rapido pos-registro
    "R8": 30,   # Free Spin Abuser
}

def score_tier(score: float) -> str:
    """Classifica risk score em tiers."""
    if score >= 81:
        return "CRITICAL"
    elif score >= 51:
        return "HIGH"
    elif score >= 21:
        return "MEDIUM"
    else:
        return "LOW"


def consolidate_alerts(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Consolida todos os alertas em um DataFrame unico com scoring."""
    if not dfs:
        return pd.DataFrame()

    all_alerts = pd.concat(dfs, ignore_index=True)

    if all_alerts.empty:
        return all_alerts

    # Calcular score por jogador (soma dos pesos das regras violadas)
    player_scores = (
        all_alerts
        .groupby("c_ecr_id")
        .agg(
            regras_violadas=("regra", lambda x: ",".join(sorted(set(x)))),
            qty_regras=("regra", "nunique"),
            max_severidade=("severidade", "first"),
            evidencias=("descricao", lambda x: " | ".join(x)),
        )
        .reset_index()
    )

    # Calcular risk score baseado nos pesos
    player_scores["risk_score"] = player_scores["regras_violadas"].apply(
        lambda regras: sum(RULE_WEIGHTS.get(r.strip(), 0) for r in regras.split(","))
    )
    player_scores["risk_tier"] = player_scores["risk_score"].apply(score_tier)
    player_scores["data_deteccao"] = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Ordenar por score decrescente
    player_scores = player_scores.sort_values("risk_score", ascending=False).reset_index(drop=True)

    return player_scores


def generate_legend(output_path: str, date_start: str, date_end: str, results: dict):
    """Gera arquivo de legenda acompanhando o CSV."""
    legend_path = output_path.replace(".csv", "_legenda.txt")

    with open(legend_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("  LEGENDA — Relatorio de Alertas de Fraude MultiBet\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Periodo analisado: {date_start} a {date_end}\n")
        f.write(f"Data de geracao: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"Fonte: Athena (fund_ec2, bireports_ec2, bonus_ec2)\n\n")

        f.write("--- COLUNAS ---\n")
        f.write("c_ecr_id           ID unico do jogador (18 digitos Pragmatic)\n")
        f.write("regras_violadas    Regras de fraude violadas (R1-R8)\n")
        f.write("qty_regras         Quantidade de regras violadas\n")
        f.write("risk_score         Pontuacao de risco (0-175, soma dos pesos)\n")
        f.write("risk_tier          Classificacao: LOW/MEDIUM/HIGH/CRITICAL\n")
        f.write("evidencias         Detalhes de cada violacao\n")
        f.write("data_deteccao      Data/hora em que o alerta foi gerado\n\n")

        f.write("--- REGRAS E PESOS ---\n")
        f.write("R1  (peso 10)  Pico em jogo anormal — bets > 3x desvio padrao historico\n")
        f.write("R2  (peso 25)  Abuso de bonus — bonus recebidos > P95 da distribuicao\n")
        f.write("R3a (peso 35)  NUNCA depositou na vida + saque > R$50 no periodo\n")
        f.write("R3b (peso 25)  Saque desproporcional — cashout > 5x depositos, > R$200\n")
        f.write("R4  (peso 15)  Rollbacks excessivos — > P99 da distribuicao\n")
        f.write("R5  (peso 20)  Sessoes simultaneas — 3+ sessoes sobrepostas\n")
        f.write("R6  (peso 15)  Velocity check — 5+ depositos/saques em 1 hora\n")
        f.write("R7  (peso 30)  Saque rapido — sacou < 24h apos registro\n")
        f.write("R8  (peso 30)  Free Spin Abuser — revenue negativo + bonus alto + FS wins\n\n")

        f.write("--- TIERS DE RISCO ---\n")
        f.write("LOW (0-20)       Monitorar — pode ser comportamento normal\n")
        f.write("MEDIUM (21-50)   Revisar manualmente — padrao suspeito\n")
        f.write("HIGH (51-80)     Bloquear bonus, limitar saques — risco real\n")
        f.write("CRITICAL (81+)   Escalar para compliance — fraude provavel\n\n")

        f.write("--- ACAO SUGERIDA POR TIER ---\n")
        f.write("LOW:      Adicionar a watchlist, monitorar proximas 2 semanas\n")
        f.write("MEDIUM:   Revisar manualmente, verificar KYC e historico\n")
        f.write("HIGH:     Bloquear bonus, limitar saques, alertar time de risco\n")
        f.write("CRITICAL: Suspender conta, escalar para compliance imediatamente\n\n")

        f.write("--- RESULTADOS POR REGRA ---\n")
        for rule, count in results.items():
            f.write(f"{rule}: {count} jogadores flagados\n")

        f.write("\n--- GLOSSARIO ---\n")
        f.write("GGR       Gross Gaming Revenue (bets - wins da casa)\n")
        f.write("Rollback  Cancelamento de aposta (pode indicar exploits)\n")
        f.write("Velocity  Velocidade de transacoes (padrao de lavagem)\n")
        f.write("CCF       Customer Conduct Factor (score nativo da plataforma)\n")
        f.write("AML       Anti-Money Laundering (prevencao lavagem de dinheiro)\n")

    log.info(f"Legenda salva em: {legend_path}")


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="Agente de Riscos — Deteccao de Fraude MultiBet")
    parser.add_argument("--days", type=int, default=7, help="Quantidade de dias para analisar (default: 7)")
    parser.add_argument("--rule", type=str, default=None, help="Rodar apenas uma regra (R3, R4, R5, R6)")
    parser.add_argument("--all", action="store_true", help="Rodar todas as regras disponiveis")
    args = parser.parse_args()

    date_start, date_end = get_date_range(args.days)
    log.info(f"Periodo: {date_start} a {date_end} ({args.days} dias)")

    # Mapeia regras para funcoes
    # R3a e R3b compartilham a mesma query base (otimizacao de custo Athena)
    _flows_cache = {}

    def _r3a(ds, de):
        # R3a agora tem query propria com lifetime deposits (nao usa flows_cache)
        return rule_r3a_zero_deposit(ds, de)

    def _r3b(ds, de):
        if "flows" not in _flows_cache:
            log.info("Carregando fluxos financeiros (query compartilhada R3a/R3b)...")
            _flows_cache["flows"] = _query_player_flows(ds, de)
        return rule_r3b_ratio_desproporcional(ds, de, flows_df=_flows_cache["flows"])

    rule_map = {
        "R1": rule_r1_pico_jogo,
        "R2": rule_r2_bonus_abuse,
        "R3a": _r3a,
        "R3b": _r3b,
        "R4": rule_r4_rollbacks,
        "R5": rule_r5_sessoes_simultaneas,
        "R6": rule_r6_velocity,
        "R7": rule_r7_saque_rapido,
        "R8": rule_r8_freespin_abuser,
    }

    # Seleciona quais regras rodar
    if args.rule:
        key = args.rule.upper()
        rules_to_run = {key: rule_map[key]}
    elif args.all:
        rules_to_run = rule_map
    else:
        # Default: R1, R2, R3a, R3b, R4, R6 (todas exceto R5 que e pesada)
        rules_to_run = {k: v for k, v in rule_map.items() if k != "R5"}

    # Executa cada regra
    alert_dfs = []
    results_summary = {}
    for rule_name, rule_fn in rules_to_run.items():
        try:
            df = rule_fn(date_start, date_end)
            alert_dfs.append(df)
            results_summary[rule_name] = len(df)
        except Exception as e:
            log.error(f"{rule_name} falhou: {e}")
            results_summary[rule_name] = f"ERRO: {e}"

    # Consolida e salva
    consolidated = consolidate_alerts(alert_dfs)

    today = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(OUTPUT_DIR, f"risk_fraud_alerts_{today}.csv")

    if not consolidated.empty:
        consolidated.to_csv(output_path, index=False)
        log.info(f"Relatorio salvo: {output_path} ({len(consolidated)} jogadores)")

        # Resumo por tier
        tier_summary = consolidated["risk_tier"].value_counts()
        log.info("--- RESUMO POR TIER ---")
        for tier, count in tier_summary.items():
            log.info(f"  {tier}: {count} jogadores")
    else:
        log.info("Nenhum jogador flagado no periodo.")
        # Salva CSV vazio com headers
        pd.DataFrame(columns=[
            "c_ecr_id", "regras_violadas", "qty_regras", "risk_score",
            "risk_tier", "evidencias", "data_deteccao"
        ]).to_csv(output_path, index=False)

    # Gerar legenda
    generate_legend(output_path, date_start, date_end, results_summary)

    log.info("Deteccao de fraude concluida.")
    return consolidated


if __name__ == "__main__":
    main()
