"""
Alertas de Risco — Sportsbook (Live Delay + Cancelamentos)
===========================================================
Detecta padroes suspeitos em apostas esportivas:

Regras:
  R9  — Live Delay Exploitation: jogadores com win rate anormal em live betting
         + padrao test-then-bet (aposta baixa → aposta alta no mesmo evento)
  R10 — Cancelamento/Refund excessivo: jogadores com volume anormal de refunds

Contexto:
  Demanda da equipe de riscos (06/04/2026): detectar jogadores que exploram
  delay em apostas ao vivo. Padrao observado: aposta teste de R$1, depois
  entrada forte no mesmo mercado live. Alto indice de acerto.

  Castrin pediu incluir cancelamentos de sportsbook no mesmo alerta.

Dados:
  - vendor_ec2.tbl_sports_book_bets_info (header do bilhete)
  - vendor_ec2.tbl_sports_book_bet_details (legs/selecoes)
  - vendor_ec2.tbl_sports_book_info (transacoes financeiras)

Descobertas da discovery (06/04/2026):
  - c_bet_state: so tem 'O' (Open) e 'C' (Closed) — NAO existe VOID/CANCELLED
  - c_bet_type = 'Live' identifica live bets (c_is_live e sempre false)
  - Cancelamentos = c_operation_type = 'R' (Refund) em tbl_sports_book_info
  - Stakes em BRL real (NAO centavos)
  - IP do jogador: NAO disponivel no Athena (multi-account por IP = impossivel agora)

Uso:
    python scripts/risk_sportsbook_alerts.py --days 7
    python scripts/risk_sportsbook_alerts.py --days 14 --rule R9
    python scripts/risk_sportsbook_alerts.py --days 7 --rule R10

Saida:
    output/risk_sportsbook_alerts_YYYY-MM-DD.csv
    output/risk_sportsbook_alerts_YYYY-MM-DD_legenda.txt

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-06
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


def get_date_range(days: int) -> tuple[str, str]:
    """Retorna (data_inicio, data_fim) para filtro Athena. Usa D-1 como fim."""
    end = datetime.now().date() - timedelta(days=1)   # D-1 (dados completos)
    start = end - timedelta(days=days - 1)
    return str(start), str(end + timedelta(days=1))   # end+1 pra < funcionar


# ===========================================================================
# Filtro de test users — CTE reutilizavel
# ===========================================================================
TEST_USERS_CTE = """
    test_users AS (
        SELECT CAST(external_id AS BIGINT) AS customer_id
        FROM ps_bi.dim_user
        WHERE is_test = true
    )
"""

TEST_USERS_FILTER_CUSTOMER = "AND b.c_customer_id NOT IN (SELECT customer_id FROM test_users)"


# ===========================================================================
# R9 — Live Delay Exploitation
# ===========================================================================
def rule_r9_live_delay(date_start: str, date_end: str) -> pd.DataFrame:
    """
    R9: Detecta jogadores com padrao de exploracao de delay em apostas live.

    Indicadores combinados:
      1. Win rate alto em live (>= 60% com minimo 10 apostas fechadas)
      2. Lucro significativo (> R$500)
      3. Concentracao em live (>70% das apostas sao live)

    O padrao descrito pela equipe de riscos:
      - Aposta teste de baixo valor (R$1-10)
      - Em seguida aposta alta no mesmo mercado live
      - Alto indice de acerto em mercados sensiveis (under, HT, etc.)

    Severidade: HIGH (live delay e fraude ativa contra a casa)
    """
    log.info(f"R9 — Live Delay Exploitation [{date_start} a {date_end}]")

    # -----------------------------------------------------------------------
    # Parte 1: Perfil geral do apostador live (win rate, volume, lucro)
    # -----------------------------------------------------------------------
    # NOTA: Ciclo de vida do bilhete na tabela:
    #   M (Commit) = aposta feita, c_bet_state = O (Open), c_total_return = NaN
    #   P (Payout) = bilhete encerrado, c_bet_state = C (Closed), c_total_return preenchido
    # Para ver resultados, usar SOMENTE registros P (payout).
    # Para ver stakes/volume original, usar M (commit).
    sql_profile = f"""
    WITH {TEST_USERS_CTE},
    -- Apostas originais (M = commit) para stakes e volume
    commits AS (
        SELECT
            b.c_customer_id,
            b.c_bet_slip_id,
            b.c_total_stake,
            b.c_bet_type,
            b.c_created_time
        FROM vendor_ec2.tbl_sports_book_bets_info b
        WHERE b.c_transaction_type = 'M'
          AND b.c_created_time >= TIMESTAMP '{date_start}'
          AND b.c_created_time < TIMESTAMP '{date_end}'
          {TEST_USERS_FILTER_CUSTOMER}
    ),
    -- Resultados (P = payout) para returns e win/loss
    payouts AS (
        SELECT
            b.c_customer_id,
            b.c_bet_slip_id,
            b.c_total_stake,
            b.c_total_return,
            b.c_bet_type
        FROM vendor_ec2.tbl_sports_book_bets_info b
        WHERE b.c_transaction_type = 'P'
          AND b.c_bet_state = 'C'
          AND b.c_created_time >= TIMESTAMP '{date_start}'
          AND b.c_created_time < TIMESTAMP '{date_end}'
          {TEST_USERS_FILTER_CUSTOMER}
    ),
    -- Perfil consolidado por jogador
    player_profile AS (
        SELECT
            c.c_customer_id,
            -- Total geral (commits)
            COUNT(DISTINCT c.c_bet_slip_id) AS total_bets,
            ROUND(SUM(c.c_total_stake), 2) AS total_stake,
            -- Live commits
            COUNT(DISTINCT CASE WHEN c.c_bet_type = 'Live' THEN c.c_bet_slip_id END) AS live_bets,
            ROUND(SUM(CASE WHEN c.c_bet_type = 'Live' THEN c.c_total_stake ELSE 0 END), 2) AS live_stake,
            -- Stake min/max em live (pra detectar range test→bet)
            MIN(CASE WHEN c.c_bet_type = 'Live' THEN c.c_total_stake END) AS live_min_stake,
            MAX(CASE WHEN c.c_bet_type = 'Live' THEN c.c_total_stake END) AS live_max_stake
        FROM commits c
        GROUP BY c.c_customer_id
    ),
    -- Resultados live (payouts)
    live_results AS (
        SELECT
            p.c_customer_id,
            COUNT(DISTINCT p.c_bet_slip_id) AS live_closed,
            COUNT(DISTINCT CASE WHEN p.c_total_return > p.c_total_stake
                                THEN p.c_bet_slip_id END) AS live_wins,
            ROUND(SUM(p.c_total_return), 2) AS live_return,
            ROUND(SUM(p.c_total_stake), 2) AS live_stake_closed
        FROM payouts p
        WHERE p.c_bet_type = 'Live'
        GROUP BY p.c_customer_id
        -- Minimo 5 apostas live fechadas pra ter significancia
        HAVING COUNT(DISTINCT p.c_bet_slip_id) >= 5
    )
    SELECT
        pp.c_customer_id,
        pp.total_bets,
        pp.total_stake,
        pp.live_bets,
        pp.live_stake,
        lr.live_closed,
        lr.live_wins,
        ROUND(CAST(lr.live_wins AS DOUBLE) / NULLIF(lr.live_closed, 0) * 100, 1) AS live_win_rate_pct,
        lr.live_return,
        lr.live_stake_closed,
        ROUND(lr.live_return - lr.live_stake_closed, 2) AS live_profit,
        ROUND(CAST(pp.live_bets AS DOUBLE) / NULLIF(pp.total_bets, 0) * 100, 1) AS live_concentration_pct,
        pp.live_min_stake,
        pp.live_max_stake,
        CASE WHEN pp.live_max_stake > 0
             THEN ROUND(pp.live_max_stake / NULLIF(pp.live_min_stake, 0), 1)
             ELSE 0 END AS stake_range_ratio
    FROM player_profile pp
    INNER JOIN live_results lr ON pp.c_customer_id = lr.c_customer_id
    WHERE
        -- Win rate alto em live (>=55%)
        CAST(lr.live_wins AS DOUBLE) / NULLIF(lr.live_closed, 0) >= 0.55
        -- Lucro significativo
        AND (lr.live_return - lr.live_stake_closed) > 200
    ORDER BY (lr.live_return - lr.live_stake_closed) DESC
    """

    df_profile = query_athena(sql_profile, database="vendor_ec2")
    log.info(f"R9 perfil: {len(df_profile)} jogadores com win rate >=55% e lucro >R$200 em live")

    if df_profile.empty:
        df_profile["regra"] = []
        df_profile["severidade"] = []
        df_profile["descricao"] = []
        return df_profile

    # -----------------------------------------------------------------------
    # Parte 2: Detectar padrao test-then-bet (mesma event_id, stake crescente)
    # Para os jogadores ja flagados no perfil
    # -----------------------------------------------------------------------
    flagged_ids = ",".join([str(x) for x in df_profile["c_customer_id"].tolist()])

    sql_pattern = f"""
    WITH {TEST_USERS_CTE},
    -- Apostas live dos jogadores flagados, com detalhes do evento
    live_detail AS (
        SELECT
            b.c_customer_id,
            b.c_bet_slip_id,
            b.c_total_stake,
            b.c_total_return,
            b.c_bet_state,
            d.c_event_id,
            d.c_event_name,
            d.c_market_name,
            d.c_leg_status,
            d.c_sport_type_name,
            b.c_created_time
        FROM vendor_ec2.tbl_sports_book_bets_info b
        JOIN vendor_ec2.tbl_sports_book_bet_details d
            ON b.c_bet_slip_id = d.c_bet_slip_id
            AND b.c_transaction_id = d.c_transaction_id
        WHERE b.c_customer_id IN ({flagged_ids})
          AND b.c_bet_type = 'Live'
          AND b.c_transaction_type = 'M'
          AND b.c_created_time >= TIMESTAMP '{date_start}'
          AND b.c_created_time < TIMESTAMP '{date_end}'
    ),
    -- Para cada jogador+evento, verificar se ha padrao de stake crescente
    event_pattern AS (
        SELECT
            c_customer_id,
            c_event_id,
            c_event_name,
            COUNT(*) AS bets_on_event,
            MIN(c_total_stake) AS min_stake_event,
            MAX(c_total_stake) AS max_stake_event,
            -- Se tem aposta <= R$10 E aposta >= R$50 no mesmo evento = test-then-bet
            COUNT(CASE WHEN c_total_stake <= 10 THEN 1 END) AS low_bets,
            COUNT(CASE WHEN c_total_stake >= 50 THEN 1 END) AS high_bets,
            -- Win rate no evento
            COUNT(CASE WHEN c_leg_status = 'W' THEN 1 END) AS wins_event,
            ARRAY_AGG(DISTINCT c_market_name) AS markets_used
        FROM live_detail
        GROUP BY c_customer_id, c_event_id, c_event_name
        HAVING COUNT(*) >= 2
    )
    SELECT
        c_customer_id,
        COUNT(*) AS events_with_multi_bets,
        SUM(CASE WHEN low_bets > 0 AND high_bets > 0 THEN 1 ELSE 0 END) AS test_then_bet_events,
        SUM(bets_on_event) AS total_live_bets_detail,
        SUM(wins_event) AS total_wins_detail,
        MAX(max_stake_event) AS max_single_stake,
        SUM(CASE WHEN low_bets > 0 AND high_bets > 0 THEN high_bets ELSE 0 END) AS high_bets_after_test
    FROM event_pattern
    GROUP BY c_customer_id
    """

    df_pattern = query_athena(sql_pattern, database="vendor_ec2")
    log.info(f"R9 pattern: {len(df_pattern)} jogadores com apostas multiplas no mesmo evento")

    # -----------------------------------------------------------------------
    # Parte 3: Merge perfil + pattern e classificar severidade
    # -----------------------------------------------------------------------
    df = df_profile.copy()

    if not df_pattern.empty:
        df = df.merge(df_pattern, on="c_customer_id", how="left")
    else:
        df["events_with_multi_bets"] = 0
        df["test_then_bet_events"] = 0
        df["total_live_bets_detail"] = 0
        df["total_wins_detail"] = 0
        df["max_single_stake"] = 0
        df["high_bets_after_test"] = 0

    df.fillna(0, inplace=True)

    # Scoring de severidade
    def classify_r9(row):
        score = 0
        reasons = []

        # Win rate
        wr = row.get("live_win_rate_pct", 0)
        if wr >= 80:
            score += 40
            reasons.append(f"WinRate={wr}% (>=80%)")
        elif wr >= 70:
            score += 25
            reasons.append(f"WinRate={wr}% (>=70%)")
        elif wr >= 60:
            score += 15
            reasons.append(f"WinRate={wr}% (>=60%)")

        # Lucro
        profit = row.get("live_profit", 0)
        if profit >= 10000:
            score += 30
            reasons.append(f"Lucro=R${profit:,.0f} (>=10K)")
        elif profit >= 5000:
            score += 20
            reasons.append(f"Lucro=R${profit:,.0f} (>=5K)")
        elif profit >= 1000:
            score += 10
            reasons.append(f"Lucro=R${profit:,.0f} (>=1K)")

        # Test-then-bet pattern
        ttb = row.get("test_then_bet_events", 0)
        if ttb >= 5:
            score += 30
            reasons.append(f"TestBet={int(ttb)} eventos (>=5)")
        elif ttb >= 2:
            score += 15
            reasons.append(f"TestBet={int(ttb)} eventos (>=2)")

        # Live concentration
        conc = row.get("live_concentration_pct", 0)
        if conc >= 90:
            score += 10
            reasons.append(f"Concentracao={conc}% live")

        # Stake range ratio (max/min) — indica variedade de valores
        srr = row.get("stake_range_ratio", 0)
        if srr >= 50:
            score += 10
            reasons.append(f"StakeRange={srr}x (min→max)")

        # Classificacao
        if score >= 60:
            sev = "HIGH"
        elif score >= 35:
            sev = "MEDIUM"
        else:
            sev = "LOW"

        return pd.Series({"risk_score_r9": score, "severidade": sev, "evidencias": " | ".join(reasons)})

    scored = df.apply(classify_r9, axis=1)
    df = pd.concat([df, scored], axis=1)

    df["regra"] = "R9"
    df["descricao"] = "Live Delay Exploitation"

    log.info(f"R9 — {len(df)} jogadores flagados")
    if not df.empty:
        sev_counts = df["severidade"].value_counts()
        for sev, count in sev_counts.items():
            log.info(f"  {sev}: {count}")

    return df


# ===========================================================================
# R10 — Cancelamento/Refund excessivo
# ===========================================================================
def rule_r10_cancel_abuse(date_start: str, date_end: str) -> pd.DataFrame:
    """
    R10: Jogadores com volume anormal de refunds/cancelamentos em sportsbook.

    Usa tbl_sports_book_info.c_operation_type = 'R' (Refund).
    Nota: c_bet_state NAO tem VOID/CANCELLED (so O e C).

    Criterios:
      - Mais de 3 refunds no periodo OU
      - Valor refundado > R$500

    Enriquece com contexto: total apostado, ratio refund/total

    Severidade: MEDIUM por padrao, HIGH se ratio alto + volume
    """
    log.info(f"R10 — Cancelamento/Refund Abuse [{date_start} a {date_end}]")

    sql = f"""
    WITH {TEST_USERS_CTE},
    -- Refunds no periodo
    refunds AS (
        SELECT
            s.c_customer_id,
            COUNT(*) AS qty_refunds,
            ROUND(SUM(s.c_amount), 2) AS total_refund_brl,
            COUNT(DISTINCT s.c_bet_slip_id) AS slips_refundados,
            MIN(s.c_created_time AT TIME ZONE 'UTC'
                AT TIME ZONE 'America/Sao_Paulo') AS primeiro_refund_brt,
            MAX(s.c_created_time AT TIME ZONE 'UTC'
                AT TIME ZONE 'America/Sao_Paulo') AS ultimo_refund_brt
        FROM vendor_ec2.tbl_sports_book_info s
        WHERE s.c_operation_type = 'R'
          AND s.c_created_time >= TIMESTAMP '{date_start}'
          AND s.c_created_time < TIMESTAMP '{date_end}'
          AND s.c_customer_id NOT IN (SELECT customer_id FROM test_users)
        GROUP BY s.c_customer_id
        HAVING COUNT(*) >= 3 OR SUM(s.c_amount) >= 500
    ),
    -- Contexto: total apostado no mesmo periodo
    bets_context AS (
        SELECT
            b.c_customer_id,
            COUNT(DISTINCT b.c_bet_slip_id) AS total_bets,
            ROUND(SUM(b.c_total_stake), 2) AS total_stake,
            ROUND(SUM(CASE WHEN b.c_bet_state = 'C' THEN b.c_total_return ELSE 0 END), 2) AS total_return,
            COUNT(DISTINCT CASE WHEN b.c_bet_type = 'Live' THEN b.c_bet_slip_id END) AS live_bets
        FROM vendor_ec2.tbl_sports_book_bets_info b
        WHERE b.c_transaction_type = 'M'
          AND b.c_created_time >= TIMESTAMP '{date_start}'
          AND b.c_created_time < TIMESTAMP '{date_end}'
          AND b.c_customer_id IN (SELECT c_customer_id FROM refunds)
        GROUP BY b.c_customer_id
    )
    SELECT
        r.c_customer_id,
        r.qty_refunds,
        r.total_refund_brl,
        r.slips_refundados,
        r.primeiro_refund_brt,
        r.ultimo_refund_brt,
        COALESCE(bc.total_bets, 0) AS total_bets,
        COALESCE(bc.total_stake, 0) AS total_stake,
        COALESCE(bc.total_return, 0) AS total_return,
        COALESCE(bc.live_bets, 0) AS live_bets,
        CASE WHEN COALESCE(bc.total_stake, 0) > 0
             THEN ROUND(r.total_refund_brl / bc.total_stake * 100, 1)
             ELSE 100.0 END AS refund_pct_of_stake,
        ROUND(COALESCE(bc.total_return, 0) - COALESCE(bc.total_stake, 0), 2) AS profit_brl
    FROM refunds r
    LEFT JOIN bets_context bc ON r.c_customer_id = bc.c_customer_id
    ORDER BY r.total_refund_brl DESC
    """

    df = query_athena(sql, database="vendor_ec2")
    log.info(f"R10 — {len(df)} jogadores com refunds excessivos")

    if df.empty:
        df["regra"] = []
        df["severidade"] = []
        df["descricao"] = []
        df["evidencias"] = []
        df["risk_score_r10"] = []
        return df

    # Classificacao de severidade
    def classify_r10(row):
        score = 0
        reasons = []

        qty = row.get("qty_refunds", 0)
        if qty >= 10:
            score += 30
            reasons.append(f"Refunds={qty} (>=10)")
        elif qty >= 5:
            score += 15
            reasons.append(f"Refunds={qty} (>=5)")
        else:
            score += 5
            reasons.append(f"Refunds={qty}")

        val = row.get("total_refund_brl", 0)
        if val >= 5000:
            score += 30
            reasons.append(f"ValorRefund=R${val:,.0f} (>=5K)")
        elif val >= 1000:
            score += 20
            reasons.append(f"ValorRefund=R${val:,.0f} (>=1K)")
        elif val >= 500:
            score += 10
            reasons.append(f"ValorRefund=R${val:,.0f} (>=500)")

        ratio = row.get("refund_pct_of_stake", 0)
        if ratio >= 50:
            score += 20
            reasons.append(f"RefundRatio={ratio}% do stake")
        elif ratio >= 20:
            score += 10
            reasons.append(f"RefundRatio={ratio}% do stake")

        if score >= 50:
            sev = "HIGH"
        elif score >= 25:
            sev = "MEDIUM"
        else:
            sev = "LOW"

        return pd.Series({"risk_score_r10": score, "severidade": sev, "evidencias": " | ".join(reasons)})

    scored = df.apply(classify_r10, axis=1)
    df = pd.concat([df, scored], axis=1)

    df["regra"] = "R10"
    df["descricao"] = "Cancelamento/Refund Excessivo"

    if not df.empty:
        sev_counts = df["severidade"].value_counts()
        for sev, count in sev_counts.items():
            log.info(f"  {sev}: {count}")

    return df


# ===========================================================================
# Consolidacao e output
# ===========================================================================
def consolidate_sportsbook_alerts(dfs: list[pd.DataFrame]) -> pd.DataFrame:
    """Consolida alertas de todas as regras sportsbook."""
    valid = [df for df in dfs if not df.empty]
    if not valid:
        return pd.DataFrame()

    # Normalizar colunas — cada regra pode ter colunas diferentes
    all_rows = []
    for df in valid:
        for _, row in df.iterrows():
            all_rows.append({
                "customer_id": row.get("c_customer_id"),
                "regra": row.get("regra"),
                "severidade": row.get("severidade"),
                "evidencias": row.get("descricao", "") + ": " + row.get("evidencias", ""),
                "data_deteccao": datetime.now().strftime("%Y-%m-%d"),
            })

    result = pd.DataFrame(all_rows)

    # Agregar por jogador (pode aparecer em R9 e R10)
    if result.empty:
        return result

    grouped = result.groupby("customer_id").agg(
        regras_violadas=("regra", lambda x: "+".join(sorted(set(x)))),
        qty_regras=("regra", "nunique"),
        max_severidade=("severidade", lambda x: max(x, key=lambda s: {"LOW": 1, "MEDIUM": 2, "HIGH": 3}.get(s, 0))),
        todas_evidencias=("evidencias", lambda x: " || ".join(x)),
        data_deteccao=("data_deteccao", "first"),
    ).reset_index()

    # Ordenar por severidade e qtd regras
    sev_order = {"HIGH": 3, "MEDIUM": 2, "LOW": 1}
    grouped["_sev_ord"] = grouped["max_severidade"].map(sev_order)
    grouped = grouped.sort_values(["_sev_ord", "qty_regras"], ascending=[False, False])
    grouped.drop(columns=["_sev_ord"], inplace=True)

    return grouped


def generate_legend(csv_path: str, date_start: str, date_end: str, results: dict):
    """Gera arquivo de legenda acompanhando o CSV."""
    legend_path = csv_path.replace(".csv", "_legenda.txt")

    with open(legend_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("ALERTA DE RISCOS — SPORTSBOOK (Live Delay + Cancelamentos)\n")
        f.write("=" * 70 + "\n\n")

        f.write(f"Periodo analisado: {date_start} a {date_end}\n")
        f.write(f"Data geracao: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n")
        f.write(f"Fonte: Athena (vendor_ec2) — dados em BRL real\n\n")

        f.write("--- REGRAS ---\n\n")

        f.write("R9 — Live Delay Exploitation\n")
        f.write("  O que detecta: jogadores com win rate anormalmente alto em apostas live,\n")
        f.write("  combinado com padrao de 'teste' (aposta baixa) seguido de aposta alta\n")
        f.write("  no mesmo evento. Indicativo de exploracao de delay/latencia.\n")
        f.write("  Criterios: win rate >=55%, lucro >R$200, min 5 apostas live fechadas\n")
        f.write("  Fonte: tbl_sports_book_bets_info + tbl_sports_book_bet_details\n\n")

        f.write("R10 — Cancelamento/Refund Excessivo\n")
        f.write("  O que detecta: jogadores com volume anormal de refunds em sportsbook.\n")
        f.write("  Criterios: >=3 refunds no periodo OU valor refundado >=R$500\n")
        f.write("  Fonte: tbl_sports_book_info (c_operation_type = 'R')\n")
        f.write("  NOTA: c_bet_state NAO tem VOID/CANCELLED — so O (Open) e C (Closed).\n")
        f.write("  Cancelamentos sao registrados como Refund na tabela de transacoes.\n\n")

        f.write("--- COLUNAS DO CSV ---\n")
        f.write("customer_id       External ID do jogador (join com Smartico user_ext_id)\n")
        f.write("regras_violadas   Regras que o jogador violou (R9, R10, R9+R10)\n")
        f.write("qty_regras        Quantidade de regras violadas\n")
        f.write("max_severidade    Severidade mais alta entre as regras\n")
        f.write("todas_evidencias  Detalhes de cada violacao\n")
        f.write("data_deteccao     Data em que o alerta foi gerado\n\n")

        f.write("--- SEVERIDADE ---\n")
        f.write("LOW:    Monitorar, volume baixo mas padrao presente\n")
        f.write("MEDIUM: Revisar manualmente, verificar historico de apostas\n")
        f.write("HIGH:   Investigar urgente — forte indicativo de exploracao\n\n")

        f.write("--- RESULTADOS POR REGRA ---\n")
        for rule, count in results.items():
            f.write(f"{rule}: {count} jogadores flagados\n")

        f.write("\n--- NOTA SOBRE MULTI-CONTA POR IP ---\n")
        f.write("A equipe de riscos solicitou deteccao de multi-account por IP.\n")
        f.write("Porem, NAO temos dados de IP no Athena (ecr_ec2 nao armazena IP).\n")
        f.write("Para implementar, seria necessario acesso a logs de sessao/login\n")
        f.write("ou integracao com sistema de autenticacao que registre IPs.\n")

        f.write("\n--- GLOSSARIO ---\n")
        f.write("Win Rate        % de apostas vencedoras sobre total fechadas\n")
        f.write("Live Bet        Aposta feita durante o evento ao vivo\n")
        f.write("Test-then-Bet   Padrao: aposta baixa (<=R$10) + alta (>=R$50) no mesmo evento\n")
        f.write("Refund          Cancelamento/estorno de aposta (c_operation_type = 'R')\n")
        f.write("Stake           Valor apostado em BRL\n")
        f.write("GGR Sportsbook  Gross Gaming Revenue = Stake - Returns (receita da casa)\n")

    log.info(f"Legenda salva: {legend_path}")


# ===========================================================================
# MAIN
# ===========================================================================
def main():
    parser = argparse.ArgumentParser(description="Alertas de Risco — Sportsbook")
    parser.add_argument("--days", type=int, default=7, help="Dias para analisar (default: 7)")
    parser.add_argument("--rule", type=str, default=None, help="Rodar apenas uma regra (R9, R10)")
    args = parser.parse_args()

    date_start, date_end = get_date_range(args.days)
    log.info(f"Periodo: {date_start} a {date_end} ({args.days} dias)")

    rule_map = {
        "R9": rule_r9_live_delay,
        "R10": rule_r10_cancel_abuse,
    }

    if args.rule:
        key = args.rule.upper()
        if key not in rule_map:
            log.error(f"Regra '{key}' nao existe. Disponiveis: {list(rule_map.keys())}")
            return
        rules_to_run = {key: rule_map[key]}
    else:
        rules_to_run = rule_map

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

    # Consolida
    consolidated = consolidate_sportsbook_alerts(alert_dfs)

    today = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(OUTPUT_DIR, f"risk_sportsbook_alerts_{today}.csv")

    if not consolidated.empty:
        consolidated.to_csv(output_path, index=False)
        log.info(f"Relatorio salvo: {output_path} ({len(consolidated)} jogadores)")

        sev_summary = consolidated["max_severidade"].value_counts()
        log.info("--- RESUMO POR SEVERIDADE ---")
        for sev, count in sev_summary.items():
            log.info(f"  {sev}: {count}")
    else:
        log.info("Nenhum jogador flagado no periodo.")
        pd.DataFrame(columns=[
            "customer_id", "regras_violadas", "qty_regras",
            "max_severidade", "todas_evidencias", "data_deteccao"
        ]).to_csv(output_path, index=False)

    generate_legend(output_path, date_start, date_end, results_summary)
    log.info("Alertas sportsbook concluidos.")
    return consolidated


if __name__ == "__main__":
    main()
