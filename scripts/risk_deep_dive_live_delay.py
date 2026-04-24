"""
Deep Dive: Live Delay Exploitation — 2 jogadores com WR 100% (caso 07/04/2026)
==============================================================================

Investiga em profundidade os 2 jogadores que foram flagados com Win Rate 100%
no R9 (Live Delay Exploitation):

  - 764641775223027 — Lucro R$16.005 (100% WR)
  - 777971772567301 — Lucro R$12.045 (100% WR)

Objetivo: identificar EXATAMENTE quais esportes, ligas, eventos e mercados
esses jogadores exploraram. Validar o padrao "R$1 de teste + aposta alta em
live". Gerar relatorio .md consolidado.

Fonte: Athena vendor_ec2 (tbl_sports_book_bets_info + tbl_sports_book_bet_details)
Periodo: ultimos 60 dias (janela ampla para pegar padroes historicos)

Autor: Squad 3 Intelligence Engine
Data: 2026-04-10
"""

import sys
import os
import logging
from datetime import datetime, timedelta
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
REPORTS_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)

# IDs dos jogadores flagados com WR 100% no R9 (07/04/2026)
SUSPECTS = [764641775223027, 777971772567301]

# Janela ampla: 60 dias para pegar historico completo
END_DATE = datetime.now().date() - timedelta(days=1)  # D-1
START_DATE = END_DATE - timedelta(days=60)


def fetch_all_bets(customer_ids: list[int], start: str, end: str) -> pd.DataFrame:
    """
    Puxa TODAS as apostas (slips) dos jogadores no periodo.
    Junta bets_info (financeiro) com bet_details (evento/esporte).

    Retorna DataFrame com 1 linha por slip (agregando legs em listas).
    """
    ids_str = ",".join(str(x) for x in customer_ids)
    sql = f"""
    -- Deep dive: apostas completas dos 2 suspeitos com detalhe de evento/esporte
    WITH commits AS (
        -- transaction_type = 'M' = aposta feita
        SELECT
            b.c_customer_id,
            b.c_bet_slip_id,
            b.c_transaction_id,
            b.c_total_stake,
            b.c_total_odds,
            b.c_bet_type,
            b.c_bet_state,
            b.c_created_time
        FROM vendor_ec2.tbl_sports_book_bets_info b
        WHERE b.c_customer_id IN ({ids_str})
          AND b.c_transaction_type = 'M'
          AND b.c_created_time >= TIMESTAMP '{start}'
          AND b.c_created_time < TIMESTAMP '{end}'
    ),
    payouts AS (
        -- transaction_type = 'P' = resultado fechado
        -- IMPORTANTE: sem filtro de data no payout — o settlement pode ser posterior
        SELECT
            b.c_customer_id,
            b.c_bet_slip_id,
            b.c_total_stake AS payout_stake,
            b.c_total_return,
            b.c_bet_state AS payout_state,
            b.c_bet_closure_time
        FROM vendor_ec2.tbl_sports_book_bets_info b
        WHERE b.c_customer_id IN ({ids_str})
          AND b.c_transaction_type = 'P'
    ),
    -- Refunds (operacao R em tbl_sports_book_info)
    refunds AS (
        SELECT
            s.c_bet_slip_id,
            COUNT(*) AS qty_refunds,
            SUM(s.c_amount) AS total_refund_brl
        FROM vendor_ec2.tbl_sports_book_info s
        WHERE s.c_customer_id IN ({ids_str})
          AND s.c_operation_type = 'R'
        GROUP BY s.c_bet_slip_id
    ),
    legs AS (
        -- Detalhes do evento (leg mais relevante por slip - pegamos a primeira)
        SELECT
            d.c_bet_slip_id,
            d.c_customer_id,
            -- Agregamos todas as legs num array para ver multiplas selecoes
            ARRAY_AGG(d.c_sport_type_name) AS sports,
            ARRAY_AGG(d.c_tournament_name) AS tournaments,
            ARRAY_AGG(d.c_event_name) AS events,
            ARRAY_AGG(d.c_market_name) AS markets,
            ARRAY_AGG(d.c_selection_name) AS selections,
            ARRAY_AGG(d.c_leg_status) AS leg_statuses,
            ARRAY_AGG(d.c_odds) AS leg_odds,
            COUNT(*) AS legs_count,
            -- c_ts_realstart/realend sao VARCHAR — retornamos como string
            MIN(d.c_ts_realstart) AS event_realstart,
            MAX(d.c_ts_realend) AS event_realend,
            ARRAY_AGG(d.c_is_live) AS is_live_legs,
            ARRAY_AGG(d.c_is_4in_running) AS is_4in_running_legs
        FROM vendor_ec2.tbl_sports_book_bet_details d
        WHERE d.c_customer_id IN ({ids_str})
          AND d.c_created_time >= TIMESTAMP '{start}'
          AND d.c_created_time < TIMESTAMP '{end}'
        GROUP BY d.c_bet_slip_id, d.c_customer_id
    )
    SELECT
        c.c_customer_id,
        c.c_bet_slip_id,
        c.c_total_stake AS stake,
        c.c_total_odds,
        c.c_bet_type,
        c.c_bet_state AS commit_state,
        p.payout_state,
        CASE WHEN p.c_bet_slip_id IS NOT NULL THEN 'SETTLED' ELSE 'OPEN' END AS settlement_status,
        c.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS commit_brt,
        p.c_total_return AS total_return,
        (COALESCE(p.c_total_return, 0) - c.c_total_stake) AS profit,
        p.c_bet_closure_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS closure_brt,
        COALESCE(r.qty_refunds, 0) AS qty_refunds,
        COALESCE(r.total_refund_brl, 0) AS total_refund_brl,
        l.sports,
        l.tournaments,
        l.events,
        l.markets,
        l.selections,
        l.leg_statuses,
        l.leg_odds,
        l.legs_count,
        l.event_realstart,
        l.event_realend,
        l.is_live_legs,
        l.is_4in_running_legs
    FROM commits c
    LEFT JOIN payouts p ON c.c_bet_slip_id = p.c_bet_slip_id
    LEFT JOIN legs l ON c.c_bet_slip_id = l.c_bet_slip_id
    LEFT JOIN refunds r ON c.c_bet_slip_id = r.c_bet_slip_id
    ORDER BY c.c_customer_id, c.c_created_time
    """
    log.info(f"Query: fetch all bets for {len(customer_ids)} suspects, {start} → {end}")
    df = query_athena(sql, database="vendor_ec2")
    log.info(f"  → {len(df)} slips retrieved")
    return df


def profile_player(df: pd.DataFrame, customer_id: int) -> dict:
    """Gera perfil agregado de um jogador."""
    p = df[df["c_customer_id"] == customer_id].copy()
    if p.empty:
        return {}

    # Fechadas vs abertas — usar payout_state (do payout row), nao commit_state
    closed = p[p["settlement_status"] == "SETTLED"].copy()
    wins = closed[closed["total_return"] > closed["stake"]]
    losses = closed[closed["total_return"] < closed["stake"]]
    open_bets = p[p["settlement_status"] == "OPEN"].copy()
    refunded = p[p["qty_refunds"] > 0]

    # Live vs PreLive
    live = p[p["c_bet_type"] == "Live"]
    prelive = p[p["c_bet_type"] == "PreLive"]

    # Esportes e torneios
    all_sports = []
    all_tournaments = []
    all_events = []
    all_markets = []
    for _, row in p.iterrows():
        if row["sports"] is not None:
            all_sports.extend([s for s in row["sports"] if s])
        if row["tournaments"] is not None:
            all_tournaments.extend([t for t in row["tournaments"] if t])
        if row["events"] is not None:
            all_events.extend([e for e in row["events"] if e])
        if row["markets"] is not None:
            all_markets.extend([m for m in row["markets"] if m])

    sport_counts = Counter(all_sports)
    tournament_counts = Counter(all_tournaments)
    market_counts = Counter(all_markets)

    # Stake range
    stake_min = p["stake"].min()
    stake_max = p["stake"].max()
    stake_median = p["stake"].median()
    stake_ratio = (stake_max / stake_min) if stake_min > 0 else 0

    # Win rate
    wr = (len(wins) / len(closed) * 100) if len(closed) > 0 else 0

    # Profit
    total_stake = closed["stake"].sum()
    total_return = closed["total_return"].sum()
    profit = total_return - total_stake

    # Test-bet pattern: quantas apostas tinham stake <= R$5
    test_bets = p[p["stake"] <= 5.0]
    high_bets = p[p["stake"] >= 100.0]

    return {
        "customer_id": customer_id,
        "total_slips": len(p),
        "total_closed": len(closed),
        "total_open": len(open_bets),
        "total_wins": len(wins),
        "total_losses": len(losses),
        "total_refunded": len(refunded),
        "total_refund_value_brl": round(p["total_refund_brl"].sum(), 2),
        "win_rate_pct": round(wr, 2),
        "total_stake_brl": round(total_stake, 2),
        "total_return_brl": round(total_return, 2),
        "profit_brl": round(profit, 2),
        "live_count": len(live),
        "prelive_count": len(prelive),
        "live_pct": round(len(live) / len(p) * 100, 1) if len(p) > 0 else 0,
        "stake_min": round(stake_min, 2),
        "stake_max": round(stake_max, 2),
        "stake_median": round(stake_median, 2),
        "stake_ratio_max_min": round(stake_ratio, 1),
        "test_bets_count": len(test_bets),  # apostas <= R$5
        "high_bets_count": len(high_bets),  # apostas >= R$100
        "top_sports": sport_counts.most_common(5),
        "top_tournaments": tournament_counts.most_common(10),
        "top_markets": market_counts.most_common(10),
        "first_bet": p["commit_brt"].min(),
        "last_bet": p["commit_brt"].max(),
    }


def analyze_test_then_bet_pattern(df: pd.DataFrame, customer_id: int) -> pd.DataFrame:
    """
    Identifica eventos onde o jogador fez aposta baixa (test) e depois alta (bet).
    Retorna eventos com stake_min E stake_max muito diferentes.
    """
    p = df[df["c_customer_id"] == customer_id].copy()
    if p.empty:
        return pd.DataFrame()

    # Extrai primeiro evento de cada slip (top-level)
    def first_event(row):
        if row["events"] is not None and len(row["events"]) > 0:
            return row["events"][0]
        return None

    p["primary_event"] = p.apply(first_event, axis=1)

    # Agrupa por evento
    event_agg = p.groupby("primary_event").agg(
        slips=("c_bet_slip_id", "count"),
        min_stake=("stake", "min"),
        max_stake=("stake", "max"),
        total_stake=("stake", "sum"),
        total_return=("total_return", "sum"),
        live_bets=("c_bet_type", lambda x: (x == "Live").sum()),
        first_bet=("commit_brt", "min"),
        last_bet=("commit_brt", "max"),
    ).reset_index()

    event_agg["stake_ratio"] = event_agg["max_stake"] / event_agg["min_stake"].replace(0, 1)
    event_agg["profit"] = event_agg["total_return"] - event_agg["total_stake"]

    # Padrao test-then-bet: evento com min <= R$5 E max >= R$50 E 2+ slips
    test_then_bet = event_agg[
        (event_agg["slips"] >= 2) &
        (event_agg["min_stake"] <= 5.0) &
        (event_agg["max_stake"] >= 50.0)
    ].sort_values("stake_ratio", ascending=False)

    return test_then_bet


def main():
    start_str = START_DATE.isoformat()
    end_str = (END_DATE + timedelta(days=1)).isoformat()

    log.info("=" * 70)
    log.info("DEEP DIVE: Live Delay Exploitation — 2 jogadores com WR 100%")
    log.info("=" * 70)
    log.info(f"Periodo: {start_str} → {end_str}")
    log.info(f"Suspeitos: {SUSPECTS}")

    # Busca todas as apostas
    df = fetch_all_bets(SUSPECTS, start_str, end_str)

    if df.empty:
        log.warning("Nenhuma aposta encontrada. Verificar period ou IDs.")
        return

    # Salva raw
    raw_path = f"{OUTPUT_DIR}/deep_dive_live_delay_raw_{END_DATE}.csv"
    df.to_csv(raw_path, index=False)
    log.info(f"Raw salvo em: {raw_path}")

    # Gera perfis
    profiles = {}
    for cid in SUSPECTS:
        log.info(f"\nProfilling {cid}...")
        profiles[cid] = profile_player(df, cid)
        if profiles[cid]:
            p = profiles[cid]
            log.info(f"  Slips: {p['total_slips']} | WR: {p['win_rate_pct']}% | Profit: R${p['profit_brl']:,.2f}")
            log.info(f"  Stake range: R${p['stake_min']} → R${p['stake_max']} ({p['stake_ratio_max_min']}x)")
            log.info(f"  Live: {p['live_count']}/{p['total_slips']} ({p['live_pct']}%)")
            log.info(f"  Top sport: {p['top_sports'][:3]}")

    # Padrao test-then-bet por evento
    test_bet_patterns = {}
    for cid in SUSPECTS:
        log.info(f"\nAnalisando test-then-bet pattern para {cid}...")
        ttb = analyze_test_then_bet_pattern(df, cid)
        test_bet_patterns[cid] = ttb
        log.info(f"  {len(ttb)} eventos com padrao test-then-bet confirmado")

    return df, profiles, test_bet_patterns


if __name__ == "__main__":
    result = main()
    if result:
        df, profiles, patterns = result
        print("\n\n=== RESUMO ===\n")
        for cid, prof in profiles.items():
            if prof:
                print(f"\n>>> Customer {cid}")
                for k, v in prof.items():
                    if isinstance(v, list) and len(v) > 0:
                        print(f"  {k}:")
                        for item in v[:5]:
                            print(f"    - {item}")
                    else:
                        print(f"  {k}: {v}")
                print(f"\n  Eventos test-then-bet ({len(patterns[cid])}):")
                if len(patterns[cid]) > 0:
                    print(patterns[cid].head(10).to_string())
