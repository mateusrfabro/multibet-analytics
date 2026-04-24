"""
Deteccao de Apostas Pos-Evento (Post-Match Bets) — Plano B
============================================================
Script standalone que identifica apostas feitas APOS o fim real do evento
esportivo. Explora falha no feed Altenar que nao fecha mercado quando o
jogo termina.

Uso:
    python scripts/detect_post_match_bets.py                  # D-0+D-1 (2 dias)
    python scripts/detect_post_match_bets.py --days 7         # ultimos 7 dias
    python scripts/detect_post_match_bets.py --days 90        # varredura ampla
    python scripts/detect_post_match_bets.py --days 2 --csv   # exporta CSV
    python scripts/detect_post_match_bets.py --days 90 --csv --report   # CSV + .md

Criterio:
    Data da aposta > Data fim real do evento + 30 minutos

Fonte:
    vendor_ec2.tbl_sports_book_bets_info (commits/payouts)
    vendor_ec2.tbl_sports_book_bet_details (evento, esporte, c_ts_realend)

Autor: Squad 3 Intelligence Engine
Data: 2026-04-13
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
REPORTS_DIR = "reports"
os.makedirs(OUTPUT_DIR, exist_ok=True)
os.makedirs(REPORTS_DIR, exist_ok=True)


def scan_post_match_bets(days: int = 2) -> pd.DataFrame:
    """
    Busca todas as apostas feitas > 30 minutos apos o fim real do evento.
    Retorna DataFrame com detalhes de cada aposta suspeita.
    """
    sql = f"""
    WITH test_users AS (
        SELECT CAST(external_id AS BIGINT) AS customer_id
        FROM ps_bi.dim_user
        WHERE is_test = true
    ),

    commits AS (
        SELECT
            b.c_customer_id,
            b.c_bet_slip_id,
            b.c_total_stake,
            b.c_total_odds,
            b.c_bet_type,
            b.c_bet_state,
            b.c_created_time
        FROM vendor_ec2.tbl_sports_book_bets_info b
        WHERE b.c_transaction_type = 'M'
          AND b.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '{days}' DAY
          AND b.c_customer_id NOT IN (SELECT customer_id FROM test_users)
    ),

    details AS (
        SELECT DISTINCT
            d.c_bet_slip_id,
            d.c_event_name,
            d.c_sport_type_name,
            d.c_tournament_name,
            d.c_market_name,
            d.c_selection_name,
            d.c_leg_status,
            d.c_ts_realend
        FROM vendor_ec2.tbl_sports_book_bet_details d
        WHERE d.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '{days}' DAY
          AND d.c_ts_realend IS NOT NULL
          AND d.c_ts_realend != ''
          AND TRY_CAST(d.c_ts_realend AS TIMESTAMP) IS NOT NULL
    ),

    payouts AS (
        SELECT
            b.c_bet_slip_id,
            b.c_total_return,
            b.c_bet_closure_time
        FROM vendor_ec2.tbl_sports_book_bets_info b
        WHERE b.c_transaction_type = 'P'
          AND b.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '{days}' DAY
    ),

    refunds AS (
        SELECT
            s.c_bet_slip_id,
            COUNT(*) AS qty_refunds,
            SUM(s.c_amount) AS total_refund
        FROM vendor_ec2.tbl_sports_book_info s
        WHERE s.c_operation_type = 'R'
          AND s.c_created_time >= CURRENT_TIMESTAMP - INTERVAL '{days}' DAY
        GROUP BY s.c_bet_slip_id
    )

    SELECT
        c.c_customer_id,
        c.c_bet_slip_id,
        c.c_total_stake,
        c.c_total_odds,
        c.c_bet_type,
        c.c_bet_state,
        c.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS aposta_brt,
        d.c_event_name,
        d.c_sport_type_name,
        d.c_tournament_name,
        d.c_market_name,
        d.c_selection_name,
        d.c_leg_status,
        d.c_ts_realend AS fim_evento,
        COALESCE(p.c_total_return, 0) AS total_return,
        p.c_bet_closure_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS closure_brt,
        COALESCE(r.qty_refunds, 0) AS qty_refunds,
        COALESCE(r.total_refund, 0) AS total_refund,
        date_diff('minute', CAST(d.c_ts_realend AS TIMESTAMP), c.c_created_time) AS minutos_pos_fim,
        CASE
            WHEN p.c_total_return IS NOT NULL AND p.c_total_return > c.c_total_stake THEN 'WIN'
            WHEN p.c_total_return IS NOT NULL AND p.c_total_return <= c.c_total_stake THEN 'LOSS'
            WHEN r.qty_refunds > 0 THEN 'REFUND'
            ELSE 'OPEN'
        END AS resultado,
        ROUND(COALESCE(p.c_total_return, 0) - c.c_total_stake, 2) AS profit_jogador
    FROM commits c
    JOIN details d ON c.c_bet_slip_id = d.c_bet_slip_id
    LEFT JOIN payouts p ON c.c_bet_slip_id = p.c_bet_slip_id
    LEFT JOIN refunds r ON c.c_bet_slip_id = r.c_bet_slip_id
    WHERE CAST(d.c_ts_realend AS TIMESTAMP) < c.c_created_time
      AND date_diff('minute', CAST(d.c_ts_realend AS TIMESTAMP), c.c_created_time) > 30
    ORDER BY c.c_total_stake DESC
    """
    log.info(f"Buscando apostas pos-evento (ultimos {days} dias, threshold >30min)...")
    df = query_athena(sql, database="vendor_ec2")
    log.info(f"  -> {len(df)} apostas pos-evento encontradas")
    return df


def summarize(df: pd.DataFrame) -> dict:
    """Gera resumo agregado dos resultados."""
    if df.empty:
        return {"total_apostas": 0}

    wins = df[df["resultado"] == "WIN"]
    losses = df[df["resultado"] == "LOSS"]
    refunds = df[df["resultado"] == "REFUND"]
    opens = df[df["resultado"] == "OPEN"]

    top_players = (
        df.groupby("c_customer_id")
        .agg(
            slips=("c_bet_slip_id", "nunique"),
            stake_total=("c_total_stake", "sum"),
            profit_total=("profit_jogador", "sum"),
            refund_total=("total_refund", "sum"),
            min_delay_min=("minutos_pos_fim", "min"),
            max_delay_min=("minutos_pos_fim", "max"),
            wins=("resultado", lambda x: (x == "WIN").sum()),
        )
        .sort_values("profit_total", ascending=False)
    )

    top_leagues = (
        df.groupby("c_tournament_name")
        .agg(
            apostas=("c_bet_slip_id", "nunique"),
            jogadores=("c_customer_id", "nunique"),
            stake=("c_total_stake", "sum"),
            profit=("profit_jogador", "sum"),
        )
        .sort_values("apostas", ascending=False)
    )

    top_sports = (
        df.groupby("c_sport_type_name")
        .agg(
            apostas=("c_bet_slip_id", "nunique"),
            jogadores=("c_customer_id", "nunique"),
        )
        .sort_values("apostas", ascending=False)
    )

    delays = df["minutos_pos_fim"]

    return {
        "total_apostas": len(df),
        "jogadores_unicos": df["c_customer_id"].nunique(),
        "slips_unicos": df["c_bet_slip_id"].nunique(),
        "stake_total": round(df["c_total_stake"].sum(), 2),
        "retorno_pago": round(df["total_return"].sum(), 2),
        "profit_jogadores": round(df["profit_jogador"].sum(), 2),
        "prejuizo_casa": round(-df["profit_jogador"].sum(), 2),
        "refunds_total": round(df["total_refund"].sum(), 2),
        "wins": len(wins),
        "losses": len(losses),
        "refunds_count": len(refunds),
        "opens": len(opens),
        "win_stake": round(wins["c_total_stake"].sum(), 2) if not wins.empty else 0,
        "win_profit": round(wins["profit_jogador"].sum(), 2) if not wins.empty else 0,
        "delay_min": int(delays.min()),
        "delay_mediana": int(delays.median()),
        "delay_max": int(delays.max()),
        "top_players": top_players,
        "top_leagues": top_leagues,
        "top_sports": top_sports,
    }


def print_summary(s: dict, days: int):
    """Imprime resumo formatado no terminal."""
    if s["total_apostas"] == 0:
        print(f"\nNenhuma aposta pos-evento encontrada nos ultimos {days} dias.")
        return

    print(f"\n{'='*70}")
    print(f"  APOSTAS POS-EVENTO — ultimos {days} dias")
    print(f"{'='*70}")
    print(f"  Apostas suspeitas:   {s['total_apostas']}")
    print(f"  Jogadores unicos:    {s['jogadores_unicos']}")
    print(f"  Stake total:         R$ {s['stake_total']:,.2f}")
    print(f"  Retorno pago:        R$ {s['retorno_pago']:,.2f}")
    print(f"  PREJUIZO DA CASA:    R$ {s['prejuizo_casa']:,.2f}")
    print(f"  Refunds:             R$ {s['refunds_total']:,.2f}")
    print()
    print(f"  Resultados: {s['wins']} WIN | {s['losses']} LOSS | {s['refunds_count']} REFUND | {s['opens']} OPEN")
    print(f"  Wins: Stake R$ {s['win_stake']:,.2f} → Profit R$ {s['win_profit']:,.2f}")
    print()
    print(f"  Delay: min {s['delay_min']} min | mediana {s['delay_mediana']} min | max {s['delay_max']} min")

    print(f"\n--- TOP JOGADORES POR PROFIT ---")
    print(s["top_players"].head(15).to_string())

    print(f"\n--- TOP LIGAS EXPLORADAS ---")
    print(s["top_leagues"].head(10).to_string())

    print(f"\n--- POR ESPORTE ---")
    print(s["top_sports"].to_string())


def generate_report(df: pd.DataFrame, s: dict, days: int) -> str:
    """Gera relatorio .md consolidado para o Castrin levar a Altenar."""
    today = datetime.now().strftime("%Y-%m-%d")

    if s["total_apostas"] == 0:
        return f"# Apostas Pos-Evento — {today}\n\nNenhuma aposta encontrada."

    # Top 15 jogadores
    top_p = s["top_players"].head(15).reset_index()
    top_p_lines = []
    for _, r in top_p.iterrows():
        top_p_lines.append(
            f"| {int(r['c_customer_id'])} | {int(r['slips'])} | R$ {r['stake_total']:,.2f} | "
            f"R$ {r['profit_total']:,.2f} | R$ {r['refund_total']:,.2f} | {int(r['wins'])} | "
            f"{int(r['min_delay_min'])}-{int(r['max_delay_min'])} min |"
        )

    # Top ligas
    top_l = s["top_leagues"].head(10).reset_index()
    top_l_lines = []
    for _, r in top_l.iterrows():
        top_l_lines.append(
            f"| {r['c_tournament_name']} | {int(r['apostas'])} | {int(r['jogadores'])} | "
            f"R$ {r['stake']:,.2f} | R$ {r['profit']:,.2f} |"
        )

    # Top esportes
    top_s = s["top_sports"].reset_index()
    top_s_lines = []
    for _, r in top_s.iterrows():
        top_s_lines.append(
            f"| {r['c_sport_type_name']} | {int(r['apostas'])} | {int(r['jogadores'])} |"
        )

    # Exemplos detalhados (top 10 por stake)
    examples = df.nlargest(10, "c_total_stake")
    ex_lines = []
    for _, r in examples.iterrows():
        ex_lines.append(
            f"| {int(r['c_customer_id'])} | {r.get('aposta_brt', '')} | "
            f"{r.get('c_event_name', '')} | {r.get('c_tournament_name', '')} | "
            f"{r.get('c_market_name', '')} | R$ {r['c_total_stake']:,.2f} | "
            f"{r.get('resultado', '')} | R$ {r['profit_jogador']:,.2f} | "
            f"{int(r['minutos_pos_fim'])} min |"
        )

    md = f"""# Impacto Financeiro — Apostas Pos-Evento (Bug Altenar)

> **Objetivo:** Quantificar o prejuizo causado por apostas aceitas apos o fim real de eventos esportivos.
> **Destinatario:** Altenar (via Castrin/Gusta).
> **Periodo analisado:** Ultimos {days} dias (ate {today}).
> **Criterio de deteccao:** Data da aposta > Data fim real do evento + 30 minutos.
> **Fonte:** AWS Athena (`vendor_ec2.tbl_sports_book_bets_info` + `tbl_sports_book_bet_details`).
> **Autor:** Squad 3 Intelligence Engine.

---

## Resumo Executivo

| Metrica | Valor |
|---|---|
| **Apostas pos-evento detectadas** | **{s['total_apostas']}** |
| **Jogadores envolvidos** | **{s['jogadores_unicos']}** |
| **Stake total exposto** | **R$ {s['stake_total']:,.2f}** |
| **Retorno pago pela casa** | **R$ {s['retorno_pago']:,.2f}** |
| **PREJUIZO LIQUIDO DA CASA** | **R$ {s['prejuizo_casa']:,.2f}** |
| Refunds (valor recuperado/pendente) | R$ {s['refunds_total']:,.2f} |
| Apostas ganhas pelo jogador (WIN) | {s['wins']} (Stake R$ {s['win_stake']:,.2f} / Profit R$ {s['win_profit']:,.2f}) |
| Apostas perdidas (LOSS) | {s['losses']} |
| Apostas refundadas | {s['refunds_count']} |
| Apostas em aberto | {s['opens']} |

### Delay entre fim do evento e hora da aposta

| Metrica | Valor |
|---|---|
| Minimo | {s['delay_min']} minutos |
| Mediana | {s['delay_mediana']} minutos |
| Maximo | {s['delay_max']} minutos |

---

## O que esta acontecendo

Jogadores estao fazendo apostas em eventos esportivos que **ja terminaram**. O mercado (odds) deveria ter sido fechado pela Altenar quando o jogo acabou, mas por algum motivo o sinal de "match end" nao chegou ou nao foi processado. Isso deixa o mercado aberto por minutos ou horas apos o fim real.

O jogador consulta o resultado em outra fonte (site oficial da liga, Flashscore, etc), volta na casa e aposta **no resultado ja conhecido** — com certeza de ganhar.

**Evidencia tecnica:** A data de criacao da aposta (`c_created_time`) e posterior a data de fim real do evento (`c_ts_realend`) em todos os casos listados. O delay medio e de {s['delay_mediana']} minutos.

---

## Jogadores — Top {min(15, len(top_p))} por profit

| Customer ID | Slips | Stake Total | Profit | Refund | Wins | Delay |
|---|---|---|---|---|---|---|
{chr(10).join(top_p_lines)}

---

## Ligas Exploradas — Top {min(10, len(top_l))}

| Liga / Torneio | Apostas | Jogadores | Stake | Profit |
|---|---|---|---|---|
{chr(10).join(top_l_lines)}

---

## Esportes

| Esporte | Apostas | Jogadores |
|---|---|---|
{chr(10).join(top_s_lines)}

---

## Exemplos Detalhados — Top 10 por valor de aposta

| Customer ID | Hora Aposta (BRT) | Evento | Liga | Mercado | Stake | Resultado | Profit | Delay |
|---|---|---|---|---|---|---|---|---|
{chr(10).join(ex_lines)}

---

## Causa Raiz (hipotese tecnica)

O feed de dados da Altenar/Sportradar envia um sinal de "match end" quando o evento termina. Esse sinal deveria:
1. Fechar todos os mercados do evento
2. Impedir novas apostas
3. Disparar o settlement (liquidar as apostas abertas)

**Nos casos detectados, o passo 1 falhou** — os mercados ficaram abertos apos o fim do evento. O passo 3 funcionou normalmente (as apostas foram liquidadas com o resultado correto), o que confirma que o resultado ja era conhecido pelo sistema.

---

## Impacto e Recomendacao

### Para a Altenar corrigir (Opcao A — preferencial):
1. **Implementar timeout de mercado:** se passaram `duracao_esporte + 30 min` desde o inicio do evento, fechar o mercado automaticamente — independente do sinal de "match end"
2. **Validacao pre-settlement:** antes de liquidar, checar se `c_ts_realend < c_created_time`. Se sim, anular a aposta
3. **Monitoramento do feed:** alertar quando um evento finaliza no Sportradar mas o mercado continua aberto na Altenar

### Enquanto nao corrigem (Opcao B — deteccao propria):
Script standalone `scripts/detect_post_match_bets.py` que roda sob demanda ou periodicamente.
Detecta apostas pos-evento e gera CSV + relatorio para o time de Risk.

---

**Gerado em:** {today}
**Script:** `scripts/detect_post_match_bets.py --days {days} --csv --report`
"""
    return md


def main():
    parser = argparse.ArgumentParser(description="Detecta apostas pos-evento (post-match bets)")
    parser.add_argument("--days", type=int, default=2, help="Dias de lookback (default: 2)")
    parser.add_argument("--csv", action="store_true", help="Exporta CSV raw")
    parser.add_argument("--report", action="store_true", help="Gera relatorio .md")
    args = parser.parse_args()

    today = datetime.now().strftime("%Y-%m-%d")

    df = scan_post_match_bets(args.days)
    s = summarize(df)
    print_summary(s, args.days)

    if args.csv and not df.empty:
        csv_path = f"{OUTPUT_DIR}/post_match_bets_{args.days}d_{today}.csv"
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        log.info(f"CSV salvo: {csv_path}")

    if args.report:
        md = generate_report(df, s, args.days)
        md_path = f"{REPORTS_DIR}/impacto_apostas_pos_evento_{args.days}d_{today}.md"
        with open(md_path, "w", encoding="utf-8") as f:
            f.write(md)
        log.info(f"Relatorio salvo: {md_path}")


if __name__ == "__main__":
    main()