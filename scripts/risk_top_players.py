"""
Top Players — Ranking por vertical (Winners, Losers, Abusers)
==============================================================
Gera 3 rankings com os jogadores mais relevantes para o time de risco:

  1. TOP WINNERS  — Jogadores que mais geram GGR positivo (casa ganha)
  2. TOP LOSERS   — Jogadores com revenue mais negativo (casa perde)
  3. TOP ABUSERS  — Jogadores com alto bonus + revenue negativo

Demanda: Castrin (Head) em 06/04/2026 — "seria legal trazer tabela de
top player (winners, losers, abusers)"

Uso:
    python scripts/risk_top_players.py --days 30
    python scripts/risk_top_players.py --days 7 --top 50
    python scripts/risk_top_players.py --days 30 --html

Saida:
    output/risk_top_players_YYYY-MM-DD.csv
    output/risk_top_players_YYYY-MM-DD.html  (com --html)
    output/risk_top_players_YYYY-MM-DD_legenda.txt

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-06
"""

import sys
import os
import argparse
import logging
import json
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Mapeamento de c_txn_type
TXN_DEPOSIT = 1
TXN_WITHDRAW = 2
TXN_CASINO_BET = 27
TXN_CASINO_WIN = 45
TXN_SB_BET = 59
TXN_JACKPOT_WIN = 65
TXN_ROLLBACK = 72
TXN_FREESPIN_WIN = 80
TXN_SB_WIN = 112

TEST_USERS_CTE = """
    test_users AS (
        SELECT ecr_id FROM ps_bi.dim_user WHERE is_test = true
    )
"""
TEST_USERS_FILTER = "AND f.c_ecr_id NOT IN (SELECT ecr_id FROM test_users)"


def get_date_range(days: int) -> tuple[str, str]:
    """Retorna (data_inicio, data_fim) para filtro Athena. Usa D-1 como fim."""
    end = datetime.now().date() - timedelta(days=1)
    start = end - timedelta(days=days - 1)
    return str(start), str(end + timedelta(days=1))


def query_top_players(date_start: str, date_end: str, top_n: int = 50) -> pd.DataFrame:
    """
    Query unificada que traz todos os jogadores com atividade relevante.
    Inclui financeiro, free spins, bonus e vertical (casino/SB).
    """
    log.info(f"Buscando top players [{date_start} a {date_end}]...")

    sql = f"""
    -- Top Players: financeiro completo por jogador no periodo
    -- Fonte: fund_ec2.tbl_real_fund_txn (centavos /100)
    WITH {TEST_USERS_CTE},
    player_fin AS (
        SELECT
            f.c_ecr_id,
            -- Depositos
            SUM(CASE WHEN f.c_txn_type = {TXN_DEPOSIT}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS dep_brl,
            COUNT_IF(f.c_txn_type = {TXN_DEPOSIT}) AS dep_qty,
            -- Saques
            SUM(CASE WHEN f.c_txn_type = {TXN_WITHDRAW}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS saque_brl,
            COUNT_IF(f.c_txn_type = {TXN_WITHDRAW}) AS saque_qty,
            -- Casino
            SUM(CASE WHEN f.c_txn_type = {TXN_CASINO_BET}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS casino_bets_brl,
            SUM(CASE WHEN f.c_txn_type = {TXN_CASINO_WIN}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS casino_wins_brl,
            -- Free Spin wins
            SUM(CASE WHEN f.c_txn_type = {TXN_FREESPIN_WIN}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS freespin_wins_brl,
            COUNT_IF(f.c_txn_type = {TXN_FREESPIN_WIN}) AS freespin_wins_qty,
            -- Jackpot
            SUM(CASE WHEN f.c_txn_type = {TXN_JACKPOT_WIN}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS jackpot_wins_brl,
            -- Sportsbook
            SUM(CASE WHEN f.c_txn_type = {TXN_SB_BET}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS sb_bets_brl,
            SUM(CASE WHEN f.c_txn_type = {TXN_SB_WIN}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS sb_wins_brl,
            -- Rollbacks
            COUNT_IF(f.c_txn_type = {TXN_ROLLBACK}) AS rollbacks_qty,
            SUM(CASE WHEN f.c_txn_type = {TXN_ROLLBACK}
                     THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS rollbacks_brl,
            -- Dias ativos no periodo
            COUNT(DISTINCT date_trunc('day', f.c_start_time)) AS dias_ativos
        FROM fund_ec2.tbl_real_fund_txn f
        WHERE f.c_start_time >= TIMESTAMP '{date_start}'
          AND f.c_start_time < TIMESTAMP '{date_end}'
          AND f.c_txn_status = 'SUCCESS'
          {TEST_USERS_FILTER}
        GROUP BY f.c_ecr_id
        -- Filtrar micro-jogadores: pelo menos R$100 em bets
        HAVING SUM(CASE WHEN f.c_txn_type IN ({TXN_CASINO_BET}, {TXN_SB_BET})
                        THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) > 100
    )
    SELECT
        pf.*,
        -- GGR total (perspectiva casa: bets - wins)
        ROUND(
            (pf.casino_bets_brl + pf.sb_bets_brl)
            - (pf.casino_wins_brl + pf.freespin_wins_brl + pf.jackpot_wins_brl + pf.sb_wins_brl)
            + pf.rollbacks_brl,
            2
        ) AS ggr_total_brl,
        -- GGR Casino
        ROUND(
            pf.casino_bets_brl - pf.casino_wins_brl - pf.freespin_wins_brl - pf.jackpot_wins_brl + pf.rollbacks_brl,
            2
        ) AS ggr_casino_brl,
        -- GGR SB
        ROUND(pf.sb_bets_brl - pf.sb_wins_brl, 2) AS ggr_sb_brl,
        -- Vertical principal
        CASE
            WHEN pf.casino_bets_brl > 0 AND pf.sb_bets_brl = 0 THEN 'CASINO'
            WHEN pf.sb_bets_brl > 0 AND pf.casino_bets_brl = 0 THEN 'SPORTSBOOK'
            WHEN pf.casino_bets_brl > pf.sb_bets_brl THEN 'CASINO+SB'
            ELSE 'SB+CASINO'
        END AS vertical,
        -- Player profit (saques - depositos)
        ROUND(pf.saque_brl - pf.dep_brl, 2) AS player_profit_brl
    FROM player_fin pf
    ORDER BY
        (pf.casino_bets_brl + pf.sb_bets_brl)
        - (pf.casino_wins_brl + pf.freespin_wins_brl + pf.jackpot_wins_brl + pf.sb_wins_brl)
        + pf.rollbacks_brl
    """
    return query_athena(sql, database="fund_ec2")


def query_bonus_by_players(ecr_ids: list) -> pd.DataFrame:
    """Busca bonus emitidos por uma lista de ECR IDs.
    Usa c_actual_issued_amount (validado 06/04 — c_total_bonus_offered nao existe).
    """
    if not ecr_ids:
        return pd.DataFrame()
    ids_str = ",".join([str(int(eid)) for eid in ecr_ids])
    sql = f"""
    -- Bonus emitidos por jogador (top players)
    -- c_actual_issued_amount = valor real emitido (centavos)
    SELECT
        bs.c_ecr_id,
        COUNT(*) AS bonus_qty,
        SUM(COALESCE(bs.c_actual_issued_amount, 0)) / 100.0 AS bonus_emitido_brl,
        SUM(COALESCE(bs.c_freespin_win, 0)) / 100.0 AS bonus_fs_win_brl,
        COUNT(DISTINCT bs.c_bonus_id) AS bonus_ids_distintos
    FROM bonus_ec2.tbl_bonus_summary_details bs
    WHERE bs.c_ecr_id IN ({ids_str})
    GROUP BY bs.c_ecr_id
    """
    return query_athena(sql, database="bonus_ec2")


def classify_top_players(df: pd.DataFrame, top_n: int = 50) -> dict:
    """
    Classifica jogadores em 3 rankings:
    1. Winners (GGR mais positivo — bom pra casa)
    2. Losers (GGR mais negativo — casa perdendo)
    3. Abusers (alto bonus + revenue negativo)
    """
    results = {}

    # TOP WINNERS (GGR positivo, casa ganha)
    winners = df[df["ggr_total_brl"] > 0].nlargest(top_n, "ggr_total_brl").copy()
    winners["ranking"] = "WINNER"
    winners["rank_pos"] = range(1, len(winners) + 1)
    results["winners"] = winners
    log.info(f"Top Winners: {len(winners)} jogadores (max GGR: R${winners['ggr_total_brl'].max():,.0f})")

    # TOP LOSERS (GGR negativo, casa perde)
    losers = df[df["ggr_total_brl"] < 0].nsmallest(top_n, "ggr_total_brl").copy()
    losers["ranking"] = "LOSER"
    losers["rank_pos"] = range(1, len(losers) + 1)
    results["losers"] = losers
    log.info(f"Top Losers: {len(losers)} jogadores (min GGR: R${losers['ggr_total_brl'].min():,.0f})")

    # TOP ABUSERS (bonus alto + revenue negativo + freespin)
    abuser_candidates = df[
        (df["ggr_total_brl"] < 0) &
        (df["freespin_wins_brl"] > 100)
    ].copy()
    # Score de abuso: peso no freespin + revenue negativo
    if not abuser_candidates.empty:
        abuser_candidates["abuse_score"] = (
            abuser_candidates["freespin_wins_brl"].abs()
            + abuser_candidates["ggr_total_brl"].abs() * 0.5
        )
        abusers = abuser_candidates.nlargest(top_n, "abuse_score")
        abusers["ranking"] = "ABUSER"
        abusers["rank_pos"] = range(1, len(abusers) + 1)
    else:
        abusers = pd.DataFrame()
    results["abusers"] = abusers
    log.info(f"Top Abusers: {len(abusers)} jogadores")

    return results


def fmt_brl(val):
    """Formata valor em BRL."""
    if pd.isna(val) or val is None:
        return "R$ 0"
    val = float(val)
    if val < 0:
        return f"-R$ {abs(val):,.2f}"
    return f"R$ {val:,.2f}"


def generate_html(results: dict, bonus_df: pd.DataFrame,
                  date_start: str, date_end: str, top_n: int) -> str:
    """Gera HTML dashboard com os 3 rankings."""

    def make_table(df: pd.DataFrame, category: str, bonus_data: pd.DataFrame) -> str:
        if df.empty:
            return f'<p style="color:#aaa;text-align:center">Nenhum jogador encontrado</p>'

        # Merge com bonus
        if not bonus_data.empty:
            df = df.merge(bonus_data, left_on="c_ecr_id", right_on="c_ecr_id", how="left")

        rows = ""
        for _, r in df.iterrows():
            ggr = r.get("ggr_total_brl", 0) or 0
            ggr_color = "#2ecc71" if ggr > 0 else "#e74c3c"
            fs = r.get("freespin_wins_brl", 0) or 0
            bonus = r.get("bonus_emitido_brl", 0) or 0
            dep = r.get("dep_brl", 0) or 0
            saque = r.get("saque_brl", 0) or 0
            vertical = r.get("vertical", "N/A")

            rows += f"""<tr>
                <td>{int(r.get('rank_pos', 0))}</td>
                <td style="font-size:11px">{int(r['c_ecr_id'])}</td>
                <td>{vertical}</td>
                <td style="color:{ggr_color};font-weight:bold">{fmt_brl(ggr)}</td>
                <td>{fmt_brl(dep)}</td>
                <td>{fmt_brl(saque)}</td>
                <td>{fmt_brl(r.get('casino_bets_brl', 0))}</td>
                <td>{fmt_brl(r.get('casino_wins_brl', 0))}</td>
                <td style="color:#e67e22">{fmt_brl(fs)}</td>
                <td>{int(r.get('freespin_wins_qty', 0) or 0)}</td>
                <td style="color:#3498db">{fmt_brl(bonus)}</td>
                <td>{int(r.get('dias_ativos', 0) or 0)}</td>
            </tr>"""

        return f"""<div style="overflow-x:auto">
        <table>
            <thead><tr>
                <th>#</th><th>ECR ID</th><th>Vertical</th><th>GGR</th>
                <th>Depositos</th><th>Saques</th><th>Bets Casino</th><th>Wins Casino</th>
                <th>FS Wins</th><th>FS Qty</th><th>Bonus Emitido</th><th>Dias Ativos</th>
            </tr></thead>
            <tbody>{rows}</tbody>
        </table>
        </div>"""

    # KPIs gerais
    all_players = pd.concat([results["winners"], results["losers"]], ignore_index=True) if not results["losers"].empty else results["winners"]
    total_ggr = all_players["ggr_total_brl"].sum() if not all_players.empty else 0
    total_positive = results["winners"]["ggr_total_brl"].sum() if not results["winners"].empty else 0
    total_negative = results["losers"]["ggr_total_brl"].sum() if not results["losers"].empty else 0
    total_fs = all_players["freespin_wins_brl"].sum() if not all_players.empty else 0

    # Concentration: top 10 losers vs total negative
    top10_loss = results["losers"].head(10)["ggr_total_brl"].sum() if not results["losers"].empty else 0
    concentration_pct = (top10_loss / total_negative * 100) if total_negative != 0 else 0

    winners_table = make_table(results["winners"], "WINNER", bonus_df)
    losers_table = make_table(results["losers"], "LOSER", bonus_df)
    abusers_table = make_table(results["abusers"], "ABUSER", bonus_df)

    # Charts data
    # Top 10 losers por ECR
    losers_labels = [str(int(eid))[-6:] for eid in results["losers"].head(10)["c_ecr_id"]] if not results["losers"].empty else []
    losers_values = [round(float(v), 2) for v in results["losers"].head(10)["ggr_total_brl"]] if not results["losers"].empty else []
    losers_fs = [round(float(v), 2) for v in results["losers"].head(10)["freespin_wins_brl"]] if not results["losers"].empty else []

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Top Players — Winners, Losers, Abusers | MultiBet Risk</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0f1923; color: #e0e0e0; padding: 20px; }}
h1 {{ color: #fff; margin-bottom: 5px; }}
.subtitle {{ color: #aaa; margin-bottom: 25px; }}
.kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 25px; }}
.kpi {{ background: #1a2332; border-radius: 8px; padding: 16px; text-align: center; border: 1px solid #2c3e50; }}
.kpi-label {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }}
.kpi-value {{ font-size: 22px; font-weight: bold; margin: 6px 0; }}
.kpi-sub {{ font-size: 11px; color: #666; }}
.section {{ background: #1a2332; border-radius: 12px; padding: 24px; margin-bottom: 25px; border: 1px solid #2c3e50; }}
.section h2 {{ color: #fff; font-size: 16px; margin-bottom: 15px; display: flex; align-items: center; gap: 8px; }}
.section h2 .badge {{ padding: 4px 10px; border-radius: 12px; font-size: 12px; color: #fff; }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
th {{ background: #2c3e50; color: #fff; padding: 8px 10px; text-align: left; position: sticky; top: 0; }}
td {{ padding: 6px 10px; border-bottom: 1px solid #2c3e50; white-space: nowrap; }}
tr:hover {{ background: #1e2d3d; }}
.chart-container {{ max-width: 800px; margin: 0 auto 20px; }}
.legend-box {{ background: #2c3e50; padding: 14px; border-radius: 8px; margin-top: 20px; font-size: 12px; line-height: 1.8; }}
.footer {{ text-align: center; color: #555; font-size: 11px; margin-top: 40px; padding-top: 20px; border-top: 1px solid #2c3e50; }}
</style>
</head>
<body>
<h1>Top Players — Rankings por Vertical</h1>
<p class="subtitle">Periodo: {date_start} a {date_end} | Top {top_n} por categoria | MultiBet Risk Agent v1.3</p>

<div class="kpi-row">
    <div class="kpi">
        <div class="kpi-label">GGR Total (Top Players)</div>
        <div class="kpi-value" style="color:{'#2ecc71' if total_ggr > 0 else '#e74c3c'}">{fmt_brl(total_ggr)}</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Top Winners (Soma)</div>
        <div class="kpi-value" style="color:#2ecc71">{fmt_brl(total_positive)}</div>
        <div class="kpi-sub">{len(results['winners'])} jogadores</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Top Losers (Soma)</div>
        <div class="kpi-value" style="color:#e74c3c">{fmt_brl(total_negative)}</div>
        <div class="kpi-sub">{len(results['losers'])} jogadores</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Free Spin Wins Total</div>
        <div class="kpi-value" style="color:#e67e22">{fmt_brl(total_fs)}</div>
    </div>
    <div class="kpi">
        <div class="kpi-label">Concentracao Top 10 Losers</div>
        <div class="kpi-value" style="color:#e74c3c">{concentration_pct:.1f}%</div>
        <div class="kpi-sub">do total de perdas</div>
    </div>
</div>

<div class="section">
    <h2><span class="badge" style="background:#e74c3c">LOSERS</span> Top {top_n} — Maior prejuizo para a casa</h2>
    <div class="chart-container">
        <canvas id="chartLosers" height="250"></canvas>
    </div>
    {losers_table}
</div>

<div class="section">
    <h2><span class="badge" style="background:#e67e22">ABUSERS</span> Top {top_n} — Bonus + Revenue negativo + Free Spins</h2>
    {abusers_table}
</div>

<div class="section">
    <h2><span class="badge" style="background:#2ecc71">WINNERS</span> Top {top_n} — Maior GGR positivo para a casa</h2>
    {winners_table}
</div>

<div class="section">
    <h2>Como ler este relatorio</h2>
    <div class="legend-box">
        <b>GGR (Gross Gaming Revenue):</b> bets - wins (perspectiva da casa). Positivo = casa ganha, negativo = casa perde.<br>
        <b>FS Wins:</b> Ganhos de Free Spin — quando alto com GGR negativo, indica abuso de bonus.<br>
        <b>Bonus Emitido:</b> Total de bonus emitidos ao jogador (LIFETIME). Fonte: bonus_ec2.<br>
        <b>Vertical:</b> CASINO = so joga casino | SPORTSBOOK = so aposta | CASINO+SB = ambos, maioria casino.<br>
        <b>Dias Ativos:</b> Dias com pelo menos 1 transacao no periodo analisado.<br>
        <br>
        <b>WINNER:</b> Jogador que gera receita consistente. Tratar como VIP, manter engajamento.<br>
        <b>LOSER:</b> Jogador que esta custando dinheiro. Investigar se e sazonal ou padrao de abuso.<br>
        <b>ABUSER:</b> Jogador com combinacao de: bonus alto + free spin wins + revenue negativo. Acao: revisar campanha CRM, bloquear bonus se confirmado.<br>
        <br>
        <b>Fonte:</b> Athena (fund_ec2.tbl_real_fund_txn + bonus_ec2.tbl_bonus_summary_details)<br>
        <b>Valores:</b> BRL (convertido de centavos). Timezone: UTC.<br>
        <b>Filtros:</b> Excluidos test users (ps_bi.dim_user.is_test = true). Min R$100 em bets.
    </div>
</div>

<div class="footer">
    <p>MultiBet Risk Intelligence | Squad 3 — Intelligence Engine | Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')}</p>
</div>

<script>
new Chart(document.getElementById('chartLosers'), {{
    type: 'bar',
    data: {{
        labels: {json.dumps(losers_labels)},
        datasets: [
            {{label: 'GGR (prejuizo casa)', data: {json.dumps(losers_values)}, backgroundColor: 'rgba(231,76,60,0.7)', borderWidth: 0}},
            {{label: 'Free Spin Wins', data: {json.dumps(losers_fs)}, backgroundColor: 'rgba(230,126,34,0.7)', borderWidth: 0}}
        ]
    }},
    options: {{
        responsive: true,
        plugins: {{
            legend: {{labels: {{color: '#ccc'}}}},
            title: {{display: true, text: 'Top 10 Losers — GGR vs Free Spin Wins', color: '#fff'}}
        }},
        scales: {{
            x: {{ticks: {{color: '#aaa'}}}},
            y: {{ticks: {{color: '#aaa', callback: v => 'R$ ' + v.toLocaleString()}}}}
        }}
    }}
}});
</script>
</body>
</html>"""
    return html


def generate_legend(output_path: str, date_start: str, date_end: str, results: dict, top_n: int):
    """Gera arquivo de legenda."""
    legend_path = output_path.replace(".csv", "_legenda.txt")
    with open(legend_path, "w", encoding="utf-8") as f:
        f.write("=" * 70 + "\n")
        f.write("  LEGENDA — Top Players (Winners, Losers, Abusers)\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Periodo: {date_start} a {date_end}\n")
        f.write(f"Top N: {top_n} por categoria\n")
        f.write(f"Gerado: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n")

        f.write("--- COLUNAS ---\n")
        f.write("c_ecr_id            ID unico do jogador (Pragmatic)\n")
        f.write("vertical            CASINO, SPORTSBOOK, CASINO+SB, SB+CASINO\n")
        f.write("ggr_total_brl       GGR = bets - wins (positivo = casa ganha)\n")
        f.write("dep_brl             Total de depositos reais no periodo (BRL)\n")
        f.write("saque_brl           Total de saques no periodo (BRL)\n")
        f.write("casino_bets_brl     Total apostado em casino (BRL)\n")
        f.write("casino_wins_brl     Total ganho em casino (BRL)\n")
        f.write("freespin_wins_brl   Ganhos de Free Spin (BRL)\n")
        f.write("freespin_wins_qty   Quantidade de Free Spin wins\n")
        f.write("bonus_emitido_brl   Total de bonus emitidos LIFETIME (BRL)\n")
        f.write("dias_ativos         Dias com transacao no periodo\n")
        f.write("ranking             WINNER, LOSER, ou ABUSER\n\n")

        f.write("--- CATEGORIAS ---\n")
        f.write(f"WINNERS: {len(results['winners'])} jogadores (GGR mais positivo)\n")
        f.write(f"LOSERS:  {len(results['losers'])} jogadores (GGR mais negativo)\n")
        f.write(f"ABUSERS: {len(results['abusers'])} jogadores (bonus + FS + revenue negativo)\n\n")

        f.write("--- ACAO SUGERIDA ---\n")
        f.write("WINNERS: Tratar como VIP, reter, oferecer beneficios exclusivos\n")
        f.write("LOSERS:  Investigar — se bonus-driven, rever campanha CRM\n")
        f.write("ABUSERS: Bloquear bonus, rever segmentacao, escalar se necessario\n\n")

        f.write("--- GLOSSARIO ---\n")
        f.write("GGR   Gross Gaming Revenue (bets - wins, perspectiva casa)\n")
        f.write("FS    Free Spin — rodadas gratis oferecidas ao jogador\n")
        f.write("SB    Sportsbook — apostas esportivas\n")
    log.info(f"Legenda salva: {legend_path}")


def main():
    parser = argparse.ArgumentParser(description="Top Players — Rankings por vertical")
    parser.add_argument("--days", type=int, default=30, help="Periodo em dias (default: 30)")
    parser.add_argument("--top", type=int, default=50, help="Quantidade por ranking (default: 50)")
    parser.add_argument("--html", action="store_true", default=True, help="Gerar HTML (default: true)")
    parser.add_argument("--no-html", action="store_true", help="Nao gerar HTML")
    args = parser.parse_args()

    date_start, date_end = get_date_range(args.days)
    top_n = args.top
    generate_html_flag = not args.no_html

    log.info(f"Periodo: {date_start} a {date_end} ({args.days} dias) | Top {top_n}")

    # 1. Query financeiro
    df = query_top_players(date_start, date_end, top_n)
    if df.empty:
        log.warning("Nenhum jogador encontrado no periodo!")
        return

    log.info(f"Total jogadores com atividade: {len(df)}")

    # 2. Classificar
    results = classify_top_players(df, top_n)

    # 3. Buscar bonus dos jogadores relevantes (losers + abusers)
    relevant_ids = []
    for cat in ["losers", "abusers", "winners"]:
        if not results[cat].empty:
            relevant_ids.extend(results[cat]["c_ecr_id"].tolist())
    relevant_ids = list(set(relevant_ids))
    log.info(f"Buscando bonus de {len(relevant_ids)} jogadores relevantes...")
    bonus_df = query_bonus_by_players(relevant_ids) if relevant_ids else pd.DataFrame()

    # 4. Salvar CSV
    today = datetime.now().strftime("%Y-%m-%d")
    all_ranked = pd.concat(
        [results["winners"], results["losers"], results["abusers"]],
        ignore_index=True
    )

    # Merge bonus
    if not bonus_df.empty and not all_ranked.empty:
        all_ranked = all_ranked.merge(bonus_df, on="c_ecr_id", how="left")

    csv_path = os.path.join(OUTPUT_DIR, f"risk_top_players_{today}.csv")
    all_ranked.to_csv(csv_path, index=False)
    log.info(f"CSV salvo: {csv_path} ({len(all_ranked)} jogadores)")

    # 5. Legenda
    generate_legend(csv_path, date_start, date_end, results, top_n)

    # 6. HTML
    if generate_html_flag:
        html = generate_html(results, bonus_df, date_start, date_end, top_n)
        html_path = os.path.join(OUTPUT_DIR, f"risk_top_players_{today}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"HTML salvo: {html_path}")

    log.info("Top Players concluido.")


if __name__ == "__main__":
    main()
