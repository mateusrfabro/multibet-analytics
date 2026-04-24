"""
Gera dashboard HTML standalone com os alertas de fraude.

Le o CSV de output do risk_fraud_detection.py e gera uma pagina HTML
interativa com graficos, KPIs e tabela de alertas.

Uso:
    python scripts/risk_generate_dashboard.py
    python scripts/risk_generate_dashboard.py --input output/risk_fraud_alerts_2026-04-03.csv

Saida:
    output/risk_fraud_dashboard_YYYY-MM-DD.html

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-03
"""

import sys
import os
import argparse
import json
from datetime import datetime
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd

OUTPUT_DIR = "output"


def generate_dashboard(csv_path: str) -> str:
    """Le CSV de alertas e gera HTML dashboard."""
    df = pd.read_csv(csv_path)

    # ======== KPIs ========
    total_flagged = len(df)
    critical = len(df[df["risk_tier"] == "CRITICAL"])
    high = len(df[df["risk_tier"] == "HIGH"])
    medium = len(df[df["risk_tier"] == "MEDIUM"])
    low = len(df[df["risk_tier"] == "LOW"])
    multi_rule = len(df[df["qty_regras"] >= 2])

    # ======== Distribuicao de regras ========
    all_rules = []
    for regras in df["regras_violadas"]:
        all_rules.extend([r.strip() for r in str(regras).split(",")])
    rule_counts = Counter(all_rules)
    rule_labels = json.dumps(list(rule_counts.keys()))
    rule_values = json.dumps(list(rule_counts.values()))

    # ======== Tier distribution ========
    tier_order = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
    tier_counts = {t: len(df[df["risk_tier"] == t]) for t in tier_order}
    tier_labels = json.dumps(tier_order)
    tier_values = json.dumps([tier_counts[t] for t in tier_order])

    # ======== Combinacoes de regras (multi-regra) ========
    combo_counts = df[df["qty_regras"] >= 2]["regras_violadas"].value_counts().head(10)
    combo_labels = json.dumps(combo_counts.index.tolist())
    combo_values = json.dumps(combo_counts.values.tolist())

    # ======== Tabela HIGH/CRITICAL ========
    high_risk = df[df["risk_tier"].isin(["HIGH", "CRITICAL"])].sort_values("risk_score", ascending=False)
    high_risk_rows = ""
    for _, r in high_risk.iterrows():
        tier_class = "critical" if r["risk_tier"] == "CRITICAL" else "high"
        high_risk_rows += f"""
        <tr class="{tier_class}">
            <td class="mono">{r['c_ecr_id']}</td>
            <td><span class="badge badge-{tier_class}">{r['risk_tier']}</span></td>
            <td class="center">{r['risk_score']}</td>
            <td>{r['regras_violadas']}</td>
            <td class="center">{r['qty_regras']}</td>
            <td class="evidence">{r['evidencias'][:200]}...</td>
        </tr>"""

    # ======== Tabela ZERO DEPOSIT LIFETIME (R3a) ========
    zero_dep = df[df["regras_violadas"].str.contains("R3a", na=False)].sort_values("risk_score", ascending=False)
    zero_dep_count = len(zero_dep)
    zero_dep_rows = ""
    for _, r in zero_dep.iterrows():
        zero_dep_rows += f"""
        <tr class="zero-dep">
            <td class="mono">{r['c_ecr_id']}</td>
            <td><span class="badge badge-critical">ZERO DEP</span></td>
            <td class="center">{r['risk_score']}</td>
            <td>{r['regras_violadas']}</td>
            <td class="center">{r['qty_regras']}</td>
            <td class="evidence">{str(r['evidencias'])[:200]}...</td>
        </tr>"""

    # ======== Tabela MEDIUM (top 30, excluindo R3a que ja tem secao propria) ========
    medium_risk = df[
        (df["risk_tier"] == "MEDIUM") &
        (~df["regras_violadas"].str.contains("R3a", na=False))
    ].sort_values("risk_score", ascending=False).head(30)
    medium_risk_rows = ""
    for _, r in medium_risk.iterrows():
        medium_risk_rows += f"""
        <tr>
            <td class="mono">{r['c_ecr_id']}</td>
            <td><span class="badge badge-medium">MEDIUM</span></td>
            <td class="center">{r['risk_score']}</td>
            <td>{r['regras_violadas']}</td>
            <td class="center">{r['qty_regras']}</td>
            <td class="evidence">{str(r['evidencias'])[:150]}...</td>
        </tr>"""

    # ======== Score distribution histogram data ========
    score_bins = [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 120]
    score_hist = pd.cut(df["risk_score"], bins=score_bins, right=True).value_counts().sort_index()
    hist_labels = json.dumps([str(b) for b in score_hist.index])
    hist_values = json.dumps(score_hist.values.tolist())

    today = datetime.now().strftime("%Y-%m-%d %H:%M")

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Risk & Fraud Dashboard - MultiBet</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.1/dist/chart.umd.min.js"></script>
<style>
:root {{
    --bg-primary: #0f0f1a;
    --bg-secondary: #1a1a2e;
    --bg-card: #1a1a2e;
    --border: #2d2d44;
    --purple: #8b5cf6;
    --purple-dark: #6d28d9;
    --purple-light: #a78bfa;
    --purple-muted: #4c2d8a;
    --green: #10b981;
    --red: #ef4444;
    --yellow: #eab308;
    --orange: #f97316;
    --text-primary: #ffffff;
    --text-secondary: #9ca3af;
    --text-muted: #6b7280;
    --table-row-alt: #111122;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:var(--bg-primary); color:var(--text-primary); font-family:'Inter',system-ui,-apple-system,sans-serif; font-size:14px; line-height:1.5; }}
.main {{ max-width:1400px; margin:0 auto; padding:24px 32px; }}
.header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:24px; padding-bottom:16px; border-bottom:1px solid var(--border); }}
.header-title {{ font-size:24px; font-weight:700; }}
.header-title .shield {{ font-size:28px; margin-right:8px; }}
.header-meta {{ color:var(--text-secondary); font-size:12px; text-align:right; }}
.header-meta .live {{ color:var(--red); font-weight:600; }}

/* KPIs */
.kpi-row {{ display:grid; grid-template-columns:repeat(6, 1fr); gap:16px; margin-bottom:24px; }}
.kpi-card {{ background:var(--bg-card); border:1px solid var(--border); border-radius:10px; padding:20px; text-align:center; }}
.kpi-card.alert {{ border-color:var(--red); background:rgba(239,68,68,0.08); }}
.kpi-card.warn {{ border-color:var(--orange); background:rgba(249,115,22,0.08); }}
.kpi-label {{ color:var(--text-secondary); font-size:11px; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }}
.kpi-value {{ font-size:32px; font-weight:700; }}

/* Tooltips */
[data-tooltip] {{ position:relative; cursor:help; }}
[data-tooltip]:hover::after {{
    content:attr(data-tooltip);
    position:absolute; bottom:calc(100% + 8px); left:50%; transform:translateX(-50%);
    background:#1a1a2e; color:#e5e7eb; border:1px solid var(--purple); border-radius:8px;
    padding:10px 14px; font-size:12px; white-space:normal; width:280px; z-index:100;
    box-shadow:0 4px 20px rgba(0,0,0,0.5); line-height:1.5; text-transform:none; letter-spacing:0; font-weight:400;
}}
[data-tooltip]:hover::before {{
    content:''; position:absolute; bottom:calc(100% + 2px); left:50%; transform:translateX(-50%);
    border:6px solid transparent; border-top-color:var(--purple); z-index:101;
}}
th[data-tooltip]:hover::after {{ bottom:auto; top:calc(100% + 8px); }}
th[data-tooltip]:hover::before {{ bottom:auto; top:calc(100% + 2px); border-top-color:transparent; border-bottom-color:var(--purple); }}
.kpi-value.red {{ color:var(--red); }}
.kpi-value.orange {{ color:var(--orange); }}
.kpi-value.yellow {{ color:var(--yellow); }}
.kpi-value.purple {{ color:var(--purple); }}
.kpi-value.green {{ color:var(--green); }}

/* Charts */
.chart-grid {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; margin-bottom:24px; }}
.chart-card {{ background:var(--bg-card); border:1px solid var(--border); border-radius:10px; padding:20px; }}
.chart-title {{ font-size:14px; font-weight:600; margin-bottom:12px; color:var(--text-secondary); text-transform:uppercase; letter-spacing:0.5px; }}

/* Tables */
.section-title {{ font-size:18px; font-weight:700; margin:24px 0 12px; }}
.section-subtitle {{ color:var(--text-secondary); font-size:12px; margin-bottom:12px; }}
.table-container {{ background:var(--bg-card); border:1px solid var(--border); border-radius:10px; overflow-x:auto; margin-bottom:24px; }}
table {{ width:100%; border-collapse:collapse; font-size:13px; }}
th {{ background:var(--bg-secondary); color:var(--text-secondary); text-transform:uppercase; font-size:11px; letter-spacing:0.5px; padding:12px 14px; text-align:left; border-bottom:1px solid var(--border); white-space:nowrap; font-weight:600; }}
td {{ padding:10px 14px; border-bottom:1px solid var(--border); }}
tr:nth-child(even) td {{ background:var(--table-row-alt); }}
tr.critical td {{ background:rgba(239,68,68,0.1); }}
tr.high td {{ background:rgba(249,115,22,0.08); }}
tr.zero-dep td {{ background:rgba(139,92,246,0.1); }}
.mono {{ font-family:'Courier New',monospace; font-size:11px; color:var(--purple-light); }}
.center {{ text-align:center; }}
.evidence {{ font-size:11px; color:var(--text-secondary); max-width:400px; white-space:normal; line-height:1.4; }}

/* Badges */
.badge {{ padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; text-transform:uppercase; }}
.badge-critical {{ background:rgba(239,68,68,0.2); color:var(--red); }}
.badge-high {{ background:rgba(249,115,22,0.2); color:var(--orange); }}
.badge-medium {{ background:rgba(234,179,8,0.2); color:var(--yellow); }}
.badge-low {{ background:rgba(16,185,129,0.2); color:var(--green); }}

/* How to read */
.howto {{ background:var(--bg-card); border:1px solid var(--border); border-radius:10px; padding:20px; margin-bottom:24px; }}
.howto h3 {{ font-size:14px; font-weight:600; margin-bottom:12px; color:var(--purple-light); }}
.howto-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:16px; font-size:12px; color:var(--text-secondary); }}
.howto-col h4 {{ color:var(--text-primary); font-size:12px; margin-bottom:6px; text-transform:uppercase; letter-spacing:0.5px; }}
.howto-col p {{ margin-bottom:4px; }}

/* Footer */
.footer {{ text-align:center; color:var(--text-muted); font-size:11px; margin-top:32px; padding-top:16px; border-top:1px solid var(--border); }}

@media (max-width: 1024px) {{
    .kpi-row {{ grid-template-columns:repeat(3, 1fr); }}
    .chart-grid {{ grid-template-columns:1fr; }}
    .howto-grid {{ grid-template-columns:1fr; }}
}}
</style>
</head>
<body>
<div class="main">

    <!-- HEADER -->
    <div class="header">
        <div>
            <div class="header-title"><span class="shield">&#128737;</span> Risk & Fraud Dashboard</div>
            <div style="color:var(--text-secondary); font-size:13px; margin-top:4px;">MultiBet - Squad 3 Intelligence Engine</div>
        </div>
        <div class="header-meta">
            <div>Gerado: {today}</div>
            <div>Fonte: Athena (fund_ec2, bonus_ec2, bireports_ec2)</div>
            <div class="live">7 regras de deteccao ativas</div>
        </div>
    </div>

    <!-- HOW TO READ -->
    <div class="howto">
        <h3>Como ler este relatorio</h3>
        <div class="howto-grid">
            <div class="howto-col">
                <h4>Tiers de Risco</h4>
                <p><span class="badge badge-critical">CRITICAL</span> Score 81+ — Suspender conta, escalar compliance</p>
                <p><span class="badge badge-high">HIGH</span> Score 51-80 — Bloquear bonus, limitar saques</p>
                <p><span class="badge badge-medium">MEDIUM</span> Score 21-50 — Revisar manualmente, verificar KYC</p>
                <p><span class="badge badge-low">LOW</span> Score 0-20 — Monitorar, watchlist 2 semanas</p>
            </div>
            <div class="howto-col">
                <h4>Regras de Deteccao</h4>
                <p><strong>R1</strong> (10pts) Pico em jogo — bets > 3x desvio padrao do jogador</p>
                <p><strong>R2</strong> (25pts) Abuso bonus — bonus > P95 da distribuicao</p>
                <p><strong>R3a</strong> (35pts) NUNCA depositou na vida + saque > R$50 no periodo</p>
                <p><strong>R3b</strong> (25pts) Saque > 5x depositos, > R$200</p>
                <p><strong>R4</strong> (15pts) Rollbacks > P99 e > 50</p>
                <p><strong>R5</strong> (20pts) 3+ sessoes simultaneas</p>
                <p><strong>R6</strong> (15pts) 5+ depositos/saques em 1 hora</p>
                <p><strong>R7</strong> (30pts) Sacou dinheiro em menos de 24h apos criar a conta</p>
            </div>
        </div>
    </div>

    <!-- KPIs -->
    <div class="kpi-row">
        <div class="kpi-card" data-tooltip="Total de jogadores unicos que violaram pelo menos 1 regra de fraude no periodo analisado. Nao inclui test users.">
            <div class="kpi-label">Total Flagados</div>
            <div class="kpi-value purple">{total_flagged:,}</div>
        </div>
        <div class="kpi-card alert" data-tooltip="Score 81+. Acao: suspender conta imediatamente e escalar para compliance. Indica fraude provavel com multiplas regras violadas.">
            <div class="kpi-label">Critical</div>
            <div class="kpi-value red">{critical}</div>
        </div>
        <div class="kpi-card warn" data-tooltip="Score 51-80. Acao: bloquear bonus, limitar saques, alertar time de risco. Padroes claros de abuso identificados.">
            <div class="kpi-label">High</div>
            <div class="kpi-value orange">{high}</div>
        </div>
        <div class="kpi-card" data-tooltip="Score 21-50. Acao: revisar manualmente, verificar KYC e historico. Padrao suspeito mas pode ser jogador legitimo com sorte.">
            <div class="kpi-label">Medium</div>
            <div class="kpi-value yellow">{medium:,}</div>
        </div>
        <div class="kpi-card" data-tooltip="Score 0-20. Acao: adicionar a watchlist e monitorar por 2 semanas. Violou apenas 1 regra de baixo peso.">
            <div class="kpi-label">Low</div>
            <div class="kpi-value green">{low}</div>
        </div>
        <div class="kpi-card" data-tooltip="Jogadores que violaram 2 ou mais regras simultaneamente. Quanto mais regras violadas, maior a probabilidade de fraude real.">
            <div class="kpi-label">Multi-regra (2+)</div>
            <div class="kpi-value purple">{multi_rule}</div>
        </div>
    </div>

    <!-- CHARTS -->
    <div class="chart-grid">
        <div class="chart-card">
            <div class="chart-title">Distribuicao por Tier</div>
            <canvas id="chartTier"></canvas>
        </div>
        <div class="chart-card">
            <div class="chart-title">Jogadores por Regra</div>
            <canvas id="chartRules"></canvas>
        </div>
        <div class="chart-card">
            <div class="chart-title">Distribuicao de Score</div>
            <canvas id="chartScore"></canvas>
        </div>
    </div>

    <!-- HIGH/CRITICAL TABLE -->
    <div class="section-title" style="color:var(--red);">&#9888; Jogadores HIGH & CRITICAL — Acao Imediata</div>
    <div class="section-subtitle">Jogadores que violaram 3+ regras simultaneamente. Recomendacao: bloquear bonus, limitar saques, escalar para compliance.</div>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th data-tooltip="ID unico do jogador na plataforma Pragmatic (18 digitos). Usar para buscar no backoffice.">ECR ID</th>
                    <th data-tooltip="Classificacao de risco: CRITICAL (81+), HIGH (51-80), MEDIUM (21-50), LOW (0-20).">Tier</th>
                    <th data-tooltip="Pontuacao de risco consolidada (0-120). Soma dos pesos de cada regra violada.">Score</th>
                    <th data-tooltip="Regras violadas: R1=pico jogo, R2=bonus abuse, R3a=zero dep, R3b=ratio 5x, R4=rollbacks, R5=sessoes, R6=velocity.">Regras</th>
                    <th data-tooltip="Quantidade de regras distintas violadas. Mais regras = maior confianca na deteccao.">Qty</th>
                    <th data-tooltip="Detalhes especificos de cada violacao: valores, ratios, quantidades. Usar para investigacao manual.">Evidencias</th>
                </tr>
            </thead>
            <tbody>
                {high_risk_rows if high_risk_rows else '<tr><td colspan="6" class="center" style="color:var(--green);">Nenhum jogador CRITICAL/HIGH no periodo</td></tr>'}
            </tbody>
        </table>
    </div>

    <!-- MEDIUM TABLE (top 30) -->
    <!-- ZERO DEPOSIT LIFETIME -->
    <div class="section-title" style="color:var(--purple-light);">&#128680; Fraude Pura — NUNCA depositaram dinheiro real ({zero_dep_count} jogadores)</div>
    <div class="section-subtitle">Jogadores que sacaram dinheiro sem NUNCA ter feito um deposito real na vida inteira. Possivel abuso de bonus, freespins ou exploit. Acao: investigar origem do saldo e bloquear saque.</div>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th data-tooltip="ID unico do jogador na plataforma Pragmatic (18 digitos). Usar para buscar no backoffice.">ECR ID</th>
                    <th data-tooltip="ZERO DEP = jogador que NUNCA depositou dinheiro real na plataforma.">Tier</th>
                    <th data-tooltip="Pontuacao de risco consolidada (0-120). Soma dos pesos de cada regra violada.">Score</th>
                    <th data-tooltip="Regras violadas: R3a = zero deposito lifetime. Pode combinar com outras regras.">Regras</th>
                    <th data-tooltip="Quantidade de regras distintas violadas.">Qty</th>
                    <th data-tooltip="Detalhes: valor sacado, bets e wins. Se tem wins sem deposito, veio de bonus/freespin.">Evidencias</th>
                </tr>
            </thead>
            <tbody>
                {zero_dep_rows if zero_dep_rows else '<tr><td colspan="6" class="center" style="color:var(--green);">Nenhum jogador zero-deposit no periodo</td></tr>'}
            </tbody>
        </table>
    </div>

    <div class="section-title" style="color:var(--yellow);">Jogadores MEDIUM — Revisar Manualmente (top 30)</div>
    <div class="section-subtitle">Jogadores com padrao suspeito mas que podem ser legitimos. Verificar KYC e historico antes de agir.</div>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th data-tooltip="ID unico do jogador na plataforma Pragmatic (18 digitos). Usar para buscar no backoffice.">ECR ID</th>
                    <th data-tooltip="Classificacao de risco: CRITICAL (81+), HIGH (51-80), MEDIUM (21-50), LOW (0-20).">Tier</th>
                    <th data-tooltip="Pontuacao de risco consolidada (0-120). Soma dos pesos de cada regra violada.">Score</th>
                    <th data-tooltip="Regras violadas: R1=pico jogo, R2=bonus abuse, R3a=zero dep, R3b=ratio 5x, R4=rollbacks, R5=sessoes, R6=velocity.">Regras</th>
                    <th data-tooltip="Quantidade de regras distintas violadas. Mais regras = maior confianca na deteccao.">Qty</th>
                    <th data-tooltip="Detalhes especificos de cada violacao: valores, ratios, quantidades. Usar para investigacao manual.">Evidencias</th>
                </tr>
            </thead>
            <tbody>
                {medium_risk_rows if medium_risk_rows else '<tr><td colspan="6" class="center">Nenhum jogador MEDIUM no periodo</td></tr>'}
            </tbody>
        </table>
    </div>

    <!-- FOOTER -->
    <div class="footer">
        Squad 3 — Intelligence Engine | MultiBet Risk Agent v1.0 | Dados: Athena (fund_ec2, bonus_ec2, bireports_ec2)
    </div>

</div>

<script>
// Cores padrao
const COLORS = {{
    critical: '#ef4444',
    high: '#f97316',
    medium: '#eab308',
    low: '#10b981',
    purple: '#8b5cf6',
    purpleLight: '#a78bfa',
}};

// Chart defaults
Chart.defaults.color = '#9ca3af';
Chart.defaults.borderColor = '#2d2d44';
Chart.defaults.font.family = "'Inter', system-ui, sans-serif";

// 1. Tier Doughnut
new Chart(document.getElementById('chartTier'), {{
    type: 'doughnut',
    data: {{
        labels: {tier_labels},
        datasets: [{{
            data: {tier_values},
            backgroundColor: [COLORS.critical, COLORS.high, COLORS.medium, COLORS.low],
            borderWidth: 0,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{
            legend: {{ position: 'bottom', labels: {{ padding: 16, usePointStyle: true }} }}
        }}
    }}
}});

// 2. Rules Bar
new Chart(document.getElementById('chartRules'), {{
    type: 'bar',
    data: {{
        labels: {rule_labels},
        datasets: [{{
            label: 'Jogadores',
            data: {rule_values},
            backgroundColor: COLORS.purple,
            borderRadius: 4,
        }}]
    }},
    options: {{
        responsive: true,
        indexAxis: 'y',
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
            x: {{ grid: {{ color: '#2d2d44' }} }},
            y: {{ grid: {{ display: false }} }}
        }}
    }}
}});

// 3. Score Histogram
new Chart(document.getElementById('chartScore'), {{
    type: 'bar',
    data: {{
        labels: {hist_labels},
        datasets: [{{
            label: 'Jogadores',
            data: {hist_values},
            backgroundColor: (ctx) => {{
                const v = ctx.parsed.x || ctx.dataIndex;
                if (v >= 8) return COLORS.critical;
                if (v >= 5) return COLORS.high;
                if (v >= 2) return COLORS.medium;
                return COLORS.low;
            }},
            borderRadius: 4,
        }}]
    }},
    options: {{
        responsive: true,
        plugins: {{ legend: {{ display: false }} }},
        scales: {{
            x: {{ grid: {{ display: false }}, title: {{ display: true, text: 'Risk Score' }} }},
            y: {{ grid: {{ color: '#2d2d44' }}, title: {{ display: true, text: 'Jogadores' }} }}
        }}
    }}
}});
</script>
</body>
</html>"""

    return html


def main():
    parser = argparse.ArgumentParser(description="Gera dashboard HTML de alertas de fraude")
    parser.add_argument("--input", type=str, default=None, help="Caminho do CSV de alertas")
    args = parser.parse_args()

    # Encontra o CSV mais recente se nao especificado
    if args.input:
        csv_path = args.input
    else:
        csvs = sorted(
            [f for f in os.listdir(OUTPUT_DIR) if f.startswith("risk_fraud_alerts_") and f.endswith(".csv")],
            reverse=True
        )
        if not csvs:
            print("Nenhum CSV de alertas encontrado em output/. Rode risk_fraud_detection.py primeiro.")
            sys.exit(1)
        csv_path = os.path.join(OUTPUT_DIR, csvs[0])

    print(f"Lendo: {csv_path}")
    html = generate_dashboard(csv_path)

    today = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(OUTPUT_DIR, f"risk_fraud_dashboard_{today}.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard gerado: {output_path}")


if __name__ == "__main__":
    main()
