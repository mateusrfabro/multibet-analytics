"""
Dashboard Unificado de Riscos — Casino + Sportsbook
=====================================================
Le os CSVs de risk_fraud_detection.py (R1-R8) e risk_sportsbook_alerts.py (R9-R10)
e gera uma pagina HTML interativa unificada.

Uso:
    python scripts/risk_unified_dashboard.py
    python scripts/risk_unified_dashboard.py --casino output/risk_fraud_alerts_2026-04-03.csv --sportsbook output/risk_sportsbook_alerts_2026-04-07.csv

Saida:
    output/risk_unified_dashboard_YYYY-MM-DD.html

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-07
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


def find_latest_csv(prefix: str) -> str | None:
    """Encontra o CSV mais recente com o prefixo dado."""
    csvs = sorted(
        [f for f in os.listdir(OUTPUT_DIR) if f.startswith(prefix) and f.endswith(".csv")],
        reverse=True
    )
    return os.path.join(OUTPUT_DIR, csvs[0]) if csvs else None


def generate_unified_dashboard(casino_path: str | None, sb_path: str | None) -> str:
    """Gera HTML unificado a partir dos CSVs de casino e sportsbook."""

    # ------------------------------------------------------------------
    # Load data
    # ------------------------------------------------------------------
    df_casino = pd.read_csv(casino_path) if casino_path and os.path.exists(casino_path) else pd.DataFrame()
    df_sb = pd.read_csv(sb_path) if sb_path and os.path.exists(sb_path) else pd.DataFrame()

    # Casino stats
    c_total = len(df_casino)
    c_high = len(df_casino[df_casino["risk_tier"].isin(["HIGH", "CRITICAL"])]) if c_total else 0
    c_medium = len(df_casino[df_casino["risk_tier"] == "MEDIUM"]) if c_total else 0
    c_low = len(df_casino[df_casino["risk_tier"] == "LOW"]) if c_total else 0
    c_multi = len(df_casino[df_casino["qty_regras"] >= 2]) if c_total else 0

    # Casino rule counts
    c_rules = []
    if c_total:
        for regras in df_casino["regras_violadas"]:
            c_rules.extend([r.strip() for r in str(regras).split(",")])
    c_rule_counts = Counter(c_rules)

    # Sportsbook stats
    sb_total = len(df_sb)
    sb_high = len(df_sb[df_sb["max_severidade"] == "HIGH"]) if sb_total else 0
    sb_medium = len(df_sb[df_sb["max_severidade"] == "MEDIUM"]) if sb_total else 0
    sb_low = len(df_sb[df_sb["max_severidade"] == "LOW"]) if sb_total else 0
    sb_multi = len(df_sb[df_sb["qty_regras"] >= 2]) if sb_total else 0

    # Sportsbook rule counts
    sb_rules = []
    if sb_total:
        for regras in df_sb["regras_violadas"]:
            sb_rules.extend([r.strip() for r in str(regras).split("+")])
    sb_rule_counts = Counter(sb_rules)

    # Combined KPIs
    total_flagged = c_total + sb_total
    total_high = c_high + sb_high
    total_medium = c_medium + sb_medium
    total_low = c_low + sb_low

    # Combined rules for chart
    all_rule_counts = Counter()
    all_rule_counts.update(c_rule_counts)
    all_rule_counts.update(sb_rule_counts)
    rule_labels = json.dumps(list(all_rule_counts.keys()))
    rule_values = json.dumps(list(all_rule_counts.values()))

    # ------------------------------------------------------------------
    # Casino HIGH/CRITICAL table
    # ------------------------------------------------------------------
    casino_high_rows = ""
    if c_total:
        casino_high = df_casino[df_casino["risk_tier"].isin(["HIGH", "CRITICAL"])].sort_values("risk_score", ascending=False)
        for _, r in casino_high.iterrows():
            tier_class = "critical" if r["risk_tier"] == "CRITICAL" else "high"
            casino_high_rows += f"""
            <tr class="{tier_class}">
                <td class="mono">{r['c_ecr_id']}</td>
                <td><span class="badge badge-{tier_class}">{r['risk_tier']}</span></td>
                <td class="center">{r['risk_score']}</td>
                <td>{r['regras_violadas']}</td>
                <td class="center">{r['qty_regras']}</td>
                <td class="evidence">{str(r['evidencias'])[:200]}...</td>
            </tr>"""

    # Casino ZERO DEPOSIT
    zero_dep_rows = ""
    zero_dep_count = 0
    if c_total:
        zero_dep = df_casino[df_casino["regras_violadas"].str.contains("R3a", na=False)].sort_values("risk_score", ascending=False)
        zero_dep_count = len(zero_dep)
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

    # ------------------------------------------------------------------
    # Sportsbook HIGH table
    # ------------------------------------------------------------------
    sb_high_rows = ""
    if sb_total:
        sb_high_df = df_sb[df_sb["max_severidade"] == "HIGH"].sort_values("qty_regras", ascending=False)
        for _, r in sb_high_df.iterrows():
            sb_high_rows += f"""
            <tr class="high">
                <td class="mono">{r['customer_id']}</td>
                <td><span class="badge badge-high">HIGH</span></td>
                <td>{r['regras_violadas']}</td>
                <td class="center">{r['qty_regras']}</td>
                <td class="evidence">{str(r['todas_evidencias'])[:250]}...</td>
            </tr>"""

    # Sportsbook MEDIUM table (top 20)
    sb_medium_rows = ""
    if sb_total:
        sb_medium_df = df_sb[df_sb["max_severidade"] == "MEDIUM"].head(20)
        for _, r in sb_medium_df.iterrows():
            sb_medium_rows += f"""
            <tr>
                <td class="mono">{r['customer_id']}</td>
                <td><span class="badge badge-medium">MEDIUM</span></td>
                <td>{r['regras_violadas']}</td>
                <td class="center">{r['qty_regras']}</td>
                <td class="evidence">{str(r['todas_evidencias'])[:200]}...</td>
            </tr>"""

    # ------------------------------------------------------------------
    # Sportsbook R9 detail: Live Delay specific data
    # ------------------------------------------------------------------
    r9_detail_path = sb_path.replace(".csv", "_r9_detail.csv") if sb_path else None
    # We'll use the sb CSV directly for R9 entries
    sb_r9_rows = ""
    r9_count = 0
    if sb_total:
        r9_df = df_sb[df_sb["regras_violadas"].str.contains("R9", na=False)]
        r9_count = len(r9_df)
        for _, r in r9_df.sort_values("max_severidade", key=lambda x: x.map({"HIGH": 3, "MEDIUM": 2, "LOW": 1}), ascending=False).head(20).iterrows():
            sev = r["max_severidade"]
            sev_class = sev.lower()
            sb_r9_rows += f"""
            <tr class="{sev_class if sev == 'HIGH' else ''}">
                <td class="mono">{r['customer_id']}</td>
                <td><span class="badge badge-{sev_class}">{sev}</span></td>
                <td>{r['regras_violadas']}</td>
                <td class="evidence">{str(r['todas_evidencias'])[:250]}...</td>
            </tr>"""

    # R10 detail
    sb_r10_rows = ""
    r10_count = 0
    if sb_total:
        r10_df = df_sb[df_sb["regras_violadas"].str.contains("R10", na=False)]
        r10_count = len(r10_df)
        for _, r in r10_df.sort_values("max_severidade", key=lambda x: x.map({"HIGH": 3, "MEDIUM": 2, "LOW": 1}), ascending=False).head(20).iterrows():
            sev = r["max_severidade"]
            sev_class = sev.lower()
            sb_r10_rows += f"""
            <tr class="{sev_class if sev == 'HIGH' else ''}">
                <td class="mono">{r['customer_id']}</td>
                <td><span class="badge badge-{sev_class}">{sev}</span></td>
                <td>{r['regras_violadas']}</td>
                <td class="evidence">{str(r['todas_evidencias'])[:250]}...</td>
            </tr>"""

    # ------------------------------------------------------------------
    # Chart data
    # ------------------------------------------------------------------
    # Tier distribution combined
    tier_data = json.dumps([total_high, total_medium, total_low])

    # Casino vs Sportsbook comparison
    casino_vs_sb = json.dumps([c_total, sb_total])

    today = datetime.now().strftime("%Y-%m-%d %H:%M")
    casino_date = df_casino["data_deteccao"].iloc[0] if c_total else "N/A"
    sb_date = df_sb["data_deteccao"].iloc[0] if sb_total else "N/A"

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Risk Dashboard Unificado - MultiBet</title>
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
    --green: #10b981;
    --red: #ef4444;
    --yellow: #eab308;
    --orange: #f97316;
    --blue: #3b82f6;
    --cyan: #06b6d4;
    --text-primary: #ffffff;
    --text-secondary: #9ca3af;
    --text-muted: #6b7280;
    --table-row-alt: #111122;
}}
* {{ margin:0; padding:0; box-sizing:border-box; }}
body {{ background:var(--bg-primary); color:var(--text-primary); font-family:'Inter',system-ui,-apple-system,sans-serif; font-size:14px; line-height:1.5; }}
.main {{ max-width:1400px; margin:0 auto; padding:24px 32px; }}

/* Header */
.header {{ display:flex; align-items:center; justify-content:space-between; margin-bottom:24px; padding-bottom:16px; border-bottom:1px solid var(--border); }}
.header-title {{ font-size:26px; font-weight:700; }}
.header-meta {{ color:var(--text-secondary); font-size:12px; text-align:right; }}
.header-meta .live {{ color:var(--red); font-weight:600; }}

/* Tabs */
.tabs {{ display:flex; gap:0; margin-bottom:24px; border-bottom:2px solid var(--border); }}
.tab {{ padding:12px 28px; cursor:pointer; font-weight:600; font-size:14px; color:var(--text-secondary); border-bottom:2px solid transparent; margin-bottom:-2px; transition:all 0.2s; }}
.tab:hover {{ color:var(--text-primary); background:rgba(139,92,246,0.05); }}
.tab.active {{ color:var(--purple); border-bottom-color:var(--purple); }}
.tab-badge {{ background:rgba(239,68,68,0.2); color:var(--red); font-size:11px; padding:2px 8px; border-radius:10px; margin-left:8px; font-weight:700; }}
.tab-content {{ display:none; }}
.tab-content.active {{ display:block; }}

/* KPIs */
.kpi-row {{ display:grid; grid-template-columns:repeat(5, 1fr); gap:16px; margin-bottom:24px; }}
.kpi-card {{ background:var(--bg-card); border:1px solid var(--border); border-radius:10px; padding:20px; text-align:center; }}
.kpi-card.alert {{ border-color:var(--red); background:rgba(239,68,68,0.08); }}
.kpi-card.warn {{ border-color:var(--orange); background:rgba(249,115,22,0.08); }}
.kpi-card.sb {{ border-color:var(--cyan); background:rgba(6,182,212,0.05); }}
.kpi-label {{ color:var(--text-secondary); font-size:11px; text-transform:uppercase; letter-spacing:1px; margin-bottom:8px; }}
.kpi-value {{ font-size:32px; font-weight:700; }}
.kpi-value.red {{ color:var(--red); }}
.kpi-value.orange {{ color:var(--orange); }}
.kpi-value.yellow {{ color:var(--yellow); }}
.kpi-value.purple {{ color:var(--purple); }}
.kpi-value.green {{ color:var(--green); }}
.kpi-value.cyan {{ color:var(--cyan); }}
.kpi-sub {{ color:var(--text-muted); font-size:11px; margin-top:4px; }}

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
.evidence {{ font-size:11px; color:var(--text-secondary); max-width:450px; white-space:normal; line-height:1.4; }}

/* Badges */
.badge {{ padding:3px 10px; border-radius:12px; font-size:11px; font-weight:600; text-transform:uppercase; }}
.badge-critical {{ background:rgba(239,68,68,0.2); color:var(--red); }}
.badge-high {{ background:rgba(249,115,22,0.2); color:var(--orange); }}
.badge-medium {{ background:rgba(234,179,8,0.2); color:var(--yellow); }}
.badge-low {{ background:rgba(16,185,129,0.2); color:var(--green); }}

/* How to read */
.howto {{ background:var(--bg-card); border:1px solid var(--border); border-radius:10px; padding:20px; margin-bottom:24px; }}
.howto h3 {{ font-size:14px; font-weight:600; margin-bottom:12px; color:var(--purple-light); }}
.howto-grid {{ display:grid; grid-template-columns:1fr 1fr 1fr; gap:16px; font-size:12px; color:var(--text-secondary); }}
.howto-col h4 {{ color:var(--text-primary); font-size:12px; margin-bottom:6px; text-transform:uppercase; letter-spacing:0.5px; }}
.howto-col p {{ margin-bottom:4px; }}

/* Separator */
.section-sep {{ border:0; border-top:2px solid var(--purple-dark); margin:32px 0; opacity:0.4; }}

/* Footer */
.footer {{ text-align:center; color:var(--text-muted); font-size:11px; margin-top:32px; padding-top:16px; border-top:1px solid var(--border); }}

/* Alert callout */
.callout {{ background:rgba(239,68,68,0.08); border:1px solid rgba(239,68,68,0.3); border-radius:10px; padding:16px 20px; margin-bottom:20px; }}
.callout.info {{ background:rgba(6,182,212,0.08); border-color:rgba(6,182,212,0.3); }}
.callout-title {{ font-weight:700; font-size:14px; margin-bottom:6px; }}
.callout-body {{ font-size:12px; color:var(--text-secondary); line-height:1.5; }}

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
            <div class="header-title">&#128737; Risk Dashboard Unificado</div>
            <div style="color:var(--text-secondary); font-size:13px; margin-top:4px;">MultiBet - Squad 3 Intelligence Engine</div>
        </div>
        <div class="header-meta">
            <div>Gerado: {today}</div>
            <div>Casino: {casino_date} | Sportsbook: {sb_date}</div>
            <div class="live">10 regras de deteccao ativas (R1-R10)</div>
        </div>
    </div>

    <!-- HOW TO READ -->
    <div class="howto">
        <h3>Como ler este relatorio</h3>
        <div class="howto-grid">
            <div class="howto-col">
                <h4>Tiers de Risco</h4>
                <p><span class="badge badge-critical">CRITICAL</span> Suspender conta, escalar compliance</p>
                <p><span class="badge badge-high">HIGH</span> Investigar urgente, bloquear bonus</p>
                <p><span class="badge badge-medium">MEDIUM</span> Revisar manualmente, verificar KYC</p>
                <p><span class="badge badge-low">LOW</span> Monitorar, watchlist 2 semanas</p>
            </div>
            <div class="howto-col">
                <h4>Regras Casino (R1-R8)</h4>
                <p><strong>R1</strong> Pico em jogo (bets > 3x desvio padrao)</p>
                <p><strong>R2</strong> Abuso bonus (> P95 distribuicao)</p>
                <p><strong>R3a</strong> NUNCA depositou + sacou > R$50</p>
                <p><strong>R3b</strong> Saque > 5x depositos</p>
                <p><strong>R4</strong> Rollbacks excessivos</p>
                <p><strong>R6</strong> Velocity (5+ txns/hora)</p>
                <p><strong>R7</strong> Saque < 24h pos-registro</p>
                <p><strong>R8</strong> Free Spin Abuser</p>
            </div>
            <div class="howto-col">
                <h4>Regras Sportsbook (R9-R10)</h4>
                <p><strong>R9</strong> Live Delay — win rate alto em live + padrao test-then-bet (aposta baixa seguida de alta no mesmo evento)</p>
                <p><strong>R10</strong> Refund excessivo — volume anormal de cancelamentos/refunds</p>
                <p style="margin-top:8px; color:var(--text-muted);"><em>IP multi-account: nao disponivel no Athena</em></p>
            </div>
        </div>
    </div>

    <!-- GLOBAL KPIs -->
    <div class="kpi-row">
        <div class="kpi-card" data-tooltip="Total de jogadores unicos flagados em TODAS as regras (casino + sportsbook). Nao inclui test users.">
            <div class="kpi-label">Total Flagados</div>
            <div class="kpi-value purple">{total_flagged:,}</div>
            <div class="kpi-sub">Casino: {c_total:,} | SB: {sb_total}</div>
        </div>
        <div class="kpi-card alert" data-tooltip="Jogadores HIGH/CRITICAL que precisam de acao imediata. Investigar, bloquear bonus, limitar saques.">
            <div class="kpi-label">HIGH / CRITICAL</div>
            <div class="kpi-value red">{total_high}</div>
            <div class="kpi-sub">Casino: {c_high} | SB: {sb_high}</div>
        </div>
        <div class="kpi-card warn" data-tooltip="Jogadores com padrao suspeito. Revisar manualmente.">
            <div class="kpi-label">MEDIUM</div>
            <div class="kpi-value yellow">{total_medium:,}</div>
        </div>
        <div class="kpi-card" data-tooltip="Baixo risco, monitorar.">
            <div class="kpi-label">LOW</div>
            <div class="kpi-value green">{total_low}</div>
        </div>
        <div class="kpi-card sb" data-tooltip="Jogadores do sportsbook com padrao de exploracao de delay em apostas live. WIN RATE >= 55% com min 5 apostas fechadas e lucro > R$200.">
            <div class="kpi-label">Live Delay (R9)</div>
            <div class="kpi-value cyan">{r9_count}</div>
            <div class="kpi-sub">Padrao test-then-bet</div>
        </div>
    </div>

    <!-- CHARTS -->
    <div class="chart-grid">
        <div class="chart-card">
            <div class="chart-title">Casino vs Sportsbook</div>
            <canvas id="chartVs"></canvas>
        </div>
        <div class="chart-card">
            <div class="chart-title">Severidade Global</div>
            <canvas id="chartTier"></canvas>
        </div>
        <div class="chart-card">
            <div class="chart-title">Jogadores por Regra</div>
            <canvas id="chartRules"></canvas>
        </div>
    </div>

    <!-- ============================================================ -->
    <!-- TABS: Casino | Sportsbook -->
    <!-- ============================================================ -->
    <div class="tabs">
        <div class="tab active" onclick="switchTab('casino')">Casino (R1-R8) <span class="tab-badge">{c_high}</span></div>
        <div class="tab" onclick="switchTab('sportsbook')">Sportsbook (R9-R10) <span class="tab-badge">{sb_high}</span></div>
    </div>

    <!-- CASINO TAB -->
    <div id="tab-casino" class="tab-content active">

        <div class="section-title" style="color:var(--red);">&#9888; Casino — HIGH & CRITICAL ({c_high} jogadores)</div>
        <div class="section-subtitle">Jogadores com multiplas regras violadas. Acao: bloquear bonus, limitar saques, escalar compliance.</div>
        <div class="table-container">
            <table>
                <thead><tr>
                    <th>ECR ID</th><th>Tier</th><th>Score</th><th>Regras</th><th>Qty</th><th>Evidencias</th>
                </tr></thead>
                <tbody>
                    {casino_high_rows if casino_high_rows else '<tr><td colspan="6" class="center" style="color:var(--green);">Nenhum jogador HIGH/CRITICAL</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="section-title" style="color:var(--purple-light);">&#128680; Fraude Pura — NUNCA depositaram ({zero_dep_count} jogadores)</div>
        <div class="section-subtitle">Sacaram sem nunca depositar. Possivel abuso de bonus/freespins.</div>
        <div class="table-container">
            <table>
                <thead><tr>
                    <th>ECR ID</th><th>Tier</th><th>Score</th><th>Regras</th><th>Qty</th><th>Evidencias</th>
                </tr></thead>
                <tbody>
                    {zero_dep_rows if zero_dep_rows else '<tr><td colspan="6" class="center">Nenhum jogador zero-deposit</td></tr>'}
                </tbody>
            </table>
        </div>
    </div>

    <!-- SPORTSBOOK TAB -->
    <div id="tab-sportsbook" class="tab-content">

        <div class="callout">
            <div class="callout-title">&#9888; Live Delay Exploitation — Demanda Equipe de Riscos</div>
            <div class="callout-body">
                Padrao detectado: jogadores fazem aposta de teste (R$1-10), confirmam que o delay funciona,
                e entram com aposta alta (R$50-5.000+) no mesmo evento live. Win rate anormalmente alto (>55%).
                Mercados sensiveis: under 0.5 HT, proximos eventos, etc.
            </div>
        </div>

        <div class="section-title" style="color:var(--orange);">&#9888; Sportsbook — HIGH ({sb_high} jogadores)</div>
        <div class="section-subtitle">Jogadores com forte indicativo de exploracao de delay ou cancelamentos abusivos.</div>
        <div class="table-container">
            <table>
                <thead><tr>
                    <th>Customer ID</th><th>Severidade</th><th>Regras</th><th>Qty</th><th>Evidencias</th>
                </tr></thead>
                <tbody>
                    {sb_high_rows if sb_high_rows else '<tr><td colspan="5" class="center" style="color:var(--green);">Nenhum HIGH</td></tr>'}
                </tbody>
            </table>
        </div>

        <hr class="section-sep">

        <div class="section-title" style="color:var(--cyan);">&#127919; R9 — Live Delay Exploitation ({r9_count} jogadores)</div>
        <div class="section-subtitle">Win rate >=55% em live + lucro >R$200 + padrao test-then-bet. Top 20 por severidade.</div>
        <div class="table-container">
            <table>
                <thead><tr>
                    <th>Customer ID</th><th>Severidade</th><th>Regras</th><th>Evidencias</th>
                </tr></thead>
                <tbody>
                    {sb_r9_rows if sb_r9_rows else '<tr><td colspan="4" class="center">Nenhum jogador R9</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="section-title" style="color:var(--yellow);">&#128680; R10 — Cancelamento/Refund Excessivo ({r10_count} jogadores)</div>
        <div class="section-subtitle">>=3 refunds OU valor >=R$500. Cancelamentos = c_operation_type 'R' (Refund). Top 20 por severidade.</div>
        <div class="table-container">
            <table>
                <thead><tr>
                    <th>Customer ID</th><th>Severidade</th><th>Regras</th><th>Evidencias</th>
                </tr></thead>
                <tbody>
                    {sb_r10_rows if sb_r10_rows else '<tr><td colspan="4" class="center">Nenhum jogador R10</td></tr>'}
                </tbody>
            </table>
        </div>

        <div class="callout info">
            <div class="callout-title">&#128270; Nota: Multi-account por IP</div>
            <div class="callout-body">
                A equipe de riscos solicitou deteccao de multiplas contas no mesmo IP.
                O Athena (ecr_ec2) nao armazena IP dos jogadores. Para implementar, seria
                necessario acesso a logs de sessao/login ou integracao com sistema de autenticacao.
            </div>
        </div>
    </div>

    <!-- SPORTSBOOK MEDIUM (below tabs) -->
    <div id="sb-medium-section" style="display:none;">
        <div class="section-title" style="color:var(--yellow);">Sportsbook MEDIUM — Revisar (top 20)</div>
        <div class="table-container">
            <table>
                <thead><tr>
                    <th>Customer ID</th><th>Severidade</th><th>Regras</th><th>Qty</th><th>Evidencias</th>
                </tr></thead>
                <tbody>{sb_medium_rows}</tbody>
            </table>
        </div>
    </div>

    <!-- FOOTER -->
    <div class="footer">
        Squad 3 — Intelligence Engine | MultiBet Risk Agent v1.5 | Casino: fund_ec2, bonus_ec2 | Sportsbook: vendor_ec2
    </div>

</div>

<script>
// Colors
const C = {{
    critical: '#ef4444', high: '#f97316', medium: '#eab308', low: '#10b981',
    purple: '#8b5cf6', cyan: '#06b6d4', blue: '#3b82f6',
}};
Chart.defaults.color = '#9ca3af';
Chart.defaults.borderColor = '#2d2d44';
Chart.defaults.font.family = "'Inter', system-ui, sans-serif";

// 1. Casino vs Sportsbook
new Chart(document.getElementById('chartVs'), {{
    type: 'doughnut',
    data: {{
        labels: ['Casino (R1-R8)', 'Sportsbook (R9-R10)'],
        datasets: [{{ data: {casino_vs_sb}, backgroundColor: [C.purple, C.cyan], borderWidth: 0 }}]
    }},
    options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom', labels: {{ padding: 16, usePointStyle: true }} }} }} }}
}});

// 2. Tier
new Chart(document.getElementById('chartTier'), {{
    type: 'doughnut',
    data: {{
        labels: ['HIGH', 'MEDIUM', 'LOW'],
        datasets: [{{ data: {tier_data}, backgroundColor: [C.high, C.medium, C.low], borderWidth: 0 }}]
    }},
    options: {{ responsive: true, plugins: {{ legend: {{ position: 'bottom', labels: {{ padding: 16, usePointStyle: true }} }} }} }}
}});

// 3. Rules Bar
new Chart(document.getElementById('chartRules'), {{
    type: 'bar',
    data: {{
        labels: {rule_labels},
        datasets: [{{
            label: 'Jogadores',
            data: {rule_values},
            backgroundColor: (ctx) => {{
                const label = ctx.chart.data.labels[ctx.dataIndex];
                return (label === 'R9' || label === 'R10') ? C.cyan : C.purple;
            }},
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

// Tab switching
function switchTab(tab) {{
    document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
    document.querySelectorAll('.tab').forEach(el => el.classList.remove('active'));
    document.getElementById('tab-' + tab).classList.add('active');
    document.querySelectorAll('.tab')[tab === 'casino' ? 0 : 1].classList.add('active');
    document.getElementById('sb-medium-section').style.display = tab === 'sportsbook' ? 'block' : 'none';
}}
</script>
</body>
</html>"""

    return html


def main():
    parser = argparse.ArgumentParser(description="Dashboard Unificado de Riscos")
    parser.add_argument("--casino", type=str, default=None, help="CSV de alertas casino (R1-R8)")
    parser.add_argument("--sportsbook", type=str, default=None, help="CSV de alertas sportsbook (R9-R10)")
    args = parser.parse_args()

    casino_path = args.casino or find_latest_csv("risk_fraud_alerts_")
    sb_path = args.sportsbook or find_latest_csv("risk_sportsbook_alerts_")

    print(f"Casino: {casino_path}")
    print(f"Sportsbook: {sb_path}")

    html = generate_unified_dashboard(casino_path, sb_path)

    today = datetime.now().strftime("%Y-%m-%d")
    output_path = os.path.join(OUTPUT_DIR, f"risk_unified_dashboard_{today}.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Dashboard gerado: {output_path}")


if __name__ == "__main__":
    main()
