"""Gera HTML report das promocoes CRM no padrao visual MultiBet (dark theme)."""
import os

# ============ DADOS v2 CORRIGIDOS ============
promos = [
    {
        "name": "Tigre Sortudo", "periodo": "14h 07/03 as 23h59 08/03",
        "before": {"turnover": 308612.60, "uap": 1570},
        "during": {"turnover": 358801.20, "uap": 1742, "ggr": 15587.04},
        "after":  {"turnover": 400686.80, "uap": 1650},
        "faixas": [
            ("Gire entre R$50 a R$199",   464, 46381.20,  26.6, 6662.00),
            ("Gire entre R$200 a R$499",  166, 51948.20,   9.5, 1352.24),
            ("Gire entre R$500 a R$999",   71, 49221.20,   4.1, -1442.16),
            ("Gire R$1.000 ou mais",        61, 193453.80,  3.5, 4634.56),
            ("Abaixo de R$50",             980, 17796.80,  56.3, 4380.40),
        ],
    },
    {
        "name": "Fortune Rabbit", "periodo": "16h as 23h59 09/03",
        "before": {"turnover": 166349.50, "uap": 578},
        "during": {"turnover": 312367.50, "uap": 841, "ggr": 18400.60},
        "after":  {"turnover": 267753.50, "uap": 760},
        "faixas": [
            ("Gire entre R$30 a R$99",    240, 14273.50,  28.5, 2039.65),
            ("Gire entre R$100 a R$299",  143, 24321.50,  17.0, 4630.35),
            ("Gire entre R$300 a R$599",   50, 21437.00,   5.9, -5834.95),
            ("Gire R$600 ou mais",          79, 248228.00,  9.4, 16137.25),
            ("Abaixo de R$30",             329, 4107.50,   39.1, 1428.30),
        ],
    },
    {
        "name": "Gates of Olympus", "periodo": "11h as 23h59 10/03",
        "before": {"turnover": 74985.05, "uap": 384},
        "during": {"turnover": 78995.45, "uap": 619, "ggr": 4976.05},
        "after":  {"turnover": 78674.80, "uap": 380},
        "faixas": [
            ("Gire entre R$50 a R$99",     70, 4954.75,   11.3, 758.67),
            ("Gire entre R$100 a R$299",   83, 14256.95,  13.4, 1431.46),
            ("Gire entre R$300 a R$499",   20, 7625.65,    3.2, 136.82),
            ("Gire R$500 ou mais",          36, 47274.65,   5.8, 1785.36),
            ("Abaixo de R$50",             410, 4883.45,   66.2, 863.74),
        ],
    },
    {
        "name": "Sweet Bonanza", "periodo": "11h as 23h59 11/03",
        "before": {"turnover": 8579.25, "uap": 158},
        "during": {"turnover": 33056.60, "uap": 383, "ggr": 3827.80},
        "after":  {"turnover": 20895.25, "uap": 155},
        "faixas": [
            ("Gire entre R$15 e R$49",     87, 2447.80,   22.7, 236.16),
            ("Gire entre R$50 a R$99",     55, 3858.95,   14.4, 359.25),
            ("Gire entre R$100 a R$299",   63, 9825.10,   16.4, 688.98),
            ("Gire R$300 ou mais",          30, 16172.80,   7.8, 2364.93),
            ("Abaixo de R$15",             148, 751.95,    38.6, 178.48),
        ],
    },
    {
        "name": "Fortune Ox", "periodo": "18h as 22h 12/03",
        "before": {"turnover": 16366.00, "uap": 136},
        "during": {"turnover": 85376.50, "uap": 454, "ggr": -9059.80},
        "after":  {"turnover": 43650.00, "uap": 355},
        "faixas": [
            ("Gire entre R$30 a R$99",    128, 7137.00,   28.2, 603.45),
            ("Gire entre R$100 a R$299",   78, 12321.00,  17.2, 933.40),
            ("Gire entre R$300 a R$599",   17, 6924.50,    3.7, 825.40),
            ("Gire R$600 ou mais",          24, 56682.50,   5.3, -12054.10),
            ("Abaixo de R$30",             207, 2311.50,   45.6, 632.05),
        ],
    },
    {
        "name": "Combo FDS (Ratinho+Tigre+Macaco)", "periodo": "17h 13/03 as 23h59 15/03",
        "before": {"turnover": 627598.20, "uap": 2326},
        "during": {"turnover": 1216840.40, "uap": 3309, "ggr": 11903.47},
        "after":  {"turnover": 762976.20, "uap": 2876},
        "faixas": [
            ("Gire entre R$50 a R$199",   860, 86793.00,  26.0, 10658.85),
            ("Gire entre R$200 a R$399",  272, 76842.40,   8.2, 3114.62),
            ("Gire entre R$400 a R$799",  129, 72659.80,   3.9, 5099.64),
            ("Gire entre R$800 a R$999",   37, 33001.80,   1.1, -838.93),
            ("Gire R$1.000 ou mais",       140, 915560.20,  4.2, -13008.66),
            ("Abaixo de R$50",            1871, 31983.20,  56.5, 6877.95),
        ],
    },
]


def fmt_brl(v):
    """Formata valor BRL sem simbolo."""
    if abs(v) >= 1_000_000:
        return f"{v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def pct_change(before, after):
    if before == 0:
        return "N/A"
    pct = (after - before) / before * 100
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


def var_class(before, after):
    if before == 0:
        return ""
    pct = (after - before) / before * 100
    if pct > 10:
        return "green"
    if pct < -10:
        return "red"
    return "yellow"


def ggr_class(v):
    return "red" if v < 0 else "green"


def build_promo_section(i, p):
    bef = p["before"]
    dur = p["during"]
    aft = p["after"]
    var_bd = pct_change(bef["turnover"], dur["turnover"])
    var_da = pct_change(dur["turnover"], aft["turnover"])

    # KPI cards
    kpis = f"""
    <div class="kpi-grid">
        <div class="kpi-card">
            <div class="label">UAP Durante</div>
            <div class="value">{dur['uap']:,}</div>
            <div class="sub">vs {bef['uap']:,} mes anterior</div>
        </div>
        <div class="kpi-card">
            <div class="label">Turnover Durante</div>
            <div class="value">R$ {fmt_brl(dur['turnover'])}</div>
            <div class="sub class="{var_class(bef['turnover'], dur['turnover'])}">{var_bd} vs mes anterior</div>
        </div>
        <div class="kpi-card">
            <div class="label">GGR Durante</div>
            <div class="value {ggr_class(dur['ggr'])}">R$ {fmt_brl(dur['ggr'])}</div>
            <div class="sub">{'Casa lucrou' if dur['ggr'] >= 0 else 'Casa perdeu'}</div>
        </div>
    </div>"""

    # Period comparison table
    period_table = f"""
    <table>
        <thead>
            <tr>
                <th>Periodo</th>
                <th class="num">Turnover (R$)</th>
                <th class="num">UAP</th>
                <th class="num">Variacao</th>
            </tr>
        </thead>
        <tbody>
            <tr>
                <td>Mes Anterior (mesma janela em fev)</td>
                <td class="num">{fmt_brl(bef['turnover'])}</td>
                <td class="num">{bef['uap']:,}</td>
                <td class="num">-</td>
            </tr>
            <tr>
                <td class="highlight">Durante a Promocao</td>
                <td class="num highlight">{fmt_brl(dur['turnover'])}</td>
                <td class="num highlight">{dur['uap']:,}</td>
                <td class="num {var_class(bef['turnover'], dur['turnover'])}">{var_bd}</td>
            </tr>
            <tr>
                <td>Dia(s) Seguinte(s)</td>
                <td class="num">{fmt_brl(aft['turnover'])}</td>
                <td class="num">{aft['uap']:,}</td>
                <td class="num {var_class(dur['turnover'], aft['turnover'])}">{var_da}</td>
            </tr>
        </tbody>
    </table>"""

    # Faixas table
    faixa_rows = ""
    total_users = sum(f[1] for f in p["faixas"])
    total_turn = sum(f[2] for f in p["faixas"])
    total_ggr = sum(f[4] for f in p["faixas"])

    for nome, users, turn, pct_u, ggr in p["faixas"]:
        ggr_cls = ggr_class(ggr)
        faixa_rows += f"""
            <tr>
                <td>{nome}</td>
                <td class="num">{users:,}</td>
                <td class="num">{pct_u:.1f}%</td>
                <td class="num">{fmt_brl(turn)}</td>
                <td class="num {ggr_cls}">{fmt_brl(ggr)}</td>
            </tr>"""

    faixa_rows += f"""
            <tr class="row-total">
                <td>Total</td>
                <td class="num">{total_users:,}</td>
                <td class="num">100%</td>
                <td class="num">{fmt_brl(total_turn)}</td>
                <td class="num {ggr_class(total_ggr)}">{fmt_brl(total_ggr)}</td>
            </tr>"""

    faixa_table = f"""
    <h3 style="font-size:14px; color:#a1a1aa; margin:20px 0 12px; font-weight:600;">Distribuicao por Faixa (durante a promo)</h3>
    <table>
        <thead>
            <tr>
                <th>Faixa</th>
                <th class="num">Users</th>
                <th class="num">%</th>
                <th class="num">Turnover (R$)</th>
                <th class="num">GGR (R$)</th>
            </tr>
        </thead>
        <tbody>{faixa_rows}
        </tbody>
    </table>"""

    return f"""
        <div class="section">
            <h2><span class="icon">{i}.</span> {p['name']} <span style="font-weight:400; font-size:13px; color:#71717a;">| {p['periodo']}</span></h2>
            {kpis}
            {period_table}
            {faixa_table}
        </div>"""


# ============ BUILD HTML ============

# Summary KPIs
total_turnover = sum(p["during"]["turnover"] for p in promos)
total_uap = sum(p["during"]["uap"] for p in promos)
total_ggr = sum(p["during"]["ggr"] for p in promos)

promo_sections = ""
for i, p in enumerate(promos, 1):
    promo_sections += build_promo_section(i, p)

html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Report Promocoes CRM - Marco 2026 | MultiBet</title>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #0f1117;
            color: #e4e4e7;
            padding: 24px;
            line-height: 1.5;
        }}
        .container {{ max-width: 960px; margin: 0 auto; }}
        .header {{
            background: linear-gradient(135deg, #1e1b4b, #312e81);
            border-radius: 12px;
            padding: 32px;
            margin-bottom: 24px;
            border: 1px solid #3730a3;
        }}
        .header h1 {{ font-size: 24px; font-weight: 700; color: #c7d2fe; }}
        .header p {{ color: #a5b4fc; font-size: 14px; margin-top: 8px; }}
        .badge {{
            display: inline-block;
            background: #22c55e20;
            color: #4ade80;
            border: 1px solid #22c55e40;
            border-radius: 6px;
            padding: 2px 10px;
            font-size: 12px;
            font-weight: 600;
            margin-top: 12px;
        }}
        .badge.warn {{
            background: #f59e0b20;
            color: #fbbf24;
            border-color: #f59e0b40;
        }}
        .section {{
            background: #18181b;
            border: 1px solid #27272a;
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 20px;
        }}
        .section h2 {{
            font-size: 16px;
            font-weight: 600;
            color: #a5b4fc;
            margin-bottom: 16px;
            display: flex;
            align-items: center;
            gap: 8px;
        }}
        h3 {{ color: #a1a1aa; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        th {{
            text-align: left;
            padding: 10px 12px;
            background: #27272a;
            color: #a1a1aa;
            font-weight: 600;
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #27272a;
        }}
        tr:hover td {{ background: #27272a40; }}
        .num {{ text-align: right; font-variant-numeric: tabular-nums; }}
        .highlight {{ color: #a5b4fc; font-weight: 600; }}
        .green {{ color: #4ade80; }}
        .yellow {{ color: #fbbf24; }}
        .red {{ color: #f87171; }}
        .row-total td {{
            background: #1e1b4b20;
            font-weight: 600;
            border-top: 2px solid #3730a3;
        }}
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 16px;
        }}
        .kpi-card {{
            background: #1e1b4b20;
            border: 1px solid #3730a340;
            border-radius: 8px;
            padding: 16px;
        }}
        .kpi-card .label {{
            font-size: 11px;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            color: #a1a1aa;
        }}
        .kpi-card .value {{
            font-size: 28px;
            font-weight: 700;
            color: #c7d2fe;
            margin-top: 4px;
        }}
        .kpi-card .sub {{
            font-size: 12px;
            color: #71717a;
            margin-top: 4px;
        }}
        .note {{
            background: #27272a;
            border-left: 3px solid #f59e0b;
            padding: 12px 16px;
            border-radius: 0 8px 8px 0;
            font-size: 13px;
            color: #a1a1aa;
            margin-top: 16px;
        }}
        .note.danger {{
            border-left-color: #f87171;
        }}
        .footer {{
            text-align: center;
            color: #52525b;
            font-size: 12px;
            margin-top: 32px;
            padding: 16px;
        }}
        @media (max-width: 640px) {{
            body {{ padding: 12px; }}
            .header {{ padding: 20px; }}
            .section {{ padding: 16px; }}
            table {{ font-size: 12px; }}
            th, td {{ padding: 8px 6px; }}
            .kpi-card .value {{ font-size: 22px; }}
        }}
    </style>
</head>
<body>
    <div class="container">

        <div class="header">
            <h1>Report Promocoes CRM &mdash; Marco 2026</h1>
            <p>6 promocoes | 8 jogos | Periodo: 07/03 a 15/03/2026 | Extraido: 20/03/2026</p>
            <p>Fonte: Athena fund_ec2 | Validacao cruzada: BigQuery Smartico (6/6 OK, &lt;1% divergencia)</p>
            <span class="badge">APROVADO PELO ARQUITETO</span>
            <span class="badge">VALIDADO BIGQUERY</span>
        </div>

        <!-- RESUMO GERAL -->
        <div class="section">
            <h2><span class="icon">0.</span> Visao Geral (todas as promos somadas)</h2>
            <div class="kpi-grid">
                <div class="kpi-card">
                    <div class="label">Turnover Total (durante)</div>
                    <div class="value">R$ {fmt_brl(total_turnover)}</div>
                    <div class="sub">6 promocoes combinadas</div>
                </div>
                <div class="kpi-card">
                    <div class="label">UAP Total</div>
                    <div class="value">{total_uap:,}</div>
                    <div class="sub">usuarios unicos (pode haver sobreposicao)</div>
                </div>
                <div class="kpi-card">
                    <div class="label">GGR Total</div>
                    <div class="value {ggr_class(total_ggr)}">R$ {fmt_brl(total_ggr)}</div>
                    <div class="sub">{'Casa lucrou no agregado' if total_ggr >= 0 else 'Casa perdeu no agregado'}</div>
                </div>
            </div>
            <div class="note">
                Turnover = apostas liquidas (bets - rollbacks). Test users excluidos via ps_bi.dim_user.
                GGR = Bets - Rollbacks - Wins (receita bruta da casa).
            </div>
        </div>

        <!-- PROMOS INDIVIDUAIS -->
        {promo_sections}

        <!-- OBSERVACOES DE RISCO -->
        <div class="section">
            <h2><span class="icon">&#9888;</span> Observacoes de Risco</h2>
            <table>
                <thead>
                    <tr>
                        <th>Promocao</th>
                        <th>Faixa</th>
                        <th class="num">Users</th>
                        <th class="num">GGR (R$)</th>
                        <th>Observacao</th>
                    </tr>
                </thead>
                <tbody>
                    <tr>
                        <td>Fortune Ox</td>
                        <td>R$600+</td>
                        <td class="num">24</td>
                        <td class="num red">-12.054,10</td>
                        <td>Taxa de acerto acima da media. Sugerimos revisao na mecanica de bonus.</td>
                    </tr>
                    <tr>
                        <td>Combo FDS</td>
                        <td>R$1.000+</td>
                        <td class="num">140</td>
                        <td class="num red">-13.008,66</td>
                        <td>Whales com GGR negativo. Avaliar cap de bonificacao nessa faixa.</td>
                    </tr>
                    <tr>
                        <td>Fortune Rabbit</td>
                        <td>R$300-599</td>
                        <td class="num">50</td>
                        <td class="num red">-5.834,95</td>
                        <td>GGR negativo moderado. Monitorar recorrencia em proximas promos.</td>
                    </tr>
                </tbody>
            </table>
            <div class="note danger">
                GGR negativo em faixas altas indica que jogadores com maior volume de aposta
                tiveram taxa de retorno acima do esperado. Recomendacao: revisar mecanica de
                bonus e avaliar limites (caps) para preservar margem nas proximas campanhas.
            </div>
        </div>

        <div class="footer">
            MultiBet Analytics | Dados: Athena fund_ec2 + BigQuery Smartico | Gerado: 20/03/2026
        </div>

    </div>
</body>
</html>"""

# ============ WRITE ============
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "report_crm_promocoes_mar2026_FINAL.html")

with open(output_path, "w", encoding="utf-8") as f:
    f.write(html)

print(f"HTML gerado: {output_path}")
