"""
HTML executivo safra esportivas IDs Meta 464673 e 532571 — versao auditada.
Funil correto: Cadastros totais -> FTD -> Sports bettors. Universo total + subset sports.
"""
import os
import pandas as pd
from datetime import datetime

OUT_DIR = "reports/safra_esportivas_464673_532571"
CONSOL  = pd.read_csv(os.path.join(OUT_DIR, "1_consolidado.csv"))
SEMANAL = pd.read_csv(os.path.join(OUT_DIR, "2_safra_semanal.csv"))
UTM     = pd.read_csv(os.path.join(OUT_DIR, "3_utm_subset_sports.csv"))

DT_FROM = "2026-03-01"
DT_TO   = "2026-04-16"


def fmt_brl(v):
    try: v = float(v)
    except Exception: return "—"
    neg = v < 0
    s = f"R$ {abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-" + s) if neg else s


def fmt_int(v):
    try: return f"{int(v):,}".replace(",", ".")
    except Exception: return "—"


def fmt_pct(num, den):
    if not den: return "—"
    return f"{(num/den)*100:.1f}%".replace(".", ",")


def delta_class(v):
    try: v = float(v)
    except Exception: return ""
    if v > 0: return "pos"
    if v < 0: return "neg"
    return ""


def build_card(row):
    aff = int(row["affiliate_id"])
    cad = int(row["cadastros_total"])
    ftd = int(row["ftd_total"])
    sp  = int(row["sports_bettors"])
    cas = int(row["casino_bettors"])
    ftd_sp = int(row["ftd_sports"])
    dep = float(row["dep_total"])
    saq = float(row["saque_total"])
    net = float(row["net_deposit"])
    ngr = float(row["ngr"])
    ftd_amount = float(row["ftd_amount"])
    sports_ggr = float(row["sports_ggr"])
    sports_to  = float(row["sports_turnover"])
    sports_bets = int(row["sports_bets"])
    net_sp = float(row["net_deposit_sports_subset"])
    ngr_sp = float(row["ngr_sports_subset"])

    ticket_ftd = ftd_amount / ftd if ftd else 0
    margin = (sports_ggr / sports_to * 100) if sports_to else 0
    tx_ftd     = fmt_pct(ftd, cad)
    tx_sports  = fmt_pct(sp, cad)
    tx_sp_ftd  = fmt_pct(sp, ftd)
    tx_casino  = fmt_pct(cas, cad)

    return f"""
    <div class="card">
      <div class="card-header">
        <span class="badge">Meta</span>
        <h2>ID {aff}</h2>
        <span class="sub-h">{fmt_int(cad)} cadastros no periodo</span>
      </div>

      <h3>Funil de aquisicao</h3>
      <div class="funnel">
        <div class="funnel-row">
          <span class="stage" title="Todos os cadastros do tracker no periodo">1. Cadastros totais</span>
          <span class="bar" style="width:100%"></span>
          <span class="num">{fmt_int(cad)}</span><span class="pctg">100%</span>
        </div>
        <div class="funnel-row">
          <span class="stage" title="Cadastros que fizeram pelo menos 1 deposito">2. Depositaram (FTD)</span>
          <span class="bar" style="width:{(ftd/cad*100) if cad else 0:.1f}%"></span>
          <span class="num">{fmt_int(ftd)}</span><span class="pctg">{tx_ftd}</span>
        </div>
        <div class="funnel-row">
          <span class="stage" title="Do total, quantos apostaram em esportivas">3. Apostou em sports</span>
          <span class="bar orange" style="width:{(sp/cad*100) if cad else 0:.1f}%"></span>
          <span class="num">{fmt_int(sp)}</span><span class="pctg">{tx_sports}</span>
        </div>
        <div class="funnel-row">
          <span class="stage" title="Do total, quantos apostaram em casino">3b. Apostou em casino</span>
          <span class="bar gray" style="width:{(cas/cad*100) if cad else 0:.1f}%"></span>
          <span class="num">{fmt_int(cas)}</span><span class="pctg">{tx_casino}</span>
        </div>
      </div>

      <h3>Indicadores de qualidade</h3>
      <div class="kpi-grid">
        <div class="kpi" title="Taxa de conversao: cadastro -> primeiro deposito">
          <span class="lbl">Taxa FTD</span><span class="val">{tx_ftd}</span></div>
        <div class="kpi" title="Valor medio do primeiro deposito">
          <span class="lbl">Ticket FTD</span><span class="val">{fmt_brl(ticket_ftd)}</span></div>
        <div class="kpi" title="Do total de depositantes, quantos apostaram em sports — indicador de qualidade do perfil esportivo que o tracker traz">
          <span class="lbl">% sports / FTD</span><span class="val"><strong>{tx_sp_ftd}</strong></span></div>
        <div class="kpi money" title="Depositos - saques de TODOS os cadastros do tracker">
          <span class="lbl">Net Deposit total</span><span class="val {delta_class(net)}"><strong>{fmt_brl(net)}</strong></span></div>
        <div class="kpi money" title="NGR (P&amp;L liquido) de TODOS os cadastros do tracker">
          <span class="lbl">NGR total (P&amp;L)</span><span class="val {delta_class(ngr)}"><strong>{fmt_brl(ngr)}</strong></span></div>
        <div class="kpi" title="Margem do sportsbook: GGR / Turnover no subset sports">
          <span class="lbl">Margem sports</span><span class="val {delta_class(margin)}">{margin:.1f}%</span></div>
      </div>

      <h3>Performance — subset sports bettors ({fmt_int(sp)} jogadores)</h3>
      <table class="compact">
        <tr><td title="Total apostado por jogadores sports do tracker">Sports Turnover</td>
            <td class="right">{fmt_brl(sports_to)}</td></tr>
        <tr><td title="GGR do sportsbook gerado por essa cohort">Sports GGR</td>
            <td class="right {delta_class(sports_ggr)}">{fmt_brl(sports_ggr)}</td></tr>
        <tr><td title="Numero de apostas realizadas">Apostas realizadas</td>
            <td class="right">{fmt_int(sports_bets)}</td></tr>
        <tr><td title="Net deposit do subset (so sports bettors)">Net Deposit (subset)</td>
            <td class="right"><strong class="{delta_class(net_sp)}">{fmt_brl(net_sp)}</strong></td></tr>
        <tr><td title="NGR gerado pelo subset sports">NGR (subset)</td>
            <td class="right"><strong class="{delta_class(ngr_sp)}">{fmt_brl(ngr_sp)}</strong></td></tr>
      </table>
    </div>
    """


def build_weekly_chart_data(aff_id):
    rows = SEMANAL[SEMANAL["affiliate_id"] == int(aff_id)].sort_values("week_start")
    return {
        "labels":         rows["week_start"].tolist(),
        "cadastros":      rows["cadastros_total"].astype(int).tolist(),
        "ftd":            rows["ftd_total"].astype(int).tolist(),
        "sports_bettors": rows["sports_bettors"].astype(int).tolist(),
        "net_dep":        rows["net_deposit"].round(2).tolist(),
        "ngr":            rows["ngr"].round(2).tolist(),
    }


def build_weekly_table():
    rows = []
    for _, r in SEMANAL.sort_values(["affiliate_id","week_start"]).iterrows():
        cad = int(r["cadastros_total"])
        tx_ftd = fmt_pct(r["ftd_total"], cad)
        tx_sp  = fmt_pct(r["sports_bettors"], cad)
        rows.append(f"""
        <tr>
          <td>{int(r['affiliate_id'])}</td>
          <td>{r['week_start']}</td>
          <td class="right">{fmt_int(r['cadastros_total'])}</td>
          <td class="right">{fmt_int(r['ftd_total'])}</td>
          <td class="right">{tx_ftd}</td>
          <td class="right">{fmt_int(r['sports_bettors'])}</td>
          <td class="right">{tx_sp}</td>
          <td class="right"><strong class="{delta_class(r['net_deposit'])}">{fmt_brl(r['net_deposit'])}</strong></td>
          <td class="right">{fmt_brl(r['sports_turnover'])}</td>
          <td class="right">{fmt_brl(r['sports_ggr'])}</td>
          <td class="right"><strong class="{delta_class(r['ngr'])}">{fmt_brl(r['ngr'])}</strong></td>
        </tr>""")
    return "\n".join(rows)


def build_utm_section():
    com_utm = UTM[UTM["utm_source"] != "(sem UTM)"]
    tot_464 = int(CONSOL.loc[CONSOL["affiliate_id"]==464673, "sports_bettors"].iloc[0])
    tot_532 = int(CONSOL.loc[CONSOL["affiliate_id"]==532571, "sports_bettors"].iloc[0])
    cov_464 = int(com_utm[com_utm["affiliate_id"]==464673]["cadastros_sports"].sum()) if not com_utm.empty else 0
    cov_532 = int(com_utm[com_utm["affiliate_id"]==532571]["cadastros_sports"].sum()) if not com_utm.empty else 0

    if com_utm.empty:
        body = "<p><em>Nenhum cadastro com UTM rastreada no periodo.</em></p>"
    else:
        top = (com_utm.groupby(["affiliate_id","utm_source","utm_campaign"])
                .agg(cadastros=("cadastros_sports","sum"), ftd=("ftd","sum"),
                     net_deposit=("net_deposit","sum"), ngr=("ngr","sum"))
                .reset_index().sort_values("cadastros", ascending=False).head(15))
        rows = "".join(f"""
            <tr>
              <td>{int(r['affiliate_id'])}</td><td>{r['utm_source']}</td><td>{r['utm_campaign']}</td>
              <td class="right">{fmt_int(r['cadastros'])}</td>
              <td class="right">{fmt_int(r['ftd'])}</td>
              <td class="right"><strong class="{delta_class(r['net_deposit'])}">{fmt_brl(r['net_deposit'])}</strong></td>
              <td class="right"><strong class="{delta_class(r['ngr'])}">{fmt_brl(r['ngr'])}</strong></td>
            </tr>""" for _, r in top.iterrows())
        body = f"""
        <table class="compact">
          <thead><tr><th>ID</th><th>utm_source</th><th>utm_campaign</th>
          <th>Cadastros sports</th><th>FTD</th><th>Net Deposit</th><th>NGR</th></tr></thead>
          <tbody>{rows}</tbody>
        </table>"""
    return f"""
    <div class="warning">
      <strong>Cobertura de UTM muito baixa</strong> — ID 464673: {cov_464}/{tot_464} ({cov_464/tot_464*100:.1f}% do subset sports) ·
      ID 532571: {cov_532}/{tot_532} ({(cov_532/tot_532*100) if tot_532 else 0:.1f}%).
      Meta nao exporta UTMs nativamente para o app Super Nova — tratar como indicativo.
    </div>
    {body}
    """


def main():
    cards   = "\n".join(build_card(r) for _, r in CONSOL.iterrows())
    c464    = build_weekly_chart_data(464673)
    c532    = build_weekly_chart_data(532571)
    weekly  = build_weekly_table()
    utm_sec = build_utm_section()
    gerado  = datetime.now().strftime("%d/%m/%Y %H:%M")

    # totais consolidados (para o topo)
    tot_cad    = int(CONSOL["cadastros_total"].sum())
    tot_ftd    = int(CONSOL["ftd_total"].sum())
    tot_sp     = int(CONSOL["sports_bettors"].sum())
    tot_net    = float(CONSOL["net_deposit"].sum())
    tot_ngr    = float(CONSOL["ngr"].sum())

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Safra IDs Meta 464673 & 532571 — {DT_FROM} a {DT_TO}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
 * {{ box-sizing: border-box; }}
 body {{ font-family: -apple-system, 'Segoe UI', Roboto, Arial, sans-serif; margin:0; padding:24px; background:#f5f7fa; color:#1f2937; }}
 header {{ background: linear-gradient(135deg,#0f172a,#1e3a8a); color:white; padding:28px 32px; border-radius:12px; margin-bottom:24px; }}
 header h1 {{ margin:0 0 6px 0; font-size:26px; }}
 header .sub {{ opacity:0.85; font-size:13px; }}
 .hero {{ display:grid; grid-template-columns:repeat(5,1fr); gap:12px; margin-top:16px; }}
 .hero .pill {{ background:rgba(255,255,255,0.12); padding:10px 14px; border-radius:8px; }}
 .hero .pill .l {{ font-size:10px; text-transform:uppercase; opacity:0.7; letter-spacing:0.5px; }}
 .hero .pill .v {{ font-size:18px; font-weight:700; margin-top:3px; }}
 h2 {{ margin:0; font-size:20px; color:#0f172a; }}
 h3 {{ margin:18px 0 8px 0; font-size:12px; text-transform:uppercase; letter-spacing:0.5px; color:#6b7280; }}
 .intro {{ background:white; padding:18px 20px; border-radius:12px; margin-bottom:24px; border-left:4px solid #3b82f6; font-size:14px; }}
 .intro p {{ margin:4px 0; }}
 .cards {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(540px,1fr)); gap:20px; margin-bottom:24px; }}
 .card {{ background:white; padding:24px; border-radius:12px; box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
 .card-header {{ display:flex; align-items:center; gap:12px; margin-bottom:8px; }}
 .card-header .sub-h {{ margin-left:auto; font-size:12px; color:#6b7280; }}
 .badge {{ background:#1877f2; color:white; padding:4px 10px; border-radius:6px; font-size:11px; font-weight:600; }}
 .kpi-grid {{ display:grid; grid-template-columns:repeat(3,1fr); gap:10px; margin:4px 0 8px 0; }}
 .kpi {{ background:#f9fafb; padding:10px 12px; border-radius:8px; display:flex; flex-direction:column; cursor:help; }}
 .kpi.money {{ background:#eff6ff; }}
 .kpi .lbl {{ font-size:10px; color:#6b7280; text-transform:uppercase; letter-spacing:0.3px; }}
 .kpi .val {{ font-size:16px; font-weight:600; margin-top:3px; color:#111827; }}
 .pos {{ color:#059669 !important; }} .neg {{ color:#dc2626 !important; }}
 .funnel {{ display:flex; flex-direction:column; gap:5px; background:#f9fafb; padding:12px; border-radius:8px; }}
 .funnel-row {{ display:grid; grid-template-columns:140px 1fr 80px 55px; align-items:center; gap:8px; font-size:13px; }}
 .funnel-row .stage {{ color:#374151; font-weight:500; cursor:help; }}
 .funnel-row .bar {{ height:18px; background: linear-gradient(90deg,#3b82f6,#8b5cf6); border-radius:4px; min-width:2px; }}
 .funnel-row .bar.orange {{ background: linear-gradient(90deg,#f59e0b,#ef4444); }}
 .funnel-row .bar.gray   {{ background: linear-gradient(90deg,#9ca3af,#6b7280); }}
 .funnel-row .num {{ text-align:right; font-weight:600; }}
 .funnel-row .pctg {{ text-align:right; color:#6b7280; font-size:12px; }}
 table {{ width:100%; border-collapse:collapse; font-size:13px; background:white; border-radius:8px; overflow:hidden; }}
 table.compact {{ font-size:12px; }}
 th,td {{ padding:8px 10px; text-align:left; border-bottom:1px solid #e5e7eb; }}
 th {{ background:#f3f4f6; font-weight:600; color:#374151; font-size:11px; text-transform:uppercase; }}
 td.right, th.right {{ text-align:right; }}
 .section {{ background:white; padding:24px; border-radius:12px; margin-bottom:24px; box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
 .chart-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-top:16px; }}
 .chart-box {{ background:#fafafa; padding:12px; border-radius:8px; height:280px; }}
 .chart-box h4 {{ margin:0 0 8px 0; font-size:13px; color:#374151; }}
 .warning {{ background:#fef3c7; border-left:4px solid #f59e0b; padding:12px 16px; border-radius:6px; margin:0 0 12px 0; font-size:13px; }}
 .footer {{ text-align:center; font-size:11px; color:#9ca3af; margin-top:32px; }}
 details {{ background:#f9fafb; padding:12px 16px; border-radius:8px; margin-top:12px; }}
 details summary {{ cursor:pointer; font-weight:600; color:#374151; }}
 details ul {{ font-size:13px; line-height:1.6; }}
</style>
</head>
<body>

<header>
  <h1>Safra de Aquisicao — IDs Meta 464673 & 532571</h1>
  <div class="sub">Cadastros no periodo + funil: FTD -> Apostou em esportivas · Janela <strong>{DT_FROM}</strong> a <strong>{DT_TO}</strong> (D-1) · Gerado {gerado}</div>
  <div class="hero">
    <div class="pill"><div class="l">Cadastros totais</div><div class="v">{fmt_int(tot_cad)}</div></div>
    <div class="pill"><div class="l">Depositaram (FTD)</div><div class="v">{fmt_int(tot_ftd)} ({fmt_pct(tot_ftd,tot_cad)})</div></div>
    <div class="pill"><div class="l">Apostou em sports</div><div class="v">{fmt_int(tot_sp)} ({fmt_pct(tot_sp,tot_cad)})</div></div>
    <div class="pill"><div class="l">Net Deposit</div><div class="v">{fmt_brl(tot_net)}</div></div>
    <div class="pill"><div class="l">NGR (P&amp;L)</div><div class="v">{fmt_brl(tot_ngr)}</div></div>
  </div>
</header>

<div class="intro">
  <p><strong>Como ler este relatorio:</strong> o <em>denominador</em> e sempre o total de cadastros do tracker no periodo (nao apenas quem apostou). O funil vai se afunilando: todos os cadastros -> depositantes (FTD) -> quem apostou em esportivas. Metricas financeiras sao em reais (BRL).</p>
  <p><strong>Subset sports</strong> = corte so dos jogadores que apostaram em esportivas, usado para avaliar a performance financeira desse publico especifico. Passe o mouse sobre os labels e KPIs pra ver a definicao de cada um.</p>
</div>

<div class="cards">{cards}</div>

<div class="section">
  <h2>Evolucao semanal (safra por semana de cadastro)</h2>
  <div class="chart-grid">
    <div class="chart-box"><h4>ID 464673 — Cadastros / FTD / Sports bettors</h4><canvas id="chart464_vol"></canvas></div>
    <div class="chart-box"><h4>ID 464673 — Net Deposit e NGR (R$)</h4><canvas id="chart464_fin"></canvas></div>
    <div class="chart-box"><h4>ID 532571 — Cadastros / FTD / Sports bettors</h4><canvas id="chart532_vol"></canvas></div>
    <div class="chart-box"><h4>ID 532571 — Net Deposit e NGR (R$)</h4><canvas id="chart532_fin"></canvas></div>
  </div>
  <h3>Detalhamento semanal</h3>
  <table class="compact">
    <thead><tr>
      <th>ID</th><th>Semana (segunda)</th>
      <th class="right">Cadastros</th><th class="right">FTD</th><th class="right">%FTD</th>
      <th class="right">Sports bettors</th><th class="right">%Sports</th>
      <th class="right">Net Deposit</th>
      <th class="right">Sports Turnover</th><th class="right">Sports GGR</th>
      <th class="right">NGR</th>
    </tr></thead>
    <tbody>{weekly}</tbody>
  </table>
</div>

<div class="section">
  <h2>Quebra por UTM (subset sports)</h2>
  {utm_sec}
</div>

<div class="section">
  <h2>Metodologia & Glossario</h2>
  <details open>
    <summary>Como os numeros foram apurados</summary>
    <ul>
      <li><strong>Cohort (source of truth):</strong> Athena <code>ps_bi.dim_user</code> com <code>tracker_id IN (464673, 532571)</code> e <code>signup_datetime</code> (BRT) no periodo. Fonte canonica de cadastro — <em>migrada da tab_user_affiliate</em> apos detectar que aquela inflava +20,9% devido a logica de atribuicao/reatribuicao.</li>
      <li><strong>FTD / Depositos / Saques / Net Deposit:</strong> Athena <code>ps_bi.dim_user.has_ftd</code>, <code>lifetime_deposit_amount_inhouse</code>, <code>lifetime_withdrawal_amount_inhouse</code>, <code>net_deposit_withdrawal_inhouse</code>. Valores ja em BRL.</li>
      <li><strong>Sports / Casino bettors:</strong> Super Nova DB <code>multibet.tab_user_daily</code> (join por <code>c_ecr_id</code>). Flags nao mutuamente exclusivas.</li>
      <li><strong>UTM:</strong> Super Nova DB <code>multibet.trackings</code> (join <code>user_id = external_id</code>).</li>
      <li><strong>Test users:</strong> excluidos via <code>ps_bi.dim_user.is_test = FALSE</code> (fonte canonica).</li>
    </ul>
  </details>
  <details>
    <summary>Glossario</summary>
    <ul>
      <li><strong>Cadastros totais:</strong> unicos c_ecr_id cadastrados pelo tracker no periodo.</li>
      <li><strong>FTD:</strong> First Time Deposit — primeiro deposito. Aqui = "tem ao menos 1 deposito".</li>
      <li><strong>Sports bettors:</strong> cadastros que apostaram ao menos 1 vez em esportivas.</li>
      <li><strong>Casino bettors:</strong> cadastros que apostaram ao menos 1 vez em casino.</li>
      <li><strong>Overlap sports + casino:</strong> flags nao sao mutuamente exclusivas — um mesmo jogador pode aparecer nas duas contagens. A soma nao e jogadores distintos.</li>
      <li><strong>Net Deposit:</strong> dep total - saques (BRL). "Dinheiro novo que ficou na casa".</li>
      <li><strong>NGR</strong> (Net Gaming Revenue): GGR - custos diretos (bonus, etc.). O P&amp;L da casa.</li>
      <li><strong>Sports Turnover:</strong> total apostado no sportsbook por esses jogadores.</li>
      <li><strong>Sports GGR:</strong> apostas - pagamentos no sportsbook.</li>
      <li><strong>Margem sports:</strong> GGR / Turnover. Referencia MultiBet ~10-12%.</li>
    </ul>
  </details>
  <details>
    <summary>Ressalvas & auditoria</summary>
    <ul>
      <li>Janela <strong>01/03 a 16/04</strong> (D-1 conforme feedback <code>sempre_usar_d_menos_1</code>; dados de 17/04 ainda sao parciais).</li>
      <li><strong>Test users filtrados</strong> via <code>ps_bi.dim_user.is_test = FALSE</code> diretamente na cohort Athena.</li>
      <li><strong>Spot-check Athena vs SuperNova:</strong> ID 464673 bate (delta &lt;0,1% vs <code>ecr_ec2.tbl_ecr.c_signup_time</code>); ID 532571 bate (0%).</li>
      <li>Cobertura de UTM &lt; 1% — Meta nao exporta UTMs no fluxo do app Super Nova.</li>
      <li>Semana ISO inicia na segunda; 01/03 (domingo) entra na semana da cohort anterior (23/02) no <code>date_trunc('week',...)</code>, mas e contabilizado no total e no consolidado.</li>
      <li>Relatorio auditado em 17/04/2026 por agente auditor — racional revisado (INNER JOIN substituido por LEFT JOIN + flag is_sports, para nao inflar %FTD).</li>
    </ul>
  </details>
</div>

<div class="footer">Relatorio gerado automaticamente · Squad Intelligence Engine · Super Nova Gaming</div>

<script>
const opts = (isMoney) => ({{
  responsive:true, maintainAspectRatio:false,
  plugins:{{ legend:{{ position:'bottom', labels:{{ boxWidth:12, font:{{size:11}} }} }} }},
  scales:{{ y:{{ ticks:{{ font:{{size:10}}, callback:v=>isMoney?'R$ '+v.toLocaleString('pt-BR'):v }} }}, x:{{ ticks:{{ font:{{size:10}} }} }} }}
}});
function bars(id, labels, series, money) {{
  new Chart(document.getElementById(id), {{ type:'bar', data:{{ labels, datasets:series }}, options:opts(money) }});
}}
const c464 = {c464!r};
const c532 = {c532!r};
bars('chart464_vol', c464.labels, [
  {{label:'Cadastros',      data:c464.cadastros,      backgroundColor:'#3b82f6'}},
  {{label:'FTD',            data:c464.ftd,            backgroundColor:'#8b5cf6'}},
  {{label:'Sports bettors', data:c464.sports_bettors, backgroundColor:'#f59e0b'}},
], false);
bars('chart464_fin', c464.labels, [
  {{label:'Net Deposit', data:c464.net_dep, backgroundColor:'#10b981'}},
  {{label:'NGR',         data:c464.ngr,     backgroundColor:'#ef4444'}},
], true);
bars('chart532_vol', c532.labels, [
  {{label:'Cadastros',      data:c532.cadastros,      backgroundColor:'#3b82f6'}},
  {{label:'FTD',            data:c532.ftd,            backgroundColor:'#8b5cf6'}},
  {{label:'Sports bettors', data:c532.sports_bettors, backgroundColor:'#f59e0b'}},
], false);
bars('chart532_fin', c532.labels, [
  {{label:'Net Deposit', data:c532.net_dep, backgroundColor:'#10b981'}},
  {{label:'NGR',         data:c532.ngr,     backgroundColor:'#ef4444'}},
], true);
</script>
</body>
</html>
"""
    path = os.path.join(OUT_DIR, "safra_esportivas_ID464673_ID532571_FINAL.html")
    with open(path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"OK: {path} ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
