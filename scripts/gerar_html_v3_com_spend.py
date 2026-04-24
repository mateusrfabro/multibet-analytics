"""
HTML v3 — safra esportivas com spend + CAC + ROAS para gestor de trafego.
Integra o relatorio anterior (funil por ID) e adiciona bloco de eficiencia
financeira alinhado com as semanas que o gestor mandou.
"""
import os
import pandas as pd
from datetime import datetime

OUT_DIR = "reports/safra_esportivas_464673_532571"
CAC     = pd.read_csv(os.path.join(OUT_DIR, "4_cac_roas_semanal.csv"))
CONSOL  = pd.read_csv(os.path.join(OUT_DIR, "1_consolidado.csv"))

DT_FROM = "2026-03-01"
DT_TO   = "2026-04-14"


def brl(v):
    try: v = float(v)
    except Exception: return "—"
    if pd.isna(v): return "—"
    s = f"R$ {abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return ("-" + s) if v < 0 else s

def num(v):
    try:
        if pd.isna(v): return "—"
        return f"{int(v):,}".replace(",", ".")
    except Exception: return "—"

def pct(v, dec=1):
    try:
        if pd.isna(v): return "—"
        return f"{float(v)*100:.{dec}f}%".replace(".", ",")
    except Exception: return "—"

def cls(v):
    try:
        v = float(v)
        if pd.isna(v): return ""
        return "pos" if v > 0 else ("neg" if v < 0 else "")
    except Exception: return ""


# --- HERO totais ---
spend_tot = float(CAC["spend"].sum())
cad_tot   = int(CAC["cadastros_total"].sum())
ftd_tot   = int(CAC["ftd_total"].sum())
sp_tot    = int(CAC["sports_bettors"].sum())
ngr_tot   = float(CAC["ngr"].sum())
sggr_tot  = float(CAC["sports_ggr"].sum())
cac_cad   = spend_tot / cad_tot  if cad_tot else 0
cac_ftd   = spend_tot / ftd_tot  if ftd_tot else 0
cac_sp    = spend_tot / sp_tot   if sp_tot  else 0
roas      = ngr_tot   / spend_tot if spend_tot else 0
roas_sggr = sggr_tot  / spend_tot if spend_tot else 0
result    = ngr_tot - spend_tot

# --- Tabela CAC/ROAS semanal ---
rows_cac = []
for _, r in CAC.iterrows():
    rows_cac.append(f"""
    <tr>
      <td><strong>{r['bucket']}</strong></td>
      <td class="right">{brl(r['spend'])}</td>
      <td class="right">{num(r['cadastros_total'])}</td>
      <td class="right">{num(r['ftd_total'])}</td>
      <td class="right">{num(r['std_total'])}</td>
      <td class="right">{num(r['ttd_total'])}</td>
      <td class="right">{num(r['qtd_plus'])}</td>
      <td class="right">{num(r['sports_bettors'])}</td>
      <td class="right">{brl(r['cac_cadastro'])}</td>
      <td class="right">{brl(r['cac_ftd'])}</td>
      <td class="right">{brl(r['sports_ggr'])}</td>
      <td class="right"><strong class="{cls(r['ngr'])}">{brl(r['ngr'])}</strong></td>
      <td class="right {cls(r['roas_ngr']-0.5)}">{pct(r['roas_ngr'], 1)}</td>
      <td class="right"><strong class="{cls(r['resultado_liquido'])}">{brl(r['resultado_liquido'])}</strong></td>
    </tr>""")

# --- Chart data ---
import json
chart_data = {
    "labels": CAC["bucket"].tolist(),
    "spend":  [float(x) for x in CAC["spend"].tolist()],
    "ngr":    [float(x) for x in CAC["ngr"].tolist()],
    "cad":    [int(x)   for x in CAC["cadastros_total"].tolist()],
    "ftd":    [int(x)   for x in CAC["ftd_total"].tolist()],
    "std":    [int(x)   for x in CAC["std_total"].tolist()],
    "ttd":    [int(x)   for x in CAC["ttd_total"].tolist()],
    "qtd":    [int(x)   for x in CAC["qtd_plus"].tolist()],
    "sports": [int(x)   for x in CAC["sports_bettors"].tolist()],
    "cac_cad":[float(x) for x in CAC["cac_cadastro"].tolist()],
    "cac_ftd":[float(x) for x in CAC["cac_ftd"].tolist()],
}

# --- Consolidado por ID (reuso da v2) ---
rows_id = []
for _, r in CONSOL.iterrows():
    aff = int(r["affiliate_id"])
    cad = int(r["cadastros_total"])
    ftd = int(r["ftd_total"])
    std = int(r["std_total"])
    ttd = int(r["ttd_total"])
    qtd = int(r["qtd_plus"])
    sp  = int(r["sports_bettors"])
    rows_id.append(f"""
    <tr>
      <td><strong>ID {aff}</strong> <span class="badge small">Meta</span></td>
      <td class="right">{num(cad)}</td>
      <td class="right">{num(ftd)} <span class="pctg">({pct(ftd/cad if cad else 0)})</span></td>
      <td class="right">{num(std)} <span class="pctg">({pct(std/ftd if ftd else 0)})</span></td>
      <td class="right">{num(ttd)} <span class="pctg">({pct(ttd/ftd if ftd else 0)})</span></td>
      <td class="right">{num(qtd)} <span class="pctg">({pct(qtd/ftd if ftd else 0)})</span></td>
      <td class="right">{num(sp)} <span class="pctg">({pct(sp/cad if cad else 0)})</span></td>
      <td class="right">{brl(r['ftd_amount']/ftd if ftd else 0)}</td>
      <td class="right"><strong class="{cls(r['net_deposit'])}">{brl(r['net_deposit'])}</strong></td>
      <td class="right"><strong class="{cls(r['sports_ggr'])}">{brl(r['sports_ggr'])}</strong></td>
      <td class="right"><strong class="{cls(r['ngr'])}">{brl(r['ngr'])}</strong></td>
    </tr>""")

gerado = datetime.now().strftime("%d/%m/%Y %H:%M")

html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Safra Esportes Meta — CAC & ROAS | {DT_FROM} a {DT_TO}</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
 * {{ box-sizing: border-box; }}
 body {{ font-family: -apple-system, 'Segoe UI', Roboto, Arial, sans-serif; margin:0; padding:24px; background:#f5f7fa; color:#1f2937; }}
 header {{ background: linear-gradient(135deg,#0f172a,#1e3a8a); color:white; padding:28px 32px; border-radius:12px; margin-bottom:24px; }}
 header h1 {{ margin:0 0 6px 0; font-size:26px; }}
 header .sub {{ opacity:0.85; font-size:13px; }}
 .hero {{ display:grid; grid-template-columns:repeat(6,1fr); gap:10px; margin-top:16px; }}
 .hero .pill {{ background:rgba(255,255,255,0.12); padding:10px 12px; border-radius:8px; }}
 .hero .pill .l {{ font-size:10px; text-transform:uppercase; opacity:0.7; letter-spacing:0.5px; }}
 .hero .pill .v {{ font-size:17px; font-weight:700; margin-top:3px; }}
 h2 {{ margin:0; font-size:20px; color:#0f172a; }}
 h3 {{ margin:18px 0 8px 0; font-size:12px; text-transform:uppercase; letter-spacing:0.5px; color:#6b7280; }}
 .section {{ background:white; padding:24px; border-radius:12px; margin-bottom:24px; box-shadow:0 1px 3px rgba(0,0,0,0.08); }}
 .intro {{ border-left:4px solid #3b82f6; }}
 .intro p {{ margin:4px 0; font-size:14px; }}
 .warning {{ background:#fef3c7; border-left:4px solid #f59e0b; padding:14px 18px; border-radius:6px; font-size:13px; margin:12px 0; }}
 .warning strong {{ color:#92400e; }}
 table {{ width:100%; border-collapse:collapse; font-size:13px; background:white; border-radius:8px; overflow:hidden; }}
 table.compact {{ font-size:12px; }}
 th,td {{ padding:9px 10px; text-align:left; border-bottom:1px solid #e5e7eb; vertical-align:middle; }}
 th {{ background:#f3f4f6; font-weight:600; color:#374151; font-size:11px; text-transform:uppercase; }}
 td.right, th.right {{ text-align:right; }}
 .pos {{ color:#059669 !important; }} .neg {{ color:#dc2626 !important; }}
 .pctg {{ font-size:11px; color:#6b7280; }}
 .badge {{ background:#1877f2; color:white; padding:3px 8px; border-radius:6px; font-size:10px; font-weight:600; margin-left:6px; }}
 .chart-grid {{ display:grid; grid-template-columns:1fr 1fr; gap:20px; margin-top:16px; }}
 .chart-box {{ background:#fafafa; padding:12px; border-radius:8px; height:280px; }}
 .chart-box h4 {{ margin:0 0 8px 0; font-size:13px; color:#374151; }}
 details {{ background:#f9fafb; padding:12px 16px; border-radius:8px; margin-top:12px; }}
 details summary {{ cursor:pointer; font-weight:600; color:#374151; }}
 details ul {{ font-size:13px; line-height:1.6; }}
 .footer {{ text-align:center; font-size:11px; color:#9ca3af; margin-top:32px; }}
</style>
</head>
<body>

<header>
  <h1>Safra Esportes Meta — CAC & ROAS</h1>
  <div class="sub">Trackers <strong>464673 + 532571</strong> · Janela <strong>{DT_FROM}</strong> a <strong>{DT_TO}</strong> · Spend confirmado pelo gestor · Gerado {gerado}</div>
  <div class="hero">
    <div class="pill"><div class="l">Spend total</div><div class="v">{brl(spend_tot)}</div></div>
    <div class="pill"><div class="l">Cadastros</div><div class="v">{num(cad_tot)}</div></div>
    <div class="pill"><div class="l">CAC (cadastro)</div><div class="v">{brl(cac_cad)}</div></div>
    <div class="pill"><div class="l">CAC FTD</div><div class="v">{brl(cac_ftd)}</div></div>
    <div class="pill"><div class="l">NGR lifetime</div><div class="v">{brl(ngr_tot)}</div></div>
    <div class="pill"><div class="l">ROAS (NGR/spend)</div><div class="v">{pct(roas,1)}</div></div>
  </div>
</header>

<div class="section intro">
  <p><strong>O que voce esta vendo:</strong> performance das duas campanhas Meta de esporte (<code>464673</code> e <code>532571</code>) no periodo, com cruzamento entre <strong>spend informado</strong> (WhatsApp) e <strong>resultados observados no banco</strong> (Athena + Super Nova). Buckets semanais iguais aos que voce enviou.</p>
  <div class="warning">
    <strong>Leitura critica — maturacao:</strong> o NGR exibido e <em>lifetime ate hoje</em> (17/04). Jogadores cadastrados na <em>Sem 2 abr (08-14)</em> tem apenas 3-10 dias jogando; e normal o ROAS estar baixo no curto prazo. Em iGaming o payback tipico e <strong>90-180 dias</strong>. A coluna <strong>Resultado liquido</strong> e uma foto do momento, nao e o resultado final da cohort.
  </div>
</div>

<div class="section">
  <h2>Eficiencia de aquisicao — semanal</h2>
  <h3>Spend × Cadastros × FTD × Sports bettors × NGR</h3>
  <table class="compact">
    <thead><tr>
      <th>Semana</th>
      <th class="right">Spend</th>
      <th class="right" title="Total de cadastros do tracker na semana">Cadastros</th>
      <th class="right" title="First Time Deposit — ao menos 1 deposito">FTD</th>
      <th class="right" title="Second Time Deposit — ao menos 2 depositos">STD</th>
      <th class="right" title="Third Time Deposit — ao menos 3 depositos">TTD</th>
      <th class="right" title="4+ depositos">QTD+</th>
      <th class="right" title="Cadastros que apostaram em esportivas">Sports</th>
      <th class="right" title="Spend / Cadastros">CAC cad.</th>
      <th class="right" title="Spend / FTDs">CAC FTD</th>
      <th class="right">Sports GGR</th>
      <th class="right">NGR lifetime</th>
      <th class="right" title="NGR / Spend">ROAS</th>
      <th class="right" title="NGR - Spend (foto de hoje)">Resultado</th>
    </tr></thead>
    <tbody>{''.join(rows_cac)}</tbody>
    <tfoot>
      <tr style="background:#f9fafb; font-weight:700">
        <td>TOTAL</td>
        <td class="right">{brl(spend_tot)}</td>
        <td class="right">{num(cad_tot)}</td>
        <td class="right">{num(ftd_tot)}</td>
        <td class="right">{num(int(CAC['std_total'].sum()))}</td>
        <td class="right">{num(int(CAC['ttd_total'].sum()))}</td>
        <td class="right">{num(int(CAC['qtd_plus'].sum()))}</td>
        <td class="right">{num(sp_tot)}</td>
        <td class="right">{brl(cac_cad)}</td>
        <td class="right">{brl(cac_ftd)}</td>
        <td class="right">{brl(sggr_tot)}</td>
        <td class="right {cls(ngr_tot)}">{brl(ngr_tot)}</td>
        <td class="right">{pct(roas,1)}</td>
        <td class="right {cls(result)}">{brl(result)}</td>
      </tr>
    </tfoot>
  </table>

  <h3>Graficos</h3>
  <div class="chart-grid">
    <div class="chart-box"><h4>Spend × NGR lifetime por semana</h4><canvas id="chart_fin"></canvas></div>
    <div class="chart-box"><h4>Volume: Cadastros / FTD / Sports bettors</h4><canvas id="chart_vol"></canvas></div>
    <div class="chart-box"><h4>CAC por semana (cadastro vs FTD)</h4><canvas id="chart_cac"></canvas></div>
    <div class="chart-box"><h4>ROAS (NGR / Spend) por semana</h4><canvas id="chart_roas"></canvas></div>
    <div class="chart-box" style="grid-column:1/-1; height:320px"><h4>Funil de depositos (FTD → STD → TTD → QTD+) por semana</h4><canvas id="chart_funil"></canvas></div>
  </div>
</div>

<div class="section">
  <h2>Performance consolidada por ID</h2>
  <table class="compact">
    <thead><tr>
      <th>Tracker</th>
      <th class="right">Cadastros</th>
      <th class="right" title="1o deposito">FTD</th>
      <th class="right" title="2o deposito (% sobre FTD)">STD</th>
      <th class="right" title="3o deposito (% sobre FTD)">TTD</th>
      <th class="right" title="4+ depositos (% sobre FTD)">QTD+</th>
      <th class="right">Sports bettors</th>
      <th class="right">Ticket FTD</th>
      <th class="right">Net Deposit</th>
      <th class="right">Sports GGR</th>
      <th class="right">NGR lifetime</th>
    </tr></thead>
    <tbody>{''.join(rows_id)}</tbody>
  </table>
  <p style="font-size:12px;color:#6b7280;margin-top:8px">
    Obs: consolidado por ID usa janela <strong>01/03 a 16/04</strong> (D-1). A tabela de CAC/ROAS acima usa <strong>01/03 a 14/04</strong> para bater com o spend que voce enviou.
  </p>
</div>

<div class="section">
  <h2>Como ler os numeros — guia rapido</h2>
  <details open>
    <summary>CAC, ROAS e maturacao</summary>
    <ul>
      <li><strong>FTD</strong> (First Time Deposit) = cadastros com ao menos 1 deposito bem-sucedido.</li>
      <li><strong>STD</strong> (Second Time Deposit) = cadastros com ao menos 2 depositos. Mede recorrencia inicial.</li>
      <li><strong>TTD</strong> (Third Time Deposit) = cadastros com ao menos 3 depositos.</li>
      <li><strong>QTD+</strong> = cadastros com 4 ou mais depositos. Jogadores fidelizados da cohort.</li>
      <li><strong>CAC cadastro</strong> = spend / cadastros brutos. Util pra medir eficiencia do funil-topo.</li>
      <li><strong>CAC FTD</strong> = spend / depositantes. O mais relevante pra operacao — quanto custa pra converter um depositante.</li>
      <li><strong>NGR lifetime</strong> = Net Gaming Revenue acumulado ate hoje pelos cadastros da semana. Cohorts mais velhas tem mais NGR acumulado.</li>
      <li><strong>ROAS</strong> = NGR / spend. No curto prazo (primeiras semanas) tende a ser baixo e sobe com a maturacao. Nao use ROAS de cohort com menos de 30 dias como veredicto.</li>
      <li><strong>Resultado liquido</strong> = NGR - spend. <em>Foto do momento</em>; vai melhorar conforme a cohort joga.</li>
    </ul>
  </details>
  <details>
    <summary>Fontes e metodologia</summary>
    <ul>
      <li><strong>Spend:</strong> informado pelo gestor de trafego (WhatsApp 17/04, 18:31). Refere-se exclusivamente aos trackers 464673 + 532571, vertical esportes.</li>
      <li><strong>Cohort:</strong> Athena <code>ps_bi.dim_user</code> (signup_datetime BRT, is_test excluido) — fonte canonica de cadastro.</li>
      <li><strong>Atividade sports/casino:</strong> Super Nova DB <code>multibet.tab_user_daily</code>.</li>
      <li><strong>FTD/Net Deposit/NGR:</strong> <code>ps_bi.dim_user</code> (lifetime_* ja em BRL).</li>
      <li><strong>Ressalvas:</strong> semana 4 de marco tem 10 dias (22-31) pra alinhar com o agrupamento do gestor. Sem 2 abr (08-14) tem cohort mais jovem — resultado lifetime tende a ser mais baixo.</li>
    </ul>
  </details>
</div>

<div class="footer">Relatorio gerado automaticamente · Squad Intelligence Engine · Super Nova Gaming</div>

<script>
const C = {json.dumps(chart_data)};
const fmtBRL = v => 'R$ ' + v.toLocaleString('pt-BR', {{maximumFractionDigits:0}});
const opts = (money) => ({{
  responsive:true, maintainAspectRatio:false,
  plugins:{{ legend:{{ position:'bottom', labels:{{ boxWidth:12, font:{{size:11}} }} }} }},
  scales:{{ y:{{ ticks:{{ font:{{size:10}}, callback: v => money ? fmtBRL(v) : v }} }}, x:{{ ticks:{{ font:{{size:10}} }} }} }}
}});
new Chart(document.getElementById('chart_fin'), {{
  type:'bar', data:{{ labels:C.labels, datasets:[
    {{ label:'Spend',        data:C.spend, backgroundColor:'#ef4444' }},
    {{ label:'NGR lifetime', data:C.ngr,   backgroundColor:'#10b981' }},
  ]}}, options: opts(true)
}});
new Chart(document.getElementById('chart_vol'), {{
  type:'bar', data:{{ labels:C.labels, datasets:[
    {{ label:'Cadastros',      data:C.cad,    backgroundColor:'#3b82f6' }},
    {{ label:'FTD',            data:C.ftd,    backgroundColor:'#8b5cf6' }},
    {{ label:'Sports bettors', data:C.sports, backgroundColor:'#f59e0b' }},
  ]}}, options: opts(false)
}});
new Chart(document.getElementById('chart_cac'), {{
  type:'line', data:{{ labels:C.labels, datasets:[
    {{ label:'CAC cadastro', data:C.cac_cad, borderColor:'#3b82f6', backgroundColor:'#3b82f680', tension:0.2 }},
    {{ label:'CAC FTD',      data:C.cac_ftd, borderColor:'#8b5cf6', backgroundColor:'#8b5cf680', tension:0.2 }},
  ]}}, options: opts(true)
}});
new Chart(document.getElementById('chart_roas'), {{
  type:'bar', data:{{ labels:C.labels, datasets:[
    {{ label:'ROAS (NGR/Spend)', data: C.ngr.map((n,i)=> +(n/C.spend[i]).toFixed(3)), backgroundColor:'#0ea5e9' }},
  ]}}, options:{{
    responsive:true, maintainAspectRatio:false,
    plugins:{{ legend:{{ position:'bottom' }} }},
    scales:{{ y:{{ ticks:{{ callback: v => (v*100).toFixed(0) + '%' }} }} }}
  }}
}});
new Chart(document.getElementById('chart_funil'), {{
  type:'bar', data:{{ labels:C.labels, datasets:[
    {{ label:'FTD (1o dep.)',  data:C.ftd, backgroundColor:'#3b82f6' }},
    {{ label:'STD (2o dep.)',  data:C.std, backgroundColor:'#8b5cf6' }},
    {{ label:'TTD (3o dep.)',  data:C.ttd, backgroundColor:'#ec4899' }},
    {{ label:'QTD+ (4+ dep.)', data:C.qtd, backgroundColor:'#ef4444' }},
  ]}}, options: opts(false)
}});
</script>
</body>
</html>
"""

path = os.path.join(OUT_DIR, "safra_esportes_Meta_CAC_ROAS_FINAL.html")
with open(path, "w", encoding="utf-8") as f:
    f.write(html)
print(f"OK: {path} ({len(html):,} bytes)")
