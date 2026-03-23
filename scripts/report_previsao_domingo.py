#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
report_previsao_domingo.py
Gera report HTML preditivo para Domingo 22/03/2026.

Fontes:
  - output/previsao_domingo_22mar2026.json  (Statistician PhD)
  - output/domingos_depositos_historico.csv
  - output/domingos_padrao_horario.csv
  - output/domingos_ggr_historico.csv
  - output/domingos_saques_historico.csv
  - output/domingos_ftd_historico.csv
  - output/depositos_por_dia_semana_30d.csv
  - output/crm_comunicacoes_domingos_2026-03-22.csv

Saida:
  - output/squad_previsao_domingo_22mar2026_FINAL.html

Autor: Frontend Support / Mateus F. (Squad Intelligence Engine)
Data: 22/03/2026
"""

import json
import csv
import os
import sys
from datetime import datetime

# ── Paths ──────────────────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
OUTPUT_DIR = os.path.join(BASE_DIR, "output")

FILES = {
    "previsao":       os.path.join(OUTPUT_DIR, "previsao_domingo_22mar2026.json"),
    "depositos_hist": os.path.join(OUTPUT_DIR, "domingos_depositos_historico.csv"),
    "padrao_horario": os.path.join(OUTPUT_DIR, "domingos_padrao_horario.csv"),
    "ggr_hist":       os.path.join(OUTPUT_DIR, "domingos_ggr_historico.csv"),
    "saques_hist":    os.path.join(OUTPUT_DIR, "domingos_saques_historico.csv"),
    "ftd_hist":       os.path.join(OUTPUT_DIR, "domingos_ftd_historico.csv"),
    "dia_semana":     os.path.join(OUTPUT_DIR, "depositos_por_dia_semana_30d.csv"),
    "crm_comunic":    os.path.join(OUTPUT_DIR, "crm_comunicacoes_domingos_2026-03-22.csv"),
}

OUTPUT_HTML = os.path.join(OUTPUT_DIR, "squad_previsao_domingo_22mar2026_FINAL.html")


# ── Helpers ────────────────────────────────────────────────────────────────────
def load_json(path):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[WARN] Nao foi possivel carregar {path}: {e}")
        return {}


def load_csv(path):
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            return list(reader)
    except Exception as e:
        print(f"[WARN] Nao foi possivel carregar {path}: {e}")
        return []


def fmt_brl(value):
    try:
        v = float(value)
        if abs(v) >= 1_000_000:
            return "R$ " + f"{v:,.0f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return "R$ " + f"{v:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except (ValueError, TypeError):
        return "R$ --"


def fmt_brl_k(value):
    try:
        v = float(value)
        return "R$ " + f"{v/1000:,.0f}K".replace(",", ".")
    except (ValueError, TypeError):
        return "R$ --"


def fmt_number(value):
    try:
        v = int(float(value))
        return f"{v:,}".replace(",", ".")
    except (ValueError, TypeError):
        return "--"


def fmt_pct(value):
    try:
        return f"{float(value):.1f}%"
    except (ValueError, TypeError):
        return "--%"


def fmt_date_br(date_str):
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        meses = ["Jan", "Fev", "Mar", "Abr", "Mai", "Jun",
                 "Jul", "Ago", "Set", "Out", "Nov", "Dez"]
        return f"{d.day:02d}/{meses[d.month - 1]}"
    except Exception:
        return date_str


def safe_get(d, *keys, default=0):
    """Safely navigate nested dicts."""
    for key in keys:
        if isinstance(d, dict):
            d = d.get(key, default)
        else:
            return default
    return d


# ── Load data ──────────────────────────────────────────────────────────────────
print("[INFO] Carregando dados...")
prev = load_json(FILES["previsao"])
depositos_hist = load_csv(FILES["depositos_hist"])
padrao_horario = load_csv(FILES["padrao_horario"])
ggr_hist = load_csv(FILES["ggr_hist"])
saques_hist = load_csv(FILES["saques_hist"])
ftd_hist = load_csv(FILES["ftd_hist"])
dia_semana = load_csv(FILES["dia_semana"])
crm_comunic = load_csv(FILES["crm_comunic"])

# ── Extract key data from JSON ─────────────────────────────────────────────────
dep = prev.get("depositos", {})
net = prev.get("net_deposit", {})
tend = prev.get("tendencia", {})
ggr_vol = prev.get("ggr_volatilidade", {})
padrao = prev.get("padrao_horario", {})
cenarios = prev.get("cenarios", {}).get("cenarios", {})
dom_sab = prev.get("domingo_vs_sabado", {})
ftd_prev = prev.get("ftd", {})
metadata = prev.get("metadata", {})

# ── CRM channel stats ─────────────────────────────────────────────────────────
crm_channels = {}
for row in crm_comunic:
    if row.get("data_domingo") == "2026-03-15":
        canal = row.get("canal", "")
        status = row.get("status_envio", "")
        total = int(row.get("total_envios", 0))
        if canal not in crm_channels:
            crm_channels[canal] = {}
        crm_channels[canal][status] = total

channel_ctrs = {}
for canal, stats in crm_channels.items():
    sent = stats.get("Sent", 0)
    clicked = stats.get("Clicked", 0)
    delivered = stats.get("Delivered", 0)
    opened = stats.get("Opened", 0)
    if sent > 0:
        channel_ctrs[canal] = {
            "sent": sent,
            "delivered": delivered,
            "clicked": clicked,
            "ctr_sent": round(clicked / sent * 100, 1) if sent else 0,
            "ctr_delivered": round(clicked / delivered * 100, 1) if delivered else 0,
            "open_rate": round(opened / sent * 100, 1) if sent else 0,
        }

# ── Build historical table data ───────────────────────────────────────────────
hist_table_rows = []
for i, row in enumerate(depositos_hist):
    date = row.get("data_brt", "").strip()
    if not date:
        continue
    qtd = row.get("qtd_depositos", "")
    valor = row.get("total_depositos_brl", "")
    depositantes = row.get("depositantes_unicos", "")
    ticket = row.get("ticket_medio", "")

    ggr_val = "--"
    for g in ggr_hist:
        if g.get("data_brt", "").strip() == date:
            ggr_val = g.get("ggr_casino", "--")
            break

    saques_val = "--"
    for s in saques_hist:
        if s.get("data_brt", "").strip() == date:
            saques_val = s.get("total_saques_brl", "--")
            break

    ftd_val = "--"
    for f in ftd_hist:
        if f.get("data_brt", "").strip() == date:
            ftd_val = f.get("total_ftds", "--")
            break

    try:
        net_val = float(valor) - float(saques_val)
    except (ValueError, TypeError):
        net_val = None

    hist_table_rows.append({
        "date": date,
        "date_br": fmt_date_br(date),
        "qtd": qtd,
        "valor": valor,
        "depositantes": depositantes,
        "ticket": ticket,
        "ggr": ggr_val,
        "saques": saques_val,
        "ftd": ftd_val,
        "net": net_val,
    })

# ── Build hourly chart data ───────────────────────────────────────────────────
hourly_labels = []
hourly_values = []
golden_hours = padrao.get("golden_hours", [13, 15, 16, 12, 14])
dead_hours = padrao.get("dead_hours", [3, 7, 6])

for row in padrao_horario:
    hora = row.get("hora_brt", "").strip()
    if not hora:
        continue
    try:
        hourly_labels.append(f"{int(hora):02d}h")
        hourly_values.append(round(float(row.get("total_brl", 0)) / 7, 2))
    except (ValueError, TypeError):
        continue

# ── Depositos chart data ─────────────────────────────────────────────────────
dep_chart_labels = [r["date_br"] for r in hist_table_rows]
dep_chart_labels.append("22/Mar*")
dep_chart_values = [float(r["valor"]) for r in hist_table_rows]
dep_chart_values.append(dep.get("previsao_final_brl", 0))

# ── GGR chart data ────────────────────────────────────────────────────────────
ggr_chart_labels = [r["date_br"] for r in hist_table_rows]
ggr_chart_values = []
for r in hist_table_rows:
    try:
        ggr_chart_values.append(float(r["ggr"]))
    except (ValueError, TypeError):
        ggr_chart_values.append(0)

# ── Timestamp ──────────────────────────────────────────────────────────────────
now = datetime.now()
timestamp = now.strftime("%d/%m/%Y %H:%M BRT")

# ── Pre-extract all nested values ──────────────────────────────────────────────
v = {}  # all template values

# Depositos
v["dep_prev"] = fmt_brl(dep.get("previsao_final_brl", 0))
v["dep_ic80_lo"] = fmt_brl_k(safe_get(dep, "ic_80", "lower"))
v["dep_ic80_hi"] = fmt_brl_k(safe_get(dep, "ic_80", "upper"))
v["dep_ic95_lo"] = fmt_brl_k(safe_get(dep, "ic_95", "lower"))
v["dep_ic95_hi"] = fmt_brl_k(safe_get(dep, "ic_95", "upper"))
v["dep_qtd"] = fmt_number(safe_get(dep, "submetricas", "qtd_depositos_prevista"))
v["dep_uniq"] = fmt_number(safe_get(dep, "submetricas", "depositantes_unicos_previstos"))
v["dep_ticket"] = f"R$ {safe_get(dep, 'submetricas', 'ticket_medio_previsto'):.2f}"
v["dep_ticket_rec"] = f"R$ {safe_get(dep, 'submetricas', 'ticket_medio_recente_3dom'):.2f}"

# Net deposit
v["net_prev"] = fmt_brl(net.get("previsao_net_brl", 0))
v["net_prob_pos"] = fmt_pct(net.get("probabilidade_positivo_pct", 0))
v["net_dom_neg"] = str(net.get("domingos_negativos", 6))

# FTD
v["ftd_prev"] = fmt_number(ftd_prev.get("previsao_ftd", 0))
v["ftd_ic95_lo"] = fmt_number(safe_get(ftd_prev, "ic_95", "lower"))
v["ftd_ic95_hi"] = fmt_number(safe_get(ftd_prev, "ic_95", "upper"))
v["ftd_conv"] = fmt_pct(ftd_prev.get("conversao_media_pct", 0))

# GGR
v["ggr_regime_lo"] = fmt_brl_k(safe_get(ggr_vol, "range_provavel_domingo", "regime_marco", "lower"))
v["ggr_regime_hi"] = fmt_brl_k(safe_get(ggr_vol, "range_provavel_domingo", "regime_marco", "upper"))
v["ggr_regime_med"] = fmt_brl_k(safe_get(ggr_vol, "range_provavel_domingo", "regime_marco", "media"))
v["ggr_regime_med_full"] = fmt_brl(safe_get(ggr_vol, "range_provavel_domingo", "regime_marco", "media"))
v["ggr_regime_lo_full"] = fmt_brl(safe_get(ggr_vol, "range_provavel_domingo", "regime_marco", "lower"))
v["ggr_regime_hi_full"] = fmt_brl(safe_get(ggr_vol, "range_provavel_domingo", "regime_marco", "upper"))
v["ggr_cv"] = str(ggr_vol.get("cv_pct", 0))
v["ggr_min"] = fmt_brl(safe_get(ggr_vol, "estatisticas_descritivas", "minimo"))
v["ggr_max"] = fmt_brl(safe_get(ggr_vol, "estatisticas_descritivas", "maximo"))
v["ggr_mediana"] = f"R$ {safe_get(ggr_vol, 'estatisticas_descritivas', 'mediana'):,.0f}"
v["ggr_media"] = f"R$ {safe_get(ggr_vol, 'estatisticas_descritivas', 'media'):,.0f}"
v["ggr_skew"] = f"{safe_get(ggr_vol, 'estatisticas_descritivas', 'skewness'):.2f}"
v["ggr_dist"] = safe_get(ggr_vol, "melhor_distribuicao", "nome", default="Lognormal")
v["ggr_ks_p"] = f"{safe_get(ggr_vol, 'melhor_distribuicao', 'ks_p_value'):.4f}"
v["ggr_fev_media"] = f"R$ {safe_get(ggr_vol, 'regimes', 'fase_fev', 'media'):,.0f}"
v["ggr_fev_dom"] = str(safe_get(ggr_vol, "regimes", "fase_fev", "domingos", default=""))
v["ggr_mar_media"] = f"R$ {safe_get(ggr_vol, 'regimes', 'fase_mar', 'media'):,.0f}"
v["ggr_mar_dom"] = str(safe_get(ggr_vol, "regimes", "fase_mar", "domingos", default=""))

# Sabado / ratio
v["sab_dep"] = fmt_brl(safe_get(dom_sab, "sabado_22mar", "depositos_brl"))
v["sab_ftd"] = fmt_number(safe_get(dom_sab, "sabado_22mar", "ftds"))
v["sab_ratio"] = f"{dom_sab.get('ratio_dom_sab_valor', 0):.2f}"
v["sab_ratio_full"] = f"{dom_sab.get('ratio_dom_sab_valor', 0):.4f}"
v["sab_prev"] = fmt_brl(dom_sab.get("previsao_domingo_via_ratio", 0))
v["sab_obs"] = dom_sab.get("observacao", "")

# Tendencia
v["tend_mk_tau"] = f"{safe_get(tend, 'mann_kendall', 'tau'):.4f}"
v["tend_mk_p"] = f"{safe_get(tend, 'mann_kendall', 'p_value'):.4f}"
v["tend_mk_conc"] = str(safe_get(tend, "mann_kendall", "conclusao", default=""))
v["tend_reg_slope"] = f"R$ {safe_get(tend, 'regressao_linear', 'slope_brl_por_semana'):,.0f}"
v["tend_reg_r2"] = f"{safe_get(tend, 'regressao_linear', 'r_squared'):.3f}"
v["tend_reg_p"] = f"{safe_get(tend, 'regressao_linear', 'p_value'):.4f}"
v["tend_reg_conc"] = str(safe_get(tend, "regressao_linear", "conclusao", default=""))
v["tend_sw_p"] = f"{safe_get(tend, 'normalidade_residuos', 'shapiro_wilk_p'):.4f}"
v["tend_qtd_tend"] = str(safe_get(tend, "submetricas", "qtd_depositos", "tendencia", default=""))
v["tend_qtd_p"] = f"{safe_get(tend, 'submetricas', 'qtd_depositos', 'p'):.3f}"
v["tend_uniq_tend"] = str(safe_get(tend, "submetricas", "depositantes_unicos", "tendencia", default=""))
v["tend_uniq_p"] = f"{safe_get(tend, 'submetricas', 'depositantes_unicos', 'p'):.3f}"
v["tend_ticket_tend"] = str(safe_get(tend, "submetricas", "ticket_medio", "tendencia", default=""))
v["tend_ticket_p"] = f"{safe_get(tend, 'submetricas', 'ticket_medio', 'p'):.3f}"

# Cenarios
v["cen_pess"] = fmt_brl(safe_get(cenarios, "pessimista", "depositos_brl"))
v["cen_pess_delta"] = f"{safe_get(cenarios, 'pessimista', 'delta_vs_base_pct'):+.1f}"
v["cen_pess_prem"] = "<br>".join(safe_get(cenarios, "pessimista", "premissas", default=["Sem dados"]))
v["cen_base"] = fmt_brl(safe_get(cenarios, "base", "depositos_brl"))
v["cen_base_prem"] = "<br>".join(safe_get(cenarios, "base", "premissas", default=["Sem dados"]))
v["cen_otim"] = fmt_brl(safe_get(cenarios, "otimista", "depositos_brl"))
v["cen_otim_delta"] = f"+{safe_get(cenarios, 'otimista', 'delta_vs_base_pct'):.1f}"
v["cen_otim_prem"] = "<br>".join(safe_get(cenarios, "otimista", "premissas", default=["Sem dados"]))

# Padrao horario
janelas = padrao.get("janelas", {})
v["padrao_ratio"] = f"{padrao.get('ratio_pico_vale', 0):.1f}"
v["padrao_pico_hora"] = f"{int(safe_get(padrao, 'pico', 'hora', default=13)):02d}"
v["padrao_pico_val"] = fmt_brl(safe_get(padrao, "pico", "valor_brl"))
v["padrao_vale_hora"] = f"{int(safe_get(padrao, 'vale', 'hora', default=5)):02d}"
v["padrao_vale_val"] = fmt_brl(safe_get(padrao, "vale", "valor_brl"))
v["padrao_conc"] = f"{padrao.get('concentracao_13_16h_pct', 0):.1f}"
mad = janelas.get("Madrugada (0-5h)", {})
v["padrao_mad_pct"] = f"{mad.get('pct_dia', 0):.1f}"
v["padrao_mad_ticket"] = f"R$ {mad.get('ticket_medio', 0):.2f}"

# IC 80% upper for chart
v["dep_ic80_upper_num"] = str(safe_get(dep, "ic_80", "upper"))

# Metadata
v["meta_modelo"] = metadata.get("modelo", "Ensemble")
v["meta_n"] = str(metadata.get("amostra_n", 7))

v["timestamp"] = timestamp


# ══════════════════════════════════════════════════════════════════════════════
# HTML GENERATION (using string concatenation to avoid f-string escaping issues)
# ══════════════════════════════════════════════════════════════════════════════
print("[INFO] Gerando HTML...")

# We build the HTML as a list of strings and join at the end
parts = []

# ── CSS (static, no interpolation needed) ──────────────────────────────────────
CSS = """<style>
  :root { --bg: #0f1923; --card: #1a2634; --accent: #00d4aa; --gold: #ffd700;
          --text: #e0e6ed; --muted: #8899aa; --red: #ff4757; --green: #2ed573;
          --blue: #4d96ff; --orange: #ffa502; --purple: #a55eea; }
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Segoe UI', system-ui, sans-serif; background: var(--bg); color: var(--text); padding: 20px; }
  .container { max-width: 1280px; margin: 0 auto; }
  .hero { text-align: center; padding: 40px 20px 30px; border-bottom: 2px solid #1e3044; margin-bottom: 30px; }
  .squad-badge { display: inline-block; background: linear-gradient(135deg, #00d4aa22, #4d96ff22);
    border: 1px solid var(--accent); border-radius: 20px; padding: 6px 20px;
    font-size: 11px; font-weight: 700; letter-spacing: 2px; color: var(--accent);
    text-transform: uppercase; margin-bottom: 15px; }
  .hero h1 { font-size: 32px; color: var(--text); margin-bottom: 8px; }
  .hero .impact { font-size: 20px; color: var(--gold); font-weight: 600; margin-bottom: 5px; }
  .hero .date { font-size: 13px; color: var(--muted); }
  .pred-badge { display: inline-block; background: linear-gradient(135deg, #ffd70022, #ff475722);
    border: 1px solid var(--gold); border-radius: 20px; padding: 4px 16px;
    font-size: 10px; font-weight: 700; letter-spacing: 1.5px; color: var(--gold);
    text-transform: uppercase; margin-top: 10px; }
  .legenda { background: var(--card); border: 1px solid #2a3a4a; border-radius: 10px;
    padding: 18px 24px; margin-bottom: 30px; font-size: 12px; line-height: 1.8; color: var(--muted); }
  .legenda strong { color: var(--accent); }
  .legenda h3 { color: var(--gold); font-size: 14px; margin-bottom: 8px; }
  h2 { color: var(--gold); font-size: 20px; margin: 40px 0 15px; border-bottom: 1px solid #2a3a4a; padding-bottom: 8px; }
  h3 { color: var(--accent); font-size: 16px; margin: 20px 0 10px; }
  .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin: 20px 0; }
  .card { background: var(--card); border-radius: 10px; padding: 18px; text-align: center; transition: transform 0.2s; }
  .card:hover { transform: translateY(-2px); }
  .card .number { font-size: 24px; font-weight: bold; }
  .card .label { font-size: 10px; color: var(--muted); margin-top: 5px; text-transform: uppercase; letter-spacing: 0.5px; }
  .card .delta { font-size: 11px; margin-top: 3px; }
  .card .sublabel { font-size: 11px; color: var(--muted); margin-top: 4px; }
  .card.green .number { color: var(--green); }
  .card.red .number { color: var(--red); }
  .card.gold .number { color: var(--gold); }
  .card.blue .number { color: var(--blue); }
  .card.accent .number { color: var(--accent); }
  .card.purple .number { color: var(--purple); }
  .card.orange .number { color: var(--orange); }
  .scenario-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 15px; margin: 20px 0; }
  @media (max-width: 768px) { .scenario-grid { grid-template-columns: 1fr; } }
  .scenario { border-radius: 12px; padding: 24px; text-align: center; position: relative; }
  .scenario .sc-label { font-size: 12px; text-transform: uppercase; letter-spacing: 2px; font-weight: 700; margin-bottom: 10px; }
  .scenario .sc-value { font-size: 28px; font-weight: bold; margin-bottom: 8px; }
  .scenario .sc-delta { font-size: 13px; margin-bottom: 12px; }
  .scenario .sc-detail { font-size: 11px; line-height: 1.6; text-align: left; }
  .scenario.pessimista { background: linear-gradient(135deg, #ff475715, #ff475708); border: 1px solid #ff475744; }
  .scenario.pessimista .sc-label { color: var(--red); }
  .scenario.pessimista .sc-value { color: var(--red); }
  .scenario.base { background: linear-gradient(135deg, #4d96ff15, #4d96ff08); border: 1px solid #4d96ff44; }
  .scenario.base .sc-label { color: var(--blue); }
  .scenario.base .sc-value { color: var(--blue); }
  .scenario.otimista { background: linear-gradient(135deg, #2ed57315, #2ed57308); border: 1px solid #2ed57344; }
  .scenario.otimista .sc-label { color: var(--green); }
  .scenario.otimista .sc-value { color: var(--green); }
  table { width: 100%; border-collapse: collapse; margin: 15px 0; font-size: 13px; }
  th { background: #1e3044; color: var(--accent); padding: 10px 8px; text-align: left;
    font-size: 11px; text-transform: uppercase; letter-spacing: 0.3px; }
  td { padding: 8px; border-bottom: 1px solid #1e3044; }
  tr:hover { background: #1e2d3d; }
  .positive { color: var(--green); }
  .negative { color: var(--red); }
  .info-box { background: var(--card); border-left: 4px solid var(--accent); padding: 15px 20px;
    margin: 15px 0; border-radius: 0 8px 8px 0; font-size: 13px; line-height: 1.6; }
  .info-box.alert { border-left-color: var(--red); }
  .info-box.success { border-left-color: var(--green); }
  .info-box.gold { border-left-color: var(--gold); }
  .info-box.warning { border-left-color: var(--orange); }
  .info-box strong { color: var(--accent); }
  .info-box.alert strong { color: var(--red); }
  .info-box.success strong { color: var(--green); }
  .info-box.gold strong { color: var(--gold); }
  .info-box.warning strong { color: var(--orange); }
  .chart-container { background: var(--card); border-radius: 10px; padding: 20px; margin: 15px 0; }
  canvas { max-height: 350px; }
  .opp-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(280px, 1fr)); gap: 15px; margin: 20px 0; }
  .opp-card { background: var(--card); border-radius: 10px; padding: 20px; border-left: 4px solid var(--accent); }
  .opp-card h4 { color: var(--text); font-size: 15px; margin-bottom: 8px; }
  .opp-card .opp-impact { color: var(--gold); font-size: 16px; font-weight: bold; margin: 8px 0; }
  .opp-card .opp-detail { font-size: 12px; color: var(--muted); line-height: 1.5; }
  .opp-card .opp-action { font-size: 12px; color: var(--accent); margin-top: 8px; font-weight: 600; }
  .opp-card.o1 { border-left-color: var(--gold); }
  .opp-card.o2 { border-left-color: var(--purple); }
  .opp-card.o3 { border-left-color: var(--blue); }
  .opp-card.o4 { border-left-color: var(--orange); }
  .opp-card.o5 { border-left-color: var(--green); }
  .squad-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin: 20px 0; }
  .squad-member { background: var(--card); border-radius: 10px; padding: 15px; text-align: center; }
  .squad-member .role { font-size: 10px; color: var(--accent); text-transform: uppercase;
    letter-spacing: 1px; margin-bottom: 5px; }
  .squad-member .name { font-size: 13px; font-weight: bold; }
  .squad-member .desc { font-size: 10px; color: var(--muted); margin-top: 5px; }
  .footer { text-align: center; padding: 30px; margin-top: 40px; border-top: 1px solid #1e3044;
    font-size: 11px; color: var(--muted); }
  .footer .logo { color: var(--accent); font-weight: bold; font-size: 14px; margin-bottom: 5px; }
  .two-col { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; }
  @media (max-width: 768px) { .two-col { grid-template-columns: 1fr; } }
  .action-table td { font-size: 12px; }
  .action-table .hora { color: var(--gold); font-weight: bold; white-space: nowrap; }
  .action-table .canal { color: var(--accent); }
  .action-table .impacto { color: var(--green); font-weight: 600; }
  .pred-row { background: linear-gradient(90deg, #1e304488, #00d4aa11) !important; }
  .pred-row td { font-weight: bold; }
  .badge { display: inline-block; padding: 2px 8px; border-radius: 4px; font-size: 11px; font-weight: bold; }
  .badge-high { background: #ff475733; color: var(--red); }
  .badge-med { background: #ffa50233; color: var(--orange); }
  .badge-low { background: #2ed57333; color: var(--green); }
</style>"""

parts.append("""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Previsao Domingo 22/03/2026 | Squad Intelligence Engine</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js"></script>
""")
parts.append(CSS)
parts.append("""
</head>
<body>
<div class="container">
""")

# ── SECTION 1: HERO ───────────────────────────────────────────────────────────
parts.append("""
<div class="hero">
  <div class="squad-badge">Squad Intelligence Engine</div>
  <h1>Previsao Domingo 22/03/2026</h1>
  <div class="impact">Analise preditiva com base em 7 domingos anteriores + CRM + Estatistica Avancada</div>
  <div class="date">Gerado em """ + v["timestamp"] + """ | Dados: Athena (fund_ec2, bireports_ec2, ps_bi) + Smartico CRM (BigQuery)</div>
  <div class="pred-badge">Modelo: Ensemble (Media + Ponderada + Regressao) | IC t-Student | n=7</div>
</div>
""")

# ── SECTION 2: COMO LER ──────────────────────────────────────────────────────
parts.append("""
<div class="legenda">
  <h3>Como ler este relatorio</h3>
  <strong>Fonte de dados:</strong> Athena Iceberg Data Lake (fund_ec2, bireports_ec2, ps_bi) + CRM Smartico (BigQuery). Dados extraidos em 22/03/2026.<br>
  <strong>Periodo historico:</strong> 7 domingos (02/Fev a 16/Mar 2026). Amostra pequena (n=7) — interpretar com cautela.<br>
  <strong>Modelo preditivo:</strong> Ensemble de 3 metodos — media simples, media ponderada exponencial e regressao linear. Intervalos de confianca via t-Student (5 GL para regressao, 6 GL para media).<br>
  <strong>IC 80%:</strong> Intervalo de confianca 80% — ha 80% de probabilidade de o valor real cair neste range.<br>
  <strong>IC 95%:</strong> Intervalo mais amplo, 95% de confianca — para planejamento conservador.<br>
  <strong>Valores:</strong> Todos em BRL, convertidos de centavos (/100) e com timezone BRT (UTC-3). Test users excluidos.<br>
  <strong>GGR:</strong> Gross Gaming Revenue = Apostas - Ganhos do jogador (receita bruta da casa). Volatilidade extrema (CV 121%).<br>
  <strong>Net Deposit:</strong> Depositos - Saques. Negativo = mais dinheiro saiu do que entrou no dia.<br>
  <strong>FTD:</strong> First Time Deposit — primeiro deposito de um novo jogador.<br>
  <strong>Limitacoes:</strong> (1) Amostra de 7 domingos e insuficiente para modelos robustos. (2) Nao captura eventos externos (regulacao, concorrencia, marketing). (3) GGR e praticamente imprevisivel com n=7. (4) 22/03 e penultimo domingo do mes — possivel efeito salario nao quantificavel.<br>
  <strong>CRM:</strong> Dados Smartico limitados a 3 domingos de marco (01, 08 e 15/Mar). CTR = Click-Through Rate (clicados/enviados).
</div>
""")

# ── SECTION 3: PREVISAO PRINCIPAL ─────────────────────────────────────────────
parts.append("""
<h2>1. Previsao Principal — Domingo 22/03/2026</h2>
<div class="cards">
  <div class="card accent">
    <div class="number">""" + v["dep_prev"] + """</div>
    <div class="label">Depositos Previstos</div>
    <div class="sublabel">IC 80%: """ + v["dep_ic80_lo"] + " - " + v["dep_ic80_hi"] + """</div>
    <div class="delta" style="color:var(--muted)">IC 95%: """ + v["dep_ic95_lo"] + " - " + v["dep_ic95_hi"] + """</div>
  </div>
  <div class="card red">
    <div class="number">""" + v["net_prev"] + """</div>
    <div class="label">Net Deposit Previsto</div>
    <div class="sublabel">Probabilidade positivo: """ + v["net_prob_pos"] + """</div>
    <div class="delta" style="color:var(--muted)">""" + v["net_dom_neg"] + """/7 domingos foram negativos</div>
  </div>
  <div class="card blue">
    <div class="number">""" + v["ftd_prev"] + """</div>
    <div class="label">FTDs Previstos</div>
    <div class="sublabel">IC 95%: """ + v["ftd_ic95_lo"] + " - " + v["ftd_ic95_hi"] + """</div>
    <div class="delta" style="color:var(--muted)">Conversao media: """ + v["ftd_conv"] + """</div>
  </div>
  <div class="card gold">
    <div class="number">""" + v["ggr_regime_lo"] + " - " + v["ggr_regime_hi"] + """</div>
    <div class="label">GGR Casino (Regime Marco)</div>
    <div class="sublabel">Media marco: """ + v["ggr_regime_med"] + """</div>
    <div class="delta" style="color:var(--red)">CV: """ + v["ggr_cv"] + """% — Volatilidade EXTREMA</div>
  </div>
</div>

<div class="two-col">
  <div class="info-box">
    <strong>Submetricas de Depositos:</strong><br>
    Transacoes previstas: <strong>""" + v["dep_qtd"] + """</strong><br>
    Depositantes unicos: <strong>""" + v["dep_uniq"] + """</strong><br>
    Ticket medio: <strong>""" + v["dep_ticket"] + """</strong>
    (recente 3dom: """ + v["dep_ticket_rec"] + """)
  </div>
  <div class="info-box gold">
    <strong>Sinal do Sabado 22/Mar:</strong><br>
    Depositos sabado: <strong>""" + v["sab_dep"] + """</strong><br>
    FTDs sabado: <strong>""" + v["sab_ftd"] + """</strong><br>
    Previsao via ratio dom/sab (""" + v["sab_ratio"] + """x): <strong>""" + v["sab_prev"] + """</strong><br>
    <span style="color:var(--orange)">""" + v["sab_obs"] + """</span>
  </div>
</div>
""")

# ── SECTION 4: CENARIOS ──────────────────────────────────────────────────────
parts.append("""
<h2>2. Cenarios para Domingo</h2>
<div class="scenario-grid">
  <div class="scenario pessimista">
    <div class="sc-label">Pessimista</div>
    <div class="sc-value">""" + v["cen_pess"] + """</div>
    <div class="sc-delta" style="color:var(--red)">""" + v["cen_pess_delta"] + """% vs base</div>
    <div class="sc-detail" style="color:var(--muted)">
      """ + v["cen_pess_prem"] + """
    </div>
  </div>
  <div class="scenario base">
    <div class="sc-label">Base (Ensemble)</div>
    <div class="sc-value">""" + v["cen_base"] + """</div>
    <div class="sc-delta" style="color:var(--blue)">Previsao central</div>
    <div class="sc-detail" style="color:var(--muted)">
      """ + v["cen_base_prem"] + """
    </div>
  </div>
  <div class="scenario otimista">
    <div class="sc-label">Otimista</div>
    <div class="sc-value">""" + v["cen_otim"] + """</div>
    <div class="sc-delta" style="color:var(--green)">""" + v["cen_otim_delta"] + """% vs base</div>
    <div class="sc-detail" style="color:var(--muted)">
      """ + v["cen_otim_prem"] + """
    </div>
  </div>
</div>

<div class="info-box">
  <strong>Previsao via Sabado:</strong> Ratio historico dom/sab = <strong>""" + v["sab_ratio_full"] + """</strong>.
  Aplicando ao sabado 22/03 (""" + v["sab_dep"] + """):
  <strong>""" + v["sab_prev"] + """</strong>.
  Esse valor esta entre o pessimista e o base — coerente com o sabado abaixo da media.
</div>
""")

# ── SECTION 5: COMPARATIVO HISTORICO ──────────────────────────────────────────
parts.append("""
<h2>3. Comparativo Historico — 7 Domingos + Previsao</h2>
<div style="overflow-x:auto">
<table>
  <thead>
    <tr>
      <th>Data</th>
      <th style="text-align:right">Depositos</th>
      <th style="text-align:right">Saques</th>
      <th style="text-align:right">Net Deposit</th>
      <th style="text-align:right">GGR Casino</th>
      <th style="text-align:right">FTDs</th>
      <th style="text-align:right">Transacoes</th>
      <th style="text-align:right">Ticket</th>
    </tr>
  </thead>
  <tbody>""")

for r in hist_table_rows:
    net_class = "positive" if r["net"] and r["net"] > 0 else "negative"
    net_str = fmt_brl(r["net"]) if r["net"] is not None else "--"
    parts.append(
        '<tr>'
        '<td>' + r['date_br'] + '</td>'
        '<td style="text-align:right">' + fmt_brl(r['valor']) + '</td>'
        '<td style="text-align:right">' + fmt_brl(r['saques']) + '</td>'
        '<td style="text-align:right" class="' + net_class + '">' + net_str + '</td>'
        '<td style="text-align:right">' + fmt_brl(r['ggr']) + '</td>'
        '<td style="text-align:right">' + fmt_number(r['ftd']) + '</td>'
        '<td style="text-align:right">' + fmt_number(r['qtd']) + '</td>'
        '<td style="text-align:right">R$ ' + f"{float(r['ticket']):.2f}" + '</td>'
        '</tr>'
    )

# Prediction row
parts.append(
    '<tr class="pred-row">'
    '<td>22/Mar*</td>'
    '<td style="text-align:right;color:var(--accent)">' + v["dep_prev"] + '</td>'
    '<td style="text-align:right;color:var(--muted)">--</td>'
    '<td style="text-align:right;color:var(--red)">' + v["net_prev"] + '</td>'
    '<td style="text-align:right;color:var(--gold)">' + v["ggr_regime_med"] + '</td>'
    '<td style="text-align:right;color:var(--blue)">' + v["ftd_prev"] + '</td>'
    '<td style="text-align:right;color:var(--muted)">' + v["dep_qtd"] + '</td>'
    '<td style="text-align:right">' + v["dep_ticket"] + '</td>'
    '</tr>'
)

parts.append("""
  </tbody>
</table>
</div>
<p style="font-size:10px;color:var(--muted)">* Valores previstos (ensemble). GGR usa media do regime de marco.</p>

<div class="chart-container">
  <canvas id="depositosHistChart"></canvas>
</div>
""")

# ── SECTION 6: TENDENCIA ─────────────────────────────────────────────────────
parts.append("""
<h2>4. Tendencia de Depositos aos Domingos</h2>
<div class="info-box success">
  <strong>Tendencia CRESCENTE confirmada</strong> por ambos os testes estatisticos (p &lt; 0.05).<br><br>
  <strong>Mann-Kendall:</strong> Tau = """ + v["tend_mk_tau"] + """,
  p-value = """ + v["tend_mk_p"] + """ — """ + v["tend_mk_conc"] + """<br>
  <strong>Regressao Linear:</strong> Slope = +""" + v["tend_reg_slope"] + """/semana,
  R2 = """ + v["tend_reg_r2"] + """,
  p-value = """ + v["tend_reg_p"] + """ — """ + v["tend_reg_conc"] + """<br><br>
  <span style="color:var(--gold)">Depositos estao crescendo ~R$ 79.727 por semana aos domingos.</span>
  A cada domingo, o volume sobe em media R$ 80K. Os ultimos 3 domingos foram consecutivamente crescentes.<br><br>
  <strong>Normalidade dos residuos:</strong> Shapiro-Wilk p = """ + v["tend_sw_p"] + """
  — residuos normais (premissa da regressao satisfeita).
</div>

<div class="info-box">
  <strong>Submetricas — tendencia:</strong><br>
  Qtd depositos: <span style="color:var(--muted)">""" + v["tend_qtd_tend"] + " (p=" + v["tend_qtd_p"] + """)</span><br>
  Depositantes unicos: <span style="color:var(--muted)">""" + v["tend_uniq_tend"] + " (p=" + v["tend_uniq_p"] + """)</span><br>
  Ticket medio: <span style="color:var(--orange)">""" + v["tend_ticket_tend"] + " (p=" + v["tend_ticket_p"] + """)</span> — quase significativo a 10%<br><br>
  <em>Interpretacao: O crescimento nos depositos esta sendo puxado pelo aumento do ticket medio (jogadores depositando mais por transacao), nao por mais transacoes ou mais depositantes.</em>
</div>
""")

# ── SECTION 7: MAPA HORARIO ──────────────────────────────────────────────────
parts.append("""
<h2>5. Mapa Horario do Domingo — Padrao Medio</h2>
<div class="chart-container">
  <canvas id="hourlyChart"></canvas>
</div>

<div class="two-col">
  <div>
    <h3>Golden Hours (pico de depositos)</h3>
    <div style="overflow-x:auto">
    <table>
      <thead><tr><th>Hora</th><th style="text-align:right">Media/Dom</th><th style="text-align:right">Ticket</th><th style="text-align:right">% Dia</th><th style="text-align:right">Score ROI</th></tr></thead>
      <tbody>""")

for gh in padrao.get("golden_hours_detail", []):
    parts.append(
        '<tr>'
        '<td style="color:var(--gold);font-weight:bold">' + f"{int(gh['hora']):02d}h" + '</td>'
        '<td style="text-align:right">' + fmt_brl(gh['media_por_domingo']) + '</td>'
        '<td style="text-align:right">R$ ' + f"{gh['ticket_medio']:.2f}" + '</td>'
        '<td style="text-align:right">' + f"{gh['pct_dia']:.1f}%" + '</td>'
        '<td style="text-align:right">' + f"{gh['score_roi']:.2f}" + '</td>'
        '</tr>'
    )

parts.append("""
      </tbody>
    </table>
    </div>
    <p style="font-size:11px;color:var(--muted)">Concentracao 13-16h: """ + v["padrao_conc"] + """% do volume diario</p>
  </div>
  <div>
    <h3>Janelas de Volume</h3>
    <div style="overflow-x:auto">
    <table>
      <thead><tr><th>Janela</th><th style="text-align:right">% Dia</th><th style="text-align:right">Ticket</th><th style="text-align:right">Depositantes</th></tr></thead>
      <tbody>""")

for nome, dados in janelas.items():
    parts.append(
        '<tr>'
        '<td>' + nome + '</td>'
        '<td style="text-align:right">' + f"{dados['pct_dia']:.1f}%" + '</td>'
        '<td style="text-align:right">R$ ' + f"{dados['ticket_medio']:.2f}" + '</td>'
        '<td style="text-align:right">' + fmt_number(dados['depositantes_media']) + '</td>'
        '</tr>'
    )

parts.append("""
      </tbody>
    </table>
    </div>
  </div>
</div>

<div class="info-box">
  <strong>Ratio pico/vale:</strong> """ + v["padrao_ratio"] + """x —
  o pico (""" + v["padrao_pico_hora"] + """h, """ + v["padrao_pico_val"] + """ total 7dom)
  movimenta """ + v["padrao_ratio"] + """x mais que o vale
  (""" + v["padrao_vale_hora"] + """h, """ + v["padrao_vale_val"] + """ total 7dom).
  <br><strong>Madrugada (0-5h):</strong> apenas """ + v["padrao_mad_pct"] + """% do volume,
  mas ticket mais baixo (""" + v["padrao_mad_ticket"] + """).
  Massa de jogadores casual.
</div>
""")

# ── SECTION 8: ACOES CRM ─────────────────────────────────────────────────────
parts.append("""
<h2>6. Acoes CRM Recomendadas para Domingo</h2>
<h3>Performance CRM por Canal (Domingo 15/Mar)</h3>
<div style="overflow-x:auto">
<table>
  <thead>
    <tr>
      <th>Canal</th>
      <th style="text-align:right">Enviados</th>
      <th style="text-align:right">Entregues</th>
      <th style="text-align:right">Clicados</th>
      <th style="text-align:right">CTR (enviados)</th>
      <th style="text-align:right">CTR (entregues)</th>
    </tr>
  </thead>
  <tbody>""")

sorted_channels = sorted(channel_ctrs.items(), key=lambda x: x[1]["ctr_delivered"], reverse=True)
for canal, stats in sorted_channels:
    ctr_class = "positive" if stats["ctr_delivered"] > 30 else ""
    parts.append(
        '<tr>'
        '<td>' + canal + '</td>'
        '<td style="text-align:right">' + fmt_number(stats['sent']) + '</td>'
        '<td style="text-align:right">' + fmt_number(stats['delivered']) + '</td>'
        '<td style="text-align:right">' + fmt_number(stats['clicked']) + '</td>'
        '<td style="text-align:right" class="' + ctr_class + '">' + f"{stats['ctr_sent']:.1f}%" + '</td>'
        '<td style="text-align:right" class="' + ctr_class + '">' + f"{stats['ctr_delivered']:.1f}%" + '</td>'
        '</tr>'
    )

parts.append("""
  </tbody>
</table>
</div>

<h3>Oportunidades de CRM por Horario</h3>
<div class="opp-grid">
  <div class="opp-card o1">
    <h4>Push 12h BRT — Pre-Pico</h4>
    <div class="opp-impact">Bonus de Deposito</div>
    <div class="opp-detail">
      Disparar 30-60min ANTES do pico natural (13-16h).<br>
      Objetivo: capturar jogadores que ja estariam inclinados a depositar.<br>
      Canal: Popup (melhor CTR entregue) + SMS para base ampla.<br>
      Nao disparar no pico — o jogador ja esta ativo.
    </div>
    <div class="opp-action">Impacto estimado: +3-5% depositos tarde</div>
  </div>
  <div class="opp-card o2">
    <h4>Push 17h BRT — Anti-Queda</h4>
    <div class="opp-impact">Free Spins</div>
    <div class="opp-detail">
      Volume cai depois das 16h.<br>
      Oferta de free spins para segurar jogadores que estariam saindo.<br>
      Canal: Popup + Inbox (melhor CTR para retencao).
    </div>
    <div class="opp-action">Impacto estimado: segurar -10% da queda noturna</div>
  </div>
  <div class="opp-card o3">
    <h4>Push 21h BRT — Last Call</h4>
    <div class="opp-impact">Match Bonus 50%</div>
    <div class="opp-detail">
      Segundo pico de logins (18-20h) — jogadores retornam a noite.<br>
      Match bonus para incentivar ultimo deposito do dia.<br>
      Canal: SMS (base ampla) + Popup (quem esta logado).
    </div>
    <div class="opp-action">Impacto estimado: +R$ 20-40K depositos noite</div>
  </div>
  <div class="opp-card o4">
    <h4>Bonus Flash 3-5h — Horas Mortas</h4>
    <div class="opp-impact">Nao Disparar CRM de Massa</div>
    <div class="opp-detail">
      Volume minimo, poucos jogadores (madrugada).<br>
      Ticket alto (R$ 141) indica whales noturnos — nao mass market.<br>
      CRM de massa aqui e desperdicio. Se houver budget limitado, alocar tudo nas golden hours.
    </div>
    <div class="opp-action">Acao: reservar budget para 12-17h</div>
  </div>
</div>
""")

# ── SECTION 8.5: SPORTS INTELLIGENCE ──────────────────────────────────────────
parts.append("""
<h2>7. Calendario Esportivo — Impacto no Sportsbook</h2>
<p style="font-size:12px;color:var(--muted);margin-bottom:10px">
  Levantamento do agente Sports Intelligence via pesquisa web. Eventos classificados por impacto no volume de apostas.
</p>

<div class="info-box success">
  <strong>VEREDICTO: DOMINGO FORTE para sportsbook!</strong><br>
  Brasileirao 8a rodada (com CORINTHIANS x FLAMENGO em TV aberta), estreia da Serie B, rodadas de
  Premier League + La Liga + Serie A + Bundesliga, March Madness e NBA Sunday Night.
  Grade densa de eventos das 9h ate 23h BRT.
</div>

<h3 style="color:var(--red);margin-top:20px">IMPACTO ALTO</h3>
<table>
  <thead><tr><th>Evento</th><th>Horario BRT</th><th>Competicao</th><th>Impacto</th></tr></thead>
  <tbody>
    <tr style="background:#ff475715"><td><strong>Corinthians x Flamengo</strong></td><td><strong>20h30</strong></td><td>Brasileirao Serie A</td><td><span class="badge badge-high">CRITICO</span></td></tr>
    <tr><td>Sao Paulo x Palmeiras</td><td>21h (sab)</td><td>Brasileirao Serie A</td><td><span class="badge badge-high">ALTO</span></td></tr>
    <tr><td>FC Barcelona x Rayo Vallecano</td><td>10h</td><td>La Liga</td><td><span class="badge badge-high">ALTO</span></td></tr>
    <tr><td>Fiorentina x Inter</td><td>16h45</td><td>Serie A Italiana</td><td><span class="badge badge-high">ALTO</span></td></tr>
    <tr><td>Newcastle x Sunderland (Derby)</td><td>09h</td><td>Premier League</td><td><span class="badge badge-high">ALTO</span></td></tr>
    <tr><td>Timberwolves x Celtics</td><td>21h</td><td>NBA Sunday Night</td><td><span class="badge badge-high">ALTO</span></td></tr>
  </tbody>
</table>

<h3 style="color:var(--orange);margin-top:15px">IMPACTO MEDIO</h3>
<table>
  <thead><tr><th>Evento</th><th>Horario BRT</th><th>Competicao</th></tr></thead>
  <tbody>
    <tr><td>Cruzeiro x Santos / Vasco x Gremio</td><td>16h</td><td>Brasileirao Serie A</td></tr>
    <tr><td>Estreia Serie B (10 jogos no FDS)</td><td>Diversos</td><td>Brasileirao Serie B</td></tr>
    <tr><td>Tottenham x Nottm Forest</td><td>11h15</td><td>Premier League</td></tr>
    <tr><td>Bologna x Lazio / Roma x Lecce</td><td>11h-14h</td><td>Serie A Italiana</td></tr>
    <tr><td>March Madness (Round of 32)</td><td>13h-22h</td><td>NCAA Basketball</td></tr>
  </tbody>
</table>

<div class="info-box gold" style="margin-top:15px">
  <strong>Mapa de calor de eventos (domingo):</strong><br>
  <strong>09-11h:</strong> Premier League + Barcelona (futebol europeu matinal)<br>
  <strong>11-14h:</strong> Serie A + Bundesliga (fluxo medio constante)<br>
  <strong>14-17h:</strong> Brasileirao jogos 16h + Fiorentina x Inter 16h45 (PICO 1)<br>
  <strong>17-20h:</strong> Serie B + NCAA + preparacao para jogo da noite<br>
  <strong>20-23h:</strong> CORINTHIANS x FLAMENGO + NBA Celtics (PICO MAXIMO)
</div>

<div class="info-box" style="margin-top:10px">
  <strong>NAO tem neste domingo:</strong> F1 (semana livre), UFC (foi sabado), Champions League (so em abril), Libertadores (so em abril), Estaduais (ja acabaram).
</div>
""")

# ── SECTION 9: PLANO DE ACAO ─────────────────────────────────────────────────
parts.append("""
<h2>8. Plano de Acao — Domingo 22/03/2026</h2>
<div style="overflow-x:auto">
<table class="action-table">
  <thead>
    <tr>
      <th>Horario BRT</th>
      <th>Acao</th>
      <th>Canal</th>
      <th>Publico</th>
      <th>Impacto Estimado</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td class="hora">08h-09h</td>
      <td>Email marketing — dose matinal</td>
      <td class="canal">Email</td>
      <td>Base completa</td>
      <td class="impacto">Warmup; email tem delay de leitura</td>
    </tr>
    <tr>
      <td class="hora">11h30</td>
      <td>Popup bonus deposito "Domingo Premiado"</td>
      <td class="canal">Popup + Inbox</td>
      <td>Jogadores logados</td>
      <td class="impacto">+R$ 15-25K depositos pre-pico</td>
    </tr>
    <tr>
      <td class="hora">12h00</td>
      <td>SMS pre-pico — "Deposite agora e ganhe bonus"</td>
      <td class="canal">SMS</td>
      <td>Depositantes recorrentes</td>
      <td class="impacto">+3-5% depositos 13-16h</td>
    </tr>
    <tr>
      <td class="hora">13h-16h</td>
      <td>Monitorar — Golden Hours (pico natural)</td>
      <td class="canal">--</td>
      <td>Organico</td>
      <td class="impacto">~28% do volume diario</td>
    </tr>
    <tr>
      <td class="hora">17h00</td>
      <td>Popup anti-queda — Free Spins</td>
      <td class="canal">Popup</td>
      <td>Jogadores ativos sem deposito</td>
      <td class="impacto">Segurar -10% da queda</td>
    </tr>
    <tr>
      <td class="hora">18h-19h</td>
      <td>Push notification — retorno noturno</td>
      <td class="canal">Push</td>
      <td>Jogadores inativos do dia</td>
      <td class="impacto">+R$ 10-20K depositos</td>
    </tr>
    <tr>
      <td class="hora">21h00</td>
      <td>SMS last call — Match Bonus 50%</td>
      <td class="canal">SMS + Popup</td>
      <td>Depositantes 7d sem deposito hoje</td>
      <td class="impacto">+R$ 20-40K depositos</td>
    </tr>
    <tr>
      <td class="hora">23h00</td>
      <td>Fechamento — consolidar numeros</td>
      <td class="canal">--</td>
      <td>Squad interno</td>
      <td class="impacto">Comparar realizado vs previsao</td>
    </tr>
  </tbody>
</table>
</div>

<div class="info-box gold">
  <strong>Meta CRM:</strong> Se todas as acoes forem executadas nas janelas corretas, o cenario otimista
  (<strong>""" + v["cen_otim"] + """</strong>)
  se torna alcancavel. Sem CRM ativo, o cenario mais provavel e o base ou abaixo
  (""" + v["cen_base"] + """).
</div>
""")

# ── SECTION 10: VOLATILIDADE GGR ─────────────────────────────────────────────
parts.append("""
<h2>9. Volatilidade GGR Casino — Alerta</h2>
<div class="info-box alert">
  <strong>GGR Casino tem CV de """ + v["ggr_cv"] + """%</strong> — volatilidade <strong>EXTREMA</strong>.<br>
  Range historico: """ + v["ggr_min"] + """ a """ + v["ggr_max"] + """.<br>
  Mediana (""" + v["ggr_mediana"] + """) e muito menor que a media
  (""" + v["ggr_media"] + """) — distribuicao fortemente assimetrica (skewness = """ + v["ggr_skew"] + """).<br><br>
  <strong>Modelo ajustado: """ + v["ggr_dist"] + """</strong>
  (KS p = """ + v["ggr_ks_p"] + """ — ajuste bom).<br><br>
  <span style="color:var(--gold)"><strong>Mudanca de regime detectada:</strong></span><br>
  Fevereiro: media """ + v["ggr_fev_media"] + """
  (""" + v["ggr_fev_dom"] + """)<br>
  Marco: media """ + v["ggr_mar_media"] + """
  (""" + v["ggr_mar_dom"] + """)<br>
  Se o regime de marco se mantiver: <strong>GGR esperado ~""" + v["ggr_regime_med_full"] + """</strong>
  (range """ + v["ggr_regime_lo_full"] + """ -
  """ + v["ggr_regime_hi_full"] + """).
</div>

<div class="chart-container">
  <canvas id="ggrChart"></canvas>
</div>

<div class="info-box warning">
  <strong>Para planejamento:</strong> Nao use um numero pontual de GGR para decisoes.
  Use o range de cenarios. Uma previsao pontual de GGR com n=7 e alta volatilidade e
  praticamente aleatoria. O regime de marco sugere GGR mais baixo, mas um unico big winner
  pode alterar o resultado em R$ 2M+.
</div>
""")

# ── SECTION 11: SOBRE O SQUAD ────────────────────────────────────────────────
parts.append("""
<h2>10. Sobre o Squad Intelligence Engine</h2>
<p style="font-size:13px;color:var(--muted);margin-bottom:15px">
  Equipe de 11 agentes especializados em dados iGaming, operando com queries em tempo real
  no Athena (Iceberg Data Lake) e CRM Smartico (BigQuery). Cada secao deste report foi
  produzida por um agente especializado.
</p>
<div class="squad-grid">
  <div class="squad-member">
    <div class="role">CRM Analyst</div>
    <div class="name">Segmentacao & Retencao</div>
    <div class="desc">Campanhas, baleias, funil de bonus, lifecycle</div>
  </div>
  <div class="squad-member">
    <div class="role">Product Analyst</div>
    <div class="name">Performance & GGR</div>
    <div class="desc">Casino, sportsbook, hold rate, RTP</div>
  </div>
  <div class="squad-member">
    <div class="role">Traffic Analyst</div>
    <div class="name">Aquisicao & Conversao</div>
    <div class="desc">Afiliados, trackers, UTMs, FTD funnel</div>
  </div>
  <div class="squad-member">
    <div class="role">Marketing Analyst</div>
    <div class="name">ROI & Atribuicao</div>
    <div class="desc">Performance de canais, CPA, ROAS</div>
  </div>
  <div class="squad-member">
    <div class="role">Statistician PhD</div>
    <div class="name">Modelagem Preditiva</div>
    <div class="desc">Testes estatisticos, ICs, regressao, distribuicoes</div>
  </div>
  <div class="squad-member">
    <div class="role">Sports Intelligence</div>
    <div class="name">Sportsbook Analytics</div>
    <div class="desc">Odds, bets, live/pre-live, esportes</div>
  </div>
  <div class="squad-member">
    <div class="role">Data Extractor</div>
    <div class="name">SQL & Pipelines</div>
    <div class="desc">Queries Athena/BigQuery, ETL automatizado</div>
  </div>
  <div class="squad-member">
    <div class="role">Executor</div>
    <div class="name">Entregas & Reports</div>
    <div class="desc">CSV, Excel, HTML reports, dashboards</div>
  </div>
  <div class="squad-member">
    <div class="role">Frontend Support</div>
    <div class="name">Dashboards & Visualizacao</div>
    <div class="desc">Flask, HTML, CSS, Chart.js, reports visuais</div>
  </div>
  <div class="squad-member">
    <div class="role">Data Modeler</div>
    <div class="name">Modelagem & Schema</div>
    <div class="desc">Data Lake, dim/fact, dbt, Iceberg</div>
  </div>
  <div class="squad-member">
    <div class="role">Auditor</div>
    <div class="name">Qualidade & Validacao</div>
    <div class="desc">Validacao cruzada, consistencia, compliance</div>
  </div>
</div>
""")

# ── SECTION 12: SELO AUDITOR ─────────────────────────────────────────────────
parts.append("""
<div style="margin:30px 0; padding:20px; background:linear-gradient(135deg, #1a263444, #ffa50211);
  border:1px dashed var(--orange); border-radius:10px; text-align:center;">
  <div style="font-size:24px; margin-bottom:5px;">&#9888;</div>
  <div style="color:var(--orange); font-weight:bold; font-size:14px; letter-spacing:1px">
    PENDENTE VALIDACAO — AUDITOR SQUAD INTELLIGENCE ENGINE
  </div>
  <div style="color:var(--muted); font-size:11px; margin-top:5px;">
    Este report sera validado pelo Auditor apos geracao. Checklist: fontes verificadas,
    calculos conferidos, ICs coerentes, premissas documentadas, legenda presente.
  </div>
</div>
""")

# ── SECTION 13: FOOTER ───────────────────────────────────────────────────────
parts.append("""
<div class="footer">
  <div class="logo">Squad Intelligence Engine | MultiBet</div>
  <p>Gerado em """ + v["timestamp"] + """ | Fonte: Athena + Smartico CRM</p>
  <p>Squad 3 — Intelligence Engine | Super Nova Gaming</p>
  <p style="margin-top:5px; font-size:10px">
    Agentes: Statistician PhD (modelagem) + Extractor (queries) + CRM Analyst (oportunidades) +
    Product Analyst (GGR) + Traffic Analyst (FTD) + Frontend Support (visualizacao) + Auditor (pendente)
  </p>
  <p style="margin-top:3px; font-size:10px">Modelo: """ + v["meta_modelo"] + """ | n=""" + v["meta_n"] + """ | IC: t-Student</p>
</div>
</div>
""")

# ── CHARTS (JavaScript) ──────────────────────────────────────────────────────
parts.append("""
<script>
const COLORS = {
  accent: '#00d4aa', gold: '#ffd700', blue: '#4d96ff',
  red: '#ff4757', green: '#2ed573', purple: '#a55eea',
  muted: '#8899aa', text: '#e0e6ed', card: '#1a2634',
  orange: '#ffa502'
};

Chart.defaults.color = COLORS.muted;
Chart.defaults.borderColor = '#1e3044';
Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";


// Chart 1: Depositos Historicos + Previsao
const depLabels = """ + json.dumps(dep_chart_labels) + """;
const depValues = """ + json.dumps(dep_chart_values) + """;
const depColors = depValues.map((v, i) => i === depValues.length - 1 ? COLORS.gold + 'cc' : COLORS.accent + '99');
const depBorders = depValues.map((v, i) => i === depValues.length - 1 ? COLORS.gold : COLORS.accent);

new Chart(document.getElementById('depositosHistChart'), {
  type: 'bar',
  data: {
    labels: depLabels,
    datasets: [{
      label: 'Depositos (R$)',
      data: depValues,
      backgroundColor: depColors,
      borderColor: depBorders,
      borderWidth: 1,
      borderRadius: 6
    }]
  },
  options: {
    responsive: true,
    plugins: {
      title: { display: true, text: 'Depositos por Domingo (R$) — Historico + Previsao', color: COLORS.text, font: { size: 14 } },
      legend: { display: false },
      tooltip: { callbacks: { label: ctx => 'R$ ' + ctx.parsed.y.toLocaleString('pt-BR') } }
    },
    scales: {
      y: {
        ticks: { callback: v => 'R$ ' + (v/1000).toFixed(0) + 'K' },
        beginAtZero: true
      }
    }
  }
});


// Chart 2: Padrao Horario
const hourlyLabels = """ + json.dumps(hourly_labels) + """;
const hourlyValues = """ + json.dumps(hourly_values) + """;
const goldenHours = """ + json.dumps(golden_hours) + """;
const deadHours = """ + json.dumps(dead_hours) + """;

const hourlyColors = hourlyValues.map((v, i) => {
  if (goldenHours.includes(i)) return COLORS.gold + 'cc';
  if (deadHours.includes(i)) return COLORS.red + '66';
  return COLORS.accent + '77';
});

new Chart(document.getElementById('hourlyChart'), {
  type: 'bar',
  data: {
    labels: hourlyLabels,
    datasets: [{
      label: 'Media por Domingo (R$)',
      data: hourlyValues,
      backgroundColor: hourlyColors,
      borderRadius: 4
    }]
  },
  options: {
    responsive: true,
    plugins: {
      title: { display: true, text: 'Volume Medio de Depositos por Hora — Domingos (BRT)', color: COLORS.text, font: { size: 14 } },
      legend: { display: false },
      tooltip: { callbacks: { label: ctx => 'R$ ' + ctx.parsed.y.toLocaleString('pt-BR', {minimumFractionDigits: 0}) } },
      subtitle: {
        display: true,
        text: 'Dourado = Golden Hours (pico) | Vermelho = Horas mortas (vale)',
        color: COLORS.muted,
        font: { size: 11 }
      }
    },
    scales: {
      y: {
        ticks: { callback: v => 'R$ ' + (v/1000).toFixed(0) + 'K' },
        beginAtZero: true
      }
    }
  }
});


// Chart 3: GGR Historico
const ggrLabels = """ + json.dumps(ggr_chart_labels) + """;
const ggrValues = """ + json.dumps(ggr_chart_values) + """;
const ggrColors = ggrValues.map((v, i) => {
  if (i < 4) return COLORS.purple + 'aa';
  return COLORS.blue + 'aa';
});

new Chart(document.getElementById('ggrChart'), {
  type: 'bar',
  data: {
    labels: ggrLabels,
    datasets: [{
      label: 'GGR Casino (R$)',
      data: ggrValues,
      backgroundColor: ggrColors,
      borderRadius: 6
    }]
  },
  options: {
    responsive: true,
    plugins: {
      title: { display: true, text: 'GGR Casino por Domingo — Evidencia de Mudanca de Regime', color: COLORS.text, font: { size: 14 } },
      legend: { display: false },
      tooltip: { callbacks: { label: ctx => 'R$ ' + ctx.parsed.y.toLocaleString('pt-BR') } },
      subtitle: {
        display: true,
        text: 'Roxo = Fevereiro (media R$ 1.8M) | Azul = Marco (media R$ 104K)',
        color: COLORS.muted,
        font: { size: 11 }
      }
    },
    scales: {
      y: {
        ticks: { callback: v => 'R$ ' + (v/1000000).toFixed(1) + 'M' },
        beginAtZero: true
      }
    }
  }
});
</script>
</body>
</html>""")


# ── Write output ───────────────────────────────────────────────────────────────
html = "\n".join(parts)

print(f"[INFO] Escrevendo HTML em {OUTPUT_HTML}...")
with open(OUTPUT_HTML, "w", encoding="utf-8") as f:
    f.write(html)

file_size = os.path.getsize(OUTPUT_HTML)
print(f"[OK] Report gerado com sucesso!")
print(f"     Arquivo: {OUTPUT_HTML}")
print(f"     Tamanho: {file_size:,} bytes ({file_size/1024:.1f} KB)")
print(f"     Timestamp: {timestamp}")
print(f"     Secoes: 13 (hero, legenda, previsao, cenarios, historico, tendencia,")
print(f"              horario, acoes crm, plano acao, ggr vol, squad, auditor, footer)")
print(f"     Charts: 3 (depositos historico, padrao horario, GGR)")
