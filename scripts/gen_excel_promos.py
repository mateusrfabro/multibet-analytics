"""Gera Excel final com dados ja obtidos do Athena + validacao BigQuery."""
import pandas as pd
import os

# ============ DADOS OBTIDOS DO ATHENA (run 15:24) ============
period_summaries = {
    "P1_before": {"uap": 1570, "turnover": 308612.60},
    "P2_before": {"uap": 578,  "turnover": 484989.50},
    "P3_before": {"uap": 384,  "turnover": 112585.25},
    "P4_before": {"uap": 158,  "turnover": 8579.25},
    "P5_before": {"uap": 136,  "turnover": 16366.00},
    "P6_before": {"uap": 2326, "turnover": 1112641.40},
    "P1_during": {"uap": 1740, "turnover": 358756.80},
    "P2_during": {"uap": 841,  "turnover": 312372.50},
    "P3_during": {"uap": 618,  "turnover": 2322918.45},
    "P4_during": {"uap": 383,  "turnover": 33056.60},
    "P5_during": {"uap": 454,  "turnover": 85376.50},
    "P6_during": {"uap": 3309, "turnover": 1216975.80},
    "P1_after":  {"uap": 1650, "turnover": 406700.80},
    "P2_after":  {"uap": 760,  "turnover": 268159.00},
    "P3_after":  {"uap": 380,  "turnover": 78714.80},
    "P4_after":  {"uap": 155,  "turnover": 20895.25},
    "P5_after":  {"uap": 355,  "turnover": 43650.50},
    "P6_after":  {"uap": 2876, "turnover": 783179.50},
}

# Tier distributions por promo
tiers = {
    "P1": pd.DataFrame([
        {"Faixa": "Gire entre R$50 a R$199",   "Qtd Usuarios": 464, "Turnover Total (R$)": 46381.20, "% Usuarios": 26.7, "% Turnover": 12.9},
        {"Faixa": "Gire entre R$200 a R$499",  "Qtd Usuarios": 166, "Turnover Total (R$)": 51948.20, "% Usuarios": 9.5,  "% Turnover": 14.5},
        {"Faixa": "Gire entre R$500 a R$999",  "Qtd Usuarios": 71,  "Turnover Total (R$)": 49221.20, "% Usuarios": 4.1,  "% Turnover": 13.7},
        {"Faixa": "Gire R$1.000 ou mais",       "Qtd Usuarios": 61,  "Turnover Total (R$)": 193453.80,"% Usuarios": 3.5,  "% Turnover": 53.9},
        {"Faixa": "Abaixo de R$50",              "Qtd Usuarios": 978, "Turnover Total (R$)": 17752.40, "% Usuarios": 56.2, "% Turnover": 4.9},
    ]),
    "P2": pd.DataFrame([
        {"Faixa": "Gire entre R$30 a R$99",    "Qtd Usuarios": 240, "Turnover Total (R$)": 14273.50, "% Usuarios": 28.5, "% Turnover": 4.6},
        {"Faixa": "Gire entre R$100 a R$299",  "Qtd Usuarios": 143, "Turnover Total (R$)": 24321.50, "% Usuarios": 17.0, "% Turnover": 7.8},
        {"Faixa": "Gire entre R$300 a R$599",  "Qtd Usuarios": 50,  "Turnover Total (R$)": 21437.00, "% Usuarios": 5.9,  "% Turnover": 6.9},
        {"Faixa": "Gire R$600 ou mais",          "Qtd Usuarios": 79,  "Turnover Total (R$)": 248233.00,"% Usuarios": 9.4,  "% Turnover": 79.5},
        {"Faixa": "Abaixo de R$30",              "Qtd Usuarios": 329, "Turnover Total (R$)": 4107.50,  "% Usuarios": 39.1, "% Turnover": 1.3},
    ]),
    "P3": pd.DataFrame([
        {"Faixa": "Gire entre R$50 a R$99",    "Qtd Usuarios": 70,  "Turnover Total (R$)": 4954.75,    "% Usuarios": 11.3, "% Turnover": 0.2},
        {"Faixa": "Gire entre R$100 a R$299",  "Qtd Usuarios": 83,  "Turnover Total (R$)": 14256.95,   "% Usuarios": 13.4, "% Turnover": 0.6},
        {"Faixa": "Gire entre R$300 a R$499",  "Qtd Usuarios": 20,  "Turnover Total (R$)": 7625.65,    "% Usuarios": 3.2,  "% Turnover": 0.3},
        {"Faixa": "Gire R$500 ou mais",          "Qtd Usuarios": 40,  "Turnover Total (R$)": 2291197.65, "% Usuarios": 6.5,  "% Turnover": 98.6},
        {"Faixa": "Abaixo de R$50",              "Qtd Usuarios": 405, "Turnover Total (R$)": 4883.45,    "% Usuarios": 65.5, "% Turnover": 0.2},
    ]),
    "P4": pd.DataFrame([
        {"Faixa": "Gire entre R$15 e R$49",    "Qtd Usuarios": 87,  "Turnover Total (R$)": 2447.80,  "% Usuarios": 22.7, "% Turnover": 7.4},
        {"Faixa": "Gire entre R$50 a R$99",    "Qtd Usuarios": 55,  "Turnover Total (R$)": 3858.95,  "% Usuarios": 14.4, "% Turnover": 11.7},
        {"Faixa": "Gire entre R$100 a R$299",  "Qtd Usuarios": 63,  "Turnover Total (R$)": 9825.10,  "% Usuarios": 16.4, "% Turnover": 29.7},
        {"Faixa": "Gire R$300 ou mais",          "Qtd Usuarios": 30,  "Turnover Total (R$)": 16172.80, "% Usuarios": 7.8,  "% Turnover": 48.9},
        {"Faixa": "Abaixo de R$15",              "Qtd Usuarios": 148, "Turnover Total (R$)": 751.95,   "% Usuarios": 38.6, "% Turnover": 2.3},
    ]),
    "P5": pd.DataFrame([
        {"Faixa": "Gire entre R$30 a R$99",    "Qtd Usuarios": 128, "Turnover Total (R$)": 7137.00,  "% Usuarios": 28.2, "% Turnover": 8.4},
        {"Faixa": "Gire entre R$100 a R$299",  "Qtd Usuarios": 78,  "Turnover Total (R$)": 12321.00, "% Usuarios": 17.2, "% Turnover": 14.4},
        {"Faixa": "Gire entre R$300 a R$599",  "Qtd Usuarios": 17,  "Turnover Total (R$)": 6924.50,  "% Usuarios": 3.7,  "% Turnover": 8.1},
        {"Faixa": "Gire R$600 ou mais",          "Qtd Usuarios": 24,  "Turnover Total (R$)": 56682.50, "% Usuarios": 5.3,  "% Turnover": 66.4},
        {"Faixa": "Abaixo de R$30",              "Qtd Usuarios": 207, "Turnover Total (R$)": 2311.50,  "% Usuarios": 45.6, "% Turnover": 2.7},
    ]),
    "P6": pd.DataFrame([
        {"Faixa": "Gire entre R$50 a R$199",   "Qtd Usuarios": 861, "Turnover Total (R$)": 86961.60,  "% Usuarios": 26.0, "% Turnover": 7.1},
        {"Faixa": "Gire entre R$200 a R$399",  "Qtd Usuarios": 272, "Turnover Total (R$)": 76842.40,  "% Usuarios": 8.2,  "% Turnover": 6.3},
        {"Faixa": "Gire entre R$400 a R$799",  "Qtd Usuarios": 129, "Turnover Total (R$)": 72659.80,  "% Usuarios": 3.9,  "% Turnover": 6.0},
        {"Faixa": "Gire entre R$800 a R$999",  "Qtd Usuarios": 37,  "Turnover Total (R$)": 33001.80,  "% Usuarios": 1.1,  "% Turnover": 2.7},
        {"Faixa": "Gire R$1.000 ou mais",       "Qtd Usuarios": 140, "Turnover Total (R$)": 915560.20, "% Usuarios": 4.2,  "% Turnover": 75.2},
        {"Faixa": "Abaixo de R$50",              "Qtd Usuarios": 1870,"Turnover Total (R$)": 31950.00,  "% Usuarios": 56.5, "% Turnover": 2.6},
    ]),
}

# Validacao BigQuery completa (6/6 OK)
validation = pd.DataFrame([
    {"Promocao": "Tigre Sortudo",                     "Athena UAP": 1740, "BQ UAP": 1744, "Diff UAP (%)": 0.2, "Athena Turnover (R$)": 358756.80,  "BQ Turnover (R$)": 360779.60,  "Diff Turnover (%)": 0.6,  "OK?": "OK"},
    {"Promocao": "Fortune Rabbit",                    "Athena UAP": 841,  "BQ UAP": 842,  "Diff UAP (%)": 0.1, "Athena Turnover (R$)": 312372.50,  "BQ Turnover (R$)": 312849.00,  "Diff Turnover (%)": 0.2,  "OK?": "OK"},
    {"Promocao": "Gates of Olympus",                  "Athena UAP": 618,  "BQ UAP": 619,  "Diff UAP (%)": 0.2, "Athena Turnover (R$)": 2322918.45, "BQ Turnover (R$)": 2323418.00, "Diff Turnover (%)": 0.02, "OK?": "OK"},
    {"Promocao": "Sweet Bonanza",                     "Athena UAP": 383,  "BQ UAP": 385,  "Diff UAP (%)": 0.5, "Athena Turnover (R$)": 33056.60,   "BQ Turnover (R$)": 33168.59,   "Diff Turnover (%)": 0.3,  "OK?": "OK"},
    {"Promocao": "Fortune Ox",                        "Athena UAP": 454,  "BQ UAP": 454,  "Diff UAP (%)": 0.0, "Athena Turnover (R$)": 85376.50,   "BQ Turnover (R$)": 85313.15,   "Diff Turnover (%)": 0.1,  "OK?": "OK"},
    {"Promocao": "Combo FDS (Ratinho+Tigre+Macaco)",  "Athena UAP": 3309, "BQ UAP": 3312, "Diff UAP (%)": 0.1, "Athena Turnover (R$)": 1216975.80, "BQ Turnover (R$)": 1216658.70, "Diff Turnover (%)": 0.03, "OK?": "OK"},
])

# ============ PROMO CONFIG ============
promos = [
    {"id": "P1", "name": "Tigre Sortudo",                     "periodo_brt": "14h 07/03 as 23h59 08/03"},
    {"id": "P2", "name": "Fortune Rabbit",                    "periodo_brt": "16h as 23h59 09/03"},
    {"id": "P3", "name": "Gates of Olympus",                  "periodo_brt": "11h as 23h59 10/03"},
    {"id": "P4", "name": "Sweet Bonanza",                     "periodo_brt": "11h as 23h59 11/03"},
    {"id": "P5", "name": "Fortune Ox",                        "periodo_brt": "18h as 22h 12/03"},
    {"id": "P6", "name": "Combo FDS (Ratinho+Tigre+Macaco)",  "periodo_brt": "17h 13/03 as 23h59 15/03"},
]


def pct_change(before, after):
    if before == 0:
        return "N/A" if after == 0 else "+inf"
    pct = (after - before) / before * 100
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"


# ============ GERAR EXCEL ============
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "report_crm_promocoes_mar2026.xlsx")

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    # Sheet 1: Resumo Consolidado
    resumo_rows = []
    for p in promos:
        pid = p["id"]
        bef = period_summaries[f"{pid}_before"]
        dur = period_summaries[f"{pid}_during"]
        aft = period_summaries[f"{pid}_after"]
        resumo_rows.append({
            "Promocao": p["name"],
            "Periodo (BRT)": p["periodo_brt"],
            "Turnover Mes Anterior (R$)": round(bef["turnover"], 2),
            "UAP Mes Anterior": bef["uap"],
            "Turnover Durante (R$)": round(dur["turnover"], 2),
            "UAP Durante": dur["uap"],
            "Turnover Dia Seguinte (R$)": round(aft["turnover"], 2),
            "UAP Dia Seguinte": aft["uap"],
            "Var Turnover Antes>Durante": pct_change(bef["turnover"], dur["turnover"]),
            "Var Turnover Durante>Depois": pct_change(dur["turnover"], aft["turnover"]),
            "Var UAP Antes>Durante": pct_change(bef["uap"], dur["uap"]),
            "Var UAP Durante>Depois": pct_change(dur["uap"], aft["uap"]),
        })
    pd.DataFrame(resumo_rows).to_excel(writer, sheet_name="Resumo", index=False)

    # Sheets por promo: faixas
    for p in promos:
        pid = p["id"]
        sheet_name = f"{pid} {p['name']}"[:31]
        tiers[pid].to_excel(writer, sheet_name=sheet_name, index=False)

    # Sheet: Validacao BQ
    validation.to_excel(writer, sheet_name="Validacao BQ", index=False)

print(f"Excel gerado: {output_path}")
