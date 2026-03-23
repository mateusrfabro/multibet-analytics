"""Processa dados v2 corrigidos (rollbacks + dim_user) e gera Excel."""
import pandas as pd
import os

# Carregar dados Q1 corrigido
df_da = pd.read_parquet("output/q1_during_after_v2.parquet")

# BEFORE (do Q2 corrigido)
before = {
    "P1_before": {"uap": 1570, "turnover": 308612.60},
    "P2_before": {"uap": 578,  "turnover": 166349.50},
    "P3_before": {"uap": 384,  "turnover": 74985.05},
    "P4_before": {"uap": 158,  "turnover": 8579.25},
    "P5_before": {"uap": 136,  "turnover": 16366.00},
    "P6_before": {"uap": 2326, "turnover": 627598.20},
}

promos_cfg = [
    {"id": "P1", "name": "Tigre Sortudo",
     "periodo_brt": "14h 07/03 as 23h59 08/03",
     "faixas": [("Gire entre R$50 a R$199", 50, 199.99),
                ("Gire entre R$200 a R$499", 200, 499.99),
                ("Gire entre R$500 a R$999", 500, 999.99),
                ("Gire R$1.000 ou mais", 1000, float("inf"))]},
    {"id": "P2", "name": "Fortune Rabbit",
     "periodo_brt": "16h as 23h59 09/03",
     "faixas": [("Gire entre R$30 a R$99", 30, 99.99),
                ("Gire entre R$100 a R$299", 100, 299.99),
                ("Gire entre R$300 a R$599", 300, 599.99),
                ("Gire R$600 ou mais", 600, float("inf"))]},
    {"id": "P3", "name": "Gates of Olympus",
     "periodo_brt": "11h as 23h59 10/03",
     "faixas": [("Gire entre R$50 a R$99", 50, 99.99),
                ("Gire entre R$100 a R$299", 100, 299.99),
                ("Gire entre R$300 a R$499", 300, 499.99),
                ("Gire R$500 ou mais", 500, float("inf"))]},
    {"id": "P4", "name": "Sweet Bonanza",
     "periodo_brt": "11h as 23h59 11/03",
     "faixas": [("Gire entre R$15 e R$49", 15, 49.99),
                ("Gire entre R$50 a R$99", 50, 99.99),
                ("Gire entre R$100 a R$299", 100, 299.99),
                ("Gire R$300 ou mais", 300, float("inf"))]},
    {"id": "P5", "name": "Fortune Ox",
     "periodo_brt": "18h as 22h 12/03",
     "faixas": [("Gire entre R$30 a R$99", 30, 99.99),
                ("Gire entre R$100 a R$299", 100, 299.99),
                ("Gire entre R$300 a R$599", 300, 599.99),
                ("Gire R$600 ou mais", 600, float("inf"))]},
    {"id": "P6", "name": "Combo FDS (Ratinho+Tigre+Macaco)",
     "periodo_brt": "17h 13/03 as 23h59 15/03",
     "faixas": [("Gire entre R$50 a R$199", 50, 199.99),
                ("Gire entre R$200 a R$399", 200, 399.99),
                ("Gire entre R$400 a R$799", 400, 799.99),
                ("Gire entre R$800 a R$999", 800, 999.99),
                ("Gire R$1.000 ou mais", 1000, float("inf"))]},
]

# ============ PROCESSAR ============
period_summaries = dict(before)
tier_summaries = {}

for p in promos_cfg:
    pid = p["id"]
    for period_key in ["during", "after"]:
        label = f"{pid}_{period_key}"
        df_p = df_da[df_da["promo_period"] == label]
        period_summaries[label] = {
            "uap": int(df_p["c_ecr_id"].nunique()),
            "turnover": float(df_p["turnover_brl"].sum()),
            "ggr": float(df_p["ggr_brl"].sum()),
        }

    # Classificar em faixas (DURING)
    df_dur = df_da[df_da["promo_period"] == f"{pid}_during"].copy()
    if not df_dur.empty:
        min_thresh = min(f[1] for f in p["faixas"])
        faixas_sorted = sorted(p["faixas"], key=lambda f: f[1], reverse=True)

        def classify(t, faixas_s=faixas_sorted, mt=min_thresh):
            for nome, vmin, vmax in faixas_s:
                if vmin <= t <= vmax:
                    return nome
            if t < mt:
                return f"Abaixo de R${mt:.0f}"
            return "Sem classificacao"

        df_dur["faixa"] = df_dur["turnover_brl"].apply(classify)

        summary = (
            df_dur.groupby("faixa")
            .agg(qty_users=("c_ecr_id", "count"),
                 turnover_total=("turnover_brl", "sum"),
                 ggr_total=("ggr_brl", "sum"))
            .reset_index()
        )

        faixa_names = [f[0] for f in p["faixas"]]
        for nome in faixa_names:
            if nome not in summary["faixa"].values:
                new_row = pd.DataFrame([{
                    "faixa": nome, "qty_users": 0,
                    "turnover_total": 0.0, "ggr_total": 0.0,
                }])
                summary = pd.concat([summary, new_row], ignore_index=True)

        order_map = {nome: i for i, nome in enumerate(faixa_names)}
        summary["_order"] = summary["faixa"].map(order_map).fillna(999)
        summary = summary.sort_values("_order").drop(columns="_order").reset_index(drop=True)

        total_u = summary["qty_users"].sum()
        total_t = summary["turnover_total"].sum()
        summary["pct_users"] = (summary["qty_users"] / total_u * 100).round(1) if total_u > 0 else 0
        summary["pct_turnover"] = (summary["turnover_total"] / total_t * 100).round(1) if total_t > 0 else 0

        tier_summaries[pid] = summary


# ============ PRINT RESULTADOS ============
print("=" * 70)
print("RESULTADOS CORRIGIDOS v2 (rollbacks descontados + ps_bi.dim_user)")
print("=" * 70)

for p in promos_cfg:
    pid = p["id"]
    bef = period_summaries.get(f"{pid}_before", {"uap": 0, "turnover": 0})
    dur = period_summaries.get(f"{pid}_during", {"uap": 0, "turnover": 0, "ggr": 0})
    aft = period_summaries.get(f"{pid}_after", {"uap": 0, "turnover": 0, "ggr": 0})

    print(f"\n--- {p['name']} ({p['periodo_brt']}) ---")
    print(f"  Mes Anterior : Turnover R$ {bef['turnover']:>12,.2f} | UAP {bef['uap']:>6,}")
    print(f"  Durante      : Turnover R$ {dur['turnover']:>12,.2f} | UAP {dur['uap']:>6,} | GGR R$ {dur.get('ggr',0):>12,.2f}")
    print(f"  Dia Seguinte : Turnover R$ {aft['turnover']:>12,.2f} | UAP {aft['uap']:>6,} | GGR R$ {aft.get('ggr',0):>12,.2f}")

    if pid in tier_summaries:
        print("  Faixas:")
        for _, row in tier_summaries[pid].iterrows():
            ggr_flag = " <-- CASA PERDEU" if row["ggr_total"] < 0 else ""
            print(f"    {row['faixa']:30s}: {int(row['qty_users']):>5} users ({row['pct_users']:>5.1f}%)"
                  f" | Turnover R$ {row['turnover_total']:>12,.2f}"
                  f" | GGR R$ {row['ggr_total']:>12,.2f}{ggr_flag}")


# ============ PROVOCACAO: GGR GATES OF OLYMPUS TOP 40 ============
print("\n" + "=" * 70)
print("PROVOCACAO ARQUITETO: GGR dos top 40 users - Gates of Olympus")
print("=" * 70)

df_p3 = df_da[df_da["promo_period"] == "P3_during"].copy()
df_p3_top = df_p3.nlargest(40, "turnover_brl")

total_turn = df_p3_top["turnover_brl"].sum()
total_ggr = df_p3_top["ggr_brl"].sum()
neg_count = (df_p3_top["ggr_brl"] < 0).sum()
neg_ggr = df_p3_top[df_p3_top["ggr_brl"] < 0]["ggr_brl"].sum()
pos_ggr = df_p3_top[df_p3_top["ggr_brl"] >= 0]["ggr_brl"].sum()

print(f"\nTop 40 users por turnover:")
print(f"  Total Turnover: R$ {total_turn:>12,.2f}")
print(f"  Total GGR:      R$ {total_ggr:>12,.2f}")
print(f"  Hold Rate:      {total_ggr / total_turn * 100:.2f}%" if total_turn > 0 else "  Hold Rate: N/A")
print(f"  GGR negativo?   {'SIM - CASA PERDEU DINHEIRO' if total_ggr < 0 else 'NAO - CASA LUCROU'}")
print(f"\n  Users GGR negativo: {neg_count}/40 (GGR total: R$ {neg_ggr:>12,.2f})")
print(f"  Users GGR positivo: {40 - neg_count}/40 (GGR total: R$ {pos_ggr:>12,.2f})")
print(f"  GGR liquido top 40: R$ {total_ggr:>12,.2f}")

print("\n  Top 10 por turnover (detalhe):")
for _, row in df_p3_top.head(10).iterrows():
    status = "PERDA" if row["ggr_brl"] < 0 else "LUCRO"
    hold = row["ggr_brl"] / row["turnover_brl"] * 100 if row["turnover_brl"] > 0 else 0
    print(f"    Turnover R$ {row['turnover_brl']:>10,.2f} | GGR R$ {row['ggr_brl']:>10,.2f} | Hold {hold:>6.1f}% | {status}")


# ============ GERAR EXCEL v2 ============
def pct_change(before, after):
    if before == 0:
        return "N/A" if after == 0 else "+inf"
    pct = (after - before) / before * 100
    sign = "+" if pct > 0 else ""
    return f"{sign}{pct:.1f}%"

output_path = os.path.join("output", "report_crm_promocoes_mar2026_v2_FINAL.xlsx")

with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
    # Sheet 1: Resumo
    resumo_rows = []
    for p in promos_cfg:
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
            "GGR Durante (R$)": round(dur.get("ggr", 0), 2),
            "Turnover Dia Seguinte (R$)": round(aft["turnover"], 2),
            "UAP Dia Seguinte": aft["uap"],
            "Var Turnover Antes>Durante": pct_change(bef["turnover"], dur["turnover"]),
            "Var Turnover Durante>Depois": pct_change(dur["turnover"], aft["turnover"]),
        })
    pd.DataFrame(resumo_rows).to_excel(writer, sheet_name="Resumo", index=False)

    # Sheets por promo
    for p in promos_cfg:
        pid = p["id"]
        sheet_name = f"{pid} {p['name']}"[:31]
        if pid in tier_summaries:
            out = tier_summaries[pid].rename(columns={
                "faixa": "Faixa",
                "qty_users": "Qtd Usuarios",
                "turnover_total": "Turnover (R$)",
                "ggr_total": "GGR (R$)",
                "pct_users": "% Usuarios",
                "pct_turnover": "% Turnover",
            })
            out["Turnover (R$)"] = out["Turnover (R$)"].round(2)
            out["GGR (R$)"] = out["GGR (R$)"].round(2)
            out.to_excel(writer, sheet_name=sheet_name, index=False)

    # Sheet: GGR Gates of Olympus Top 40
    df_p3_out = df_p3_top[["c_ecr_id", "turnover_brl", "ggr_brl"]].copy()
    df_p3_out.columns = ["ECR ID", "Turnover (R$)", "GGR (R$)"]
    df_p3_out["Hold Rate (%)"] = (df_p3_out["GGR (R$)"] / df_p3_out["Turnover (R$)"] * 100).round(2)
    df_p3_out["Status"] = df_p3_out["GGR (R$)"].apply(lambda x: "PERDA" if x < 0 else "LUCRO")
    df_p3_out = df_p3_out.sort_values("Turnover (R$)", ascending=False).reset_index(drop=True)
    df_p3_out.to_excel(writer, sheet_name="GGR Gates Olympus Top40", index=False)

    # Sheet: Observacoes de Risco (solicitado pelo arquiteto — nao enviar direto ao CRM)
    obs_rows = [
        {
            "Promocao": "Fortune Ox",
            "Faixa": "R$600 ou mais",
            "Qtd Users": 24,
            "GGR (R$)": -12054.10,
            "Observacao": "Faixa atraiu jogadores com taxa de acerto acima da media. "
                          "Sugerimos revisao na mecanica de bonus para esse segmento.",
        },
        {
            "Promocao": "Combo FDS (Ratinho+Tigre+Macaco)",
            "Faixa": "R$1.000 ou mais",
            "Qtd Users": 140,
            "GGR (R$)": -13008.66,
            "Observacao": "Whales com GGR negativo concentrado. "
                          "Avaliar cap (limite) de bonificacao nessa faixa para preservar margem.",
        },
        {
            "Promocao": "Fortune Rabbit",
            "Faixa": "R$300 a R$599",
            "Qtd Users": 50,
            "GGR (R$)": -5834.95,
            "Observacao": "GGR negativo moderado. Monitorar recorrencia em proximas promos.",
        },
        {
            "Promocao": "Combo FDS (Ratinho+Tigre+Macaco)",
            "Faixa": "R$800 a R$999",
            "Qtd Users": 37,
            "GGR (R$)": -838.93,
            "Observacao": "Perda pequena mas consistente com faixa R$1.000+. Mesmo grupo de risco.",
        },
        {
            "Promocao": "Tigre Sortudo",
            "Faixa": "R$500 a R$999",
            "Qtd Users": 71,
            "GGR (R$)": -1442.16,
            "Observacao": "Perda leve. Dentro do aceitavel para volatilidade do jogo.",
        },
    ]
    df_obs = pd.DataFrame(obs_rows)
    df_obs.to_excel(writer, sheet_name="Observacoes de Risco", index=False)

print(f"\nExcel v2 FINAL: {output_path}")
