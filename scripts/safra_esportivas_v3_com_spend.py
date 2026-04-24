"""
Safra v3 — IDs Meta 464673 e 532571 — com SPEND do gestor e CAC/ROAS.
Janela ajustada para 2026-03-01 a 2026-04-14 (ultima semana completa do gestor).
Buckets semanais iguais aos do gestor (dom-sab, com 4a semana mar = 22-31).

Gestor confirmou (WhatsApp 18:31): spend e EXCLUSIVO dos trackers 464673 + 532571.
"""
import os, sys
import pandas as pd
from datetime import datetime, date
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection
from db.athena import query_athena

OUT_DIR = "reports/safra_esportivas_464673_532571"
os.makedirs(OUT_DIR, exist_ok=True)

IDS = ('464673', '532571')
DT_FROM = '2026-03-01'
DT_TO   = '2026-04-14'

# Spend confirmado pelo gestor (WhatsApp) — total dos 2 trackers, esportes
SPEND = [
    {"bucket": "Sem 1 mar (01-07)",  "inicio": "2026-03-01", "fim": "2026-03-07", "spend": 32521.51},
    {"bucket": "Sem 2 mar (08-14)",  "inicio": "2026-03-08", "fim": "2026-03-14", "spend": 60578.80},
    {"bucket": "Sem 3 mar (15-21)",  "inicio": "2026-03-15", "fim": "2026-03-21", "spend": 98008.59},
    {"bucket": "Sem 4 mar (22-31)",  "inicio": "2026-03-22", "fim": "2026-03-31", "spend": 89256.35},
    {"bucket": "Sem 1 abr (01-07)",  "inicio": "2026-04-01", "fim": "2026-04-07", "spend": 78359.09},
    {"bucket": "Sem 2 abr (08-14)",  "inicio": "2026-04-08", "fim": "2026-04-14", "spend": 116285.71},
]

SQL_ATHENA = f"""
SELECT
    tracker_id, ecr_id, external_id,
    CAST(signup_datetime AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS data_registro,
    has_ftd, ftd_date,
    COALESCE(lifetime_deposit_count, 0)            AS dep_count,
    COALESCE(ftd_amount_inhouse, 0)                AS ftd_amount,
    COALESCE(lifetime_deposit_amount_inhouse, 0)   AS dep_total,
    COALESCE(lifetime_withdrawal_amount_inhouse,0) AS saque_total,
    COALESCE(net_deposit_withdrawal_inhouse, 0)    AS net_deposit
FROM ps_bi.dim_user
WHERE tracker_id IN ({",".join(f"'{i}'" for i in IDS)})
  AND CAST(signup_datetime AT TIME ZONE 'America/Sao_Paulo' AS DATE)
      BETWEEN DATE '{DT_FROM}' AND DATE '{DT_TO}'
  AND (is_test IS NULL OR is_test = FALSE)
"""
SQL_ATIVIDADE = """
SELECT c_ecr_id,
  MAX(CASE WHEN COALESCE(qtd_bet_sports,0)>0 OR COALESCE(sports_turnover,0)>0 THEN 1 ELSE 0 END) AS is_sports,
  MAX(CASE WHEN COALESCE(qtd_bet_casino,0)>0 OR COALESCE(casino_turnover,0)>0 THEN 1 ELSE 0 END) AS is_casino,
  COALESCE(SUM(sports_turnover),0) AS sports_turnover,
  COALESCE(SUM(sports_ggr),0)      AS sports_ggr,
  COALESCE(SUM(qtd_bet_sports),0)  AS sports_bets,
  COALESCE(SUM(ngr),0) AS ngr
FROM multibet.tab_user_daily
WHERE c_ecr_id = ANY(%(ecr)s::text[])
GROUP BY c_ecr_id
"""

def bucket_of(d: date) -> str:
    d = pd.Timestamp(d).date()
    for row in SPEND:
        if date.fromisoformat(row["inicio"]) <= d <= date.fromisoformat(row["fim"]):
            return row["bucket"]
    return "(fora)"


def main():
    print(f"[{datetime.now():%H:%M:%S}] Safra v3 + Spend — janela {DT_FROM} a {DT_TO}")

    coh = query_athena(SQL_ATHENA, database="ps_bi")
    coh["ecr_id"]      = coh["ecr_id"].astype(str)
    coh["external_id"] = coh["external_id"].astype(str)
    print(f"Cohort Athena: {len(coh)} jogadores | {coh['tracker_id'].value_counts().to_dict()}")

    tunnel, conn = get_supernova_connection()
    try:
        act = pd.read_sql(SQL_ATIVIDADE, conn, params={"ecr": coh["ecr_id"].tolist()})
    finally:
        conn.close(); tunnel.stop()
    act["c_ecr_id"] = act["c_ecr_id"].astype(str)

    df = coh.merge(act, left_on="ecr_id", right_on="c_ecr_id", how="left")
    for c in ["is_sports","is_casino","sports_turnover","sports_ggr","sports_bets","ngr"]:
        df[c] = df[c].fillna(0)

    # Bucket do gestor
    df["bucket"] = df["data_registro"].apply(bucket_of)
    df = df[df["bucket"] != "(fora)"].copy()

    # Flags cumulativas de depositos (a partir de lifetime_deposit_count)
    df["ftd_flag"]  = (df["dep_count"] >= 1).astype(int)
    df["std_flag"]  = (df["dep_count"] >= 2).astype(int)
    df["ttd_flag"]  = (df["dep_count"] >= 3).astype(int)
    df["qtd_flag"]  = (df["dep_count"] >= 4).astype(int)

    # Agregado por bucket (TODOS os 2 trackers juntos — spend esta agregado)
    agg = df.groupby("bucket").agg(
        cadastros_total=("ecr_id","nunique"),
        ftd_total=("ftd_flag","sum"),
        std_total=("std_flag","sum"),
        ttd_total=("ttd_flag","sum"),
        qtd_plus=("qtd_flag","sum"),
        sports_bettors=("is_sports","sum"),
        casino_bettors=("is_casino","sum"),
        ftd_sports=("has_ftd", lambda s: int(((s==True) & (df.loc[s.index,"is_sports"]==1)).sum())),
        ftd_amount=("ftd_amount","sum"),
        dep_total=("dep_total","sum"),
        net_deposit=("net_deposit","sum"),
        sports_turnover=("sports_turnover","sum"),
        sports_ggr=("sports_ggr","sum"),
        ngr=("ngr","sum"),
    ).reset_index()

    # Join spend
    spend_df = pd.DataFrame(SPEND)
    agg = spend_df.merge(agg, on="bucket", how="left")

    for c in ["ftd_amount","dep_total","net_deposit","sports_turnover","sports_ggr","ngr"]:
        agg[c] = agg[c].astype(float).round(2)
    for c in ["cadastros_total","ftd_total","std_total","ttd_total","qtd_plus",
              "sports_bettors","casino_bettors","ftd_sports"]:
        agg[c] = agg[c].fillna(0).astype(int)

    # Eficiencia de aquisicao (o coracao do relatorio)
    agg["cac_cadastro"]    = (agg["spend"] / agg["cadastros_total"].replace(0, pd.NA)).round(2)
    agg["cac_ftd"]         = (agg["spend"] / agg["ftd_total"].replace(0, pd.NA)).round(2)
    agg["cac_sports_bet"]  = (agg["spend"] / agg["sports_bettors"].replace(0, pd.NA)).round(2)
    agg["cac_ftd_sports"]  = (agg["spend"] / agg["ftd_sports"].replace(0, pd.NA)).round(2)
    agg["roas_ngr"]        = (agg["ngr"] / agg["spend"]).round(3)
    agg["roas_sports_ggr"] = (agg["sports_ggr"] / agg["spend"]).round(3)
    agg["resultado_liquido"] = (agg["ngr"] - agg["spend"]).round(2)

    print("\n=== EFICIENCIA DE AQUISICAO — Meta Esportes (464673+532571) ===")
    show = agg[["bucket","spend","cadastros_total","ftd_total","sports_bettors",
                "cac_cadastro","cac_ftd","cac_ftd_sports","sports_ggr","ngr","roas_ngr","resultado_liquido"]].copy()
    print(show.to_string(index=False))

    total_spend = agg["spend"].sum()
    total_cad   = agg["cadastros_total"].sum()
    total_ftd   = agg["ftd_total"].sum()
    total_sp    = agg["sports_bettors"].sum()
    total_ngr   = agg["ngr"].sum()
    total_sggr  = agg["sports_ggr"].sum()
    print(f"\nTOTAIS | spend=R$ {total_spend:,.2f} | cad={total_cad} | FTD={total_ftd} | sports={total_sp}")
    print(f"       CAC={total_spend/total_cad:.2f} | CAC FTD={total_spend/total_ftd:.2f} | NGR=R$ {total_ngr:,.2f}")
    print(f"       ROAS (NGR/spend)={total_ngr/total_spend:.3f} | Sports GGR/spend={total_sggr/total_spend:.3f}")
    print(f"       Resultado liquido (NGR-spend)=R$ {total_ngr-total_spend:,.2f}")

    # Save
    path_xlsx = os.path.join(OUT_DIR, f"safra_esportivas_v3_CAC_ROAS_{DT_FROM}_a_{DT_TO}.xlsx")
    with pd.ExcelWriter(path_xlsx, engine="openpyxl") as w:
        pd.DataFrame([
            ["Escopo", f"Trackers Meta Esportes 464673 + 532571 (confirmado gestor) | {DT_FROM} a {DT_TO}"],
            ["Spend", "Informado pelo gestor por WhatsApp (18:31, 17/04). Refere-se AOS 2 IDs, esporte."],
            ["Buckets", "Dom-sab. Sem 4 marco = 22-31 (10 dias) pra alinhar com o gestor."],
            ["Cohort", "Athena ps_bi.dim_user (signup_datetime BRT, is_test excluido)"],
            ["Atividade", "Super Nova multibet.tab_user_daily (sports bettors, sports_turnover, sports_ggr, NGR)"],
            ["CAC (cadastro)", "spend / cadastros_total — custo por cadastro bruto"],
            ["CAC FTD", "spend / FTDs — custo por depositante"],
            ["CAC FTD sports", "spend / FTDs que apostaram em sports — custo por FTD-qualificado"],
            ["ROAS NGR", "NGR lifetime (ate hoje) / spend — retorno sobre investimento publicitario"],
            ["ROAS Sports GGR", "GGR sportsbook / spend — retorno so da vertical sportsbook"],
            ["Resultado liquido", "NGR - spend (BRL) — lucro direto da cohort"],
            ["Ressalva", "NGR e lifetime ate hoje; cohorts mais recentes ainda estao maturando."],
        ], columns=["Campo","Descricao"]).to_excel(w, sheet_name="0_Legenda", index=False)
        agg.to_excel(w, sheet_name="1_CAC_ROAS_Semanal", index=False)

    agg.to_csv(os.path.join(OUT_DIR, "4_cac_roas_semanal.csv"), index=False, encoding="utf-8-sig")
    print(f"\nOK: {path_xlsx}")


if __name__ == "__main__":
    main()
