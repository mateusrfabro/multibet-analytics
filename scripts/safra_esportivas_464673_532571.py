"""
Safra esportivas IDs Meta 464673 e 532571 — versao v2 (pos-auditoria).

Mudanca critica (v2):
  Cohort passou de multibet.tab_user_affiliate (Super Nova DB) para
  ps_bi.dim_user (Athena). Motivo: tab_user_affiliate.data_registro e data
  de atribuicao/touchpoint (inclui reatribuicoes historicas) e estava
  inflando a safra do 464673 em +20,9% vs Athena (29.171 vs 24.127).
  ps_bi.dim_user.signup_datetime e data real de cadastro — fonte da verdade.

Janela: 2026-03-01 a 2026-04-16 (marco + abril ate D-1).
Quebra: affiliate x semana + UTM.

Arquitetura:
  Athena  -> cohort, FTD, depositos, saques, NGR-FTD, is_test
  Super Nova -> sports_bettors, casino_bettors, sports_turnover, sports_ggr
  Super Nova -> trackings (UTM)
"""
import os
import sys
import pandas as pd
from datetime import datetime
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection
from db.athena import query_athena

OUT_DIR = "reports/safra_esportivas_464673_532571"
os.makedirs(OUT_DIR, exist_ok=True)

IDS = ('464673', '532571')
DT_FROM = '2026-03-01'
DT_TO   = '2026-04-16'


SQL_ATHENA_COHORT = f"""
SELECT
    tracker_id,
    ecr_id,
    external_id,
    signup_datetime,
    CAST(signup_datetime AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS data_registro,
    has_ftd,
    ftd_date,
    COALESCE(ftd_amount_inhouse, 0)             AS ftd_amount,
    COALESCE(lifetime_deposit_count, 0)         AS deposit_count,
    COALESCE(lifetime_deposit_amount_inhouse,0) AS dep_total,
    COALESCE(lifetime_withdrawal_amount_inhouse,0) AS saque_total,
    COALESCE(net_deposit_withdrawal_inhouse,0)  AS net_deposit
FROM ps_bi.dim_user
WHERE tracker_id IN ({",".join(f"'{i}'" for i in IDS)})
  AND CAST(signup_datetime AT TIME ZONE 'America/Sao_Paulo' AS DATE)
      BETWEEN DATE '{DT_FROM}' AND DATE '{DT_TO}'
  AND (is_test IS NULL OR is_test = FALSE)
"""


SQL_SUPERNOVA_ACTIVITY = """
WITH target_ecr AS (
    SELECT UNNEST(%(ecr_ids)s::text[]) AS c_ecr_id
)
SELECT
    ud.c_ecr_id,
    MAX(CASE WHEN COALESCE(ud.qtd_bet_sports,0) > 0 OR COALESCE(ud.sports_turnover,0) > 0 THEN 1 ELSE 0 END) AS is_sports,
    MAX(CASE WHEN COALESCE(ud.qtd_bet_casino,0) > 0 OR COALESCE(ud.casino_turnover,0) > 0 THEN 1 ELSE 0 END) AS is_casino,
    COALESCE(SUM(ud.sports_turnover), 0) AS sports_turnover,
    COALESCE(SUM(ud.sports_ggr), 0)      AS sports_ggr,
    COALESCE(SUM(ud.qtd_bet_sports), 0)  AS sports_bets,
    COALESCE(SUM(ud.ngr), 0)             AS ngr,
    COALESCE(SUM(ud.btr), 0)             AS btr
FROM multibet.tab_user_daily ud
INNER JOIN target_ecr t ON ud.c_ecr_id = t.c_ecr_id
GROUP BY ud.c_ecr_id
"""

SQL_SUPERNOVA_UTM = """
SELECT user_id,
       MAX(utm_source)   AS utm_source,
       MAX(utm_campaign) AS utm_campaign,
       MAX(utm_content)  AS utm_content
FROM multibet.trackings
WHERE user_id = ANY(%(ext_ids)s::text[])
GROUP BY user_id
"""


def main():
    print(f"[{datetime.now():%Y-%m-%d %H:%M:%S}] Safra v2 — cohort Athena ({DT_FROM} a {DT_TO})")

    # 1) Cohort no Athena (ps_bi.dim_user)
    cohort = query_athena(SQL_ATHENA_COHORT, database="ps_bi")
    cohort["ecr_id"] = cohort["ecr_id"].astype(str)
    cohort["external_id"] = cohort["external_id"].astype(str)
    print(f"[cohort] {len(cohort)} jogadores — Athena ps_bi.dim_user (is_test excluido)")
    print(cohort.groupby("tracker_id").size().to_string())

    ecr_ids = cohort["ecr_id"].tolist()
    ext_ids = cohort["external_id"].tolist()

    # 2) Atividade + UTM no Super Nova DB
    tunnel, conn = get_supernova_connection()
    try:
        act = pd.read_sql(SQL_SUPERNOVA_ACTIVITY, conn, params={"ecr_ids": ecr_ids})
        utm = pd.read_sql(SQL_SUPERNOVA_UTM,      conn, params={"ext_ids": ext_ids})
    finally:
        conn.close()
        tunnel.stop()

    act["c_ecr_id"] = act["c_ecr_id"].astype(str)
    utm["user_id"]  = utm["user_id"].astype(str)
    print(f"[atividade] {len(act)} jogadores com atividade em tab_user_daily")
    print(f"[utm] {len(utm)} jogadores com UTM rastreada em trackings")

    # 3) Join cohort + atividade + UTM
    df = cohort.merge(act, left_on="ecr_id", right_on="c_ecr_id", how="left")
    df = df.merge(utm, left_on="external_id", right_on="user_id", how="left")
    for c in ["is_sports","is_casino","sports_turnover","sports_ggr","sports_bets","ngr","btr"]:
        df[c] = df[c].fillna(0)

    # Flags de depositos (cumulativos) a partir de lifetime_deposit_count
    df["ftd_flag"] = (df["deposit_count"] >= 1).astype(int)
    df["std_flag"] = (df["deposit_count"] >= 2).astype(int)
    df["ttd_flag"] = (df["deposit_count"] >= 3).astype(int)
    df["qtd_flag"] = (df["deposit_count"] >= 4).astype(int)

    # 4) Consolidado por tracker
    agg = df.groupby("tracker_id").agg(
        cadastros_total=("ecr_id","nunique"),
        ftd_total=("ftd_flag","sum"),
        std_total=("std_flag","sum"),
        ttd_total=("ttd_flag","sum"),
        qtd_plus=("qtd_flag","sum"),
        sports_bettors=("is_sports","sum"),
        casino_bettors=("is_casino","sum"),
        ftd_amount=("ftd_amount","sum"),
        dep_total=("dep_total","sum"),
        saque_total=("saque_total","sum"),
        net_deposit=("net_deposit","sum"),
        sports_turnover=("sports_turnover","sum"),
        sports_ggr=("sports_ggr","sum"),
        sports_bets=("sports_bets","sum"),
        ngr=("ngr","sum"),
    ).reset_index()
    agg["ftd_sports"]  = df[df["is_sports"]==1].groupby("tracker_id")["has_ftd"].apply(lambda s: int((s==True).sum())).reindex(agg["tracker_id"]).values
    agg["net_deposit_sports_subset"] = df[df["is_sports"]==1].groupby("tracker_id")["net_deposit"].sum().reindex(agg["tracker_id"]).values
    agg["ngr_sports_subset"]         = df[df["is_sports"]==1].groupby("tracker_id")["ngr"].sum().reindex(agg["tracker_id"]).values
    agg = agg.rename(columns={"tracker_id":"affiliate_id"})
    agg["fonte"] = "Meta"
    agg = agg[[
        "affiliate_id","fonte","cadastros_total",
        "ftd_total","std_total","ttd_total","qtd_plus",
        "sports_bettors","casino_bettors","ftd_sports",
        "ftd_amount","dep_total","saque_total","net_deposit",
        "sports_turnover","sports_ggr","sports_bets","ngr",
        "net_deposit_sports_subset","ngr_sports_subset",
    ]]
    # arredondar reais
    for c in ["ftd_amount","dep_total","saque_total","net_deposit","sports_turnover","sports_ggr","ngr",
              "net_deposit_sports_subset","ngr_sports_subset"]:
        agg[c] = agg[c].astype(float).round(2)
    for c in ["cadastros_total","ftd_total","std_total","ttd_total","qtd_plus",
              "sports_bettors","casino_bettors","ftd_sports","sports_bets"]:
        agg[c] = agg[c].astype(int)

    # 5) Safra semanal
    df["week_start"] = pd.to_datetime(df["data_registro"]).dt.to_period("W-MON").dt.start_time.dt.date
    sem = df.groupby(["tracker_id","week_start"]).agg(
        cadastros_total=("ecr_id","nunique"),
        ftd_total=("has_ftd", lambda s: int((s==True).sum())),
        sports_bettors=("is_sports","sum"),
        casino_bettors=("is_casino","sum"),
        ftd_amount=("ftd_amount","sum"),
        dep_total=("dep_total","sum"),
        saque_total=("saque_total","sum"),
        net_deposit=("net_deposit","sum"),
        sports_turnover=("sports_turnover","sum"),
        sports_ggr=("sports_ggr","sum"),
        sports_bets=("sports_bets","sum"),
        ngr=("ngr","sum"),
    ).reset_index().rename(columns={"tracker_id":"affiliate_id"})
    for c in ["ftd_amount","dep_total","saque_total","net_deposit","sports_turnover","sports_ggr","ngr"]:
        sem[c] = sem[c].astype(float).round(2)
    for c in ["cadastros_total","ftd_total","sports_bettors","casino_bettors","sports_bets"]:
        sem[c] = sem[c].astype(int)

    # 6) UTM subset sports
    sp_only = df[df["is_sports"]==1].copy()
    sp_only["utm_source"]   = sp_only["utm_source"].fillna("(sem UTM)")
    sp_only["utm_campaign"] = sp_only["utm_campaign"].fillna("(sem UTM)")
    sp_only["utm_content"]  = sp_only["utm_content"].fillna("(sem UTM)")
    df_utm = sp_only.groupby(["tracker_id","utm_source","utm_campaign","utm_content"]).agg(
        cadastros_sports=("ecr_id","nunique"),
        ftd=("has_ftd", lambda s: int((s==True).sum())),
        dep_total=("dep_total","sum"),
        saque_total=("saque_total","sum"),
        net_deposit=("net_deposit","sum"),
        sports_turnover=("sports_turnover","sum"),
        sports_ggr=("sports_ggr","sum"),
        ngr=("ngr","sum"),
    ).reset_index().rename(columns={"tracker_id":"affiliate_id"})
    for c in ["dep_total","saque_total","net_deposit","sports_turnover","sports_ggr","ngr"]:
        df_utm[c] = df_utm[c].astype(float).round(2)
    df_utm = df_utm.sort_values(["affiliate_id","cadastros_sports"], ascending=[True, False])

    # 7) Gravar
    path_xlsx = os.path.join(OUT_DIR, f"safra_esportivas_ID464673_ID532571_{DT_FROM}_a_{DT_TO}.xlsx")
    with pd.ExcelWriter(path_xlsx, engine="openpyxl") as w:
        pd.DataFrame([
            ["Escopo", f"Cadastros trackers Meta {IDS[0]}/{IDS[1]} entre {DT_FROM} e {DT_TO}"],
            ["Cohort (fonte da verdade)", "Athena ps_bi.dim_user — signup_datetime real, is_test excluido (v2, pos-auditoria)"],
            ["Atividade (sports/casino)", "Super Nova DB multibet.tab_user_daily (join por c_ecr_id)"],
            ["UTM", "Super Nova DB multibet.trackings (join user_id = external_id)"],
            ["Deposito/Saque/FTD", "ps_bi.dim_user lifetime_* (valores em BRL via *_inhouse)"],
            ["FTD (has_ftd)", "Flag da ps_bi.dim_user = jogador tem pelo menos 1 deposito bem-sucedido"],
            ["Sports/Casino", "Flags (nao mutuamente exclusivas — um jogador pode aparecer em ambas)"],
            ["net_deposit_sports_subset", "Net deposit apenas dos sports bettors"],
            ["ngr_sports_subset", "NGR apenas dos sports bettors"],
            ["Test users", "Excluidos via ps_bi.dim_user.is_test"],
            ["Mudanca vs v1", "Cohort migrada de tab_user_affiliate (inflava +20,9%) para ps_bi.dim_user"],
        ], columns=["Campo","Descricao"]).to_excel(w, sheet_name="0_Legenda", index=False)
        agg.to_excel(w, sheet_name="1_Consolidado_por_ID", index=False)
        sem.to_excel(w, sheet_name="2_Safra_semanal", index=False)
        df_utm.to_excel(w, sheet_name="3_UTM_subset_sports", index=False)

    agg.to_csv(os.path.join(OUT_DIR, "1_consolidado.csv"), index=False, encoding="utf-8-sig")
    sem.to_csv(os.path.join(OUT_DIR, "2_safra_semanal.csv"), index=False, encoding="utf-8-sig")
    df_utm.to_csv(os.path.join(OUT_DIR, "3_utm_subset_sports.csv"), index=False, encoding="utf-8-sig")

    print(f"\nOK: {path_xlsx}")
    print("\n=== CONSOLIDADO v2 ===")
    print(agg.to_string(index=False))


if __name__ == "__main__":
    main()
