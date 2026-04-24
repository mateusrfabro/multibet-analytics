"""
REGs e FTDs de HOJE — Fonte Google (24/04/2026)

Foco: cohort fresco chegando por Google Ads hoje.
- Quem registrou hoje?
- Quem fez FTD hoje (primeiro deposito)?
- Quanto depositaram depois? Qual jogo?

Fontes (100% Athena):
- ps_bi.dim_user -> signup_datetime, ftd_datetime, ftd_amount, affiliate_id
- cashier_ec2.tbl_cashier_deposit -> depositos hoje (total do dia)
- ps_bi.fct_casino_activity_daily -> jogo principal hoje

Uso:
    python scripts/check_google_reg_ftd_today.py
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")

import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
import pytz
import pandas as pd

from db.athena import query_athena
from db.supernova import execute_supernova

BRT = pytz.timezone("America/Sao_Paulo")
HOJE = datetime.now(BRT).strftime("%Y-%m-%d")
AGORA = datetime.now(BRT).strftime("%H:%M")

GOOGLE_IDS = "('297657', '445431', '468114')"

print(f"\n{'='*82}")
print(f"  REG & FTD HOJE — Fonte Google — {HOJE} (snapshot {AGORA} BRT)")
print(f"  Affiliates: 297657 (elisa_google), 445431 (Google_Eyal), 468114 (MULTIBET APP)")
print(f"{'='*82}\n")

# =====================================================================
# 1) REGs de HOJE + FTD de HOJE via dim_user
# =====================================================================
sql = f"""
WITH google_cohort AS (
    SELECT
        ecr_id,
        external_id,
        affiliate_id,
        email,
        mobile_number,
        signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS signup_brt,
        ftd_datetime   AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ftd_brt,
        ftd_amount_inhouse,
        CAST(signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS signup_date_brt,
        CAST(ftd_datetime    AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS ftd_date_brt
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN {GOOGLE_IDS}
      AND (is_test = false OR is_test IS NULL)
),
dep_hoje AS (
    SELECT
        c_ecr_id,
        COUNT(*)                                              AS qty_dep_hoje,
        SUM(c_confirmed_amount_in_ecr_ccy) / 100.0            AS total_dep_hoje_brl,
        MAX(c_confirmed_amount_in_ecr_ccy) / 100.0            AS maior_dep_hoje_brl,
        MIN(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_dep_brt,
        MAX(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_dep_brt
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_txn_status = 'txn_confirmed_success'
      AND CAST(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{HOJE}'
    GROUP BY c_ecr_id
)
SELECT
    g.external_id,
    g.ecr_id,
    g.affiliate_id,
    g.email,
    g.mobile_number,
    g.signup_date_brt,
    g.signup_brt,
    g.ftd_date_brt,
    g.ftd_brt,
    g.ftd_amount_inhouse    AS ftd_amount,
    COALESCE(d.qty_dep_hoje, 0)           AS qty_dep_hoje,
    COALESCE(d.total_dep_hoje_brl, 0)     AS total_dep_hoje_brl,
    COALESCE(d.maior_dep_hoje_brl, 0)     AS maior_dep_hoje_brl,
    d.primeiro_dep_brt,
    d.ultimo_dep_brt,
    CASE WHEN g.signup_date_brt = DATE '{HOJE}' THEN 'SIM' ELSE 'NAO' END AS reg_hoje,
    CASE WHEN g.ftd_date_brt    = DATE '{HOJE}' THEN 'SIM' ELSE 'NAO' END AS ftd_hoje
FROM google_cohort g
LEFT JOIN dep_hoje d ON d.c_ecr_id = g.ecr_id
WHERE g.signup_date_brt = DATE '{HOJE}'
   OR g.ftd_date_brt    = DATE '{HOJE}'
ORDER BY total_dep_hoje_brl DESC, g.signup_brt DESC
"""

print("[1/2] Buscando REGs e FTDs de hoje em Google...")
df = query_athena(sql, database="ps_bi")
print(f"     {len(df)} players encontrados (REG hoje OU FTD hoje)\n")

if df.empty:
    print(">>> Nenhum REG/FTD hoje em Google ate agora.\n")
    sys.exit(0)

# =====================================================================
# 2) Jogo principal de HOJE por player (ps_bi.fct_casino_activity_daily)
# =====================================================================
ecr_ids = ",".join(str(int(x)) for x in df["ecr_id"].tolist())

sql_game = f"""
WITH activity AS (
    SELECT
        a.player_id,
        a.game_id,
        a.bet_amount_local,
        a.ggr_local,
        a.bet_count,
        ROW_NUMBER() OVER (
            PARTITION BY a.player_id
            ORDER BY a.bet_amount_local DESC NULLS LAST
        ) AS rn_bet
    FROM ps_bi.fct_casino_activity_daily a
    WHERE a.activity_date = DATE '{HOJE}'
      AND a.player_id IN ({ecr_ids})
      AND a.bet_count > 0
)
SELECT
    a.player_id,
    a.game_id,
    a.bet_amount_local  AS top_game_bet,
    a.ggr_local         AS top_game_ggr,
    a.bet_count         AS top_game_bet_count
FROM activity a
WHERE a.rn_bet = 1
"""

print("[2/2] Buscando jogo principal de hoje...")
df_game = query_athena(sql_game, database="ps_bi")
df = df.merge(df_game, left_on="ecr_id", right_on="player_id", how="left")

# =====================================================================
# 3) Enriquecer game_id com nome (Super Nova — game_image_mapping)
# =====================================================================
game_ids_unicos = [str(x) for x in df["game_id"].dropna().unique().tolist() if str(x) not in ("nan", "0")]
game_map = {}
if game_ids_unicos:
    in_list = ",".join([f"'{x}'" for x in game_ids_unicos])
    rows = execute_supernova(f"""
        SELECT provider_game_id, game_name, provider_display_name, game_category_front
        FROM multibet.game_image_mapping
        WHERE provider_game_id IN ({in_list})
    """, fetch=True)
    game_map = {r[0]: {"name": r[1], "provider": r[2], "category": r[3]} for r in rows}

def game_name(gid):
    if pd.isna(gid) or str(gid) == "nan":
        return "-"
    if str(gid) == "0":
        return "Sportsbook (altenar)"
    info = game_map.get(str(gid))
    if info:
        return f"{info['name']} ({info['provider']})"
    return f"game_id={gid}"

df["jogo"] = df["game_id"].apply(game_name)

# =====================================================================
# 4) Resumo e print
# =====================================================================
def brl(v):
    if pd.isna(v):
        return "-"
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

regs_hoje = df[df["reg_hoje"] == "SIM"]
ftds_hoje = df[df["ftd_hoje"] == "SIM"]
reg_ftd_hoje = df[(df["reg_hoje"] == "SIM") & (df["ftd_hoje"] == "SIM")]

# Split por affiliate
print("="*82)
print("  RESUMO — COHORT GOOGLE DE HOJE")
print("="*82)
print(f"  REG hoje (registraram)...............: {len(regs_hoje)}")
print(f"  FTD hoje (primeiro deposito).........: {len(ftds_hoje)}")
print(f"  REG + FTD no MESMO dia (fresh).......: {len(reg_ftd_hoje)}")
print(f"  Total depositado pelo cohort hoje....: {brl(df['total_dep_hoje_brl'].sum())}")
print(f"  Soma ftd_amount do cohort............: {brl(df['ftd_amount'].sum())}")
print("="*82)

# Por affiliate
print("\n  Distribuicao por affiliate (REG ou FTD hoje):")
by_aff = df.groupby("affiliate_id").agg(
    players=("ecr_id", "count"),
    reg_hoje=("reg_hoje", lambda s: (s == "SIM").sum()),
    ftd_hoje=("ftd_hoje", lambda s: (s == "SIM").sum()),
    dep_total=("total_dep_hoje_brl", "sum"),
).reset_index().sort_values("dep_total", ascending=False)
for _, r in by_aff.iterrows():
    print(f"    {r['affiliate_id']:>8}  players={r['players']:>3}  REG={r['reg_hoje']:>3}  FTD={r['ftd_hoje']:>3}  dep_hoje={brl(r['dep_total'])}")

# =====================================================================
# LISTA COMPLETA
# =====================================================================
print(f"\n{'='*82}")
print(f"  DETALHAMENTO — {len(df)} players (REG hoje OU FTD hoje)")
print(f"{'='*82}\n")

def tipo(row):
    if row["reg_hoje"] == "SIM" and row["ftd_hoje"] == "SIM":
        return "REG+FTD"
    if row["reg_hoje"] == "SIM":
        return "REG"
    if row["ftd_hoje"] == "SIM":
        return "FTD"
    return "-"

df["tipo"] = df.apply(tipo, axis=1)

print(f"{'Tipo':<9}{'ExtID':<18}{'Aff':<8}{'REG hora':<9}{'FTD hora':<9}{'FTD_amt':>11}{'Dep hoje':>13}{'Jogo':<35}")
print("-"*112)

df_sorted = df.sort_values(["tipo", "total_dep_hoje_brl"], ascending=[True, False])
for _, row in df_sorted.iterrows():
    reg_h = row["signup_brt"].strftime("%H:%M") if pd.notna(row["signup_brt"]) and row["reg_hoje"] == "SIM" else "-"
    ftd_h = row["ftd_brt"].strftime("%H:%M")    if pd.notna(row["ftd_brt"])    and row["ftd_hoje"] == "SIM" else "-"
    ftd_a = brl(row["ftd_amount"]) if row["ftd_hoje"] == "SIM" and pd.notna(row["ftd_amount"]) else "-"
    dep_h = brl(row["total_dep_hoje_brl"]) if row["total_dep_hoje_brl"] > 0 else "-"
    jogo  = str(row.get("jogo") or "-")[:32]
    print(f"{row['tipo']:<9}{str(row['external_id'])[:16]:<18}{str(row['affiliate_id']):<8}{reg_h:<9}{ftd_h:<9}{ftd_a:>11}{dep_h:>13}  {jogo:<35}")

# =====================================================================
# CSV entrega
# =====================================================================
cols = [
    "tipo", "external_id", "ecr_id", "affiliate_id", "email", "mobile_number",
    "signup_date_brt", "signup_brt", "ftd_date_brt", "ftd_brt", "ftd_amount",
    "reg_hoje", "ftd_hoje",
    "qty_dep_hoje", "total_dep_hoje_brl", "maior_dep_hoje_brl",
    "primeiro_dep_brt", "ultimo_dep_brt",
    "game_id", "jogo", "top_game_bet", "top_game_ggr", "top_game_bet_count",
]
out_csv = f"reports/google_reg_ftd_{HOJE}_{datetime.now(BRT).strftime('%Hh%M')}.csv"
df_sorted[cols].to_csv(out_csv, index=False, encoding="utf-8-sig")
print(f"\n  CSV: {out_csv}")
print(f"\n  Fonte: Athena | ps_bi.dim_user + cashier_ec2 + fct_casino_activity_daily | game_image_mapping (SN)")
print(f"  ATENCAO: snapshot intraday — ftd_datetime/dim_user tem delay ETL; cashier e near-real-time.\n")
