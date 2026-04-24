"""
Check Whales Google (intraday) — 24/04/2026

Responde ao gestor de trafego:
- Top depositos de hoje (BRT) vindos da fonte Google Ads
- Identifica whales, FTDs e REGs
- Mostra affiliate_id, jogo principal jogado hoje, valor depositado

Fontes (100% Athena):
- cashier_ec2.tbl_cashier_deposit  -> depositos confirmados intraday
- ps_bi.dim_user                   -> affiliate_id, signup, FTD, email, telefone
- ps_bi.fct_casino_activity_daily  -> top jogo do dia por player

Google affiliate_ids canonicos (scripts/extract_affiliates_report.py):
  297657 (elisa_google), 445431 (Google_Eyal), 468114 (MULTIBET APP)

Uso:
    python scripts/check_whales_google_today.py
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")

import warnings
warnings.filterwarnings("ignore")

from datetime import datetime
import pytz
import pandas as pd

from db.athena import query_athena

BRT = pytz.timezone("America/Sao_Paulo")
HOJE = datetime.now(BRT).strftime("%Y-%m-%d")
AGORA = datetime.now(BRT).strftime("%H:%M")

GOOGLE_IDS = "('297657', '445431', '468114')"

print(f"\n{'='*78}")
print(f"  WHALE/FTD CHECK — Fonte Google — HOJE {HOJE} (snapshot {AGORA} BRT)")
print(f"  Affiliate IDs Google: 297657, 445431, 468114")
print(f"{'='*78}\n")

# =====================================================================
# 1) TOP DEPOSITOS DE HOJE vindos de Google
# =====================================================================
sql_dep = f"""
WITH google_users AS (
    SELECT
        ecr_id,
        external_id,
        affiliate_id,
        email,
        mobile_number,
        signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS signup_brt,
        ftd_datetime   AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ftd_brt,
        ftd_amount_inhouse
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN {GOOGLE_IDS}
      AND (is_test = false OR is_test IS NULL)
),
deposits_hoje AS (
    SELECT
        c_ecr_id,
        COUNT(*)                                              AS qty_dep,
        SUM(c_confirmed_amount_in_ecr_ccy) / 100.0            AS total_brl,
        MAX(c_confirmed_amount_in_ecr_ccy) / 100.0            AS maior_dep_brl,
        MIN(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_dep_brt,
        MAX(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_dep_brt
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_txn_status = 'txn_confirmed_success'
      AND CAST(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{HOJE}'
    GROUP BY c_ecr_id
)
SELECT
    g.ecr_id,
    g.external_id,
    g.affiliate_id,
    g.email,
    g.mobile_number,
    CAST(g.signup_brt AS DATE)        AS signup_date,
    CAST(g.ftd_brt    AS DATE)        AS ftd_date,
    g.ftd_amount_inhouse              AS ftd_amount,
    d.qty_dep,
    d.total_brl,
    d.maior_dep_brl,
    d.primeiro_dep_brt,
    d.ultimo_dep_brt,
    CASE WHEN CAST(g.signup_brt AS DATE) = DATE '{HOJE}' THEN 'SIM' ELSE 'NAO' END AS reg_hoje,
    CASE WHEN CAST(g.ftd_brt    AS DATE) = DATE '{HOJE}' THEN 'SIM' ELSE 'NAO' END AS ftd_hoje
FROM google_users g
INNER JOIN deposits_hoje d ON d.c_ecr_id = g.ecr_id
ORDER BY d.total_brl DESC
LIMIT 30
"""

print("[1/2] Buscando depositos de hoje vindos de Google...")
df = query_athena(sql_dep, database="ps_bi")

if df.empty:
    print("\n>>> NENHUM deposito confirmado hoje de players com affiliate_id Google.\n")
    sys.exit(0)

print(f"     {len(df)} players com deposito hoje.\n")

# =====================================================================
# 2) TOP JOGO DE HOJE por player (ps_bi.fct_casino_activity_daily)
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
    COALESCE(g.game_desc, a.game_id)  AS game_desc,
    g.game_category,
    g.vendor_id,
    a.bet_amount_local                AS top_game_bet,
    a.ggr_local                       AS top_game_ggr,
    a.bet_count                       AS top_game_bet_count
FROM activity a
LEFT JOIN ps_bi.dim_game g ON g.game_id = a.game_id
WHERE a.rn_bet = 1
"""

print("[2/2] Buscando jogo principal de hoje por player...")
df_game = query_athena(sql_game, database="ps_bi")
print(f"     {len(df_game)} players com atividade casino hoje.\n")

df = df.merge(df_game, left_on="ecr_id", right_on="player_id", how="left")

# Resumo agregado
total_dep = df["total_brl"].sum()
n_players = len(df)
n_reg_hoje = (df["reg_hoje"] == "SIM").sum()
n_ftd_hoje = (df["ftd_hoje"] == "SIM").sum()
maior = df["maior_dep_brl"].max()

def brl(v):
    if pd.isna(v):
        return "-"
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

print("="*78)
print("  RESUMO GOOGLE — HOJE")
print("="*78)
print(f"  Players depositantes......: {n_players}")
print(f"  Registros hoje (REG)......: {n_reg_hoje}")
print(f"  FTDs hoje.................: {n_ftd_hoje}")
print(f"  Total depositado..........: {brl(total_dep)}")
print(f"  Maior deposito unico......: {brl(maior)}")
print("="*78)

# Top 15 whales
print("\n  TOP 15 DEPOSITANTES HOJE — Google\n")
print(f"  {'#':<3}{'Player(ext_id)':<18}{'Aff':<8}{'Total BRL':>14}{'Maior':>12}{'REG':>5}{'FTD':>5}{'FTD_amt':>11}{'Jogo (vendor)':<35}")
print("  " + "-"*118)

for i, row in df.head(15).iterrows():
    jogo = str(row.get("game_desc") or "-")[:22]
    vendor = str(row.get("vendor_id") or "-")[:10]
    jogo_label = f"{jogo} ({vendor})" if vendor != "-" else jogo
    ftd_amt = row.get("ftd_amount")
    ftd_amt_str = brl(ftd_amt) if pd.notna(ftd_amt) and float(ftd_amt) > 0 else "-"
    print(
        f"  {i+1:<3}"
        f"{str(row['external_id'])[:16]:<18}"
        f"{str(row['affiliate_id']):<8}"
        f"{brl(row['total_brl']):>14}"
        f"{brl(row['maior_dep_brl']):>12}"
        f"{row['reg_hoje']:>5}"
        f"{row['ftd_hoje']:>5}"
        f"{ftd_amt_str:>11}  "
        f"{jogo_label:<35}"
    )

# Salva CSV completo
out_csv = f"reports/whales_google_{HOJE}_{datetime.now(BRT).strftime('%Hh%M')}.csv"
cols = [
    "external_id", "ecr_id", "affiliate_id", "email", "mobile_number",
    "signup_date", "reg_hoje", "ftd_date", "ftd_hoje", "ftd_amount",
    "qty_dep", "total_brl", "maior_dep_brl",
    "primeiro_dep_brt", "ultimo_dep_brt",
    "game_desc", "game_category", "vendor_id",
    "top_game_bet", "top_game_ggr", "top_game_bet_count",
]
df[cols].to_csv(out_csv, index=False, encoding="utf-8-sig")
print(f"\n  CSV completo: {out_csv}")
print(f"\n  Fonte: Athena | cashier_ec2 + ps_bi.dim_user + ps_bi.fct_casino_activity_daily")
print(f"  ATENCAO: D-0 e intraday — dados parciais (ETL ps_bi pode ter delay; cashier e near-real-time).\n")
