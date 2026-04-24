"""
Top Depositos de HOJE (todas as fontes) com affiliate — 24/04/2026

Responde:
- Quais sao os maiores depositos (transacao unica) de hoje?
- Qual affiliate trouxe cada um?
- Qual jogo estao jogando?

Fontes (100% Athena):
- cashier_ec2.tbl_cashier_deposit   -> depositos confirmados intraday
- ps_bi.dim_user                    -> affiliate_id, email, phone
- multibet.dim_marketing_mapping    -> classificacao da fonte (google/meta/tiktok/etc)
- ps_bi.fct_casino_activity_daily   -> top jogo do dia

Uso:
    python scripts/check_top_deposits_today.py
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

print(f"\n{'='*90}")
print(f"  MAIORES DEPOSITOS DE HOJE — {HOJE} (snapshot {AGORA} BRT)")
print(f"{'='*90}\n")

# =====================================================================
# 1) Top transacoes unicas + top depositantes
# =====================================================================
sql = f"""
WITH dep_raw AS (
    SELECT
        c_ecr_id,
        c_confirmed_amount_in_ecr_ccy / 100.0  AS valor_brl,
        c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS hora_brt,
        c_option        AS metodo,
        c_processor_name AS processor
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_txn_status = 'txn_confirmed_success'
      AND CAST(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{HOJE}'
),
top_single AS (
    SELECT
        c_ecr_id,
        valor_brl,
        hora_brt,
        metodo,
        processor,
        ROW_NUMBER() OVER (ORDER BY valor_brl DESC) AS rn_global
    FROM dep_raw
),
agg_player AS (
    SELECT
        c_ecr_id,
        COUNT(*)          AS qty_dep,
        SUM(valor_brl)    AS total_brl,
        MAX(valor_brl)    AS maior_dep_brl,
        MIN(hora_brt)     AS primeiro_dep,
        MAX(hora_brt)     AS ultimo_dep
    FROM dep_raw
    GROUP BY c_ecr_id
),
all_ecr AS (
    SELECT c_ecr_id FROM agg_player
)
SELECT
    a.c_ecr_id                     AS ecr_id,
    u.external_id,
    CAST(u.affiliate_id AS VARCHAR) AS affiliate_id,
    u.email,
    u.mobile_number,
    CAST(u.signup_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS signup_date,
    CAST(u.ftd_datetime    AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) AS ftd_date,
    u.ftd_amount_inhouse           AS ftd_amount,
    a.qty_dep,
    a.total_brl,
    a.maior_dep_brl,
    a.primeiro_dep,
    a.ultimo_dep
FROM agg_player a
LEFT JOIN ps_bi.dim_user u ON u.ecr_id = a.c_ecr_id
WHERE (u.is_test = false OR u.is_test IS NULL)
ORDER BY a.total_brl DESC
LIMIT 30
"""

print("[1/3] Buscando top depositantes de hoje (todas fontes)...")
df_players = query_athena(sql, database="ps_bi")
print(f"     {len(df_players)} top depositantes.\n")

# Buscar tambem top transacoes unicas (maior deposito individual)
sql_single = f"""
WITH dep AS (
    SELECT
        c_ecr_id,
        c_confirmed_amount_in_ecr_ccy / 100.0 AS valor_brl,
        c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS hora_brt,
        c_option AS metodo,
        c_processor_name AS processor
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_txn_status = 'txn_confirmed_success'
      AND CAST(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{HOJE}'
)
SELECT
    d.c_ecr_id                      AS ecr_id,
    u.external_id,
    CAST(u.affiliate_id AS VARCHAR) AS affiliate_id,
    d.valor_brl,
    d.hora_brt,
    d.metodo,
    d.processor
FROM dep d
LEFT JOIN ps_bi.dim_user u ON u.ecr_id = d.c_ecr_id
WHERE (u.is_test = false OR u.is_test IS NULL)
ORDER BY d.valor_brl DESC
LIMIT 20
"""

print("[2/3] Buscando top transacoes unicas...")
df_single = query_athena(sql_single, database="ps_bi")
print(f"     {len(df_single)} transacoes top.\n")

# =====================================================================
# 2) Classificar affiliate (Google/Meta/TikTok/Organic/Outros)
# =====================================================================
# Puxar mapeamento do Super Nova
all_affs = set(df_players["affiliate_id"].dropna().astype(str).tolist() +
               df_single["affiliate_id"].dropna().astype(str).tolist())
if all_affs:
    in_list = ",".join([f"'{a}'" for a in all_affs])
    rows = execute_supernova(f"""
        SELECT DISTINCT affiliate_id, source_name, partner_name
        FROM multibet.dim_marketing_mapping
        WHERE affiliate_id IN ({in_list})
    """, fetch=True)
    # Dedup: afiliado pode ter multiplos trackers — agrupar source_name mais comum
    from collections import Counter, defaultdict
    src_per_aff = defaultdict(Counter)
    partner_per_aff = defaultdict(Counter)
    for aff, src, partner in rows:
        if src:
            src_per_aff[aff][src] += 1
        if partner:
            partner_per_aff[aff][partner] += 1
    aff_map = {}
    for aff in src_per_aff:
        src = src_per_aff[aff].most_common(1)[0][0] if src_per_aff[aff] else "unmapped"
        partner = partner_per_aff[aff].most_common(1)[0][0] if partner_per_aff[aff] else ""
        aff_map[aff] = (src, partner)
else:
    aff_map = {}

def classify(aff_id):
    aff = str(aff_id) if pd.notna(aff_id) else ""
    if aff == "0" or aff == "nan" or aff == "":
        return ("organic/direct", "")
    info = aff_map.get(aff)
    if info:
        return info
    return ("unmapped", "")

df_players[["source", "partner"]] = df_players["affiliate_id"].apply(lambda x: pd.Series(classify(x)))
df_single[["source", "partner"]]  = df_single["affiliate_id"].apply(lambda x: pd.Series(classify(x)))

# =====================================================================
# 3) Jogo principal por player (top depositantes)
# =====================================================================
ecr_ids = ",".join(str(int(x)) for x in df_players["ecr_id"].tolist())
sql_game = f"""
WITH activity AS (
    SELECT
        a.player_id, a.game_id, a.bet_amount_local,
        ROW_NUMBER() OVER (PARTITION BY a.player_id ORDER BY a.bet_amount_local DESC NULLS LAST) AS rn
    FROM ps_bi.fct_casino_activity_daily a
    WHERE a.activity_date = DATE '{HOJE}'
      AND a.player_id IN ({ecr_ids})
      AND a.bet_count > 0
)
SELECT player_id, game_id, bet_amount_local AS top_bet
FROM activity WHERE rn = 1
"""
print("[3/3] Buscando jogo principal dos top depositantes...")
df_game = query_athena(sql_game, database="ps_bi")
df_players = df_players.merge(df_game, left_on="ecr_id", right_on="player_id", how="left")

# Enriquecer nome jogo
game_ids = [str(x) for x in df_players["game_id"].dropna().unique().tolist() if str(x) not in ("nan", "0")]
game_map = {}
if game_ids:
    in_list = ",".join([f"'{x}'" for x in game_ids])
    rows = execute_supernova(f"""
        SELECT provider_game_id, game_name, provider_display_name
        FROM multibet.game_image_mapping
        WHERE provider_game_id IN ({in_list})
    """, fetch=True)
    game_map = {r[0]: (r[1], r[2]) for r in rows}

def game_name(gid):
    if pd.isna(gid) or str(gid) == "nan":
        return "-"
    if str(gid) == "0":
        return "Sportsbook (altenar)"
    info = game_map.get(str(gid))
    if info:
        return f"{info[0]} ({info[1]})"
    return f"game_id={gid}"

df_players["jogo"] = df_players["game_id"].apply(game_name)

# =====================================================================
# PRINT — 1) Top transacoes unicas
# =====================================================================
def brl(v):
    if pd.isna(v): return "-"
    return f"R$ {float(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

print("="*90)
print("  TOP 10 DEPOSITOS UNICOS (transacao) — HOJE")
print("="*90)
print(f"\n  {'#':<3}{'ExtID':<18}{'Aff':<8}{'Source':<16}{'Partner':<22}{'Valor':>13}{'Hora':>8}{'Metodo':>9}")
print("  " + "-"*97)
for i, row in df_single.head(10).iterrows():
    partner = str(row.get("partner") or "")[:20]
    source = str(row.get("source") or "unmapped")[:14]
    metodo = str(row.get("metodo") or "-")[:7]
    hora = row["hora_brt"].strftime("%H:%M") if pd.notna(row["hora_brt"]) else "-"
    print(f"  {i+1:<3}{str(row['external_id'])[:16]:<18}{str(row['affiliate_id']):<8}{source:<16}{partner:<22}{brl(row['valor_brl']):>13}{hora:>8}{metodo:>9}")

# =====================================================================
# PRINT — 2) Top depositantes (total dia)
# =====================================================================
print("\n" + "="*90)
print("  TOP 15 DEPOSITANTES (SOMA DO DIA) — HOJE")
print("="*90)
print(f"\n  {'#':<3}{'ExtID':<18}{'Aff':<8}{'Source':<14}{'Partner':<20}{'Total':>13}{'Maior':>12}{'Qty':>4}{'Jogo':<28}")
print("  " + "-"*120)
for i, row in df_players.head(15).iterrows():
    partner = str(row.get("partner") or "")[:18]
    source = str(row.get("source") or "unmapped")[:12]
    jogo = str(row.get("jogo") or "-")[:26]
    print(f"  {i+1:<3}{str(row['external_id'])[:16]:<18}{str(row['affiliate_id']):<8}{source:<14}{partner:<20}{brl(row['total_brl']):>13}{brl(row['maior_dep_brl']):>12}{int(row['qty_dep']):>4}  {jogo:<28}")

# =====================================================================
# Agregado por fonte
# =====================================================================
print("\n" + "="*90)
print("  DISTRIBUICAO POR FONTE (top 15 depositantes)")
print("="*90)
by_src = df_players.head(15).groupby("source").agg(
    players=("ecr_id", "count"),
    total=("total_brl", "sum"),
    maior=("maior_dep_brl", "max")
).reset_index().sort_values("total", ascending=False)
print(f"\n  {'Source':<20}{'Players':>9}{'Total':>15}{'Maior dep unico':>20}")
print("  " + "-"*64)
for _, r in by_src.iterrows():
    print(f"  {r['source']:<20}{int(r['players']):>9}{brl(r['total']):>15}{brl(r['maior']):>20}")

# CSVs
out1 = f"reports/top_deposits_single_{HOJE}_{datetime.now(BRT).strftime('%Hh%M')}.csv"
out2 = f"reports/top_depositors_total_{HOJE}_{datetime.now(BRT).strftime('%Hh%M')}.csv"
df_single.to_csv(out1, index=False, encoding="utf-8-sig")
df_players.to_csv(out2, index=False, encoding="utf-8-sig")
print(f"\n  CSVs: {out1}")
print(f"        {out2}")
print(f"\n  Fonte: Athena | cashier_ec2 + ps_bi.dim_user + fct_casino_activity_daily")
print(f"  Classificacao source: multibet.dim_marketing_mapping (SN)\n")
