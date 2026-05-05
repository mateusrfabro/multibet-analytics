"""
Validacao do salto +44% no PCR (128k -> 185k).

Cross-check com 3 fontes INDEPENDENTES da bireports pra confirmar
que os 185k players ativos last_90d sao reais e nao tem duplicidade
ou contaminacao.

Fontes de validacao:
  1) Athena raw — fund_ec2.tbl_real_fund_txn (transacional)
  2) Super Nova DB — multibet.fct_active_players_by_period (silver Gusta)
  3) Super Nova DB — multibet.fct_player_performance_by_period (silver Gusta)
  4) Super Nova DB — multibet.fact_gaming_activity_daily (silver alternativa)

Decomposicao dos 56k 'novos':
  A) Cadastrados antes de 06/04 mas sem atividade na fato (dormentes que voltaram)
  B) Cadastrados depois de 06/04 (totalmente novos no PCR)
  C) Cadastrados antes mas com is_test/closed/etc

Valida tambem:
  - is_test=true incluido por engano?
  - c_category=closed/fraud incluido?
  - duplicidades?
"""
import sys
sys.path.insert(0, ".")

from datetime import datetime
from pathlib import Path
import pandas as pd
from db.athena import query_athena
from db.supernova import execute_supernova

SNAPSHOT = datetime.now().strftime("%Y-%m-%d")
PLAYER_VICTOR = 305245081792208985

print("=" * 70)
print("VALIDACAO DO SALTO PCR (128k -> 185k = +44%)")
print("=" * 70)

# ----------------------------------------------------------------------
# 1) Cross-check com fund_ec2 (raw transacional 100% atualizado)
# ----------------------------------------------------------------------
print("\n[1] Cross-check com fund_ec2 (raw transacional)")
sql = """
WITH ativos_fund AS (
    SELECT DISTINCT t.c_ecr_id AS player_id
    FROM fund_ec2.tbl_real_fund_txn t
    WHERE t.c_start_time >= CAST(CURRENT_DATE - INTERVAL '90' DAY AS TIMESTAMP)
      AND t.c_start_time <  CAST(CURRENT_DATE AS TIMESTAMP)
      AND t.c_txn_status = 'SUCCESS'
)
SELECT COUNT(*) AS total_players_ativos_fund
FROM ativos_fund
"""
df = query_athena(sql)
total_fund = int(df.iloc[0, 0])
print(f"  fund_ec2 (any txn last 90d): {total_fund:,}")
print(f"  PCR novo:                    185,098")
print(f"  Delta: {185098 - total_fund:,} ({(185098 - total_fund) / total_fund * 100:+.1f}%)")

# ----------------------------------------------------------------------
# 2) Cross-check com bireports detalhado
# ----------------------------------------------------------------------
print("\n[2] Detalhamento bireports — quem entra no PCR novo")
sql = """
SELECT
    COUNT(DISTINCT c_ecr_id) AS total,
    COUNT(DISTINCT CASE WHEN c_casino_realcash_bet_count > 0 THEN c_ecr_id END) AS ativos_casino,
    COUNT(DISTINCT CASE WHEN c_sb_realcash_bet_count > 0 THEN c_ecr_id END) AS ativos_sport,
    COUNT(DISTINCT CASE WHEN c_deposit_success_count > 0 THEN c_ecr_id END) AS depositaram,
    COUNT(DISTINCT CASE WHEN c_casino_realcash_bet_count > 0
                          OR c_sb_realcash_bet_count > 0
                          OR c_deposit_success_count > 0 THEN c_ecr_id END) AS qualificados_pcr
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
WHERE c_created_date >= CURRENT_DATE - INTERVAL '90' DAY
  AND c_created_date < CURRENT_DATE
"""
df = query_athena(sql)
print(df.to_string(index=False))

# ----------------------------------------------------------------------
# 3) Decompor os 56.450 "novos" no PCR
# ----------------------------------------------------------------------
print("\n[3] Decomposicao dos 56.450 NOVOS (PCR novo \\ PCR atual)")

# Pega snapshot atual
rows = execute_supernova(
    """
    WITH ult AS (SELECT MAX(snapshot_date) AS d FROM multibet.pcr_ratings)
    SELECT player_id FROM multibet.pcr_ratings, ult
    WHERE snapshot_date = ult.d
    """,
    fetch=True,
)
ids_atual = set(int(r[0]) for r in rows)
print(f"  IDs no PCR atual (snapshot ultimo dia): {len(ids_atual):,}")

# Lista de players novos no PCR (185k - 128k = 56k)
ids_csv_atuais = ",".join(str(p) for p in list(ids_atual)[:1])  # placeholder

# Pega quem entra no PCR novo
sql = """
WITH player_novo AS (
    SELECT s.c_ecr_id AS player_id,
           MAX(s.c_created_date) AS last_active,
           MIN(s.c_created_date) AS first_active
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    WHERE s.c_created_date >= CURRENT_DATE - INTERVAL '90' DAY
      AND s.c_created_date < CURRENT_DATE
    GROUP BY s.c_ecr_id
    HAVING SUM(s.c_casino_realcash_bet_count) > 0
        OR SUM(s.c_sb_realcash_bet_count) > 0
        OR SUM(s.c_deposit_success_count) > 0
)
SELECT pn.player_id,
       pn.first_active, pn.last_active,
       u.registration_date,
       u.is_test,
       u.affiliate_id,
       ecr.c_category
FROM player_novo pn
LEFT JOIN ps_bi.dim_user u ON pn.player_id = u.ecr_id
LEFT JOIN (
    SELECT c_ecr_id, c_category
    FROM (
        SELECT c_ecr_id, c_category,
               ROW_NUMBER() OVER (PARTITION BY c_ecr_id
                                   ORDER BY c_category_updated_time DESC NULLS LAST,
                                            c_ecr_id DESC) rn
        FROM bireports_ec2.tbl_ecr
    ) WHERE rn = 1
) ecr ON pn.player_id = ecr.c_ecr_id
WHERE (u.is_test = false OR u.is_test IS NULL)
"""
print("  Carregando PCR novo do Athena (pode levar alguns segundos)...")
df_novo = query_athena(sql)
df_novo["player_id"] = df_novo["player_id"].astype("int64")
print(f"  PCR novo (apos filtro is_test): {len(df_novo):,}")

# Marca quem e novo
df_novo["status"] = df_novo["player_id"].apply(
    lambda x: "JA_EXISTIA" if x in ids_atual else "NOVO"
)
print(f"\n  Distribuicao:")
print(df_novo["status"].value_counts().to_string())

# Decomposicao dos NOVOS por registration_date
print("\n[4] Decomposicao dos NOVOS por registration_date")
df_novos = df_novo[df_novo["status"] == "NOVO"].copy()
df_novos["registration_date"] = pd.to_datetime(df_novos["registration_date"], errors="coerce")

cuts = [
    ("Cadastro >= 06/04 (totalmente novos)", df_novos["registration_date"] >= pd.Timestamp("2026-04-06")),
    ("Cadastro entre 01/01 e 06/04 (nao tinham historico na fato)", (df_novos["registration_date"] >= pd.Timestamp("2026-01-01")) & (df_novos["registration_date"] < pd.Timestamp("2026-04-06"))),
    ("Cadastro 2025 (dormentes que voltaram)", (df_novos["registration_date"] >= pd.Timestamp("2025-01-01")) & (df_novos["registration_date"] < pd.Timestamp("2026-01-01"))),
    ("Cadastro <2025 (muito antigos)", df_novos["registration_date"] < pd.Timestamp("2025-01-01")),
    ("Sem registration_date (orfaos)", df_novos["registration_date"].isna()),
]
for label, mask in cuts:
    n = mask.sum()
    pct = n / len(df_novos) * 100 if len(df_novos) > 0 else 0
    print(f"  {label}: {n:,} ({pct:.1f}%)")

# Decomposicao por c_category
print("\n[5] Decomposicao dos NOVOS por c_category")
cat_dist = df_novos["c_category"].value_counts(dropna=False)
for cat, n in cat_dist.items():
    pct = n / len(df_novos) * 100
    print(f"  {str(cat):<25} {n:,} ({pct:.1f}%)")

# Players cadastrados depois de 06/04 — sanity check com pelo dia
print("\n[6] Sanity check — players cadastrados pos 06/04 por mes")
pos_0604 = df_novos[df_novos["registration_date"] >= pd.Timestamp("2026-04-06")].copy()
pos_0604["mes"] = pos_0604["registration_date"].dt.strftime("%Y-%m")
print(pos_0604["mes"].value_counts().sort_index().to_string())

# ----------------------------------------------------------------------
# 7) Cross-check com silvers do Super Nova DB
# ----------------------------------------------------------------------
print("\n[7] Cross-check com silvers Super Nova DB (multibet.fct_*)")

try:
    rows = execute_supernova(
        """
        SELECT vertical, COUNT(*) AS n_players, MAX(refreshed_at) AS atualizada_em
        FROM multibet.fct_player_performance_by_period
        WHERE period = 'last_90d'
        GROUP BY vertical
        ORDER BY vertical
        """,
        fetch=True,
    )
    print("\n  multibet.fct_player_performance_by_period (last_90d):")
    total_silver_perf = 0
    for r in rows:
        print(f"    {r[0]:<10} {int(r[1]):,} players (atualizado em {r[2]})")
        if r[0] != "both":  # 'both' e subset, nao soma
            pass

    # Total UNICOS = casino + sports - both
    rows2 = execute_supernova(
        """
        WITH casino_or_sport AS (
            SELECT user_id FROM multibet.fct_player_performance_by_period
            WHERE period = 'last_90d' AND vertical IN ('casino','sports')
            GROUP BY user_id
        )
        SELECT COUNT(*) FROM casino_or_sport
        """,
        fetch=True,
    )
    total_silver = int(rows2[0][0])
    print(f"\n  TOTAL UNICOS (casino UNION sports): {total_silver:,}")
    print(f"  PCR novo:                            185,098")
    print(f"  Delta:                               {185098 - total_silver:+,} ({(185098 - total_silver)/total_silver*100:+.1f}%)")
except Exception as e:
    print(f"  Erro: {e}")

# Active players period
try:
    rows = execute_supernova(
        """
        SELECT product, players_count, refreshed_at
        FROM multibet.fct_active_players_by_period
        WHERE period = 'last_90d'
        ORDER BY product
        """,
        fetch=True,
    )
    print("\n  multibet.fct_active_players_by_period (last_90d):")
    for r in rows:
        print(f"    {r[0]:<10} {int(r[1]):,} players (atualizado em {r[2]})")
except Exception as e:
    print(f"  Erro: {e}")

# ----------------------------------------------------------------------
# 8) Salva evidencia
# ----------------------------------------------------------------------
out = Path("reports") / f"validar_salto_pcr_{SNAPSHOT}.csv"
df_novos.to_csv(out, sep=";", decimal=",", index=False, encoding="utf-8-sig")
print(f"\n  Evidencia (56k novos detalhados): {out}")
print("=" * 70)
