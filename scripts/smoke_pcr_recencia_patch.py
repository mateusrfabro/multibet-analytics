"""
Smoke test do patch PCR (recencia via bireports_ec2).

Compara recency_days ANTES vs DEPOIS para:
  - Player do Victor (305245081792208985) — S, recencia atual 29d
  - Top 20 players por recency_days do snapshot atual em multibet.pcr_ratings

Saida: CSV em reports/smoke_pcr_recencia_patch_YYYYMMDD.csv
"""
import sys
sys.path.insert(0, ".")

from datetime import datetime
from pathlib import Path
import pandas as pd
from db.athena import query_athena
from db.supernova import execute_supernova

PLAYER_VICTOR = 305245081792208985
JANELA_DIAS = 90
SNAPSHOT = datetime.now().strftime("%Y-%m-%d")

# 1. Pega 50 players A+S do snapshot mais recente (variedade alta de recencia)
print("[1] Carregando amostra A+S de multibet.pcr_ratings (snapshot mais recente)...")
rows = execute_supernova(
    """
    WITH ult AS (SELECT MAX(snapshot_date) AS d FROM multibet.pcr_ratings)
    SELECT player_id, rating, recency_days
    FROM multibet.pcr_ratings, ult
    WHERE snapshot_date = ult.d
      AND rating IN ('S','A')
    ORDER BY recency_days DESC
    LIMIT 50
    """,
    fetch=True,
)
amostra = pd.DataFrame(rows, columns=["player_id", "rating", "recency_atual_pcr"])

# Garante player do Victor na amostra
if PLAYER_VICTOR not in amostra["player_id"].astype("int64").values:
    rows = execute_supernova(
        "SELECT player_id, rating, recency_days FROM multibet.pcr_ratings "
        "WHERE player_id=%s ORDER BY snapshot_date DESC LIMIT 1",
        params=(PLAYER_VICTOR,), fetch=True,
    )
    if rows:
        extra = pd.DataFrame(rows, columns=["player_id", "rating", "recency_atual_pcr"])
        amostra = pd.concat([extra, amostra], ignore_index=True)

print(f"  Amostra: {len(amostra)} players")
ids_csv = ",".join(str(int(p)) for p in amostra["player_id"].tolist())

# 2. Roda a query do patch e compara recencias
print("[2] Calculando recencia ANTES vs DEPOIS no Athena...")
sql = f"""
WITH fato AS (
    SELECT player_id,
           MAX(activity_date) AS last_active_fct
    FROM ps_bi.fct_player_activity_daily
    WHERE activity_date >= CURRENT_DATE - INTERVAL '{JANELA_DIAS}' DAY
      AND activity_date < CURRENT_DATE
      AND (casino_realbet_count > 0
           OR sb_realbet_count > 0
           OR deposit_success_count > 0)
      AND player_id IN ({ids_csv})
    GROUP BY player_id
),
bireports AS (
    SELECT c_ecr_id AS player_id,
           MAX(c_created_date) AS last_active_bireports
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary
    WHERE c_created_date >= CURRENT_DATE - INTERVAL '{JANELA_DIAS}' DAY
      AND c_created_date < CURRENT_DATE
      AND (c_casino_realcash_bet_amount > 0
           OR c_sb_realcash_bet_amount  > 0
           OR c_deposit_success_amount  > 0)
      AND c_ecr_id IN ({ids_csv})
    GROUP BY c_ecr_id
)
SELECT
    COALESCE(f.player_id, b.player_id) AS player_id,
    f.last_active_fct,
    b.last_active_bireports,
    DATE_DIFF('day', CAST(f.last_active_fct AS DATE), CURRENT_DATE) AS recency_antes,
    DATE_DIFF('day',
        CAST(COALESCE(b.last_active_bireports, f.last_active_fct) AS DATE),
        CURRENT_DATE) AS recency_depois
FROM fato f
FULL OUTER JOIN bireports b ON f.player_id = b.player_id
"""
df_diff = query_athena(sql)

# 3. Junta com amostra original e calcula delta
amostra["player_id"] = amostra["player_id"].astype("int64")
df_diff["player_id"] = df_diff["player_id"].astype("int64")
final = amostra.merge(df_diff, on="player_id", how="left")
final["delta_dias"] = final["recency_antes"] - final["recency_depois"]

print("\n[3] RESULTADO — comparacao por player")
print(final.head(20).to_string(index=False))

print("\n[4] AGREGADOS")
print(f"  Players analisados:    {len(final)}")
print(f"  Recencia ANTES (med):  {final['recency_antes'].median()} dias")
print(f"  Recencia DEPOIS (med): {final['recency_depois'].median()} dias")
print(f"  Delta medio:           {final['delta_dias'].mean():.1f} dias")
print(f"  Delta maximo:          {final['delta_dias'].max()} dias")

print("\n[5] Caso do Victor (player 305245081792208985)")
victor = final[final["player_id"] == PLAYER_VICTOR]
if not victor.empty:
    print(victor.to_string(index=False))

# Persiste evidencia
out = Path("reports") / f"smoke_pcr_recencia_patch_{SNAPSHOT}.csv"
out.parent.mkdir(exist_ok=True)
final.to_csv(out, sep=";", decimal=",", index=False, encoding="utf-8-sig")
print(f"\n  Evidencia salva em: {out}")
