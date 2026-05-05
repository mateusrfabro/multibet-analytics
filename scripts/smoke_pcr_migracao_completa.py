"""
Smoke test final da migracao do PCR (fato -> bireports).

Compara:
  ANTES: multibet.pcr_ratings snapshot mais recente (com fato congelada 06/04)
  DEPOIS: roda a query nova SQL e re-calcula PVS+rating em memoria
          (sem persistir, sem efeito colateral)

Saida: reports/smoke_pcr_migracao_completa_YYYY-MM-DD.csv com
  - player_id, rating_antes, rating_depois, pvs_antes, pvs_depois,
    recency_antes, recency_depois, ggr_antes, ggr_depois, dep_antes, dep_depois

Foco em A+S do snapshot anterior + player do Victor.
"""
import sys, os
sys.path.insert(0, ".")
sys.path.insert(0, "pipelines")

from datetime import datetime
from pathlib import Path
import pandas as pd
import numpy as np

from db.athena import query_athena
from db.supernova import execute_supernova
import pcr_pipeline as pcr  # noqa

PLAYER_VICTOR = 305245081792208985
SNAPSHOT = datetime.now().strftime("%Y-%m-%d")

print("=" * 70)
print("SMOKE TEST — migracao PCR completa (fato -> bireports)")
print("=" * 70)

# 1) ANTES: pega snapshot atual (fonte: multibet.pcr_ratings)
print("\n[1] Carregando snapshot ANTES (multibet.pcr_ratings, ultimo dia)")
rows = execute_supernova(
    """
    WITH ult AS (SELECT MAX(snapshot_date) AS d FROM multibet.pcr_ratings)
    SELECT player_id, rating, pvs, recency_days, ggr_total, total_deposits
    FROM multibet.pcr_ratings, ult
    WHERE snapshot_date = ult.d
    """,
    fetch=True,
)
df_antes = pd.DataFrame(rows, columns=["player_id", "rating_antes", "pvs_antes",
                                          "recency_antes", "ggr_antes", "dep_antes"])
df_antes["player_id"] = df_antes["player_id"].astype("int64")
for col in ["pvs_antes", "recency_antes", "ggr_antes", "dep_antes"]:
    df_antes[col] = pd.to_numeric(df_antes[col], errors="coerce")
print(f"  Snapshot ANTES: {len(df_antes):,} players")
print(f"  Distribuicao rating ANTES:")
for r, n in df_antes["rating_antes"].value_counts().sort_index().items():
    print(f"    {r}: {n:,}")

# 2) DEPOIS: roda a query nova e re-calcula
print("\n[2] Rodando PCR DEPOIS (com nova query bireports)")
print("   (extracao + filtro + scoring — sem persistencia)")

# Reaproveita as funcoes do pcr_pipeline
df_raw = pcr.extrair_metricas_jogadores()
print(f"  Players extraidos: {len(df_raw):,}")
df_scored = pcr.calcular_pvs(df_raw)
print(f"  Players apos scoring: {len(df_scored):,}")
df_rated = pcr.atribuir_rating(df_scored)
print(f"  Players apos rating: {len(df_rated):,}")

df_depois = df_rated[["player_id", "rating", "pvs", "recency_days",
                       "ggr_total", "total_deposits"]].copy()
df_depois.columns = ["player_id", "rating_depois", "pvs_depois", "recency_depois",
                     "ggr_depois", "dep_depois"]
df_depois["player_id"] = df_depois["player_id"].astype("int64")

# 3) Merge antes vs depois
print("\n[3] Comparando ANTES vs DEPOIS")
final = df_antes.merge(df_depois, on="player_id", how="outer", indicator=True)

print(f"\n  Players so ANTES (sumiram):  {(final['_merge']=='left_only').sum():,}")
print(f"  Players so DEPOIS (novos):   {(final['_merge']=='right_only').sum():,}")
print(f"  Players nas duas listas:     {(final['_merge']=='both').sum():,}")

# 4) Distribuicao de ratings
print("\n[4] Distribuicao de ratings")
both = final[final["_merge"] == "both"].copy()
print("\n  Matriz transicao (rating_antes -> rating_depois):")
mat = pd.crosstab(both["rating_antes"], both["rating_depois"], margins=True, margins_name="TOTAL")
print(mat.to_string())

# 5) Recencia
print("\n[5] Recencia ANTES vs DEPOIS (so quem esta nos dois)")
recency_diff = both[["recency_antes", "recency_depois"]].dropna()
recency_diff["delta"] = recency_diff["recency_antes"] - recency_diff["recency_depois"]
print(f"  Mediana ANTES:  {recency_diff['recency_antes'].median():.0f} dias")
print(f"  Mediana DEPOIS: {recency_diff['recency_depois'].median():.0f} dias")
print(f"  Delta medio:    {recency_diff['delta'].mean():+.1f} dias (positivo = ficou menor)")
print(f"  Players c/ delta > 5 dias (recencia caiu):  {(recency_diff['delta'] > 5).sum():,}")

# 6) GGR e Depositos (sanity)
print("\n[6] GGR e Depositos — sanity check")
ggr_diff = both[["ggr_antes", "ggr_depois"]].dropna()
ggr_diff["delta_pct"] = (ggr_diff["ggr_depois"] - ggr_diff["ggr_antes"]) / ggr_diff["ggr_antes"].replace(0, np.nan) * 100
print(f"  GGR sum ANTES:   R$ {ggr_diff['ggr_antes'].sum()/1e6:.2f}M")
print(f"  GGR sum DEPOIS:  R$ {ggr_diff['ggr_depois'].sum()/1e6:.2f}M")
print(f"  Delta global:    {(ggr_diff['ggr_depois'].sum()-ggr_diff['ggr_antes'].sum())/ggr_diff['ggr_antes'].sum()*100:+.2f}%")
print(f"  Delta mediano absoluto (|%|): {ggr_diff['delta_pct'].abs().median():.2f}%")

# 7) Caso do Victor
print("\n[7] Caso do Victor (player 305245081792208985)")
victor = final[final["player_id"] == PLAYER_VICTOR]
if not victor.empty:
    cols_show = ["player_id", "rating_antes", "rating_depois",
                 "pvs_antes", "pvs_depois",
                 "recency_antes", "recency_depois",
                 "ggr_antes", "ggr_depois", "dep_antes", "dep_depois"]
    print(victor[cols_show].to_string(index=False))

# 8) Top 20 maiores mudancas de rating (downgrade ou upgrade)
print("\n[8] Top 10 maiores quedas de recencia (=corrigidos pelo fix)")
recency_top = both.dropna(subset=["recency_antes", "recency_depois"])
recency_top = recency_top.assign(delta=recency_top["recency_antes"] - recency_top["recency_depois"])
recency_top = recency_top.nlargest(10, "delta")
print(recency_top[["player_id", "rating_antes", "rating_depois",
                    "recency_antes", "recency_depois", "ggr_antes", "ggr_depois"]].to_string(index=False))

# Persiste evidencia
out = Path("reports") / f"smoke_pcr_migracao_completa_{SNAPSHOT}.csv"
final.drop(columns=["_merge"]).to_csv(out, sep=";", decimal=",", index=False, encoding="utf-8-sig")
print(f"\n  Evidencia salva em: {out}")
print("=" * 70)
