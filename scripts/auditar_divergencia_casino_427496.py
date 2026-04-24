"""
Auditoria profunda das 4 divergencias de casino no lote AffiliatesBR.
Foco no affiliate 427496 (+245%) mas roda pros 4 alvos.
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
import pandas as pd
from db.athena import query_athena

ALVOS = ["427496", "457204", "500809", "444946"]
CASINO_CSV = r"reports/afiliados_affbr_marco_auditoria/casino_affbr_marco_afiliados.csv"

print("=" * 80)
print("AUDITORIA — Divergencia casino CSV vs bireports")
print("=" * 80)

# -----------------------------------------------------------------
# PARTE 1 — Decomposicao do CSV por type_id
# -----------------------------------------------------------------
print("\n[1] Decomposicao do CSV casino por affiliate e type...")
df = pd.read_csv(CASINO_CSV, low_memory=False)
df["affiliate_id"] = df["affiliate_id"].astype(str)
alvo = df[df["affiliate_id"].isin(ALVOS)].copy()

print("\nBreakdown por affiliate x type:")
piv = alvo.groupby(["affiliate_id", "type", "type_id"])["amount"].agg(["sum", "count"]).reset_index()
piv.columns = ["affiliate_id", "type", "type_id", "sum_amount", "qty"]
for aid in ALVOS:
    sub = piv[piv["affiliate_id"] == aid].sort_values("sum_amount", ascending=False)
    if len(sub):
        print(f"\n=== affiliate_id={aid} ===")
        print(sub.to_string(index=False))
        total_2728 = sub[sub["type_id"].isin([27, 28])]["sum_amount"].sum()
        total_27   = sub[sub["type_id"] == 27]["sum_amount"].sum()
        print(f"   SUM(type_id IN 27,28)  [nosso CSV] = {total_2728:,.2f}")
        print(f"   SUM(type_id = 27 so)                = {total_27:,.2f}")

# -----------------------------------------------------------------
# PARTE 2 — Comparar com bireports detalhando colunas
# -----------------------------------------------------------------
print("\n\n[2] Consultando bireports detalhado...")
affils_in = ", ".join(f"'{x}'" for x in ALVOS)

sql = f"""
WITH affil_players AS (
    SELECT CAST(ecr_id AS VARCHAR) AS ecr_id, CAST(affiliate_id AS VARCHAR) AS affiliate_id
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in})
      AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
)
SELECT
    p.affiliate_id,
    COUNT(*)                                                        AS qty_dias_player,
    COALESCE(SUM(s.c_casino_realcash_bet_amount), 0) / 100.0        AS bi_casino_realcash_bet,
    COALESCE(SUM(s.c_casino_realcash_win_amount), 0) / 100.0        AS bi_casino_realcash_win
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
INNER JOIN affil_players p ON CAST(s.c_ecr_id AS VARCHAR) = p.ecr_id
WHERE s.c_created_date >= DATE '2026-03-01'
  AND s.c_created_date <  DATE '2026-04-01'
GROUP BY p.affiliate_id
ORDER BY p.affiliate_id
"""
bi = query_athena(sql, database="bireports_ec2")
bi["affiliate_id"] = bi["affiliate_id"].astype(str)
print(bi.to_string(index=False))

# -----------------------------------------------------------------
# PARTE 3 — Agregacao CSV por afiliado (varias formas)
# -----------------------------------------------------------------
print("\n\n[3] CSV agregado por afiliado (varias categorias)...")
agg = alvo.groupby("affiliate_id").apply(lambda g: pd.Series({
    "csv_type27_buyin":         g.loc[g["type_id"] == 27, "amount"].sum(),
    "csv_type28_rebuy":         g.loc[g["type_id"] == 28, "amount"].sum(),
    "csv_type_27_28":           g.loc[g["type_id"].isin([27, 28]), "amount"].sum(),
    "csv_all_casino_bets_bruto": g.loc[g["type_id"].isin([27, 28, 41, 43, 132]), "amount"].sum(),
    "csv_rollback_72_76":       g.loc[g["type_id"].isin([72, 76, 133]), "amount"].sum(),
    "csv_net_27_28_menos_rollback": g.loc[g["type_id"].isin([27, 28]), "amount"].sum() - g.loc[g["type_id"].isin([72, 76, 133]), "amount"].sum(),
    "csv_freespins_buyin":      g.loc[g["type_id"] == 132, "amount"].sum(),
}), include_groups=False).reset_index()
print(agg.to_string(index=False))

# -----------------------------------------------------------------
# PARTE 4 — Merge final comparativo
# -----------------------------------------------------------------
print("\n\n[4] Comparacao final:")
final = agg.merge(bi[["affiliate_id", "bi_casino_realcash_bet"]], on="affiliate_id", how="left")
final["diff_pct_27_28_vs_bi"] = ((final["csv_type_27_28"] / final["bi_casino_realcash_bet"] - 1) * 100).round(2)
final["diff_pct_27_only_vs_bi"] = ((final["csv_type27_buyin"] / final["bi_casino_realcash_bet"] - 1) * 100).round(2)
final["diff_pct_net_vs_bi"] = ((final["csv_net_27_28_menos_rollback"] / final["bi_casino_realcash_bet"] - 1) * 100).round(2)
print(final[["affiliate_id", "csv_type27_buyin", "csv_type28_rebuy", "csv_type_27_28",
             "csv_net_27_28_menos_rollback", "bi_casino_realcash_bet",
             "diff_pct_27_28_vs_bi", "diff_pct_27_only_vs_bi", "diff_pct_net_vs_bi"]].to_string(index=False))

# -----------------------------------------------------------------
# PARTE 5 — Investigar players especificos que divergem (so 427496)
# -----------------------------------------------------------------
print("\n\n[5] Top 10 players do 427496 com maior volume CSV...")
p427 = alvo[(alvo["affiliate_id"] == "427496") & (alvo["type_id"].isin([27, 28]))]
top_players = p427.groupby("user_id")["amount"].sum().sort_values(ascending=False).head(10)
print(top_players.to_string())
