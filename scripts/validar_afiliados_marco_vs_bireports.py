"""
Validacao cruzada dos 3 CSVs gerados vs bireports_ec2.tbl_ecr_wise_daily_bi_summary.

Compara totais por afiliado:
  - Sports: SUM(stake_amount onde type='M')  vs  c_sb_realcash_bet_amount
  - Casino: SUM(amount onde type_id IN (27,28))  vs  c_casino_realcash_bet_amount
  - Geral : SUM(amount onde type_id=1 e status=SUCCESS)  vs  c_deposit_success_amount

Tolerancia: 2% (valores podem divergir levemente por diferenca de filtros — CV de sanidade)

Saida: reports/afiliados_marco_auditoria/validacao_cruzada.csv
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")

import os
import pandas as pd
from db.athena import query_athena

OUT_DIR = r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet\reports\afiliados_marco_auditoria"

AFFILIATE_IDS = [
    "526453", "524476", "506920", "475425", "454861", "453598",
    "489203", "509759", "488468", "522962", "528138", "529063",
]
PERIOD_START_UTC = "2026-03-01 03:00:00"
PERIOD_END_UTC   = "2026-04-01 03:00:00"

print("=" * 75)
print("VALIDACAO CRUZADA: CSVs gerados vs bireports_ec2.tbl_ecr_wise_daily_bi_summary")
print("=" * 75)

# -------------------------------------------------------------------------
# (A) Agregado dos 3 CSVs locais
# -------------------------------------------------------------------------
print("\n[A] Agregando CSVs locais...")

p_sports = os.path.join(OUT_DIR, "sports_marco_afiliados.csv")
p_casino = os.path.join(OUT_DIR, "casino_marco_afiliados.csv")
p_geral  = os.path.join(OUT_DIR, "geral_marco_afiliados.csv")

df_s = pd.read_csv(p_sports, low_memory=False)
df_c = pd.read_csv(p_casino, low_memory=False)
df_g = pd.read_csv(p_geral,  low_memory=False)

# Sports: somar stake_amount SO no tipo M (commit) pra nao duplicar em payouts
csv_sports = df_s[df_s["type"] == "M"].groupby("affiliate_id")["stake_amount"].sum().reset_index()
csv_sports.columns = ["affiliate_id", "csv_sports_stake"]

# Casino: somar amount onde type_id IN (27,28) CASINO_BUYIN/REBUY
csv_casino = df_c[df_c["type_id"].isin([27, 28])].groupby("affiliate_id")["amount"].sum().reset_index()
csv_casino.columns = ["affiliate_id", "csv_casino_bet"]

# Geral: somar amount onde type_id=1 (depositos SUCCESS)
csv_dep = df_g[df_g["type_id"] == 1].groupby("affiliate_id")["amount"].sum().reset_index()
csv_dep.columns = ["affiliate_id", "csv_deposit"]

csv_agg = csv_sports.merge(csv_casino, on="affiliate_id", how="outer")\
                    .merge(csv_dep,    on="affiliate_id", how="outer")\
                    .fillna(0)
csv_agg["affiliate_id"] = csv_agg["affiliate_id"].astype(str)
print(f"  agregado local:\n{csv_agg.to_string(index=False)}")

# -------------------------------------------------------------------------
# (B) Query bireports — totais oficiais BI
# -------------------------------------------------------------------------
print("\n[B] Buscando totais em bireports_ec2.tbl_ecr_wise_daily_bi_summary...")
affils_in = ", ".join(f"'{x}'" for x in AFFILIATE_IDS)

sql = f"""
WITH affil_players AS (
    SELECT CAST(ecr_id AS VARCHAR) AS ecr_id, CAST(affiliate_id AS VARCHAR) AS affiliate_id
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in})
      AND is_test = false
      AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
)
SELECT
    p.affiliate_id,
    COALESCE(SUM(s.c_sb_realcash_bet_amount), 0) / 100.0     AS bi_sports_stake,
    COALESCE(SUM(s.c_casino_realcash_bet_amount), 0) / 100.0 AS bi_casino_bet,
    COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0     AS bi_deposit
FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
INNER JOIN affil_players p
    ON CAST(s.c_ecr_id AS VARCHAR) = p.ecr_id
WHERE s.c_created_date >= DATE '2026-03-01'
  AND s.c_created_date <  DATE '2026-04-01'
GROUP BY p.affiliate_id
"""
bi = query_athena(sql, database="bireports_ec2")
bi["affiliate_id"] = bi["affiliate_id"].astype(str)
print(f"  bireports:\n{bi.to_string(index=False)}")

# -------------------------------------------------------------------------
# (C) Merge e calculo de divergencia
# -------------------------------------------------------------------------
print("\n[C] Calculando divergencias...")
comp = csv_agg.merge(bi, on="affiliate_id", how="outer").fillna(0)

for col in ["sports_stake", "casino_bet", "deposit"]:
    csv_col = f"csv_{col}"
    bi_col  = f"bi_{col}"
    diff_col = f"diff_pct_{col}"
    comp[diff_col] = ((comp[csv_col] - comp[bi_col]) / comp[bi_col].replace(0, pd.NA) * 100).round(2)

# Reordenar colunas pra ler melhor
cols = ["affiliate_id",
        "csv_sports_stake", "bi_sports_stake", "diff_pct_sports_stake",
        "csv_casino_bet",   "bi_casino_bet",   "diff_pct_casino_bet",
        "csv_deposit",      "bi_deposit",      "diff_pct_deposit"]
comp = comp[cols].sort_values("affiliate_id").reset_index(drop=True)

print("\n=== RESULTADO DA VALIDACAO ===")
print(comp.to_string(index=False))

# Analisar — divergencias acima de 2%?
TOL = 2.0
alerts = []
for _, row in comp.iterrows():
    aid = row["affiliate_id"]
    for metric in ["sports_stake", "casino_bet", "deposit"]:
        d = row[f"diff_pct_{metric}"]
        if pd.notna(d) and abs(d) > TOL:
            alerts.append(f"  [ALERTA] affiliate_id={aid} {metric}: {d:+.2f}% (CSV {row[f'csv_{metric}']:,.2f} vs BI {row[f'bi_{metric}']:,.2f})")

print(f"\nTolerancia: +-{TOL}%")
if alerts:
    print(f"[{len(alerts)} divergencia(s) acima da tolerancia]:")
    for a in alerts:
        print(a)
else:
    print("Todas as divergencias dentro da tolerancia. OK para entrega.")

# Salvar
out_path = os.path.join(OUT_DIR, "validacao_cruzada.csv")
comp.to_csv(out_path, index=False, encoding="utf-8-sig")
print(f"\nSalvo em: {out_path}")
