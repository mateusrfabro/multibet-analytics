"""
Gera o CSV de resumo NET por afiliado — pronto pra auditoria calcular comissao.
Valor NET = igual ao bireports_ec2.tbl_ecr_wise_daily_bi_summary (diff 0%).
"""
import sys, os
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
import pandas as pd

BASE = r"reports/afiliados_affbr_marco_auditoria"

print("Carregando casino...")
dc = pd.read_csv(f"{BASE}/casino_affbr_marco_afiliados.csv", low_memory=False)
dc["affiliate_id"] = dc["affiliate_id"].astype(str)

# Tipos casino
BET_TYPES       = [27, 28]               # BUYIN + REBUY
BET_CANCEL      = [72, 76, 133]          # BUYIN_CANCEL, REBUY_CANCEL, FREESPIN_BUYIN_CANCEL
WIN_TYPES       = [45, 65, 79, 80, 91]   # WIN, JACKPOT, TOURN_WIN, FREESPIN_WIN, REFUND_BET
WIN_CANCEL      = [77, 86, 114]          # WIN_CANCEL, FREESPIN_WIN_CANCEL, JACKPOT_WIN_CANCEL
FREESPIN_BUYIN  = [132]                  # FREESPIN_BUYIN (normalmente 0)

print("Carregando geral (depositos/saques)...")
dg = pd.read_csv(f"{BASE}/geral_affbr_marco_afiliados.csv", low_memory=False)
dg["affiliate_id"] = dg["affiliate_id"].astype(str)

# Somas por afiliado
def agg(df, col_types, name):
    return df[df["type_id"].isin(col_types)].groupby("affiliate_id")["amount"].sum().rename(name)

bet_brute   = agg(dc, BET_TYPES,       "casino_bet_brute")
bet_cancel  = agg(dc, BET_CANCEL,      "casino_bet_cancel")
win_brute   = agg(dc, WIN_TYPES,       "casino_win_brute")
win_cancel  = agg(dc, WIN_CANCEL,      "casino_win_cancel")
deposit     = agg(dg, [1],             "deposit")
withdraw    = agg(dg, [2],             "withdraw")

res = pd.concat([bet_brute, bet_cancel, win_brute, win_cancel, deposit, withdraw], axis=1).fillna(0).reset_index()

# NET = bateria com bireports
res["casino_bet_net"]    = res["casino_bet_brute"]  - res["casino_bet_cancel"]
res["casino_win_net"]    = res["casino_win_brute"]  - res["casino_win_cancel"]
res["casino_ggr"]        = res["casino_bet_net"]    - res["casino_win_net"]

# Sports
print("Carregando sports...")
ds = pd.read_csv(f"{BASE}/sports_affbr_marco_afiliados.csv", low_memory=False)
ds["affiliate_id"] = ds["affiliate_id"].astype(str)
# Stake apenas em operacao type='M' (commit)
sp_stake = ds[ds["type"] == "M"].groupby("affiliate_id")["stake_amount"].sum().rename("sb_stake_brute")
# Gain apenas em type='P' (payout)
sp_payout = ds[ds["type"] == "P"].groupby("affiliate_id")["amount"].sum().rename("sb_payout_brute")
# Rollback/cancel sports (operacoes C, R)
sp_cancel = ds[ds["type"].isin(["C", "R"])].groupby("affiliate_id")["amount"].sum().rename("sb_cancel_brute")

sp = pd.concat([sp_stake, sp_payout, sp_cancel], axis=1).fillna(0).reset_index()
sp["sb_stake_net"] = sp["sb_stake_brute"] - sp["sb_cancel_brute"]
sp["sb_ggr"] = sp["sb_stake_net"] - sp["sb_payout_brute"]

# Merge
res = res.merge(sp, on="affiliate_id", how="outer").fillna(0)

# GGR total
res["ggr_total"] = res["casino_ggr"] + res["sb_ggr"]

# Nome dos afiliados
afs = pd.read_csv(f"{BASE}/afiliados_consolidado.csv")
afs["affiliate_id"] = afs["affiliate_id"].astype(str)
res = afs.merge(res, on="affiliate_id", how="left").fillna(0)

# Ordenar colunas
cols = [
    "affiliate_id", "nome_afiliado", "a_pagar_abril",
    "casino_bet_brute", "casino_bet_cancel", "casino_bet_net",
    "casino_win_brute", "casino_win_cancel", "casino_win_net",
    "casino_ggr",
    "sb_stake_brute", "sb_cancel_brute", "sb_stake_net",
    "sb_payout_brute", "sb_ggr",
    "ggr_total",
    "deposit", "withdraw",
]
res = res[cols].sort_values("ggr_total", ascending=False)

# Round pra 2 casas
for c in res.columns:
    if res[c].dtype == float:
        res[c] = res[c].round(2)

out = f"{BASE}/pagamento_sugerido_por_afiliado.csv"
res.to_csv(out, index=False, encoding="utf-8-sig")
print(f"\nSALVO: {out}")
print(f"\nTotais:")
print(f"  Casino GGR: R$ {res['casino_ggr'].sum():,.2f}")
print(f"  Sports GGR: R$ {res['sb_ggr'].sum():,.2f}")
print(f"  GGR Total : R$ {res['ggr_total'].sum():,.2f}")
print(f"\nPreview top 10:")
print(res.head(10)[["affiliate_id","nome_afiliado","casino_bet_net","casino_ggr","sb_ggr","ggr_total"]].to_string(index=False))
