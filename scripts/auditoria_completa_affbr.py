"""
Auditoria COMPLETA do lote AffiliatesBR — valida:
  1) Schemas Athena (colunas existem, tipos)
  2) Queries usadas (SQL documentado + sample)
  3) Integridade dos CSVs (linhas, nulls, tipos, duplicatas)
  4) Cruzamento com fonte alternativa (ps_bi.fct_casino_activity_daily)
  5) Consistencia interna (sports: bet_slip x operacoes | casino: txn_id unico)
"""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
import pandas as pd
from db.athena import query_athena

BASE = r"reports/afiliados_affbr_marco_auditoria"

AFFILIATE_IDS_STR = ", ".join(f"'{x}'" for x in [
    "526644","526453","523755","523655","523654","523106","523105","522848","522847","522633",
    "511007","508666","506089","506088","505689","505602","504946","504944","502676","501192",
    "501191","500809","500807","489458","489457","489444","489443","489307","489306","477184",
    "477182","476899","476894","476875","473708","473479","472630","471929","471922","458116",
    "457204","452463","451350","451346","449452","449417","449299","449267","449265","449113",
    "449092","447544","447195","447194","447193","446890","446237","445982","445046","445045",
    "445042","444946","444944","444940","444801","442805","442803","442733","442181","442095",
    "441962","441961","441950","441949","441724","441723","432314","432313","432311","432309",
    "431790","431788","431729","431727","431726","431725","431723","431613","431611","431608",
    "431589","431587","427530","427497","427496","427462","427398",
])

print("=" * 85)
print("AUDITORIA COMPLETA — Lote AffiliatesBR marco/2026")
print("=" * 85)

# =================================================================
# 1) SCHEMAS — confirmar colunas usadas existem
# =================================================================
print("\n[1] VALIDACAO DE SCHEMAS")
print("-" * 85)

schemas_target = [
    ("ps_bi", "dim_user", ["ecr_id","external_id","affiliate_id","affiliate","is_test","signup_datetime"]),
    ("fund_ec2", "tbl_real_fund_txn", ["c_txn_id","c_ecr_id","c_txn_type","c_txn_status","c_amount_in_ecr_ccy","c_start_time","c_game_id","c_session_id","c_op_type","c_sub_vendor_id","c_product_id","c_channel"]),
    ("vendor_ec2", "tbl_sports_book_info", ["c_customer_id","c_amount","c_operation_type","c_transaction_id","c_bet_slip_id","c_bet_slip_state","c_vendor_id","c_transaction_fee_amount","c_transaction_fee_type"]),
    ("vendor_ec2", "tbl_sports_book_bets_info", ["c_customer_id","c_bet_slip_id","c_bet_id","c_total_stake","c_total_return","c_total_odds","c_bet_type","c_bet_state","c_transaction_type","c_is_free","c_is_live","c_bonus_amount","c_pam_bonus_txn_id","c_created_time","c_updated_time","c_bet_closure_time"]),
    ("bireports_ec2", "tbl_ecr_wise_daily_bi_summary", ["c_ecr_id","c_created_date","c_casino_realcash_bet_amount","c_casino_realcash_win_amount","c_sb_realcash_bet_amount","c_deposit_success_amount"]),
]

for db, tbl, expected_cols in schemas_target:
    try:
        cols_df = query_athena(f"SHOW COLUMNS FROM {db}.{tbl}", database=db)
        real_cols = set(cols_df.iloc[:, 0].astype(str).str.strip().tolist())
        missing = [c for c in expected_cols if c not in real_cols]
        print(f"\n  {db}.{tbl}")
        print(f"    total cols: {len(real_cols)} | exigidas: {len(expected_cols)} | faltando: {len(missing)}")
        if missing:
            print(f"    !!! FALTAM: {missing}")
        else:
            print(f"    OK — todas as {len(expected_cols)} colunas usadas existem")
    except Exception as e:
        print(f"  {db}.{tbl}: ERRO {e}")

# =================================================================
# 2) INTEGRIDADE DOS CSVs
# =================================================================
print("\n\n[2] INTEGRIDADE DOS CSVs GERADOS")
print("-" * 85)

csvs = [
    ("sports_affbr_marco_afiliados.csv", "sports"),
    ("casino_affbr_marco_afiliados.csv", "casino"),
    ("geral_affbr_marco_afiliados.csv",  "geral"),
]

for fname, label in csvs:
    path = f"{BASE}/{fname}"
    df = pd.read_csv(path, low_memory=False)
    print(f"\n  {fname}")
    print(f"    linhas:    {len(df):,}")
    print(f"    colunas:   {len(df.columns)}")
    print(f"    nulls (%): {(df.isna().sum() / len(df) * 100).round(2).to_dict()}")
    print(f"    id unico?  {df['id'].nunique() == len(df)} (unicos={df['id'].nunique():,})")
    print(f"    affiliates distintos: {df['affiliate_id'].nunique()}")
    print(f"    periodo created_at: {df['created_at'].min()} -> {df['created_at'].max()}")
    if "amount" in df.columns:
        print(f"    amount: min={df['amount'].min():.2f} | max={df['amount'].max():.2f} | neg={(df['amount']<0).sum()}")
    if "user_id" in df.columns:
        print(f"    user_id distintos: {df['user_id'].nunique():,}")

# =================================================================
# 3) CONSISTENCIA SPORTS (bet_slip deve ter operacoes coerentes)
# =================================================================
print("\n\n[3] CONSISTENCIA INTERNA SPORTS")
print("-" * 85)
df_s = pd.read_csv(f"{BASE}/sports_affbr_marco_afiliados.csv", low_memory=False)
print(f"  bilhetes unicos: {df_s['ext_ticket_id'].nunique():,}")
print(f"  operacoes:       {len(df_s):,}")
print(f"  operacoes/bilhete (media): {len(df_s)/df_s['ext_ticket_id'].nunique():.2f}")
print(f"  distribuicao de types:")
print(df_s['type'].value_counts().to_string())

# =================================================================
# 4) CROSS-VALIDATION — comparar CSV com ps_bi.fct_casino_activity_daily
# =================================================================
print("\n\n[4] CROSS-VAL CASINO — CSV vs ps_bi.fct_casino_activity_daily")
print("-" * 85)
df_c = pd.read_csv(f"{BASE}/casino_affbr_marco_afiliados.csv", low_memory=False)
df_c["affiliate_id"] = df_c["affiliate_id"].astype(str)
csv_agg = df_c[df_c["type_id"].isin([27,28])].groupby("affiliate_id")["amount"].sum().reset_index(name="csv_bet_brute")
csv_net = (df_c[df_c["type_id"].isin([27,28])].groupby("affiliate_id")["amount"].sum() -
           df_c[df_c["type_id"].isin([72,76])].groupby("affiliate_id")["amount"].sum().reindex(csv_agg["affiliate_id"], fill_value=0).values).reset_index()
csv_net.columns = ["affiliate_id", "csv_bet_net"]

sql_psbi = f"""
WITH p AS (
    SELECT CAST(ecr_id AS VARCHAR) AS ecr_id, CAST(affiliate_id AS VARCHAR) AS affiliate_id
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ({AFFILIATE_IDS_STR})
      AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
)
SELECT
    p.affiliate_id,
    COALESCE(SUM(f.bet_amount_local), 0) AS psbi_total_bet,
    COALESCE(SUM(f.real_bet_amount_local), 0) AS psbi_real_bet,
    COALESCE(SUM(f.win_amount_local), 0) AS psbi_win,
    COALESCE(SUM(f.ggr_local), 0) AS psbi_ggr,
    COALESCE(SUM(f.realbet_canceled_amount_local), 0) AS psbi_cancel
FROM ps_bi.fct_casino_activity_daily f
INNER JOIN p ON CAST(f.player_id AS VARCHAR) = p.ecr_id
WHERE f.activity_date >= DATE '2026-03-01'
  AND f.activity_date <  DATE '2026-04-01'
GROUP BY p.affiliate_id
"""
psbi = query_athena(sql_psbi, database="ps_bi")
psbi["affiliate_id"] = psbi["affiliate_id"].astype(str)

comp = csv_agg.merge(csv_net, on="affiliate_id", how="outer")\
             .merge(psbi, on="affiliate_id", how="outer")\
             .fillna(0).sort_values("csv_bet_brute", ascending=False)

import numpy as np
for col_csv, col_psbi, diff_col in [("csv_bet_brute", "psbi_real_bet", "diff_bruto_vs_psbi_real"),
                                     ("csv_bet_net", "psbi_real_bet", "diff_net_vs_psbi_real")]:
    denom = comp[col_psbi].astype(float)
    num = comp[col_csv].astype(float) - denom
    comp[diff_col] = np.where(denom == 0, np.nan, num/denom*100).round(2)

print(comp.head(20).to_string(index=False))
print(f"\n  TOTAL (soma global):")
print(f"    CSV bruto (27,28)......: {comp['csv_bet_brute'].sum():,.2f}")
print(f"    CSV net (27,28 - 72,76): {comp['csv_bet_net'].sum():,.2f}")
print(f"    ps_bi real_bet_amount..: {comp['psbi_real_bet'].sum():,.2f}")
print(f"    ps_bi total_bet_amount.: {comp['psbi_total_bet'].sum():,.2f}")
print(f"    ps_bi cancel_amount....: {comp['psbi_cancel'].sum():,.2f}")

# Salvar
out = f"{BASE}/auditoria_completa.csv"
comp.to_csv(out, index=False, encoding="utf-8-sig")
print(f"\n  Salvo: {out}")
