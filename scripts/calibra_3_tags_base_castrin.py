"""
Calibracao das 3 tags na BASE do Castrin (jelly_cenarios_bonus_1305_v3_SELECTED.csv).
Filtro: SO no dia 13/05/2026 BRT.

Output: para cada player da base, marca se caiu em cada tag.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from db.athena import query_athena

# 1. Le CSV
CSV_PATH = r"C:\Users\NITRO\Downloads\jelly_cenarios_bonus_1305_v3_SELECTED.csv"
df_base = pd.read_csv(CSV_PATH, sep=";", decimal=",", skiprows=1, encoding="utf-8")
print(f"CSV lido: {len(df_base)} players")
print(f"Colunas: {list(df_base.columns)}")
print(f"\nDistribuicao por Audiencia:")
print(df_base['Audiencia'].value_counts().to_string())
print(f"\nDistribuicao por Cenario_Bonus:")
print(df_base['Cenario_Bonus'].value_counts().to_string())

# Lista de ecr_ids — converter pra string pra evitar problema de tipo
ecr_ids = df_base['ID_PGS_ecr_id'].dropna().astype('int64').astype(str).tolist()
ecr_ids_sql = ", ".join(ecr_ids)
print(f"\nTotal ecr_ids unicos: {len(set(ecr_ids))}")

DIA_INI = "2026-05-13 03:00:00"
DIA_FIM = "2026-05-14 03:00:00"


# ======================================================================
# TAG 1 - MINES_PENDING_FRAUD
# ======================================================================
print("\n" + "=" * 80)
print("[TAG 1] MINES_PENDING_FRAUD em 13/05 BRT — base Castrin")
print("=" * 80)

sql1 = f"""
WITH pares AS (
  SELECT
    b.c_ecr_id,
    b.c_amount_in_ecr_ccy AS valor_centavos
  FROM fund_ec2.tbl_real_fund_txn b
  JOIN fund_ec2.tbl_real_fund_txn r
    ON b.c_ecr_id = r.c_ecr_id
   AND r.c_txn_type = 72 AND r.c_txn_status = 'SUCCESS'
   AND r.c_amount_in_ecr_ccy = b.c_amount_in_ecr_ccy
   AND r.c_game_id = b.c_game_id
   AND r.c_start_time BETWEEN b.c_start_time AND b.c_start_time + INTERVAL '15' MINUTE
  WHERE b.c_txn_type = 27 AND b.c_txn_status = 'SUCCESS'
    AND b.c_amount_in_ecr_ccy >= 10000
    AND b.c_game_category = 158  -- MINES
    AND b.c_start_time >= TIMESTAMP '{DIA_INI}'
    AND b.c_start_time <  TIMESTAMP '{DIA_FIM}'
    AND b.c_ecr_id IN ({ecr_ids_sql})
)
SELECT
  c_ecr_id,
  COUNT(*) AS pares,
  ROUND(SUM(valor_centavos)/100.0, 2) AS soma_brl
FROM pares
GROUP BY c_ecr_id
"""
df1 = query_athena(sql1, database="fund_ec2")
print(f"Players da base que caem em MINES_PENDING_FRAUD: {len(df1)}")
if not df1.empty:
    print(df1.to_string(index=False))


# ======================================================================
# TAG 2 - CANCEL_HEAVY_DAILY
# ======================================================================
print("\n\n" + "=" * 80)
print("[TAG 2] CANCEL_HEAVY_DAILY (>=10 cancels) em 13/05 — base Castrin")
print("=" * 80)

sql2 = f"""
SELECT
  c_ecr_id,
  COUNT(*) AS n_cancels
FROM fund_ec2.tbl_real_fund_txn
WHERE c_txn_type = 72 AND c_txn_status = 'SUCCESS'
  AND c_start_time >= TIMESTAMP '{DIA_INI}'
  AND c_start_time <  TIMESTAMP '{DIA_FIM}'
  AND c_ecr_id IN ({ecr_ids_sql})
GROUP BY c_ecr_id
HAVING COUNT(*) >= 10
ORDER BY n_cancels DESC
"""
df2 = query_athena(sql2, database="fund_ec2")
print(f"Players da base com >=10 cancels em 13/05: {len(df2)}")
if not df2.empty:
    print(df2.to_string(index=False))


# ======================================================================
# TAG 3 - CRASH_FARMER (refinada)
# ======================================================================
print("\n\n" + "=" * 80)
print("[TAG 3] CRASH_FARMER refinada (intercalacao 5min + 1 crash) — base Castrin")
print("=" * 80)

sql3 = f"""
WITH bets_dia AS (
  SELECT
    c_ecr_id,
    c_start_time,
    c_game_id,
    c_game_category,
    LAG(c_game_id) OVER (PARTITION BY c_ecr_id ORDER BY c_start_time) AS prev_game_id,
    LAG(c_game_category) OVER (PARTITION BY c_ecr_id ORDER BY c_start_time) AS prev_game_category,
    LAG(c_start_time) OVER (PARTITION BY c_ecr_id ORDER BY c_start_time) AS prev_ts
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_txn_type = 27 AND c_txn_status = 'SUCCESS'
    AND c_game_id IS NOT NULL
    AND c_start_time >= TIMESTAMP '{DIA_INI}'
    AND c_start_time <  TIMESTAMP '{DIA_FIM}'
    AND c_ecr_id IN ({ecr_ids_sql})
)
SELECT
  c_ecr_id,
  COUNT(*) AS n_transicoes
FROM bets_dia
WHERE prev_game_id IS NOT NULL
  AND c_game_id <> prev_game_id
  AND date_diff('second', prev_ts, c_start_time) <= 300
  AND (c_game_category = 158 OR prev_game_category = 158)
GROUP BY c_ecr_id
ORDER BY n_transicoes DESC
"""
df3 = query_athena(sql3, database="fund_ec2")
print(f"Players da base com transicao crash 5min em 13/05: {len(df3)}")
if not df3.empty:
    print(df3.head(30).to_string(index=False))


# ======================================================================
# CRUZAMENTO FINAL — enriquece base Castrin com flags das 3 tags
# ======================================================================
print("\n\n" + "=" * 80)
print("CRUZAMENTO FINAL — enriquece base Castrin com as 3 tags")
print("=" * 80)

# Converte ecr_id pra str em todos (garantir consistencia)
df_base['ecr_id_str'] = df_base['ID_PGS_ecr_id'].astype('Int64').astype(str)

def flag(df, col_count='pares'):
    if df is None or df.empty:
        return {}
    df = df.copy()
    df['ecr_id_str'] = df['c_ecr_id'].astype('int64').astype(str)
    return dict(zip(df['ecr_id_str'], df[col_count]))

flags1 = flag(df1, 'pares')
flags2 = flag(df2, 'n_cancels')
flags3 = flag(df3, 'n_transicoes')

df_base['MINES_PENDING_pares'] = df_base['ecr_id_str'].map(flags1).fillna(0).astype(int)
df_base['CANCEL_HEAVY_cancels'] = df_base['ecr_id_str'].map(flags2).fillna(0).astype(int)
df_base['CRASH_FARMER_transicoes'] = df_base['ecr_id_str'].map(flags3).fillna(0).astype(int)

df_base['MINES_PENDING'] = (df_base['MINES_PENDING_pares'] >= 1).astype(int)
df_base['CANCEL_HEAVY'] = (df_base['CANCEL_HEAVY_cancels'] >= 10).astype(int)
df_base['CRASH_FARMER'] = (df_base['CRASH_FARMER_transicoes'] >= 1).astype(int)
df_base['caiu_alguma'] = (df_base[['MINES_PENDING','CANCEL_HEAVY','CRASH_FARMER']].sum(axis=1) >= 1).astype(int)

print(f"\nTotal players na base Castrin: {len(df_base)}")
print(f"\nQuantos CAIRAM em cada tag (no dia 13/05):")
print(f"  MINES_PENDING_FRAUD: {df_base['MINES_PENDING'].sum()}")
print(f"  CANCEL_HEAVY_DAILY:  {df_base['CANCEL_HEAVY'].sum()}")
print(f"  CRASH_FARMER:        {df_base['CRASH_FARMER'].sum()}")
print(f"\nUniao (pelo menos 1 tag): {df_base['caiu_alguma'].sum()} de {len(df_base)} ({100*df_base['caiu_alguma'].sum()/len(df_base):.1f}%)")

print(f"\nPor Audiencia:")
print(df_base.groupby('Audiencia').agg(
    total=('ecr_id_str', 'count'),
    MINES=('MINES_PENDING', 'sum'),
    CANCEL_HEAVY=('CANCEL_HEAVY', 'sum'),
    CRASH=('CRASH_FARMER', 'sum'),
    qualquer=('caiu_alguma', 'sum')
).to_string())

print(f"\nPor Cenario_Bonus:")
print(df_base.groupby('Cenario_Bonus').agg(
    total=('ecr_id_str', 'count'),
    MINES=('MINES_PENDING', 'sum'),
    CANCEL_HEAVY=('CANCEL_HEAVY', 'sum'),
    CRASH=('CRASH_FARMER', 'sum'),
    qualquer=('caiu_alguma', 'sum')
).to_string())

# Salva enriquecido
out_path = "output/jelly_cenarios_1305_v3_com_tags.csv"
os.makedirs("output", exist_ok=True)
df_base.to_csv(out_path, sep=";", decimal=",", index=False, encoding="utf-8-sig")
print(f"\nCSV enriquecido salvo: {out_path}")

# Top 30 que cairam em pelo menos 1
print("\n--- Top 30 players da base que pontuaram (alguma tag) ---")
top = df_base[df_base['caiu_alguma'] == 1].sort_values(
    ['MINES_PENDING','CANCEL_HEAVY_cancels','CRASH_FARMER_transicoes'],
    ascending=False
).head(30)
print(top[['ID_Smartico_user_ext_id','ID_PGS_ecr_id','Audiencia','Cenario_Bonus',
          'MINES_PENDING_pares','CANCEL_HEAVY_cancels','CRASH_FARMER_transicoes']].to_string(index=False))
