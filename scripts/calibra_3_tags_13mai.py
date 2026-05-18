"""
Calibracao das 3 tags propostas — APENAS DIA 13/05/2026 BRT.

Castrin pediu: "roda todas as 3. pra gente ver o resultado"

Tags:
  1. MINES_PENDING_FRAUD — bet=cancel mesmo valor 15min + MINES (c_game_category=158)
  2. CANCEL_HEAVY_DAILY — >=10 c_txn_type=72 em 13/05
  3. CRASH_FARMER (refinada) — intercalacao 5min + 1 crash
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

CASE_ID = 440908751792034786

# Janela do dia 13/05/2026 BRT = 13/05 03:00 UTC -> 14/05 03:00 UTC
DIA_INI = "2026-05-13 03:00:00"
DIA_FIM = "2026-05-14 03:00:00"

print("=" * 80)
print("CALIBRACAO 3 TAGS — DIA 13/05/2026 BRT")
print(f"Janela UTC: {DIA_INI} -> {DIA_FIM}")
print("=" * 80)


# ======================================================================
# TAG 1 - MINES_PENDING_FRAUD
# ======================================================================
print("\n" + "=" * 80)
print("[TAG 1] MINES_PENDING_FRAUD — bet=cancel mesmo valor 15min + MINES")
print("=" * 80)

sql1 = f"""
WITH pares AS (
  SELECT
    b.c_ecr_id,
    b.c_start_time AS t_bet,
    b.c_amount_in_ecr_ccy AS valor_centavos,
    b.c_game_id,
    r.c_start_time AS t_cancel,
    date_diff('second', b.c_start_time, r.c_start_time) AS gap_seg
  FROM fund_ec2.tbl_real_fund_txn b
  JOIN fund_ec2.tbl_real_fund_txn r
    ON b.c_ecr_id = r.c_ecr_id
   AND r.c_txn_type = 72
   AND r.c_txn_status = 'SUCCESS'
   AND r.c_amount_in_ecr_ccy = b.c_amount_in_ecr_ccy
   AND r.c_game_id = b.c_game_id
   AND r.c_start_time BETWEEN b.c_start_time AND b.c_start_time + INTERVAL '15' MINUTE
  WHERE b.c_txn_type = 27
    AND b.c_txn_status = 'SUCCESS'
    AND b.c_amount_in_ecr_ccy >= 10000  -- >= R$ 100
    AND b.c_game_category = 158  -- MINES
    AND b.c_start_time >= TIMESTAMP '{DIA_INI}'
    AND b.c_start_time <  TIMESTAMP '{DIA_FIM}'
)
SELECT
  COUNT(DISTINCT c_ecr_id) AS players_flagados,
  COUNT(*) AS total_pares,
  ROUND(AVG(valor_centavos)/100.0, 2) AS avg_valor_brl,
  ROUND(AVG(gap_seg), 1) AS avg_gap_seg
FROM pares
"""
df1 = query_athena(sql1, database="fund_ec2")
print("\n--- Resumo geral em 13/05 ---")
print(df1.to_string(index=False))

# Top players
sql1_top = f"""
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
    AND b.c_game_category = 158
    AND b.c_start_time >= TIMESTAMP '{DIA_INI}'
    AND b.c_start_time <  TIMESTAMP '{DIA_FIM}'
)
SELECT
  c_ecr_id,
  COUNT(*) AS pares,
  ROUND(SUM(valor_centavos)/100.0, 2) AS soma_valor_brl
FROM pares
GROUP BY c_ecr_id
ORDER BY pares DESC, soma_valor_brl DESC
LIMIT 30
"""
df1_top = query_athena(sql1_top, database="fund_ec2")
print("\n--- Top 30 players ---")
print(df1_top.to_string(index=False))

# 30311442 caiu?
if CASE_ID in df1_top['c_ecr_id'].astype(int).values:
    rank = df1_top['c_ecr_id'].astype(int).tolist().index(CASE_ID) + 1
    print(f"\n[OK] 30311442 caiu em MINES_PENDING_FRAUD — posicao {rank} no top 30")
else:
    print(f"\n[INFO] 30311442 nao esta no top 30 (mas pode estar abaixo)")


# ======================================================================
# TAG 2 - CANCEL_HEAVY_DAILY
# ======================================================================
print("\n\n" + "=" * 80)
print("[TAG 2] CANCEL_HEAVY_DAILY — >=10 c_txn_type=72 em 13/05")
print("=" * 80)

sql2 = f"""
SELECT
  c_ecr_id,
  COUNT(*) AS n_cancels
FROM fund_ec2.tbl_real_fund_txn
WHERE c_txn_type = 72
  AND c_txn_status = 'SUCCESS'
  AND c_start_time >= TIMESTAMP '{DIA_INI}'
  AND c_start_time <  TIMESTAMP '{DIA_FIM}'
GROUP BY c_ecr_id
HAVING COUNT(*) >= 10
ORDER BY n_cancels DESC
"""
df2 = query_athena(sql2, database="fund_ec2")
print(f"\nTotal players com >=10 cancelamentos em 13/05: {len(df2)}")
if not df2.empty:
    print("\n--- Top 30 ---")
    print(df2.head(30).to_string(index=False))

# 30311442
sql2_case = f"""
SELECT COUNT(*) AS cancels_em_13mai
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {CASE_ID}
  AND c_txn_type = 72 AND c_txn_status = 'SUCCESS'
  AND c_start_time >= TIMESTAMP '{DIA_INI}'
  AND c_start_time <  TIMESTAMP '{DIA_FIM}'
"""
print(f"\n--- 30311442 em 13/05 ---")
print(query_athena(sql2_case, database="fund_ec2").to_string(index=False))


# ======================================================================
# TAG 3 - CRASH_FARMER (refinada — intercalacao 5min + 1 crash)
# ======================================================================
print("\n\n" + "=" * 80)
print("[TAG 3] CRASH_FARMER refinada — intercalacao 5min + 1 crash em 13/05")
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
),
transicoes AS (
  SELECT c_ecr_id
  FROM bets_dia
  WHERE prev_game_id IS NOT NULL
    AND c_game_id <> prev_game_id  -- mudou de jogo
    AND date_diff('second', prev_ts, c_start_time) <= 300  -- 5 min
    AND (c_game_category = 158 OR prev_game_category = 158)  -- 1 dos 2 e crash
)
SELECT
  c_ecr_id,
  COUNT(*) AS n_transicoes
FROM transicoes
GROUP BY c_ecr_id
ORDER BY n_transicoes DESC
"""
df3 = query_athena(sql3, database="fund_ec2")
print(f"\nTotal players com transicao crash em 13/05: {len(df3)}")
if not df3.empty:
    print("\n--- Distribuicao ---")
    import pandas as pd
    df3['faixa'] = pd.cut(
        df3['n_transicoes'],
        bins=[0, 1, 5, 20, 50, 99999],
        labels=['1', '2-5', '6-20', '21-50', '>50']
    )
    print(df3.groupby('faixa', observed=True).size().to_string())
    print("\n--- Top 30 ---")
    print(df3.head(30).to_string(index=False))

if CASE_ID in df3['c_ecr_id'].astype(int).values:
    rank = df3['c_ecr_id'].astype(int).tolist().index(CASE_ID) + 1
    n = df3[df3['c_ecr_id'].astype(int) == CASE_ID]['n_transicoes'].values[0]
    print(f"\n[OK] 30311442 esta em CRASH_FARMER — posicao {rank}, {n} transicoes")
else:
    print(f"\n[INFO] 30311442 NAO caiu em CRASH_FARMER (pouca alternancia)")


# ======================================================================
# RESUMO FINAL
# ======================================================================
print("\n\n" + "=" * 80)
print("RESUMO FINAL — 13/05/2026 BRT")
print("=" * 80)
print(f"\nTAG 1 (MINES_PENDING_FRAUD): {df1_top['c_ecr_id'].nunique()} players unicos no top30 / total {df1['players_flagados'].values[0]}")
print(f"TAG 2 (CANCEL_HEAVY_DAILY): {len(df2)} players com >=10 cancels no dia")
print(f"TAG 3 (CRASH_FARMER refinada): {len(df3)} players com transicao crash 5min")

# Players unicos cobertos por pelo menos 1 tag
all_ids = set()
for d in [df1_top, df2, df3]:
    if not d.empty:
        all_ids.update(d['c_ecr_id'].astype(int).tolist())
print(f"\nUNIAO das 3 tags (players UNICOS): {len(all_ids)}")
print(f"30311442 cai em pelo menos 1: {CASE_ID in all_ids}")
