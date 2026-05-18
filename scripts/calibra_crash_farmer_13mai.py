"""
CRASH_FARMER (versao refinada do Castrin) — APENAS DIA 13/05/2026 BRT.

Regra refinada (sugerida pelo Castrin):
  - bets intercaladas em janela de 5 min
  - >= 2 c_game_id distintos
  - pelo menos 1 e crash (c_game_category=158)
  - intercalacao real: deteccao via LAG (game_id muda entre bets consecutivas)

Output:
  - Total players flagados em 13/05
  - Top 30 players com mais transicoes
  - 30311442 cai?
  - Comparacao vs regra antiga (bin 15min)
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena

print("=" * 80)
print("CRASH_FARMER REFINADA — 13/05/2026 BRT")
print("Regra: bets intercaladas em 5min + >=2 jogos distintos + 1 crash")
print("=" * 80)

# Estrategia: para cada player, ordenar bets por timestamp, calcular LAG do
# c_game_id e do timestamp. Uma "transicao" e quando:
#   - c_game_id muda vs anterior
#   - gap <= 5 minutos
#   - pelo menos 1 dos 2 game_ids (anterior ou atual) e crash (158)
sql = """
WITH bets_dia AS (
  SELECT
    c_ecr_id,
    c_start_time,
    c_game_id,
    c_game_category,
    c_amount_in_ecr_ccy / 100.0 AS amount_brl,
    LAG(c_game_id) OVER (
      PARTITION BY c_ecr_id
      ORDER BY c_start_time
    ) AS prev_game_id,
    LAG(c_game_category) OVER (
      PARTITION BY c_ecr_id
      ORDER BY c_start_time
    ) AS prev_game_category,
    LAG(c_start_time) OVER (
      PARTITION BY c_ecr_id
      ORDER BY c_start_time
    ) AS prev_ts
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_txn_type = 27
    AND c_txn_status = 'SUCCESS'
    AND c_game_id IS NOT NULL
    AND c_start_time >= TIMESTAMP '2026-05-13 03:00:00'  -- 00h BRT
    AND c_start_time <  TIMESTAMP '2026-05-14 03:00:00'  -- 00h BRT D+1
),
transicoes AS (
  SELECT
    c_ecr_id,
    c_start_time AS ts_atual,
    prev_ts,
    c_game_id AS game_atual,
    prev_game_id AS game_anterior,
    c_game_category AS cat_atual,
    prev_game_category AS cat_anterior,
    date_diff('second', prev_ts, c_start_time) AS gap_seg
  FROM bets_dia
  WHERE prev_game_id IS NOT NULL
    AND c_game_id <> prev_game_id  -- mudou de jogo
    AND date_diff('second', prev_ts, c_start_time) <= 300  -- 5 min
    AND (c_game_category = 158 OR prev_game_category = 158)  -- 1 dos 2 e crash
),
players AS (
  SELECT
    c_ecr_id,
    COUNT(*) AS n_transicoes,
    COUNT(DISTINCT game_atual) + COUNT(DISTINCT game_anterior) AS games_envolvidos
  FROM transicoes
  GROUP BY c_ecr_id
)
SELECT
  CASE
    WHEN n_transicoes = 1 THEN '01 - 1 transicao (acidente)'
    WHEN n_transicoes BETWEEN 2 AND 5 THEN '02 - 2-5 (multitasker)'
    WHEN n_transicoes BETWEEN 6 AND 20 THEN '03 - 6-20 (multitasker pesado)'
    WHEN n_transicoes BETWEEN 21 AND 50 THEN '04 - 21-50 (suspeito)'
    ELSE '05 - >50 (farmer claro)'
  END AS faixa,
  COUNT(*) AS players
FROM players
GROUP BY 1
ORDER BY 1
"""
print("\n--- Distribuicao em 13/05 ---")
df = query_athena(sql, database="fund_ec2")
print(df.to_string(index=False))

total_players_flagados = int(df['players'].sum()) if not df.empty else 0
print(f"\n=> TOTAL PLAYERS COM PELO MENOS 1 TRANSICAO CRASH EM 13/05: {total_players_flagados}")


# Top players com mais transicoes
print("\n\n--- Top 30 players (13/05) ---")
sql_top = """
WITH bets_dia AS (
  SELECT
    c_ecr_id,
    c_start_time,
    c_game_id,
    c_game_category,
    LAG(c_game_id) OVER (
      PARTITION BY c_ecr_id ORDER BY c_start_time
    ) AS prev_game_id,
    LAG(c_game_category) OVER (
      PARTITION BY c_ecr_id ORDER BY c_start_time
    ) AS prev_game_category,
    LAG(c_start_time) OVER (
      PARTITION BY c_ecr_id ORDER BY c_start_time
    ) AS prev_ts
  FROM fund_ec2.tbl_real_fund_txn
  WHERE c_txn_type = 27
    AND c_txn_status = 'SUCCESS'
    AND c_game_id IS NOT NULL
    AND c_start_time >= TIMESTAMP '2026-05-13 03:00:00'
    AND c_start_time <  TIMESTAMP '2026-05-14 03:00:00'
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
LIMIT 30
"""
df_top = query_athena(sql_top, database="fund_ec2")
print(df_top.to_string(index=False))

# 30311442 ta na lista?
case_id = 440908751792034786
if case_id in df_top['c_ecr_id'].astype(int).values:
    print(f"\n[OK] 30311442 (ecr_id={case_id}) ESTA no top 30")
else:
    # Procurar fora do top
    sql_case = f"""
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
      WHERE c_ecr_id = {case_id}
        AND c_txn_type = 27
        AND c_txn_status = 'SUCCESS'
        AND c_game_id IS NOT NULL
        AND c_start_time >= TIMESTAMP '2026-05-13 03:00:00'
        AND c_start_time <  TIMESTAMP '2026-05-14 03:00:00'
    )
    SELECT
      c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_brt,
      prev_game_id AS de,
      c_game_id AS para,
      date_diff('second', prev_ts, c_start_time) AS gap_seg,
      prev_game_category AS cat_de,
      c_game_category AS cat_para
    FROM bets_dia
    WHERE prev_game_id IS NOT NULL
      AND c_game_id <> prev_game_id
      AND date_diff('second', prev_ts, c_start_time) <= 300
      AND (c_game_category = 158 OR prev_game_category = 158)
    ORDER BY c_start_time
    """
    df_case = query_athena(sql_case, database="fund_ec2")
    print(f"\n--- 30311442 (ecr_id={case_id}) transicoes em 13/05 ---")
    if df_case is None or df_case.empty:
        print("  (NAO DETECTOU TRANSICAO crash em 13/05)")
    else:
        print(df_case.to_string(index=False))
        print(f"  Total transicoes: {len(df_case)}")
