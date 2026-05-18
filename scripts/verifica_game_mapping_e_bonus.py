"""
Duas verificacoes:
  1) Schema de game_image_mapping (e outras tabelas) no Super Nova DB
     para descobrir se temos game_id -> categoria
  2) Bonus/rollover do 30311442 - confirmar se ele usou bonus/campanha
     no exploit de ontem (hipotese: e ROLLOVER, nao cancelamento puro)
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection
from db.athena import query_athena

CASE_ECR_ID = 440908751792034786

# ======================================================================
# Parte 1 - Game category no Super Nova DB
# ======================================================================
print("=" * 80)
print("[1] Procurando tabelas com 'game' no Super Nova DB (multibet schema)")
print("=" * 80)

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('multibet', 'public', 'play4')
              AND table_name ILIKE '%game%'
            ORDER BY table_schema, table_name
        """)
        for r in cur.fetchall():
            print(f"  {r[0]}.{r[1]}")

        # Schema de game_image_mapping
        print("\n--- Colunas de multibet.game_image_mapping (se existir) ---")
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='game_image_mapping'
            ORDER BY ordinal_position
        """)
        cols = cur.fetchall()
        if cols:
            for c in cols:
                print(f"  {c[0]:30s} {c[1]}")

            # Amostra
            print("\n--- Amostra (10 linhas) ---")
            cur.execute("SELECT * FROM multibet.game_image_mapping LIMIT 10")
            descr = [d[0] for d in cur.description]
            for row in cur.fetchall():
                print("  " + " | ".join(f"{c}={v!r}" for c, v in zip(descr, row)))

            # Tem JELLY ou MINES?
            print("\n--- Procurando JELLY e MINES ---")
            cur.execute("""
                SELECT * FROM multibet.game_image_mapping
                WHERE LOWER(CAST((SELECT array_to_string(array_agg(column_name), ',')
                  FROM information_schema.columns
                  WHERE table_schema='multibet' AND table_name='game_image_mapping') AS text)) LIKE ''
            """)
        else:
            print("  (tabela nao encontrada)")
finally:
    conn.close()
    tunnel.stop()


# Re-abre conexao para fazer buscas com regex case-insensitive
tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        # Sem saber o nome exato das colunas, busca generica em todas colunas text
        # Listar colunas primeiro
        cur.execute("""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='game_image_mapping'
              AND data_type IN ('text', 'character varying')
            ORDER BY ordinal_position
        """)
        text_cols = [r[0] for r in cur.fetchall()]
        print(f"\n--- Colunas de texto: {text_cols} ---")

        if text_cols:
            # Procura JELLY
            where_or = " OR ".join([f"LOWER({c}::text) LIKE '%jelly%'" for c in text_cols])
            cur.execute(f"SELECT * FROM multibet.game_image_mapping WHERE {where_or} LIMIT 5")
            descr = [d[0] for d in cur.description]
            print("\n--- JELLY matches ---")
            for row in cur.fetchall():
                print("  " + " | ".join(f"{c}={v!r}" for c, v in zip(descr, row)))

            # Procura MINES
            where_or = " OR ".join([f"LOWER({c}::text) LIKE '%mines%'" for c in text_cols])
            cur.execute(f"SELECT * FROM multibet.game_image_mapping WHERE {where_or} LIMIT 5")
            print("\n--- MINES matches ---")
            for row in cur.fetchall():
                print("  " + " | ".join(f"{c}={v!r}" for c, v in zip(descr, row)))

            # Procura categoria/category
            cur.execute("""
                SELECT DISTINCT column_name FROM information_schema.columns
                WHERE table_schema='multibet'
                  AND (column_name ILIKE '%categ%' OR column_name ILIKE '%type%'
                       OR column_name ILIKE '%class%')
            """)
            print(f"\n--- Colunas com 'categ/type/class' em multibet: ---")
            for r in cur.fetchall():
                print(f"  {r[0]}")
finally:
    conn.close()
    tunnel.stop()


# ======================================================================
# Parte 2 - Bonus/rollover do 30311442
# ======================================================================
print("\n\n" + "=" * 80)
print("[2] Bonus/rollover do 30311442 em fund_ec2 + bonus_ec2 (13/05)")
print("=" * 80)

# Quais c_txn_type relacionados a bonus aconteceram?
sql_bonus_fund = f"""
SELECT
  c_txn_type,
  COUNT(*) AS qtd,
  ROUND(SUM(c_amount_in_ecr_ccy)/100.0, 2) AS soma_brl,
  MIN(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro,
  MAX(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_start_time >= TIMESTAMP '2026-05-13 00:00:00'
  AND c_start_time <  TIMESTAMP '2026-05-15 00:00:00'
  AND c_txn_status = 'SUCCESS'
  -- bonus-related: 20=ISSUE_BONUS, 80=FREESPIN_WIN, 19=BONUS_TRANSFER, 37=??
  AND c_txn_type IN (20, 19, 37, 38, 80, 81, 86, 87, 21, 22, 23)
GROUP BY c_txn_type
ORDER BY MIN(c_start_time)
"""
print("\n--- Transacoes BONUS no fund_ec2 (13-14/05) ---")
df = query_athena(sql_bonus_fund, database="fund_ec2")
if df is None or df.empty:
    print("  (nenhuma transacao bonus em fund_ec2 com tipos 19/20/37/38/80/...)")
else:
    print(df.to_string(index=False))

# Mas tinha aquele c_txn_type=80 (R$45,16) e tinha c_txn_type=37 e 19 no resumo anterior
# Vou listar TODOS os c_txn_type dele em 13/05
print("\n--- TODOS os c_txn_type do 30311442 em 13/05 ---")
sql_all = f"""
SELECT
  c_txn_type,
  COUNT(*) AS qtd,
  ROUND(SUM(c_amount_in_ecr_ccy)/100.0, 2) AS soma_brl
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_start_time >= TIMESTAMP '2026-05-13 00:00:00'
  AND c_start_time <  TIMESTAMP '2026-05-14 00:00:00'
  AND c_txn_status = 'SUCCESS'
GROUP BY c_txn_type
ORDER BY 1
"""
df_all = query_athena(sql_all, database="fund_ec2")
print(df_all.to_string(index=False))


# Bonus emitidos para esse player (bonus_ec2)
print("\n--- Bonus em bonus_ec2 (13-14/05) ---")
sql_bonus = f"""
SELECT
  c_bonus_txn_id,
  c_bonus_txn_status,
  c_bonus_txn_type,
  c_bonus_amount/100.0 AS amount_brl,
  c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_brt
FROM bonus_ec2.tbl_bonus_pocket_txn
WHERE c_ecr_id = {CASE_ECR_ID}
  AND c_created_time >= TIMESTAMP '2026-05-13 00:00:00'
  AND c_created_time <  TIMESTAMP '2026-05-15 00:00:00'
ORDER BY c_created_time
"""
try:
    df_bonus = query_athena(sql_bonus, database="bonus_ec2")
    if df_bonus is None or df_bonus.empty:
        print("  (nenhum bonus em bonus_ec2)")
    else:
        print(df_bonus.to_string(index=False))
except Exception as e:
    print(f"  Erro/schema diferente: {e}")
