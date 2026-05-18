"""
Fecha os 3 dados que faltam pra proposta v2.2:
  1) Lookup canonico c_txn_type=37 (confirmar nome oficial)
  2) Como linkar fund_ec2 com game_image_mapping (descobrir join key)
  3) Lista completa de jogos categoria 'crash' no game_image_mapping
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena
from db.supernova import get_supernova_connection

print("=" * 80)
print("[1] Lookup canonico Pragmatic - tipos 19, 37, 80")
print("=" * 80)
sql = """
SELECT c_txn_type, c_internal_description
FROM fund_ec2.tbl_real_fund_txn_type_mst
WHERE c_txn_type IN (1, 2, 19, 20, 36, 37, 27, 45, 72, 76, 80, 86, 91, 113, 133)
ORDER BY c_txn_type
"""
try:
    df = query_athena(sql, database="fund_ec2")
    print(df.to_string(index=False))
except Exception as e:
    print(f"  Erro tbl_real_fund_txn_type_mst: {e}")


print("\n\n" + "=" * 80)
print("[2] Tentar varias colunas pra linkar fund_ec2 ao game_image_mapping")
print("=" * 80)

# Lista colunas de fund_ec2 que parecem relacionadas a jogo
sql_cols = """
SHOW COLUMNS FROM fund_ec2.tbl_real_fund_txn
"""
print("\n--- SHOW COLUMNS fund_ec2.tbl_real_fund_txn ---")
try:
    df = query_athena(sql_cols, database="fund_ec2")
    print(df.to_string(index=False))
except Exception as e:
    print(f"  Erro: {e}")


print("\n\n" + "=" * 80)
print("[3] Lista completa jogos game_category='crash' no Super Nova DB")
print("=" * 80)
tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT game_name, provider_game_id, vendor_id, provider_display_name,
                   is_active, rounds_24h
            FROM multibet.game_image_mapping
            WHERE game_category = 'crash'
            ORDER BY rounds_24h DESC NULLS LAST, game_name
        """)
        rows = cur.fetchall()
        print(f"Total jogos crash: {len(rows)}")
        for r in rows:
            print(f"  {r[0]:<40s} pid={r[1]:<20s} vendor={r[2]:<25s} ativo={r[4]} rounds_24h={r[5]}")
finally:
    conn.close()
    tunnel.stop()


print("\n\n" + "=" * 80)
print("[4] Cruzar fund_ec2 do 30311442 com game_image_mapping")
print("=" * 80)
# Olhar c_sub_vendor_id raw (mesmo NULL) e c_vendor_id se existir
sql_check = """
SELECT
  COUNT(*) AS total,
  COUNT(DISTINCT c_session_id) AS distinct_sessions,
  COUNT(c_sub_vendor_id) AS notnull_sub_vendor
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = 440908751792034786
  AND c_start_time >= TIMESTAMP '2026-05-13 00:00:00'
  AND c_start_time <  TIMESTAMP '2026-05-14 00:00:00'
"""
try:
    df = query_athena(sql_check, database="fund_ec2")
    print(df.to_string(index=False))
except Exception as e:
    print(f"  Erro: {e}")
