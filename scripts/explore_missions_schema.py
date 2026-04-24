"""
Explorar schema das tabelas missions.event_log e missions.user_progress
no Super Nova DB para a analise de retorno das missoes.

Objetivo: descobrir colunas, tipos, e amostra de dados.
"""
import sys
sys.path.insert(0, ".")

from db.supernova import execute_supernova

def run_query(label, sql):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    try:
        rows = execute_supernova(sql, fetch=True)
        if rows:
            for row in rows:
                print(row)
        else:
            print("(sem resultados)")
    except Exception as e:
        print(f"ERRO: {e}")

# 1. Verificar se o schema missions existe
run_query("Schemas disponiveis (missions)", """
    SELECT schema_name
    FROM information_schema.schemata
    WHERE schema_name LIKE '%mission%'
    ORDER BY schema_name
""")

# 2. Tabelas no schema missions
run_query("Tabelas no schema missions", """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'missions'
    ORDER BY table_name
""")

# 3. Colunas de event_log
run_query("Colunas missions.event_log", """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'missions' AND table_name = 'event_log'
    ORDER BY ordinal_position
""")

# 4. Colunas de user_progress
run_query("Colunas missions.user_progress", """
    SELECT column_name, data_type, is_nullable
    FROM information_schema.columns
    WHERE table_schema = 'missions' AND table_name = 'user_progress'
    ORDER BY ordinal_position
""")

# 5. Contagem de registros
run_query("Contagem event_log", "SELECT COUNT(*) FROM missions.event_log")
run_query("Contagem user_progress", "SELECT COUNT(*) FROM missions.user_progress")

# 6. Amostra event_log (5 rows)
run_query("Amostra event_log (5 rows)", "SELECT * FROM missions.event_log LIMIT 5")

# 7. Amostra user_progress (5 rows)
run_query("Amostra user_progress (5 rows)", "SELECT * FROM missions.user_progress LIMIT 5")

# 8. Datas min/max em event_log
run_query("Range de datas event_log", """
    SELECT
        MIN(created_at) as min_date,
        MAX(created_at) as max_date
    FROM missions.event_log
""")

# 9. Tipos de eventos distintos
run_query("Tipos de eventos (event_log)", """
    SELECT DISTINCT event_type, COUNT(*) as qty
    FROM missions.event_log
    GROUP BY event_type
    ORDER BY qty DESC
    LIMIT 20
""")

print("\n\nDiscovery completa!")
