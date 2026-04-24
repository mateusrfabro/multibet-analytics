"""
Detalhes adicionais das tabelas missions para montar a analise.
"""
import sys
sys.path.insert(0, ".")
from db.supernova import execute_supernova

def run(label, sql):
    print(f"\n{'='*60}")
    print(f"  {label}")
    print(f"{'='*60}")
    try:
        rows = execute_supernova(sql, fetch=True)
        for row in (rows or []):
            print(row)
    except Exception as e:
        print(f"ERRO: {e}")

# 1. Missoes distintas com contagem de jogadores
run("Missoes distintas (user_progress)", """
    SELECT mission_code, game_path, status,
           COUNT(DISTINCT user_id) as jogadores,
           MIN(accepted_at) as primeiro_optin,
           MAX(accepted_at) as ultimo_optin
    FROM missions.user_progress
    WHERE mission_code IS NOT NULL
    GROUP BY mission_code, game_path, status
    ORDER BY mission_code, status
""")

# 2. Distribuicao de status
run("Status distribuicao", """
    SELECT status, COUNT(*) as qty, COUNT(DISTINCT user_id) as users
    FROM missions.user_progress
    GROUP BY status
    ORDER BY qty DESC
""")

# 3. Formato do user_id (entender se e ecr_id ou external_id)
run("User IDs - formato (amostra 10)", """
    SELECT DISTINCT user_id, LENGTH(user_id) as len
    FROM missions.user_progress
    ORDER BY LENGTH(user_id), user_id
    LIMIT 10
""")

# 4. Contagem por tamanho do user_id
run("User IDs - distribuicao por tamanho", """
    SELECT LENGTH(user_id) as len, COUNT(DISTINCT user_id) as qty
    FROM missions.user_progress
    GROUP BY LENGTH(user_id)
    ORDER BY len
""")

# 5. Game paths distintos
run("Game paths distintos", """
    SELECT DISTINCT game_path, COUNT(*) as events
    FROM missions.event_log
    GROUP BY game_path
    ORDER BY events DESC
""")

# 6. Jogadores unicos total
run("Total jogadores unicos", """
    SELECT COUNT(DISTINCT user_id) as total_unique_users
    FROM missions.user_progress
""")

# 7. Periodo por missao (accepted_at range)
run("Periodo por missao (accepted_at)", """
    SELECT mission_code,
           MIN(accepted_at) as start,
           MAX(accepted_at) as end,
           COUNT(DISTINCT user_id) as users
    FROM missions.user_progress
    WHERE accepted_at IS NOT NULL AND mission_code NOT LIKE 'test%'
    GROUP BY mission_code
    ORDER BY MIN(accepted_at)
""")

print("\nDetalhe completo!")
