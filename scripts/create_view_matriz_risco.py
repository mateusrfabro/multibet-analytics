"""
Cria/atualiza a view multibet.matriz_risco para compatibilidade
com scripts antigos que usam coluna 'classificacao' (em vez de 'tier').

A view aponta para o snapshot MAIS RECENTE de multibet.risk_tags
e filtra apenas jogadores COM classificacao (exclui SEM SCORE).

Uso:
    python scripts/create_view_matriz_risco.py
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from db.supernova import execute_supernova

DROP_SQL = "DROP VIEW IF EXISTS multibet.matriz_risco;"

VIEW_SQL = """
CREATE VIEW multibet.matriz_risco AS
SELECT
    label_id,
    user_id,
    user_ext_id,
    snapshot_date,
    score_bruto,
    score_norm,
    tier AS classificacao,
    computed_at
FROM multibet.risk_tags
WHERE snapshot_date = (
    SELECT MAX(snapshot_date) FROM multibet.risk_tags
)
AND tier != 'SEM SCORE';
"""

VERIFY_SQL = """
SELECT classificacao, COUNT(*) AS qtd_users
FROM multibet.matriz_risco
GROUP BY classificacao
ORDER BY qtd_users DESC;
"""

if __name__ == "__main__":
    print("Dropando view antiga...")
    execute_supernova(DROP_SQL)
    print("Criando view multibet.matriz_risco...")
    execute_supernova(VIEW_SQL)
    print("View criada.")

    print("\nDistribuicao na view:")
    rows = execute_supernova(VERIFY_SQL, fetch=True)
    total = 0
    for row in rows:
        print(f"  {row[0]:15s}: {row[1]:>6d}")
        total += row[1]
    print(f"  {'TOTAL':15s}: {total:>6d}")
    print("\nDone.")
