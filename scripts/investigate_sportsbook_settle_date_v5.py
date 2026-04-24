"""
STEP 21+ — Ultima camada de validacao.

Objetivo:
  A) Existe alguma OUTRA tabela (em outros databases) com calendario de eventos?
     Ex: vendor_ec2.*event*, ecr_ec2.*sport*, casino_ec2, silver.*sport*, ps_bi.*sport*
  B) Existe alguma view/tabela pre-agregada em ps_bi.*sportsbook*?
  C) Confirmar fund_ec2.tbl_real_fund_txn_type_mst — listar colunas.
  D) Como ultimo teste: ver se tbl_sports_book_info tem evento futuro via outra analise
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena


def run(sql, label, database="vendor_ec2"):
    print(f"\n>>> {label}")
    try:
        df = query_athena(sql, database=database)
        print(f"[{len(df)} linhas]")
        if len(df) > 0:
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"[ERRO] {e}")
        return None


def banner(title):
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


# ============================================================================
# STEP 21 — Procurar tabelas de evento em vendor_ec2
# ============================================================================
banner("STEP 21 — Tabelas vendor_ec2 com event/match/game/calendar")

run("SHOW TABLES IN vendor_ec2 LIKE '*event*'", "vendor_ec2 *event*", "vendor_ec2")
run("SHOW TABLES IN vendor_ec2 LIKE '*match*'", "vendor_ec2 *match*", "vendor_ec2")
run("SHOW TABLES IN vendor_ec2 LIKE '*game*'", "vendor_ec2 *game*", "vendor_ec2")
run("SHOW TABLES IN vendor_ec2 LIKE '*calendar*'", "vendor_ec2 *calendar*", "vendor_ec2")
run("SHOW TABLES IN vendor_ec2 LIKE '*feed*'", "vendor_ec2 *feed*", "vendor_ec2")
run("SHOW TABLES IN vendor_ec2 LIKE '*fixture*'", "vendor_ec2 *fixture*", "vendor_ec2")

# ============================================================================
# STEP 22 — Procurar em ps_bi e silver (camadas agregadas)
# ============================================================================
banner("STEP 22 — Tabelas ps_bi/silver com sportsbook")

run("SHOW TABLES IN ps_bi LIKE '*sport*'", "ps_bi *sport*", "ps_bi")
run("SHOW TABLES IN ps_bi LIKE '*book*'", "ps_bi *book*", "ps_bi")
run("SHOW TABLES IN ps_bi LIKE '*bet*'", "ps_bi *bet*", "ps_bi")
run("SHOW TABLES IN silver LIKE '*sport*'", "silver *sport*", "silver")

# ============================================================================
# STEP 23 — Listar todas as tabelas vendor_ec2 (caso exista algo sem nome obvio)
# ============================================================================
banner("STEP 23 — Todas as tabelas vendor_ec2")

run("SHOW TABLES IN vendor_ec2", "Todas vendor_ec2", "vendor_ec2")

# ============================================================================
# STEP 24 — fund_ec2.tbl_real_fund_txn_type_mst (colunas)
# ============================================================================
banner("STEP 24 — Validar tbl_real_fund_txn_type_mst")

run(
    "SHOW COLUMNS IN fund_ec2.tbl_real_fund_txn_type_mst",
    "Colunas tbl_real_fund_txn_type_mst",
    "fund_ec2",
)

run(
    "SELECT * FROM fund_ec2.tbl_real_fund_txn_type_mst LIMIT 10",
    "Sample 10 linhas tbl_real_fund_txn_type_mst",
    "fund_ec2",
)

# ============================================================================
# STEP 25 — Olhar tbl_sports_book_info: o que tem?
# ============================================================================
banner("STEP 25 — tbl_sports_book_info: o que guarda?")

run(
    """
    SELECT
        c_operation_type,
        COUNT(*) AS n
    FROM vendor_ec2.tbl_sports_book_info
    WHERE c_created_time >= TIMESTAMP '2026-04-05 00:00:00'
    GROUP BY c_operation_type
    ORDER BY n DESC
    """,
    "Operation types em tbl_sports_book_info",
    "vendor_ec2",
)

run(
    """
    SELECT *
    FROM vendor_ec2.tbl_sports_book_info
    WHERE c_created_time >= TIMESTAMP '2026-04-09 00:00:00'
    LIMIT 5
    """,
    "Sample tbl_sports_book_info",
    "vendor_ec2",
)

print("\n[FIM STEP 21-25]")
