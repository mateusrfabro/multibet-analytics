"""
Investigacao empirica: campos de settle/event date nas tabelas sportsbook vendor_ec2.

Objetivo: validar se e viavel construir vw_sportsbook_ggr_projection_by_settle_date
sem novo pipeline, usando apenas campos existentes nas tabelas do vendor_ec2.

Tabelas alvo:
  1) vendor_ec2.tbl_sports_book_bets_info   (header do bilhete)
  2) vendor_ec2.tbl_sports_book_bet_details (legs)
  3) vendor_ec2.tbl_sports_book_info        (info de eventos/torneios, se existir)

Tambem valida: fund_ec2.tbl_real_fund_txn_type_mst (existencia/grafia).
"""

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena


def banner(title):
    print("\n" + "=" * 90)
    print(f"  {title}")
    print("=" * 90)


def safe_query(sql, database="default", label=""):
    try:
        df = query_athena(sql, database=database)
        print(f"\n[OK] {label} - {len(df)} linha(s)")
        if len(df) > 0:
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"\n[ERRO] {label}: {e}")
        return None


# ============================================================================
# STEP 0 — Listar tabelas sportsbook disponiveis em vendor_ec2
# ============================================================================
banner("STEP 0 — Tabelas sportsbook em vendor_ec2")

safe_query(
    "SHOW TABLES IN vendor_ec2 LIKE '*sports*'",
    database="vendor_ec2",
    label="Tabelas vendor_ec2 LIKE *sports*",
)

safe_query(
    "SHOW TABLES IN vendor_ec2 LIKE '*sport*'",
    database="vendor_ec2",
    label="Tabelas vendor_ec2 LIKE *sport*",
)

safe_query(
    "SHOW TABLES IN vendor_ec2 LIKE '*book*'",
    database="vendor_ec2",
    label="Tabelas vendor_ec2 LIKE *book*",
)

# ============================================================================
# STEP 1 — Colunas de tbl_sports_book_bets_info (header do bilhete)
# ============================================================================
banner("STEP 1 — SHOW COLUMNS tbl_sports_book_bets_info")

df_bets = safe_query(
    "SHOW COLUMNS IN vendor_ec2.tbl_sports_book_bets_info",
    database="vendor_ec2",
    label="COLUMNS tbl_sports_book_bets_info",
)

# ============================================================================
# STEP 2 — Colunas de tbl_sports_book_bet_details (legs)
# ============================================================================
banner("STEP 2 — SHOW COLUMNS tbl_sports_book_bet_details")

df_details = safe_query(
    "SHOW COLUMNS IN vendor_ec2.tbl_sports_book_bet_details",
    database="vendor_ec2",
    label="COLUMNS tbl_sports_book_bet_details",
)

# ============================================================================
# STEP 3 — Colunas de tbl_sports_book_info (se existir)
# ============================================================================
banner("STEP 3 — SHOW COLUMNS tbl_sports_book_info")

df_info = safe_query(
    "SHOW COLUMNS IN vendor_ec2.tbl_sports_book_info",
    database="vendor_ec2",
    label="COLUMNS tbl_sports_book_info",
)

# ============================================================================
# STEP 4 — Bonus: validar fund_ec2.tbl_real_fund_txn_type_mst
# ============================================================================
banner("STEP 4 — Validar fund_ec2 tabelas type_mst")

safe_query(
    "SHOW TABLES IN fund_ec2 LIKE '*type_mst*'",
    database="fund_ec2",
    label="Tabelas fund_ec2 LIKE *type_mst*",
)

safe_query(
    "SHOW TABLES IN fund_ec2 LIKE '*type*'",
    database="fund_ec2",
    label="Tabelas fund_ec2 LIKE *type*",
)

print("\n\n[FIM STEP 0-4] Execute este script primeiro para descobrir colunas,")
print("depois adaptamos os SELECTs de amostra conforme o schema real.")
