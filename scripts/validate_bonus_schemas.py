"""
Valida empiricamente os schemas necessarios para o relatorio de Time-to-BTR.

Objetivo: confirmar colunas disponiveis antes de montar query historica
(jan-abr/2026) que pode escanear GB de dados.

Tabelas a validar:
- bonus_ec2.tbl_ecr_bonus_details (ativos)
- bonus_ec2.tbl_ecr_bonus_details_inactive (inativos/expirados/resgatados)
- bonus_ec2.tbl_bonus_summary_details (valor)
- bonus_ec2.tbl_bonus_pre_offer (tipo/template)
- fund_ec2.tbl_realcash_sub_fund_txn (BTR valor efetivo)
- ps_bi.dim_user (is_test, cadastro)
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.athena import query_athena

TABLES = [
    ("bonus_ec2", "tbl_ecr_bonus_details"),
    ("bonus_ec2", "tbl_ecr_bonus_details_inactive"),
    ("bonus_ec2", "tbl_bonus_summary_details"),
    ("bonus_ec2", "tbl_bonus_pre_offer"),
    ("fund_ec2", "tbl_realcash_sub_fund_txn"),
    ("ps_bi", "dim_user"),
]


def inspect(db: str, table: str) -> None:
    print(f"\n{'=' * 70}")
    print(f"  {db}.{table}")
    print(f"{'=' * 70}")
    try:
        cols = query_athena(f"SHOW COLUMNS FROM {db}.{table}")
        print(f"Colunas ({len(cols)}):")
        for _, row in cols.iterrows():
            print(f"  - {row.iloc[0]}")
    except Exception as e:
        print(f"ERRO: {e}")


if __name__ == "__main__":
    for db, table in TABLES:
        inspect(db, table)
    print("\n[ok] Validacao completa.")
