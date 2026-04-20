"""
Inventario Schema Super Nova DB - refresh automatico.

Lista todos os objetos dos schemas 'multibet' e 'play4':
- Tabelas: nome, tipo (BASE TABLE / VIEW / FOREIGN), n_colunas, n_linhas (estimativa), tamanho_bytes
- Views: nome, definicao resumida
- Materialized views: idem
- Indices: por tabela
- Total por camada (bronze/silver/gold)

Saida: reports/inventario_schema_multibet_refresh_YYYYMMDD.json
"""

import json
import os
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.supernova import execute_supernova


OUT_DIR = Path(__file__).resolve().parents[1] / "reports"
OUT_DIR.mkdir(exist_ok=True)

SCHEMAS = ["multibet", "play4"]


def list_tables(schema: str):
    sql = """
        SELECT
            c.relname,
            CASE c.relkind
                WHEN 'r' THEN 'table'
                WHEN 'v' THEN 'view'
                WHEN 'm' THEN 'matview'
                WHEN 'f' THEN 'foreign_table'
                WHEN 'p' THEN 'partitioned_table'
                ELSE c.relkind::text
            END,
            c.reltuples::bigint,
            pg_total_relation_size(c.oid),
            obj_description(c.oid, 'pg_class')
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s
          AND c.relkind IN ('r','v','m','f','p')
        ORDER BY c.relkind, c.relname
    """
    return execute_supernova(sql, params=(schema,), fetch=True)


def count_columns(schema: str):
    sql = """
        SELECT table_name, COUNT(*) AS n_cols
        FROM information_schema.columns
        WHERE table_schema = %s
        GROUP BY table_name
    """
    rows = execute_supernova(sql, params=(schema,), fetch=True)
    return {r[0]: r[1] for r in rows}


def get_exact_row_count(schema: str, table_name: str):
    """Contagem exata — caro, usar so em tabelas chave."""
    sql = f'SELECT COUNT(*) FROM "{schema}"."{table_name}"'
    try:
        rows = execute_supernova(sql, fetch=True)
        return int(rows[0][0]) if rows else None
    except Exception as e:
        return f"ERRO: {e}"


def build_report():
    report = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "schemas": {},
    }
    for schema in SCHEMAS:
        print(f"\n>>> Schema: {schema}")
        objects = list_tables(schema)
        ncols = count_columns(schema)
        items = []
        for obj in objects:
            name, otype, rows_est, size_bytes, comment = obj
            item = {
                "name": name,
                "type": otype,
                "rows_estimate": int(rows_est or 0),
                "size_bytes": int(size_bytes or 0),
                "n_columns": ncols.get(name, 0),
                "comment": comment,
            }
            items.append(item)
            print(f"  {otype:8s}  {name:55s}  rows~{item['rows_estimate']:>12,}  {item['size_bytes']:>12,} B  cols={item['n_columns']}")
        report["schemas"][schema] = items

    out_path = OUT_DIR / f"inventario_schema_refresh_{datetime.now():%Y%m%d}.json"
    out_path.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"\nSalvo em: {out_path}")
    return report, out_path


if __name__ == "__main__":
    build_report()
