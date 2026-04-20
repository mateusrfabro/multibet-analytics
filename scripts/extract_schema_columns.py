"""
Extrai dicionario de colunas de todos os objetos dos schemas 'multibet' e 'play4'.

Saida:
- reports/schema_columns_multibet_YYYYMMDD.json (estrutura machine-readable)

Formato:
{
  "multibet": {
    "fact_casino_rounds": [
      {"name": "id", "type": "bigint", "nullable": false, "default": "...", "comment": null},
      ...
    ]
  }
}
"""

import json
import sys
from datetime import datetime
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from db.supernova import execute_supernova


OUT_DIR = Path(__file__).resolve().parents[1] / "reports"
OUT_DIR.mkdir(exist_ok=True)

SCHEMAS = ["multibet", "play4"]


def get_columns(schema: str):
    sql = """
        SELECT
            c.table_name,
            c.column_name,
            c.ordinal_position,
            c.data_type,
            c.character_maximum_length,
            c.numeric_precision,
            c.numeric_scale,
            c.is_nullable,
            c.column_default,
            pgd.description
        FROM information_schema.columns c
        LEFT JOIN pg_catalog.pg_statio_all_tables st
            ON st.schemaname = c.table_schema AND st.relname = c.table_name
        LEFT JOIN pg_catalog.pg_description pgd
            ON pgd.objoid = st.relid AND pgd.objsubid = c.ordinal_position
        WHERE c.table_schema = %s
        ORDER BY c.table_name, c.ordinal_position
    """
    return execute_supernova(sql, params=(schema,), fetch=True)


def get_primary_keys(schema: str):
    sql = """
        SELECT tc.table_name, kc.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kc
          ON kc.constraint_name = tc.constraint_name
         AND kc.table_schema    = tc.table_schema
        WHERE tc.table_schema = %s
          AND tc.constraint_type = 'PRIMARY KEY'
        ORDER BY tc.table_name, kc.ordinal_position
    """
    rows = execute_supernova(sql, params=(schema,), fetch=True)
    pks = {}
    for tname, col in rows:
        pks.setdefault(tname, []).append(col)
    return pks


def build_type(row):
    dtype = row[3]
    char_len = row[4]
    num_prec = row[5]
    num_scale = row[6]
    if dtype in ("character varying", "character") and char_len:
        return f"{dtype}({char_len})"
    if dtype == "numeric" and num_prec:
        return f"numeric({num_prec},{num_scale or 0})"
    return dtype


def main():
    report = {"generated_at": datetime.now().isoformat(timespec="seconds"), "schemas": {}}
    for schema in SCHEMAS:
        print(f">>> {schema}")
        cols = get_columns(schema)
        pks = get_primary_keys(schema)
        tables = {}
        for row in cols:
            tname, cname, pos, dtype, clen, nprec, nscale, nullable, default, comment = row
            tables.setdefault(tname, []).append({
                "name": cname,
                "pos": pos,
                "type": build_type(row),
                "nullable": (nullable == "YES"),
                "default": default,
                "pk": cname in pks.get(tname, []),
                "comment": comment,
            })
        report["schemas"][schema] = {
            "tables": tables,
            "primary_keys": pks,
            "object_count": len(tables),
        }
        print(f"    {len(tables)} objetos mapeados")

    out = OUT_DIR / f"schema_columns_multibet_{datetime.now():%Y%m%d}.json"
    out.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
    print(f"\nSalvo: {out}")


if __name__ == "__main__":
    main()
