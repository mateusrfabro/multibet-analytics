"""
Exploração completa do banco Super Nova Bet (Play4Tune - Paquistão).
Mapeia schemas, tabelas, colunas, tipos, row counts e amostras.

Saída: reports/supernova_bet_schema_map.txt
"""

import os
import sys
import json
from datetime import datetime

# Adiciona raiz do projeto ao path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection


def explore():
    print("Conectando ao Super Nova Bet DB via bastion...")
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    output_lines = []

    def log(msg=""):
        print(msg)
        output_lines.append(msg)

    log("=" * 80)
    log(f"MAPEAMENTO SUPER NOVA BET DB — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log("=" * 80)

    # 1. Versão do banco
    cur.execute("SELECT version()")
    log(f"\nVersão: {cur.fetchone()[0]}")

    # 2. Listar schemas (excluindo internos)
    cur.execute("""
        SELECT schema_name
        FROM information_schema.schemata
        WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
        ORDER BY schema_name
    """)
    schemas = [r[0] for r in cur.fetchall()]
    log(f"\nSchemas encontrados ({len(schemas)}): {', '.join(schemas)}")

    # 3. Para cada schema, listar tabelas/views
    for schema in schemas:
        cur.execute("""
            SELECT table_name, table_type
            FROM information_schema.tables
            WHERE table_schema = %s
            ORDER BY table_type, table_name
        """, (schema,))
        tables = cur.fetchall()

        if not tables:
            log(f"\n--- Schema: {schema} (vazio) ---")
            continue

        log(f"\n{'=' * 80}")
        log(f"SCHEMA: {schema} ({len(tables)} objetos)")
        log(f"{'=' * 80}")

        for table_name, table_type in tables:
            full_name = f"{schema}.{table_name}"

            # Row count (estimativa rápida via pg_stat)
            try:
                cur.execute("""
                    SELECT n_live_tup
                    FROM pg_stat_user_tables
                    WHERE schemaname = %s AND relname = %s
                """, (schema, table_name))
                row_est = cur.fetchone()
                row_count_est = row_est[0] if row_est else "?"

                # Count exato para tabelas pequenas
                if row_count_est != "?" and row_count_est < 100000:
                    cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table_name}"')
                    row_count = cur.fetchone()[0]
                else:
                    row_count = f"~{row_count_est}"
            except Exception as e:
                row_count = f"erro: {e}"
                conn.rollback() if not conn.autocommit else None

            type_label = "VIEW" if "VIEW" in table_type.upper() else "TABLE"
            log(f"\n  [{type_label}] {full_name} — {row_count} linhas")

            # Colunas
            cur.execute("""
                SELECT column_name, data_type, is_nullable,
                       column_default, character_maximum_length,
                       numeric_precision, numeric_scale
                FROM information_schema.columns
                WHERE table_schema = %s AND table_name = %s
                ORDER BY ordinal_position
            """, (schema, table_name))
            columns = cur.fetchall()

            log(f"  Colunas ({len(columns)}):")
            for col in columns:
                col_name, dtype, nullable, default, max_len, num_prec, num_scale = col
                type_str = dtype
                if max_len:
                    type_str += f"({max_len})"
                elif num_prec and dtype in ('numeric', 'decimal'):
                    type_str += f"({num_prec},{num_scale or 0})"
                null_str = "" if nullable == "YES" else " NOT NULL"
                default_str = f" DEFAULT {default}" if default else ""
                log(f"    {col_name:<40} {type_str:<25}{null_str}{default_str}")

            # Sample (3 linhas)
            try:
                cur.execute(f'SELECT * FROM "{schema}"."{table_name}" LIMIT 3')
                sample_rows = cur.fetchall()
                col_names = [desc[0] for desc in cur.description]
                if sample_rows:
                    log(f"  Sample (top 3):")
                    log(f"    {' | '.join(col_names)}")
                    for row in sample_rows:
                        vals = [str(v)[:50] if v is not None else "NULL" for v in row]
                        log(f"    {' | '.join(vals)}")
            except Exception as e:
                log(f"  Sample erro: {e}")

    # 4. Foreign tables e servers
    log(f"\n{'=' * 80}")
    log("FOREIGN DATA WRAPPERS & SERVERS")
    log(f"{'=' * 80}")

    cur.execute("""
        SELECT srvname, srvowner::regrole, srvoptions
        FROM pg_foreign_server
    """)
    servers = cur.fetchall()
    if servers:
        for srv in servers:
            log(f"  Server: {srv[0]} | Owner: {srv[1]} | Options: {srv[2]}")
    else:
        log("  Nenhum foreign server encontrado.")

    cur.execute("""
        SELECT foreign_table_schema, foreign_table_name, foreign_server_name
        FROM information_schema.foreign_tables
        ORDER BY 1, 2
    """)
    ftables = cur.fetchall()
    if ftables:
        log(f"\n  Foreign Tables ({len(ftables)}):")
        for ft in ftables:
            log(f"    {ft[0]}.{ft[1]} → server: {ft[2]}")

    # 5. Materialized views
    log(f"\n{'=' * 80}")
    log("MATERIALIZED VIEWS")
    log(f"{'=' * 80}")

    cur.execute("""
        SELECT schemaname, matviewname, hasindexes
        FROM pg_matviews
        ORDER BY 1, 2
    """)
    mvs = cur.fetchall()
    if mvs:
        for mv in mvs:
            log(f"  {mv[0]}.{mv[1]} | indexed: {mv[2]}")
    else:
        log("  Nenhuma materialized view encontrada.")

    # 6. Extensions
    cur.execute("SELECT extname, extversion FROM pg_extension ORDER BY 1")
    exts = cur.fetchall()
    log(f"\nExtensions: {', '.join(f'{e[0]} ({e[1]})' for e in exts)}")

    # 7. Database size
    cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()))")
    db_size = cur.fetchone()[0]
    log(f"\nTamanho do banco: {db_size}")

    # Salvar
    cur.close()
    conn.close()
    tunnel.stop()

    os.makedirs("reports", exist_ok=True)
    output_path = "reports/supernova_bet_schema_map.txt"
    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(output_lines))

    print(f"\nMapeamento salvo em: {output_path}")


if __name__ == "__main__":
    explore()
