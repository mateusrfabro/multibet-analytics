"""
Exploração completa do schema play4 no SuperNova DB (PostgreSQL).
Objetivo: mapear todas as tabelas, colunas, tipos, volume de dados,
e identificar tabelas relevantes para análise de GGR.

Autor: Mateus Fabro | Data: 2026-04-08
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.supernova import get_supernova_connection

def run():
    tunnel, conn = get_supernova_connection()
    cur = conn.cursor()

    print("=" * 80)
    print("EXPLORAÇÃO SCHEMA play4 — SuperNova DB")
    print("=" * 80)

    # 1. Listar todas as tabelas e views do schema play4
    print("\n### 1. TABELAS E VIEWS NO SCHEMA play4 ###\n")
    cur.execute("""
        SELECT table_name, table_type
        FROM information_schema.tables
        WHERE table_schema = 'play4'
        ORDER BY table_type, table_name
    """)
    tables = cur.fetchall()
    if not tables:
        print("NENHUMA tabela encontrada no schema 'play4'.")
        # Tentar listar schemas disponíveis
        print("\nSchemas disponíveis no banco:")
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            ORDER BY schema_name
        """)
        for row in cur.fetchall():
            print(f"  - {row[0]}")

        # Tentar buscar schemas que contenham 'play'
        print("\nSchemas contendo 'play':")
        cur.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name ILIKE '%play%'
            ORDER BY schema_name
        """)
        play_schemas = cur.fetchall()
        if play_schemas:
            for row in play_schemas:
                print(f"  - {row[0]}")
        else:
            print("  Nenhum schema com 'play' encontrado.")

        # Buscar tabelas que contenham 'play4' no nome
        print("\nTabelas contendo 'play4' em qualquer schema:")
        cur.execute("""
            SELECT table_schema, table_name, table_type
            FROM information_schema.tables
            WHERE table_name ILIKE '%play4%' OR table_schema ILIKE '%play4%'
            ORDER BY table_schema, table_name
        """)
        play4_tables = cur.fetchall()
        if play4_tables:
            for row in play4_tables:
                print(f"  {row[0]}.{row[1]} ({row[2]})")
        else:
            print("  Nenhuma tabela com 'play4' encontrada.")

        cur.close()
        conn.close()
        tunnel.stop()
        return

    for t in tables:
        print(f"  {t[1]:15s} | {t[0]}")

    print(f"\nTotal: {len(tables)} objetos")

    # 2. Para cada tabela, listar colunas com tipos
    print("\n### 2. ESTRUTURA DAS TABELAS ###\n")
    for table_name, table_type in tables:
        print(f"\n--- {table_name} ({table_type}) ---")
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default,
                   character_maximum_length, numeric_precision
            FROM information_schema.columns
            WHERE table_schema = 'play4' AND table_name = %s
            ORDER BY ordinal_position
        """, (table_name,))
        cols = cur.fetchall()
        for c in cols:
            dtype = c[1]
            if c[4]:  # char length
                dtype += f"({c[4]})"
            elif c[5]:  # numeric precision
                dtype += f"(p{c[5]})"
            nullable = "NULL" if c[2] == "YES" else "NOT NULL"
            default = f" DEFAULT {c[3]}" if c[3] else ""
            print(f"  {c[0]:40s} {dtype:25s} {nullable}{default}")

        # Contagem de linhas
        try:
            cur.execute(f'SELECT COUNT(*) FROM play4."{table_name}"')
            count = cur.fetchone()[0]
            print(f"  >> LINHAS: {count:,}")
        except Exception as e:
            conn.rollback()
            print(f"  >> ERRO ao contar: {e}")

    # 3. Constraints e indexes
    print("\n### 3. CONSTRAINTS ###\n")
    cur.execute("""
        SELECT tc.table_name, tc.constraint_name, tc.constraint_type,
               kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        WHERE tc.table_schema = 'play4'
        ORDER BY tc.table_name, tc.constraint_type
    """)
    constraints = cur.fetchall()
    if constraints:
        for c in constraints:
            print(f"  {c[0]:30s} {c[2]:15s} {c[1]} → {c[3]}")
    else:
        print("  Nenhuma constraint encontrada.")

    # 4. Indexes
    print("\n### 4. INDEXES ###\n")
    cur.execute("""
        SELECT indexname, tablename, indexdef
        FROM pg_indexes
        WHERE schemaname = 'play4'
        ORDER BY tablename, indexname
    """)
    indexes = cur.fetchall()
    if indexes:
        for idx in indexes:
            print(f"  {idx[1]:30s} {idx[0]}")
            print(f"    {idx[2]}")
    else:
        print("  Nenhum index encontrado.")

    # 5. Amostras de dados (primeiros 3 registros de cada tabela com dados)
    print("\n### 5. AMOSTRAS DE DADOS (3 primeiros registros) ###\n")
    for table_name, table_type in tables:
        try:
            cur.execute(f'SELECT * FROM play4."{table_name}" LIMIT 3')
            rows = cur.fetchall()
            if rows:
                col_names = [desc[0] for desc in cur.description]
                print(f"\n--- {table_name} ---")
                print(f"  Colunas: {col_names}")
                for i, row in enumerate(rows):
                    print(f"  Row {i+1}: {row}")
        except Exception as e:
            conn.rollback()
            print(f"  {table_name}: ERRO - {e}")

    # 6. Buscar colunas que pareçam relacionadas a GGR/financeiro
    print("\n### 6. COLUNAS FINANCEIRAS (bet, win, ggr, amount, revenue) ###\n")
    cur.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'play4'
          AND (column_name ILIKE '%ggr%'
               OR column_name ILIKE '%bet%'
               OR column_name ILIKE '%win%'
               OR column_name ILIKE '%amount%'
               OR column_name ILIKE '%revenue%'
               OR column_name ILIKE '%profit%'
               OR column_name ILIKE '%loss%'
               OR column_name ILIKE '%deposit%'
               OR column_name ILIKE '%payout%')
        ORDER BY table_name, column_name
    """)
    financial_cols = cur.fetchall()
    if financial_cols:
        for fc in financial_cols:
            print(f"  {fc[0]:30s} {fc[1]:30s} {fc[2]}")
    else:
        print("  Nenhuma coluna financeira encontrada.")

    cur.close()
    conn.close()
    tunnel.stop()
    print("\n" + "=" * 80)
    print("FIM DA EXPLORAÇÃO")
    print("=" * 80)


if __name__ == "__main__":
    run()
