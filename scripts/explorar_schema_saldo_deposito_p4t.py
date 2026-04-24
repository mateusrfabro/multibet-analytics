"""
Explora schema do supernova_bet procurando sinais de:
(a) adicao manual de saldo (admin adjust, manual credit, bonus manual)
(b) confirmacao manual de deposito (manually_confirmed, confirmed_by, etc.)
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection


def run():
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    try:
        # 1. Listar tabelas candidatas (wallet, transaction, deposit, payment, balance, adjust)
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'public'
              AND (
                   table_name ILIKE '%wallet%'
                OR table_name ILIKE '%transaction%'
                OR table_name ILIKE '%deposit%'
                OR table_name ILIKE '%payment%'
                OR table_name ILIKE '%balance%'
                OR table_name ILIKE '%adjust%'
                OR table_name ILIKE '%credit%'
                OR table_name ILIKE '%bonus%'
              )
            ORDER BY table_name
        """)
        tabelas = [r[0] for r in cur.fetchall()]
        print(f"TABELAS CANDIDATAS: {len(tabelas)}")
        print("=" * 90)
        for t in tabelas:
            print(f"  - {t}")

        # 2. Para cada tabela, listar colunas + contagem de registros
        print("\n" + "=" * 90)
        print("SCHEMA DE CADA TABELA (colunas + tipo + sample count)")
        print("=" * 90)

        for t in tabelas:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                ORDER BY ordinal_position
            """, (t,))
            cols = cur.fetchall()

            cur.execute(f'SELECT COUNT(*) FROM "{t}"')
            n = cur.fetchone()[0]

            print(f"\n[{t}] -- {n:,} registros")
            for col, tipo in cols:
                # Destacar colunas suspeitas
                highlight = ""
                c = col.lower()
                if any(k in c for k in ['type', 'kind', 'status', 'method', 'source',
                                         'manual', 'admin', 'confirmed', 'approved',
                                         'created_by', 'updated_by', 'adjusted_by',
                                         'reason', 'note', 'description']):
                    highlight = "  <-- SUSPEITO"
                print(f"    {col:<35} {tipo:<20}{highlight}")

        # 3. Para as tabelas mais promissoras, amostrar distinct values das colunas chave
        print("\n" + "=" * 90)
        print("VALORES DISTINTOS DE COLUNAS CHAVE (type, status, method, source)")
        print("=" * 90)

        for t in tabelas:
            cur.execute("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                  AND column_name IN ('type', 'kind', 'status', 'method',
                                      'source', 'transaction_type', 'transaction_status',
                                      'deposit_status', 'deposit_method', 'payment_type',
                                      'payment_method', 'payment_status', 'adjustment_type',
                                      'provider', 'reason')
            """, (t,))
            cols_chave = [r[0] for r in cur.fetchall()]

            for col in cols_chave:
                try:
                    cur.execute(f'SELECT DISTINCT "{col}", COUNT(*) FROM "{t}" GROUP BY "{col}" ORDER BY COUNT(*) DESC LIMIT 20')
                    rows = cur.fetchall()
                    print(f"\n[{t}.{col}]:")
                    for v, cnt in rows:
                        print(f"    {str(v)[:60]:<62} {cnt:>10,}")
                except Exception as e:
                    print(f"  ERRO amostrando {t}.{col}: {e}")

    finally:
        cur.close()
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
