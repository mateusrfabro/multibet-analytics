"""Diagnostico: como as tags estao armazenadas em multibet.risk_tags?"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        # 1) Schema com tipos
        cur.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='risk_tags'
            ORDER BY ordinal_position
        """)
        print("=== SCHEMA multibet.risk_tags ===")
        for r in cur.fetchall():
            print(f"  {r[0]:30s} {r[1]:25s} nullable={r[2]}")

        # 2) Amostra crua de 1 player com score alto
        print("\n=== AMOSTRA: 3 players com score_bruto alto (Muito Bom) ===")
        cur.execute("""
            SELECT * FROM multibet.risk_tags
            WHERE snapshot_date='2026-05-14' AND tier='Muito Bom'
            ORDER BY score_bruto DESC LIMIT 3
        """)
        col_names = [d[0] for d in cur.description]
        for row in cur.fetchall():
            print("---")
            for col, val in zip(col_names, row):
                if val not in (None, 0, False):
                    print(f"  {col}={val!r}")

        # 3) Soma por tag pra ver se vale 0/1 ou outra coisa
        print("\n=== SOMA POR TAG (snapshot 14/05) ===")
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='risk_tags'
            ORDER BY ordinal_position
        """)
        cols = [r[0] for r in cur.fetchall()]
        META = {"user_id","user_ext_id","snapshot_date","score_bruto",
                "score_norm","tier","external_id","ecr_id","created_at",
                "updated_at","ecr_user_id","label_id","computed_at"}
        tags = [c for c in cols if c not in META]
        for t in tags:
            try:
                cur.execute(f'SELECT data_type FROM information_schema.columns '
                            f"WHERE table_schema='multibet' AND table_name='risk_tags' "
                            f"AND column_name=%s", (t,))
                dt = cur.fetchone()[0]
                cur.execute(f'SELECT COUNT(*), '
                            f'COUNT(CASE WHEN "{t}" IS NOT NULL THEN 1 END), '
                            f'COUNT(CASE WHEN "{t}"::text NOT IN (\'0\',\'\',\'false\',\'False\',\'f\') THEN 1 END) '
                            f"FROM multibet.risk_tags WHERE snapshot_date='2026-05-14'")
                tot, nn, ativo = cur.fetchone()
                print(f"  {t:30s} type={dt:15s} total={tot:>8} notnull={nn:>8} ativo={ativo:>8}")
            except Exception as e:
                print(f"  {t:30s} ERRO: {e}")
                conn.rollback()
finally:
    conn.close()
    tunnel.stop()
