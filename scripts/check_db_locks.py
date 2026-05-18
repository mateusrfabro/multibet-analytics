"""Checa atividade e locks ativos em multibet.risk_tags"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        # Atividade ativa
        cur.execute("""
            SELECT pid, usename, application_name, state, query_start,
                   NOW() - query_start AS duration,
                   LEFT(query, 100) AS query_snippet
            FROM pg_stat_activity
            WHERE state <> 'idle'
              AND query NOT LIKE '%pg_stat_activity%'
            ORDER BY query_start
        """)
        rows = cur.fetchall()
        print(f"Atividade ativa ({len(rows)} processos):")
        for r in rows:
            print(f"  pid={r[0]} user={r[1]} app={r[2]} state={r[3]}")
            print(f"    duration={r[5]} query={r[6]!r}")

        # Locks em risk_tags
        cur.execute("""
            SELECT l.pid, l.locktype, l.mode, l.granted,
                   a.state, NOW() - a.query_start AS duration
            FROM pg_locks l
            JOIN pg_stat_activity a ON l.pid = a.pid
            WHERE l.relation = 'multibet.risk_tags'::regclass
        """)
        rows = cur.fetchall()
        print(f"\nLocks em multibet.risk_tags ({len(rows)}):")
        for r in rows:
            print(f"  pid={r[0]} type={r[1]} mode={r[2]} granted={r[3]} state={r[4]} duration={r[5]}")
finally:
    conn.close()
    tunnel.stop()
