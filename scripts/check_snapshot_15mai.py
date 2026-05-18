"""Verifica se snapshot 15/05 ja foi persistido"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import get_supernova_connection

tunnel, conn = get_supernova_connection()
try:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT snapshot_date, COUNT(*) AS players
            FROM multibet.risk_tags
            WHERE snapshot_date >= '2026-05-13'
            GROUP BY snapshot_date
            ORDER BY snapshot_date DESC
        """)
        for r in cur.fetchall():
            print(f"  {r[0]}: {r[1]:,} players")
        # Verifica se coluna cancel_heavy_daily existe
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='multibet' AND table_name='risk_tags'
              AND column_name IN ('cancel_heavy_daily', 'player_not_valid')
        """)
        print("\nColunas relevantes:")
        for r in cur.fetchall():
            print(f"  {r[0]}")
finally:
    conn.close()
    tunnel.stop()
