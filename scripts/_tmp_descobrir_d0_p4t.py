"""Descobrir D-0 real da operacao Play4Tune."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor()
try:
    cur.execute("""
        SELECT MIN(date), MAX(date), COUNT(DISTINCT date),
               COUNT(DISTINCT user_id), COUNT(DISTINCT game_id),
               SUM(played_rounds), ROUND(SUM(total_bet_amount)::numeric,2)
        FROM casino_user_game_metrics
    """)
    print("casino_user_game_metrics (historico completo):")
    print(" ", cur.fetchone())

    cur.execute("""
        SELECT MIN(created_at)::date AS primeiro_user, MAX(created_at)::date AS ultimo
        FROM users WHERE role='USER'
    """)
    print("\nusers (role=USER):")
    print(" ", cur.fetchone())

    cur.execute("""
        SELECT MIN(created_at)::date FROM bets
    """)
    print("\nbets (primeira aposta):")
    print(" ", cur.fetchone())

    cur.execute("""
        SELECT date, COUNT(DISTINCT user_id) players, SUM(played_rounds) rodadas,
               ROUND(SUM(total_bet_amount)::numeric,2) apost,
               ROUND(SUM(net_revenue)::numeric,2) ggr
        FROM casino_user_game_metrics
        GROUP BY date ORDER BY date ASC LIMIT 10
    """)
    print("\nPrimeiros 10 dias com dado em casino_user_game_metrics:")
    for r in cur.fetchall():
        print(" ", r)
finally:
    cur.close(); conn.close(); tunnel.stop()
