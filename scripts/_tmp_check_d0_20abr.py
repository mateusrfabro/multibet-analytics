"""Checar se casino_user_game_metrics tem dados para 20/04 (D-0 parcial)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
conn.set_session(readonly=True, autocommit=True)
cur = conn.cursor()
try:
    # Ultima data populada
    cur.execute("SELECT MAX(date), COUNT(*) FROM casino_user_game_metrics WHERE date >= '2026-04-18'")
    print("casino_user_game_metrics max date >= 18/04:", cur.fetchone())

    # Registros por data recente
    cur.execute("""
        SELECT date, COUNT(*) rows,
               ROUND(SUM(total_bet_amount)::numeric,2) turnover,
               ROUND(SUM(net_revenue)::numeric,2) ggr
        FROM casino_user_game_metrics
        WHERE date >= '2026-04-18'
        GROUP BY date ORDER BY date
    """)
    print("Por data:")
    for r in cur.fetchall():
        print(" ", r)

    # Bets recentes (tem granularidade por timestamp)
    cur.execute("""
        SELECT DATE(created_at) AS d, COUNT(*),
               MAX(created_at) AS ultima
        FROM bets
        WHERE created_at >= '2026-04-19'
        GROUP BY DATE(created_at) ORDER BY d
    """)
    print("\nBets por dia (tabela granular):")
    for r in cur.fetchall():
        print(" ", r)

    # NOW() do banco
    cur.execute("SELECT NOW(), CURRENT_DATE, CURRENT_TIMESTAMP AT TIME ZONE 'Asia/Karachi'")
    print("\nTempo banco:", cur.fetchone())
finally:
    cur.close()
    conn.close()
    tunnel.stop()
