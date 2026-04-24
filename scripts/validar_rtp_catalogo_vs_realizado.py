"""Validar se RTP>100% do catalogo e bug ou prejuizo real.
Compara RTP catalogo vs RTP realizado (SUM(win)/SUM(bet)) em 30 dias."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # RTP catalogado vs RTP realizado
        cur.execute("""
            SELECT g.name,
                   g.rtp::numeric(6,2) AS rtp_catalogo,
                   COUNT(DISTINCT m.user_id) AS jogadores_historico,
                   SUM(m.total_bet_amount)::numeric(12,0) AS bet_total,
                   SUM(m.total_win_amount)::numeric(12,0) AS win_total,
                   SUM(m.net_revenue)::numeric(12,0) AS ggr_casa,
                   CASE WHEN SUM(m.total_bet_amount) > 0
                        THEN (SUM(m.total_win_amount) * 100.0 / SUM(m.total_bet_amount))::numeric(6,2)
                        ELSE NULL END AS rtp_realizado,
                   CASE WHEN SUM(m.total_bet_amount) > 0
                        THEN (SUM(m.net_revenue) * 100.0 / SUM(m.total_bet_amount))::numeric(6,2)
                        ELSE NULL END AS hold_casa
            FROM casino_games g
            LEFT JOIN casino_user_game_metrics m ON m.game_id = g.id
            WHERE g.rtp > 100
            GROUP BY g.name, g.rtp
            ORDER BY g.rtp DESC
        """)
        print(f"{'jogo':30} {'RTP_cat':>8} {'jogs':>5} {'bet':>8} {'win':>8} {'ggr':>8} {'RTP_real':>9}")
        print("-"*90)
        for r in cur.fetchall():
            rtp_real = str(r[6]) if r[6] is not None else "-"
            print(f"{str(r[0])[:30]:30} {str(r[1]):>8} {r[2]:>5} {r[3] or 0:>8} {r[4] or 0:>8} {r[5] or 0:>8} {rtp_real:>9}")

        # Total consolidado dos jogos RTP>100%
        cur.execute("""
            SELECT SUM(m.total_bet_amount)::numeric(12,0) bet,
                   SUM(m.total_win_amount)::numeric(12,0) win,
                   SUM(m.net_revenue)::numeric(12,0) ggr
            FROM casino_user_game_metrics m
            JOIN casino_games g ON g.id=m.game_id
            WHERE g.rtp > 100
        """)
        r = cur.fetchone()
        print(f"\nCONSOLIDADO jogos RTP>100%: bet={r[0]} win={r[1]} ggr={r[2]}")
        if r[0] and r[0] > 0:
            print(f"RTP realizado conjunto: {100*float(r[1])/float(r[0]):.2f}%")
            print(f"Hold casa conjunto:     {100*float(r[2])/float(r[0]):.2f}%")

        # Quantos jogos ja foram jogados ao menos 100x?
        cur.execute("""
            SELECT COUNT(*) FROM casino_games g
            WHERE g.rtp > 100
              AND (SELECT COALESCE(SUM(m.played_rounds),0) FROM casino_user_game_metrics m WHERE m.game_id=g.id) >= 100
        """)
        print(f"\nJogos RTP>100% com >=100 rounds historicos: {cur.fetchone()[0]} de 27")
finally:
    conn.close(); tunnel.stop()
