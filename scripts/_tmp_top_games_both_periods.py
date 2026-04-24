"""Top jogos por GGR negativo (casa perdeu) em 2 periodos, com top player por jogo. BRL."""
import os, sys
from datetime import date
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection
import report_players_afundando_ggr_play4 as mainmod
from report_players_afundando_ggr_play4 import get_test_user_ids, fetch_fx_rates, to_brl

PERIODOS = [
    ("13-19/04 (7 dias)", date(2026, 4, 13), date(2026, 4, 19)),
    ("20/04 D-0 (parcial)", date(2026, 4, 20), date(2026, 4, 20)),
]

SQL_GAMES = """
SELECT
    g.name AS jogo,
    COALESCE(pv.name, 'Unknown') AS provider,
    g.rtp AS rtp_cfg,
    COUNT(DISTINCT m.user_id) AS jogadores,
    SUM(m.played_rounds) AS rodadas,
    ROUND(SUM(m.total_bet_amount)::numeric, 2) AS apostado,
    ROUND(SUM(m.total_win_amount)::numeric, 2) AS ganho,
    ROUND(SUM(m.net_revenue)::numeric, 2) AS ggr,
    ROUND(CASE WHEN SUM(m.total_bet_amount) > 0
               THEN SUM(m.total_win_amount)/SUM(m.total_bet_amount)*100
               ELSE 0 END::numeric, 1) AS payout
FROM casino_user_game_metrics m
JOIN casino_games g ON g.id = m.game_id
LEFT JOIN casino_providers pv ON pv.id = g.provider_id
WHERE m.date BETWEEN %s AND %s
  AND m.user_id NOT IN %s
GROUP BY g.name, pv.name, g.rtp
HAVING SUM(m.net_revenue) < 0
ORDER BY SUM(m.net_revenue) ASC
LIMIT 10
"""

SQL_TOP_PLAYER_NO_JOGO = """
SELECT u.public_id, u.username, (%s::date - u.created_at::date) AS dias_conta,
       ROUND(SUM(m.total_bet_amount)::numeric, 2) AS apostado,
       ROUND(SUM(m.net_revenue)::numeric, 2) AS ggr_no_jogo,
       SUM(m.played_rounds) AS rodadas
FROM casino_user_game_metrics m
JOIN users u ON u.id = m.user_id
JOIN casino_games g ON g.id = m.game_id
WHERE m.date BETWEEN %s AND %s
  AND g.name = %s
  AND m.user_id NOT IN %s
GROUP BY u.public_id, u.username, u.created_at
ORDER BY SUM(m.net_revenue) ASC
LIMIT 1
"""

def run():
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    try:
        # Carrega taxa FX
        pkr_to_brl, fx_at = fetch_fx_rates(cur)
        mainmod.PKR_TO_BRL = pkr_to_brl
        print(f"[FX] 1 PKR = R$ {pkr_to_brl:.6f} (snapshot {fx_at} UTC)\n")

        test_ids, _, _ = get_test_user_ids(cur)
        for label, ini, fim in PERIODOS:
            print(f"\n{'=' * 80}")
            print(f"TOP JOGOS — {label} (valores em BRL)")
            print('=' * 80)
            cur.execute(SQL_GAMES, (ini, fim, test_ids))
            games = cur.fetchall()
            for g in games:
                name = g[0]
                cur.execute(SQL_TOP_PLAYER_NO_JOGO, (fim, ini, fim, name, test_ids))
                top = cur.fetchone()
                tp_str = (f"{top[0]}({top[2]}d): Apostou R$ {to_brl(top[3]):,.2f} (Rs {float(top[3]):,.0f}), "
                          f"GGR R$ {to_brl(top[4]):,.2f} (Rs {float(top[4]):,.0f}) em {top[5]} giros") if top else "-"
                print(f"  {name} | {g[1]} | RTP {g[2]} | {g[3]} players | {g[4]} giros")
                print(f"    Apostado: R$ {to_brl(g[5]):,.2f} (Rs {float(g[5]):,.0f}) | "
                      f"GGR: R$ {to_brl(g[7]):,.2f} (Rs {float(g[7]):,.0f}) | Payout {g[8]}%")
                print(f"    Top player: {tp_str}")
    finally:
        cur.close(); conn.close(); tunnel.stop()

if __name__ == "__main__":
    run()
