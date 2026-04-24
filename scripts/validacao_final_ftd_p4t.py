"""Validacao final antes de entregar ao Head (18/04 - caso bonus farming P4T)."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
PKR_BRL = 0.017881

def pkr(v): return f"Rs {float(v or 0):>12,.2f}"
def brl(v): return f"R$ {float(v or 0)*PKR_BRL:>10,.2f}"

SUSPECT_SQL = """
    SELECT id FROM users
    WHERE phone LIKE '+92341374%'
       OR username = 'utl2FFfrQR7Qj6qi'
"""

try:
    with conn.cursor() as cur:
        # 1) Ataque ainda rolando?
        print("="*78)
        print("1) ATAQUE AINDA ATIVO?")
        print("="*78)
        cur.execute("""
            SELECT
                SUM(CASE WHEN u.created_at >= NOW() - INTERVAL '15 min' THEN 1 ELSE 0 END),
                SUM(CASE WHEN u.created_at >= NOW() - INTERVAL '1 hour' THEN 1 ELSE 0 END),
                SUM(CASE WHEN u.created_at >= NOW() - INTERVAL '3 hour' THEN 1 ELSE 0 END),
                SUM(CASE WHEN u.created_at >= NOW() - INTERVAL '1 hour' AND u.phone LIKE '+92341374%' THEN 1 ELSE 0 END)
            FROM users u WHERE u.role='USER'
        """)
        r = cur.fetchone()
        print(f"  Cadastros 15min={r[0]}  1h={r[1]}  3h={r[2]}  | batch 1h={r[3]}")

        print("\n  Ultima atividade das contas do batch (16 do prefixo):")
        cur.execute("""
            SELECT u.username, u.phone,
                   (SELECT MAX(t.created_at) FROM transactions t WHERE t.user_id=u.id) ult,
                   (SELECT t.type||'/'||t.status FROM transactions t WHERE t.user_id=u.id ORDER BY t.created_at DESC LIMIT 1) ult_tipo,
                   EXTRACT(EPOCH FROM (NOW() - (SELECT MAX(t.created_at) FROM transactions t WHERE t.user_id=u.id)))/60 atras
            FROM users u WHERE u.phone LIKE '+92341374%'
            ORDER BY ult DESC NULLS LAST
        """)
        for row in cur.fetchall():
            atras = f"{float(row[4]):.0f}min" if row[4] else "nunca"
            print(f"   {str(row[0])[:18]:18} {row[1]} ult={str(row[2])[:16]:16} {str(row[3])[:25]:25} ({atras} atras)")

        # 2) Impacto financeiro
        print("\n" + "="*78)
        print("2) IMPACTO FINANCEIRO (17 contas suspeitas: 16 prefixo + 1 wetuns fora)")
        print("="*78)
        cur.execute(f"""
            WITH suspect AS ({SUSPECT_SQL})
            SELECT
                (SELECT COUNT(*) FROM suspect),
                (SELECT SUM(amount) FROM transactions t JOIN suspect s ON s.id=t.user_id WHERE t.type='DEPOSIT' AND t.status='COMPLETED'),
                (SELECT SUM(amount) FROM transactions t JOIN suspect s ON s.id=t.user_id WHERE t.type='WITHDRAW' AND t.status='COMPLETED'),
                (SELECT SUM(amount) FROM transactions t JOIN suspect s ON s.id=t.user_id WHERE t.type='WITHDRAW' AND t.status='PENDING'),
                (SELECT SUM(amount) FROM transactions t JOIN suspect s ON s.id=t.user_id WHERE t.type='BONUS_CREDIT'),
                (SELECT SUM(total_bet_amount) FROM casino_user_game_metrics m JOIN suspect s ON s.id=m.user_id),
                (SELECT SUM(net_revenue) FROM casino_user_game_metrics m JOIN suspect s ON s.id=m.user_id),
                (SELECT SUM(balance) FROM wallets w JOIN suspect s ON s.id=w.user_id WHERE w.type='REAL'),
                (SELECT SUM(balance) FROM wallets w JOIN suspect s ON s.id=w.user_id WHERE w.type='BONUS')
        """)
        r = cur.fetchone()
        n, dep, saq, saq_p, bonus, turn, ggr, sal_r, sal_b = r
        print(f"   Contas: {n}")
        print(f"   Depositos OK....... {pkr(dep)}   {brl(dep)}")
        print(f"   Saques OK.......... {pkr(saq)}   {brl(saq)}")
        print(f"   Saques PENDENTES... {pkr(saq_p)}   {brl(saq_p)}")
        print(f"   Bonus creditado.... {pkr(bonus)}   {brl(bonus)}")
        print(f"   Turnover........... {pkr(turn)}   {brl(turn)}")
        print(f"   GGR bruto.......... {pkr(ggr)}   {brl(ggr)}")
        print(f"   Saldo REAL rest.... {pkr(sal_r)}   {brl(sal_r)}")
        print(f"   Saldo BONUS rest... {pkr(sal_b)}   {brl(sal_b)}")
        net_cash = float(dep or 0) - float(saq or 0)
        exp = float(sal_r or 0) + float(saq_p or 0)
        print(f"\n   >> Fluxo caixa atual (dep-saq):    {pkr(net_cash)}   {brl(net_cash)}")
        print(f"   >> Exposicao ainda a sacar (REAL+PENDENTE): {pkr(exp)}   {brl(exp)}")
        print(f"   >> Resultado casa se pagar tudo: {pkr(net_cash - exp)} {brl(net_cash - exp)}")

        # 3) user_agent gap
        print("\n" + "="*78)
        print("3) FINGERPRINT GAP - user_agent existe nas transactions?")
        print("="*78)
        cur.execute("""
            SELECT COUNT(*) total, COUNT(user_agent) com_ua, COUNT(DISTINCT user_agent) dist,
                   COUNT(ip_address) com_ip, COUNT(DISTINCT ip_address) ip_dist
            FROM transactions WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)
        r = cur.fetchone()
        print(f"   Tx 24h: {r[0]}  com_ua={r[1]} (distintos={r[2]})  com_ip={r[3]} (distintos={r[4]})")
        if r[1] == 0:
            print("   >>> user_agent 100% NULL confirmado - gap de fingerprint eh real")

        # 4) Jogos do batch
        print("\n" + "="*78)
        print("4) JOGOS do batch (bonus hunter?)")
        print("="*78)
        cur.execute(f"""
            WITH suspect AS ({SUSPECT_SQL})
            SELECT g.name, g.rtp::numeric(5,2),
                   COUNT(DISTINCT m.user_id), SUM(m.total_bet_amount)::numeric(12,0),
                   SUM(m.played_rounds), SUM(m.net_revenue)::numeric(12,0)
            FROM casino_user_game_metrics m
            JOIN suspect s ON s.id=m.user_id
            JOIN casino_games g ON g.id=m.game_id
            GROUP BY 1,2 ORDER BY 4 DESC LIMIT 15
        """)
        print(f"   {'jogo':35} {'rtp':>6} {'jogs':>5} {'turn':>8} {'giros':>6} {'ggr':>8}")
        for row in cur.fetchall():
            print(f"   {str(row[0])[:35]:35} {str(row[1]):>6} {row[2]:>5} {row[3]:>8} {row[4]:>6} {row[5]:>8}")

        cur.execute(f"""
            WITH suspect AS ({SUSPECT_SQL})
            SELECT
                (SELECT AVG(g.rtp)::numeric(5,2) FROM casino_user_game_metrics m
                   JOIN suspect s ON s.id=m.user_id JOIN casino_games g ON g.id=m.game_id
                   WHERE m.total_bet_amount>0),
                (SELECT AVG(g.rtp)::numeric(5,2) FROM casino_user_game_metrics m
                   JOIN casino_games g ON g.id=m.game_id
                   WHERE m.date >= CURRENT_DATE - INTERVAL '7 days' AND m.total_bet_amount>0)
        """)
        r = cur.fetchone()
        print(f"\n   RTP medio (batch):      {r[0]}%")
        print(f"   RTP medio (plataf 7d):  {r[1]}%")
        if r[0] and r[1]:
            print(f"   Diferenca: {float(r[0])-float(r[1]):+.2f}pp")

        # 5) Hipotese teste interno?
        print("\n" + "="*78)
        print("5) DESCARTAR TESTE INTERNO DO DEV")
        print("="*78)
        cur.execute(f"""
            WITH suspect AS ({SUSPECT_SQL})
            SELECT
                COUNT(*) FILTER (WHERE t.reviewed_by IS NOT NULL),
                COUNT(*) FILTER (WHERE t.type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT')),
                array_agg(DISTINCT t.reviewed_by) FILTER (WHERE t.reviewed_by IS NOT NULL)
            FROM transactions t JOIN suspect s ON s.id=t.user_id
        """)
        r = cur.fetchone()
        print(f"   reviewed_by!=NULL: {r[0]}   ADJUSTMENT_*: {r[1]}   reviewers: {r[2]}")
        if r[0] == 0 and r[1] == 0:
            print("   >>> ZERO interacao interna - descartada hipotese teste dev")

finally:
    conn.close(); tunnel.stop()
