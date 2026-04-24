"""Teste definitivo: rollover_progress = sum(bets.amount) de TODAS as apostas?"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # A conta que CUMPRIU rollover: perfil completo
        cur.execute("""
            SELECT u.username, u.role, u.phone, u.created_at::timestamp(0),
                   (SELECT COUNT(*) FROM bets WHERE user_id=u.id),
                   (SELECT SUM(amount) FROM bets WHERE user_id=u.id),
                   (SELECT SUM(amount) FROM transactions WHERE user_id=u.id AND type='DEPOSIT' AND status='COMPLETED'),
                   (SELECT SUM(amount) FROM transactions WHERE user_id=u.id AND type='WITHDRAW' AND status='COMPLETED'),
                   b.rollover_progress, b.rollover_target, b.bonus_amount
            FROM users u
            JOIN bonus_activations b ON b.user_id=u.id
            WHERE u.username='mirzaibrar'
        """)
        r = cur.fetchone()
        print(f"mirzaibrar (unico que cumpriu de 122):")
        print(f"  role={r[1]} | created={r[3]} | n_bets={r[4]} | turn_bets={r[5]} | dep={r[6]} | saq={r[7]}")
        print(f"  rollover: {r[8]}/{r[9]}  | bonus={r[10]}")

        # Teste correlação rollover_progress vs sum(bets.amount)
        print("\n" + "="*80)
        print("TESTE: rollover_progress = sum(bets.amount) de TODAS as apostas?")
        print("="*80)
        cur.execute("""
            SELECT u.username,
                   (SELECT COALESCE(SUM(amount),0) FROM bets WHERE user_id=u.id) AS bets_sum,
                   b.rollover_progress,
                   b.rollover_target,
                   b.status
            FROM users u JOIN bonus_activations b ON b.user_id=u.id
            WHERE u.phone LIKE '+92341374%'
            ORDER BY bets_sum DESC LIMIT 16
        """)
        for r in cur.fetchall():
            match = "MATCH" if r[1] is not None and abs(float(r[1])-float(r[2]))<1 else f"DIFF ({float(r[1])-float(r[2]):.0f})"
            print(f"  {r[0]:18} bets_sum={float(r[1]):>8.0f}  prog={float(r[2]):>8.2f}  target={float(r[3]):>6.0f}  {r[4]:10}  [{match}]")

        # Regra declarada
        print("\n" + "="*80)
        print("PROGRAMA WELCOME — regras declaradas em bonus_programs:")
        print("="*80)
        cur.execute("""SELECT name, type, match_percent, max_bonus_amount,
                              rollover_multiplier, max_withdraw_multiplier,
                              min_deposit_amount, active
                       FROM bonus_programs""")
        for r in cur.fetchall():
            print(f"  name={r[0]} type={r[1]} match={r[2]}% max_bon={r[3]} roll_x={r[4]} maxsaq_x={r[5]} min_dep={r[6]} active={r[7]}")

finally:
    conn.close(); tunnel.stop()
