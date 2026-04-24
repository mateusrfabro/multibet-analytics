"""Entender como o farmer saca sem cumprir rollover.
Investigar fluxo transacional completo da conta mais gritante: sZ8M2Jn3BBryYd31."""
import os, sys
from datetime import datetime
from zoneinfo import ZoneInfo
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

BRT = ZoneInfo("America/Sao_Paulo")
print(f"# Snapshot: {datetime.now(BRT).strftime('%d/%m/%Y %H:%M BRT')}")

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # 1) Fluxo completo da conta sZ8M2Jn3BBryYd31 (sacou 576 com turnover 55)
        print("\n" + "="*100)
        print("FLUXO TRANSACIONAL COMPLETO - sZ8M2Jn3BBryYd31 (turnover 55, saque 576)")
        print("="*100)
        cur.execute("""
            SELECT t.created_at::timestamp(0) AS quando,
                   t.type, t.status,
                   t.amount::numeric(10,2) AS valor,
                   t.balance_before::numeric(10,2) AS saldo_antes,
                   t.balance_after::numeric(10,2) AS saldo_depois,
                   t.locked_amount_before::numeric(10,2) AS locked_antes,
                   t.locked_amount_after::numeric(10,2) AS locked_depois,
                   t.reference_type
            FROM transactions t
            JOIN users u ON u.id=t.user_id
            WHERE u.username='sZ8M2Jn3BBryYd31'
            ORDER BY t.created_at
        """)
        print(f"  {'quando':20} {'tipo':17} {'status':10} {'valor':>7} {'saldo_a':>8} {'saldo_d':>8} {'lock_a':>7} {'lock_d':>7} ref")
        for r in cur.fetchall():
            print(f"  {str(r[0]):20} {str(r[1])[:17]:17} {str(r[2])[:10]:10} {float(r[3] or 0):>7.0f} {float(r[4] or 0):>8.0f} {float(r[5] or 0):>8.0f} {float(r[6] or 0):>7.0f} {float(r[7] or 0):>7.0f} {r[8]}")

        # Estado da bonus_activation
        print("\n  Bonus activation:")
        cur.execute("""
            SELECT b.deposit_amount, b.bonus_amount, b.rollover_target, b.rollover_progress,
                   b.max_withdraw_amount, b.status, b.created_at::timestamp(0), b.updated_at::timestamp(0), b.cancelled_at::timestamp(0)
            FROM bonus_activations b JOIN users u ON u.id=b.user_id
            WHERE u.username='sZ8M2Jn3BBryYd31'
        """)
        for r in cur.fetchall():
            print(f"    dep={r[0]} bonus={r[1]} roll_target={r[2]} roll_progress={r[3]} max_saq={r[4]}")
            print(f"    status={r[5]} | created={r[6]} | updated={r[7]} | cancelled={r[8]}")

        # Bets individuais
        print("\n  Bets individuais:")
        cur.execute("""
            SELECT b.created_at::timestamp(0), b.amount::numeric(10,2), b.win_amount::numeric(10,2),
                   b.category, b.status, g.name AS jogo, g.rtp::numeric(5,2) AS rtp
            FROM bets b
            JOIN users u ON u.id=b.user_id
            LEFT JOIN casino_games g ON g.id = b.game_id
            WHERE u.username='sZ8M2Jn3BBryYd31'
            ORDER BY b.created_at
        """)
        for r in cur.fetchall():
            print(f"    {r[0]}  bet={r[1]}  win={r[2]}  cat={r[3]}  jogo={r[5]} rtp={r[6]}")

        # 2) Mesmo fluxo pra conta que sacou MAIS (KZ9JwZEeukWH3Ume — 700 de 300 dep)
        print("\n\n" + "="*100)
        print("FLUXO TRANSACIONAL - KZ9JwZEeukWH3Ume (dep 300, saque 700)")
        print("="*100)
        cur.execute("""
            SELECT t.created_at::timestamp(0), t.type, t.status,
                   t.amount::numeric(10,2), t.balance_before::numeric(10,2), t.balance_after::numeric(10,2)
            FROM transactions t JOIN users u ON u.id=t.user_id
            WHERE u.username='KZ9JwZEeukWH3Ume'
            ORDER BY t.created_at
        """)
        print(f"  {'quando':20} {'tipo':17} {'status':10} {'valor':>7} {'saldo_a':>8} {'saldo_d':>8}")
        for r in cur.fetchall():
            print(f"  {str(r[0]):20} {str(r[1])[:17]:17} {str(r[2])[:10]:10} {float(r[3] or 0):>7.0f} {float(r[4] or 0):>8.0f} {float(r[5] or 0):>8.0f}")

        # 3) Wallets dessas 2 contas
        print("\n\n" + "="*100)
        print("WALLETS - estado atual")
        print("="*100)
        cur.execute("""
            SELECT u.username, w.type, w.balance::numeric(10,2), w.locked_balance::numeric(10,2), w.active, w.blocked
            FROM wallets w JOIN users u ON u.id=w.user_id
            WHERE u.username IN ('sZ8M2Jn3BBryYd31','KZ9JwZEeukWH3Ume')
            ORDER BY u.username, w.type
        """)
        for r in cur.fetchall():
            print(f"  {r[0]:20} type={r[1]:8} balance={r[2]:>8}  locked={r[3]:>7}  active={r[4]} blocked={r[5]}")

        # 4) PROGRAMS - qual programa de bonus essas contas usam?
        print("\n\n" + "="*100)
        print("BONUS PROGRAMS - regras do Welcome")
        print("="*100)
        cur.execute("""
            SELECT b.program_id, COUNT(*) as n_ativacoes,
                   MIN(b.rollover_target / NULLIF(b.bonus_amount,0)) AS mult_min,
                   MAX(b.rollover_target / NULLIF(b.bonus_amount,0)) AS mult_max,
                   MIN(b.max_withdraw_amount / NULLIF(b.bonus_amount,0)) AS maxsaq_mult
            FROM bonus_activations b
            GROUP BY 1
        """)
        for r in cur.fetchall():
            print(f"  program={r[0]}  n={r[1]}  mult_roll={r[2]}-{r[3]}  max_saq={r[4]}×bonus")

        # 5) Check geral: quantos saques TOTAIS na plataforma nao cumpriram rollover?
        print("\n\n" + "="*100)
        print("PROBLEMA E SISTEMICO? saques COMPLETED com rollover NAO cumprido (toda plataforma)")
        print("="*100)
        cur.execute("""
            WITH saques AS (
                SELECT t.user_id, t.amount AS saq, t.processed_at
                FROM transactions t
                WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
            ),
            bonus_user AS (
                SELECT b.user_id,
                       SUM(b.bonus_amount) AS bonus_total,
                       SUM(b.rollover_target) AS roll_target,
                       SUM(b.rollover_progress) AS roll_progress,
                       MAX(b.status) AS status_bonus
                FROM bonus_activations b
                GROUP BY b.user_id
            )
            SELECT
                COUNT(*) FILTER (WHERE b.user_id IS NOT NULL) AS saques_com_bonus,
                COUNT(*) FILTER (WHERE b.user_id IS NOT NULL AND b.roll_progress < b.roll_target) AS nao_cumpriu,
                COUNT(*) FILTER (WHERE b.user_id IS NOT NULL AND b.roll_progress >= b.roll_target) AS cumpriu,
                COUNT(*) FILTER (WHERE b.user_id IS NULL) AS saque_sem_bonus,
                SUM(saq) FILTER (WHERE b.user_id IS NOT NULL AND b.roll_progress < b.roll_target)::numeric(12,0) AS valor_nao_cumpriu
            FROM saques s
            LEFT JOIN bonus_user b ON b.user_id=s.user_id
        """)
        r = cur.fetchone()
        print(f"  Total saques c/bonus ativado: {r[0]}")
        print(f"    - cumpriu rollover:    {r[2]}")
        print(f"    - NAO cumpriu:         {r[1]}  (valor total: Rs {r[4]})")
        print(f"  Saques sem bonus:       {r[3]}")
        if r[0] and r[1]:
            print(f"\n  >> {100*r[1]/r[0]:.0f}% dos saques c/bonus foram feitos SEM cumprir rollover.")

finally:
    conn.close(); tunnel.stop()
