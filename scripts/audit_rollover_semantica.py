"""AUDITORIA QA — validacao semantica rollover_progress (18/04 16:55 BRT).
Responde 4 perguntas criticas antes de reportar ao Head/CTO.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:

        # ============================================================
        # Q1) rollover_progress conta bet de real, de bonus, ou ambos?
        # Abordagem: pegar conta batch conhecida (sZ8M2Jn3BBryYd31).
        # Sabemos: apostou Rs 55 TOTAL, roll_target=15.000.
        # Checar rollover_progress atual. Se = 0, NAO conta real.
        # Checar se outras contas com bonus ATIVO tem progress > 0.
        # ============================================================
        print("="*90)
        print("Q1) SEMANTICA rollover_progress — conta bets de REAL ou so de BONUS?")
        print("="*90)
        cur.execute("""
            SELECT u.username,
                   b.status, b.bonus_amount, b.rollover_target, b.rollover_progress,
                   b.deposit_amount, b.max_withdraw_amount,
                   (SELECT COALESCE(SUM(bt.amount),0)
                      FROM bets bt WHERE bt.user_id=u.id AND bt.category='LOSS') AS turn_loss,
                   (SELECT COALESCE(SUM(bt.amount),0)
                      FROM bets bt WHERE bt.user_id=u.id AND bt.category='WIN') AS turn_win,
                   (SELECT COALESCE(SUM(bt.amount),0)
                      FROM bets bt WHERE bt.user_id=u.id) AS turn_total,
                   b.created_at::timestamp(0) AS bonus_at,
                   b.cancelled_at::timestamp(0) AS cancel_at
            FROM bonus_activations b
            JOIN users u ON u.id = b.user_id
            WHERE u.phone LIKE '+92341374%'
            ORDER BY turn_total DESC
            LIMIT 20
        """)
        rows = cur.fetchall()
        print(f"\n{'user':18} {'status':10} {'bonus':>6} {'target':>7} {'progress':>9} {'turn_loss':>9} {'turn_win':>9} {'turn_tot':>9}")
        print("-"*100)
        for r in rows:
            u, st, bon, tgt, prog, dep, maxs, tl, tw, tt, cat, cxl = r
            print(f"{str(u)[:18]:18} {str(st)[:10]:10} {float(bon or 0):>6.0f} {float(tgt or 0):>7.0f} {float(prog or 0):>9.2f} {float(tl or 0):>9.0f} {float(tw or 0):>9.0f} {float(tt or 0):>9.0f}")

        # ============================================================
        # Q2) 3 contas que cumpriram rollover — quem sao?
        # ADMIN/test/legitimo? checar role, reviewed_by, adjustment
        # ============================================================
        print("\n" + "="*90)
        print("Q2) Quem sao os 3 casos que CUMPRIRAM rollover?")
        print("="*90)
        cur.execute("""
            WITH saq AS (
              SELECT DISTINCT t.user_id
              FROM transactions t
              WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
            ),
            users_com_bonus_cumprido AS (
              SELECT b.user_id, b.bonus_amount, b.rollover_target, b.rollover_progress, b.status
              FROM bonus_activations b
              JOIN saq s ON s.user_id = b.user_id
              WHERE b.rollover_progress >= b.rollover_target
            )
            SELECT u.username, u.role, u.phone, u.email,
                   c.bonus_amount, c.rollover_target, c.rollover_progress, c.status,
                   (SELECT COUNT(*) FROM transactions t WHERE t.user_id=u.id AND t.type LIKE 'ADJUSTMENT%') AS n_adjust,
                   (SELECT COUNT(*) FROM transactions t WHERE t.user_id=u.id AND t.reviewed_by IS NOT NULL) AS n_reviewed
            FROM users_com_bonus_cumprido c
            JOIN users u ON u.id = c.user_id
        """)
        for r in cur.fetchall():
            print(f"  {r[0]} | role={r[1]} | phone={r[2]} | email={r[3]}")
            print(f"    bonus={r[4]} target={r[5]} progress={r[6]} status={r[7]} | adj={r[8]} reviewed={r[9]}")

        # ============================================================
        # Q3) programs — regra de rollover, wager_type
        # ============================================================
        print("\n" + "="*90)
        print("Q3) Tabelas bonus_programs / programs — regras declaradas")
        print("="*90)
        cur.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema='public' AND (table_name LIKE '%bonus%' OR table_name LIKE '%program%')
            ORDER BY 1
        """)
        tables = [r[0] for r in cur.fetchall()]
        print(f"  Tabelas: {tables}")

        for t in tables:
            cur.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_schema='public' AND table_name=%s
                ORDER BY ordinal_position
            """, (t,))
            print(f"\n  -- {t} --")
            for c, dt in cur.fetchall():
                print(f"    {c:30s} {dt}")

        # ============================================================
        # Q4) existe mecanismo "cancela bonus ao sacar"?
        # Buscar BONUS_DEBIT correlacionado com WITHDRAW
        # ============================================================
        print("\n" + "="*90)
        print("Q4) Mecanica BONUS_DEBIT antes de WITHDRAW? (non-sticky bonus?)")
        print("="*90)
        cur.execute("""
            WITH saq AS (
              SELECT t.user_id, t.created_at AS saq_at, t.amount AS saq_amt
              FROM transactions t
              WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
            ),
            bonus_debit_antes AS (
              SELECT s.user_id, s.saq_at, s.saq_amt,
                     (SELECT bd.amount FROM transactions bd
                        WHERE bd.user_id=s.user_id AND bd.type='BONUS_DEBIT'
                              AND bd.created_at <= s.saq_at
                              AND bd.created_at >= s.saq_at - INTERVAL '5 minutes'
                        ORDER BY bd.created_at DESC LIMIT 1) AS bonus_debit_prox
              FROM saq s
            )
            SELECT
              COUNT(*) AS total_saques,
              COUNT(bonus_debit_prox) AS com_bonus_debit_5min_antes,
              ROUND(100.0*COUNT(bonus_debit_prox)/NULLIF(COUNT(*),0),1) AS pct
            FROM bonus_debit_antes
        """)
        r = cur.fetchone()
        print(f"  Total saques COMPLETED: {r[0]}")
        print(f"  Com BONUS_DEBIT <5min antes: {r[1]} ({r[2]}%)")

        # ============================================================
        # Q5) Bonus cancelado/expirado: qual status e se cancelled_at coincide com saque?
        # ============================================================
        print("\n" + "="*90)
        print("Q5) Status dos bonus_activations das contas que sacaram — cancelled_at?")
        print("="*90)
        cur.execute("""
            SELECT b.status, COUNT(*) AS qt,
                   COUNT(*) FILTER (WHERE b.cancelled_at IS NOT NULL) AS com_cancel,
                   COUNT(*) FILTER (WHERE b.rollover_progress >= b.rollover_target) AS com_rollover_ok
            FROM bonus_activations b
            WHERE b.user_id IN (SELECT DISTINCT user_id FROM transactions WHERE type='WITHDRAW' AND status='COMPLETED')
            GROUP BY 1 ORDER BY 2 DESC
        """)
        for r in cur.fetchall():
            print(f"  status={r[0]:15} qt={r[1]:>4}  cancelados={r[2]:>4}  rollover_ok={r[3]:>4}")

finally:
    conn.close(); tunnel.stop()
