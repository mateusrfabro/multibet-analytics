"""Revalidar rollover APLICANDO filtro oficial de test users.
O user apontou que meu achado pode estar inflado por contas teste."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

REAL_WHITELIST = (
    'maharshani44377634693',
    'muhammadrehan17657797557',
    'rehmanzafar006972281',
    'saimkyani15688267',
)

TEST_USERS_CTE = f"""
    test_ids AS (
        SELECT DISTINCT u.id
        FROM users u
        WHERE (
            u.role != 'USER'
            OR LOWER(u.username) LIKE '%%test%%'
            OR LOWER(u.username) LIKE '%%teste%%'
            OR LOWER(u.username) LIKE '%%demo%%'
            OR LOWER(u.username) LIKE '%%admin%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@karinzitta%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@multi.bet%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@grupo-pgs%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@supernovagaming%%'
            OR LOWER(COALESCE(u.email, '')) LIKE '%%@play4tune%%'
            OR u.id IN (
                SELECT DISTINCT t.user_id FROM transactions t
                WHERE t.type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT')
                   OR (t.type='DEPOSIT' AND t.reviewed_by IS NOT NULL)
            )
        )
        AND u.username NOT IN {REAL_WHITELIST}
    )
"""

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # ======================================================================
        # 1) Os jogadores "histricos" que citei são testes?
        # ======================================================================
        print("="*90)
        print("1) OS JOGADORES QUE CITEI ONTEM SAO TESTES?")
        print("="*90)
        suspeitos = ('Darwash67','3XLJO3w8KUaWYivh','mehmood88','mirzaibrar',
                     'newsaher','malik7','m04nAlM4jd3lv7Rg','UVqEndzgaFQGpfa8',
                     'moazsajjad5','hGMXL4yGaidcZZSf','21EH85cwx78jzTa0')
        cur.execute(f"""
            WITH {TEST_USERS_CTE}
            SELECT u.username, u.role, u.email, u.phone,
                   (u.id IN (SELECT id FROM test_ids)) AS is_test,
                   (SELECT COUNT(*) FROM transactions t WHERE t.user_id=u.id AND t.type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT')) AS adj,
                   (SELECT COUNT(*) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.reviewed_by IS NOT NULL) AS dep_rev,
                   (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id AND t.type='ADJUSTMENT_CREDIT') AS adj_cred_total,
                   (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id AND t.type='ADJUSTMENT_DEBIT') AS adj_deb_total
            FROM users u
            WHERE u.username = ANY(%s::varchar[])
        """, (list(suspeitos),))
        print(f"   {'user':25} {'is_test':>7} {'adj':>4} {'dep_rev':>7} {'adj_cr':>7} {'adj_db':>7} role")
        for r in cur.fetchall():
            flag = "**TEST**" if r[4] else "user_ok"
            print(f"   {str(r[0])[:25]:25} {str(r[4]):>7} {r[5]:>4} {r[6]:>7} {str(r[7] or '-'):>7} {str(r[8] or '-'):>7} {r[1]} {flag}")

        # ======================================================================
        # 2) Recalcular taxa de conformidade SEM testes
        # ======================================================================
        print("\n" + "="*90)
        print("2) CONFORMIDADE ROLLOVER — filtrando teste (lógica dev oficial)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_USERS_CTE},
            saques_c_bonus AS (
                SELECT DISTINCT t.user_id
                FROM transactions t
                JOIN bonus_activations b ON b.user_id=t.user_id
                WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
            ),
            status_por_user AS (
                SELECT s.user_id,
                       SUM(b.bonus_amount) AS bonus_total,
                       SUM(b.rollover_target) AS target_total,
                       SUM(b.rollover_progress) AS progress_total,
                       BOOL_OR(b.status='COMPLETED') AS algum_completed
                FROM saques_c_bonus s
                JOIN bonus_activations b ON b.user_id=s.user_id
                GROUP BY s.user_id
            )
            SELECT
                COUNT(*) AS total_users_sacaram_com_bonus,
                COUNT(*) FILTER (WHERE user_id IN (SELECT id FROM test_ids)) AS eram_teste,
                COUNT(*) FILTER (WHERE user_id NOT IN (SELECT id FROM test_ids)) AS usuarios_reais,
                COUNT(*) FILTER (WHERE user_id NOT IN (SELECT id FROM test_ids) AND progress_total >= target_total) AS real_cumpriu,
                COUNT(*) FILTER (WHERE user_id NOT IN (SELECT id FROM test_ids) AND progress_total < target_total) AS real_nao_cumpriu,
                COUNT(*) FILTER (WHERE user_id NOT IN (SELECT id FROM test_ids) AND algum_completed) AS real_algum_completed
            FROM status_por_user
        """)
        r = cur.fetchone()
        print(f"   Users que sacaram c/bonus ativado: {r[0]}")
        print(f"     - eram TESTE:             {r[1]} ({100*r[1]/max(r[0],1):.0f}%)")
        print(f"     - usuarios reais:         {r[2]} ({100*r[2]/max(r[0],1):.0f}%)")
        print(f"\n   Dos {r[2]} usuarios reais:")
        print(f"     - cumpriram rollover:     {r[3]} ({100*r[3]/max(r[2],1):.1f}%)")
        print(f"     - NAO cumpriram:          {r[4]} ({100*r[4]/max(r[2],1):.1f}%)")
        print(f"     - tiveram algum bonus COMPLETED: {r[5]}")

        # ======================================================================
        # 3) Agora listar os usuarios REAIS que sacaram sem cumprir rollover
        # ======================================================================
        print("\n" + "="*90)
        print("3) USUARIOS REAIS que sacaram sem cumprir rollover (lista)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_USERS_CTE},
            saques_c_bonus AS (
                SELECT DISTINCT t.user_id
                FROM transactions t
                JOIN bonus_activations b ON b.user_id=t.user_id
                WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
            )
            SELECT u.username, u.phone, u.email,
                   SUM(b.bonus_amount)::numeric(10,0) AS bonus,
                   SUM(b.rollover_target)::numeric(10,0) AS target,
                   SUM(b.rollover_progress)::numeric(10,0) AS progress,
                   (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')::numeric(10,0) AS dep,
                   (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED')::numeric(10,0) AS saq,
                   (SELECT COALESCE(SUM(m.total_bet_amount),0) FROM casino_user_game_metrics m WHERE m.user_id=u.id)::numeric(10,0) AS turn,
                   MAX(b.status) AS bonus_st
            FROM users u
            JOIN saques_c_bonus s ON s.user_id=u.id
            JOIN bonus_activations b ON b.user_id=u.id
            WHERE u.id NOT IN (SELECT id FROM test_ids)
            GROUP BY u.username, u.phone, u.email, u.id
            HAVING SUM(b.rollover_progress) < SUM(b.rollover_target)
            ORDER BY saq DESC
        """)
        rows = cur.fetchall()
        print(f"   {'user':22} {'phone':15} {'bonus':>6} {'target':>7} {'progress':>8} {'dep':>6} {'saq':>6} {'turn':>7} {'status':>10}")
        for r in rows:
            pct = 100*float(r[5] or 0)/float(r[4] or 1)
            print(f"   {str(r[0])[:22]:22} {str(r[1])[:15]:15} {r[3] or 0:>6} {r[4] or 0:>7} {r[5] or 0:>8} {r[6] or 0:>6} {r[7] or 0:>6} {r[8] or 0:>7} {str(r[9])[:10]:>10}  ({pct:.0f}%)")
        print(f"\n   Total: {len(rows)} usuarios reais sacaram sem cumprir rollover")

        # ======================================================================
        # 4) Batch dos 17 - qual % do achado sobrevive?
        # ======================================================================
        print("\n" + "="*90)
        print("4) BATCH 18/04 — eles sao 'test users' pela logica oficial?")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_USERS_CTE}
            SELECT u.username, u.phone,
                   (u.id IN (SELECT id FROM test_ids)) AS is_test
            FROM users u
            WHERE u.phone LIKE '+92341374%' OR u.username='utl2FFfrQR7Qj6qi'
            ORDER BY u.phone
        """)
        batch_rows = cur.fetchall()
        batch_test = sum(1 for r in batch_rows if r[2])
        print(f"   Batch: {len(batch_rows)} contas | {batch_test} sao test-flagged pela logica oficial | {len(batch_rows)-batch_test} nao-flagged")
        for r in batch_rows:
            print(f"   {str(r[0])[:25]:25} {r[1]:15} is_test={r[2]}")

finally:
    conn.close(); tunnel.stop()
