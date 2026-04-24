"""Validacao antes de reportar - testar hipoteses alternativas sobre rollover."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # ====================================================================
        # HIPOTESE 1: As 3 contas que CUMPRIRAM rollover - quem sao?
        # ====================================================================
        print("="*90)
        print("H1 - As 3 contas que CUMPRIRAM rollover - perfil")
        print("="*90)
        cur.execute("""
            WITH saques_com_bonus AS (
                SELECT DISTINCT t.user_id
                FROM transactions t
                JOIN bonus_activations b ON b.user_id=t.user_id
                WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
            ),
            rollover_status AS (
                SELECT s.user_id,
                       SUM(b.bonus_amount) AS bonus_tot,
                       SUM(b.rollover_target) AS roll_target,
                       SUM(b.rollover_progress) AS roll_progress,
                       MAX(b.status) AS bonus_st
                FROM saques_com_bonus s
                JOIN bonus_activations b ON b.user_id=s.user_id
                GROUP BY s.user_id
                HAVING SUM(b.rollover_progress) >= SUM(b.rollover_target)
            )
            SELECT u.username, u.email, u.role, u.phone, r.bonus_tot, r.roll_target, r.roll_progress, r.bonus_st
            FROM rollover_status r
            JOIN users u ON u.id=r.user_id
        """)
        rows = cur.fetchall()
        print(f"   Total: {len(rows)} contas")
        for r in rows:
            print(f"   {r[0]} | role={r[2]} | email={r[1]} | phone={r[3]}")
            print(f"     bonus_tot={r[4]}  roll_target={r[5]}  roll_progress={r[6]}  st={r[7]}")

        # ====================================================================
        # HIPOTESE 2: rollover_progress conta apostas REAL tambem ou so BONUS?
        # ====================================================================
        print("\n" + "="*90)
        print("H2 - rollover_progress conta bets REAL ou so BONUS?")
        print("="*90)
        # Para a conta sZ8M2Jn3BBryYd31 que apostou Rs 55 com saldo REAL,
        # o rollover_progress deveria ser 0 se so conta bonus, ou ~55 se conta ambos.
        cur.execute("""
            SELECT u.username, b.rollover_progress, b.bonus_amount, b.rollover_target
            FROM bonus_activations b
            JOIN users u ON u.id=b.user_id
            WHERE u.username IN ('sZ8M2Jn3BBryYd31','KZ9JwZEeukWH3Ume','uuZSMBdC60xapNgG')
        """)
        print(f"   {'user':20} {'roll_progress':>14} {'bonus':>7} {'target':>7}")
        for r in cur.fetchall():
            print(f"   {r[0]:20} {float(r[1]):>14.2f} {float(r[2]):>7.0f} {float(r[3]):>7.0f}")
        # A conta sZ se apostou 55 com REAL e rollover_progress = 55 -> conta REAL
        # Se rollover_progress = 0 -> so conta BONUS

        # Cross-check: total de bets da conta vs rollover_progress
        print("\n   Cross-check bets totais vs rollover_progress:")
        cur.execute("""
            SELECT u.username,
                   b.rollover_progress,
                   (SELECT COALESCE(SUM(bt.amount),0) FROM bets bt WHERE bt.user_id=u.id) AS total_bets,
                   (SELECT COALESCE(SUM(m.total_bet_amount),0) FROM casino_user_game_metrics m WHERE m.user_id=u.id) AS total_metric
            FROM bonus_activations b
            JOIN users u ON u.id=b.user_id
            WHERE u.username IN ('sZ8M2Jn3BBryYd31','KZ9JwZEeukWH3Ume','uuZSMBdC60xapNgG','8x1rJh3nGkvjIlRZ','WnElZAi9fZx00cp6')
        """)
        print(f"   {'user':20} {'roll_progress':>14} {'sum_bets':>9} {'metric':>8}")
        for r in cur.fetchall():
            print(f"   {r[0]:20} {float(r[1]):>14.2f} {float(r[2]):>9.2f} {float(r[3]):>8.2f}")

        # ====================================================================
        # HIPOTESE 3: tabela "programs" - ler a politica do Welcome Bonus
        # ====================================================================
        print("\n" + "="*90)
        print("H3 - Existe tabela programs/bonus_programs com regra de rollover?")
        print("="*90)
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' AND (table_name LIKE '%program%' OR table_name LIKE '%bonus%' OR table_name LIKE '%wager%' OR table_name LIKE '%roll%')
            ORDER BY table_name
        """)
        print(f"   Tabelas relacionadas:")
        for r in cur.fetchall():
            print(f"     {r[0]}")

        # Verificar todas colunas de bonus_activations
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema='public' AND table_name='bonus_activations'
        """)
        print(f"\n   bonus_activations columns (recheck): {[r[0] for r in cur.fetchall()]}")

        # ====================================================================
        # HIPOTESE 4: Jogador "normal" historico tambem saca sem cumprir?
        # Pegar saque completo nao-batch, ver se cumpriu rollover
        # ====================================================================
        print("\n" + "="*90)
        print("H4 - Jogadores orgânicos historicos tambem 'nao cumprem' rollover?")
        print("="*90)
        cur.execute("""
            WITH historico AS (
                SELECT DISTINCT t.user_id
                FROM transactions t
                JOIN users u ON u.id=t.user_id
                WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
                  AND u.phone NOT LIKE '+92341374%'
                  AND u.role='USER'
                  AND t.processed_at < CURRENT_DATE - INTERVAL '1 day'
            )
            SELECT u.username, u.phone, u.role,
                   b.bonus_amount, b.rollover_target, b.rollover_progress, b.status,
                   (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED') AS dep,
                   (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED') AS saq,
                   (SELECT SUM(m.total_bet_amount) FROM casino_user_game_metrics m WHERE m.user_id=u.id) AS turn
            FROM historico h
            JOIN users u ON u.id=h.user_id
            JOIN bonus_activations b ON b.user_id=u.id
            ORDER BY (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED') DESC
            LIMIT 10
        """)
        print(f"   {'user':25} {'roll':>6} {'target':>7} {'prog':>7} {'dep':>6} {'saq':>6} {'turn':>7} {'st':>10}")
        for r in cur.fetchall():
            print(f"   {str(r[0])[:25]:25} {float(r[3] or 0):>6.0f} {float(r[4] or 0):>7.0f} {float(r[5] or 0):>7.0f} {float(r[7] or 0):>6.0f} {float(r[8] or 0):>6.0f} {float(r[9] or 0):>7.0f} {str(r[6]):>10}")

        # ====================================================================
        # HIPOTESE 5: Configuracao do program (ha detalhes)?
        # ====================================================================
        print("\n" + "="*90)
        print("H5 - Detalhes do programa 019d50ee-6aa6-763f-ab6b-67a4ebf6b047")
        print("="*90)
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema='public' ORDER BY table_name
        """)
        all_tables = [r[0] for r in cur.fetchall()]
        print(f"   Total tabelas public: {len(all_tables)}")
        # Procurar tabela 'programs'
        for t in all_tables:
            if 'program' in t.lower() or 'bonus' in t.lower():
                print(f"   Tabela candidata: {t}")
                cur.execute(f"""
                    SELECT column_name FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='{t}'
                """)
                cols = [r[0] for r in cur.fetchall()]
                print(f"     cols: {cols}")

finally:
    conn.close(); tunnel.stop()
