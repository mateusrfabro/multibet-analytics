"""Validar se os saques completados do batch cumpriram rollover certinho.
Demanda Castrin 18/04 16:11 - 'Esses 10 saque, eles cumpriram o rollover certinho?'"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # 1) Schema bonus_activations (ver se tem rollover_target / progress)
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema='public' AND table_name='bonus_activations'
            ORDER BY ordinal_position
        """)
        print("=== Schema bonus_activations ===")
        for r in cur.fetchall():
            print(f"  {r[0]:30s} {r[1]}")

        # 2) Amostrar 1 ativacao do batch pra ver os valores
        print("\n=== Sample bonus_activation do batch ===")
        cur.execute("""
            SELECT b.*
            FROM bonus_activations b
            JOIN users u ON u.id=b.user_id
            WHERE u.phone LIKE '+92341374%'
            LIMIT 3
        """)
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description]
        for row in rows:
            print("  ---")
            for c, v in zip(cols, row):
                print(f"  {c:30s} {v}")

        # 3) Analise completa: saque vs rollover exigido
        print("\n\n" + "="*90)
        print("3) ANALISE ROLLOVER por conta que SACOU - batch suspeito (17 contas)")
        print("="*90)

        SUSPECT = """
            SELECT id FROM users
            WHERE phone LIKE '+92341374%' OR username='utl2FFfrQR7Qj6qi'
        """

        cur.execute(f"""
            WITH suspect AS ({SUSPECT}),
            dep_user AS (
                SELECT s.id AS user_id,
                       SUM(t.amount) AS dep_total
                FROM suspect s
                JOIN transactions t ON t.user_id=s.id
                WHERE t.type='DEPOSIT' AND t.status='COMPLETED'
                GROUP BY s.id
            ),
            saq_user AS (
                SELECT s.id AS user_id,
                       SUM(t.amount) FILTER (WHERE t.status='COMPLETED') AS saq_ok,
                       SUM(t.amount) FILTER (WHERE t.status='FAILED') AS saq_fail,
                       COUNT(*) FILTER (WHERE t.status='COMPLETED') AS n_saq_ok,
                       COUNT(*) FILTER (WHERE t.status='FAILED') AS n_saq_fail,
                       MIN(t.processed_at) FILTER (WHERE t.status='COMPLETED') AS primeiro_saque_at
                FROM suspect s
                JOIN transactions t ON t.user_id=s.id
                WHERE t.type='WITHDRAW'
                GROUP BY s.id
            ),
            bonus_user AS (
                SELECT s.id AS user_id,
                       SUM(t.amount) FILTER (WHERE t.type='BONUS_CREDIT') AS bonus_credit,
                       SUM(t.amount) FILTER (WHERE t.type='BONUS_DEBIT') AS bonus_debit,
                       SUM(t.amount) FILTER (WHERE t.type='BONUS_CONVERSION') AS bonus_conv
                FROM suspect s
                JOIN transactions t ON t.user_id=s.id
                WHERE t.type IN ('BONUS_CREDIT','BONUS_DEBIT','BONUS_CONVERSION')
                GROUP BY s.id
            ),
            ativ AS (
                SELECT s.id AS user_id,
                       MAX(b.status) AS status_bonus,
                       COUNT(*) AS n_ativ
                FROM suspect s
                LEFT JOIN bonus_activations b ON b.user_id=s.id
                GROUP BY s.id
            ),
            turn_user AS (
                SELECT s.id AS user_id,
                       SUM(m.total_bet_amount) AS turnover,
                       SUM(m.played_rounds) AS giros
                FROM suspect s
                LEFT JOIN casino_user_game_metrics m ON m.user_id=s.id
                GROUP BY s.id
            )
            SELECT u.username, u.phone,
                   COALESCE(d.dep_total,0)::numeric(10,0) AS dep,
                   COALESCE(b.bonus_credit,0)::numeric(10,0) AS bon_cred,
                   COALESCE(b.bonus_conv,0)::numeric(10,0) AS bon_conv,
                   COALESCE(t.turnover,0)::numeric(10,0) AS turn,
                   COALESCE(t.giros,0) AS giros,
                   COALESCE(b.bonus_credit * 75,0)::numeric(12,0) AS roll_75x_bonus,
                   COALESCE((b.bonus_credit + d.dep_total) * 75,0)::numeric(12,0) AS roll_75x_bon_dep,
                   COALESCE(s.saq_ok,0)::numeric(10,0) AS saq,
                   COALESCE(s.n_saq_ok,0) AS n_saq,
                   COALESCE(a.status_bonus,'-') AS st_bonus,
                   COALESCE(a.n_ativ,0) AS n_ativ
            FROM users u
            JOIN (SELECT id FROM users WHERE phone LIKE '+92341374%' OR username='utl2FFfrQR7Qj6qi') x ON x.id=u.id
            LEFT JOIN dep_user d ON d.user_id=u.id
            LEFT JOIN bonus_user b ON b.user_id=u.id
            LEFT JOIN saq_user s ON s.user_id=u.id
            LEFT JOIN turn_user t ON t.user_id=u.id
            LEFT JOIN ativ a ON a.user_id=u.id
            ORDER BY saq DESC NULLS LAST
        """)
        rows = cur.fetchall()
        print(f"\n{'user':18} {'phone':15} {'dep':>5} {'bon_c':>5} {'bon_x':>5} {'turn':>7} {'giros':>5} {'req_75*b':>9} {'req_75*(b+d)':>12} {'saq':>5} {'n_saq':>5} {'bonus':>10}")
        print("-"*140)

        comprimiu_75b = 0
        comprimiu_75bd = 0
        sacaram = 0
        for r in rows:
            user, phone, dep, bon_c, bon_conv, turn, giros, req_b, req_bd, saq, n_saq, st_bon, n_ativ = r
            dep_f, bon_c_f, bon_conv_f, turn_f, req_b_f, req_bd_f, saq_f = float(dep), float(bon_c), float(bon_conv), float(turn), float(req_b), float(req_bd), float(saq)
            # Flags
            flags = []
            if saq_f > 0:
                sacaram += 1
                if turn_f >= req_b_f:
                    comprimiu_75b += 1
                    flags.append("ok75*b")
                else:
                    flags.append("NAO75*b")
                if turn_f >= req_bd_f:
                    comprimiu_75bd += 1
                else:
                    flags.append("NAO75*(b+d)")
            print(f"{str(user)[:18]:18} {str(phone)[:15]:15} {int(dep_f):>5} {int(bon_c_f):>5} {int(bon_conv_f):>5} {int(turn_f):>7} {int(giros):>5} {int(req_b_f):>9} {int(req_bd_f):>12} {int(saq_f):>5} {n_saq:>5} {str(st_bon)[:10]:>10}  {' '.join(flags)}")

        print(f"\n>> {sacaram} contas ja sacaram.")
        print(f">> Cumpriram rollover 75x * BONUS (Rs 15.000 p/ bonus 200): {comprimiu_75b}/{sacaram}")
        print(f">> Cumpriram rollover 75x * (BONUS+DEPOSIT): {comprimiu_75bd}/{sacaram}")

        # 4) Detalhe por saque - tempo reg -> saque
        print("\n\n" + "="*90)
        print("4) TIMELINE dos saques - quanto tempo entre dep e saque? Jogou o rollover?")
        print("="*90)
        cur.execute(f"""
            WITH suspect AS ({SUSPECT})
            SELECT u.username,
                   u.created_at::timestamp(0) AS cadastrado,
                   (SELECT MIN(t.processed_at) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')::timestamp(0) AS ftd_at,
                   (SELECT MIN(t.processed_at) FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED')::timestamp(0) AS saque_at,
                   EXTRACT(EPOCH FROM
                     (SELECT MIN(t.processed_at) FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED')
                     -
                     (SELECT MIN(t.processed_at) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')
                   )/60 AS min_dep_a_saque,
                   (SELECT COALESCE(SUM(m.total_bet_amount),0) FROM casino_user_game_metrics m WHERE m.user_id=u.id) AS turn,
                   (SELECT COALESCE(SUM(t.amount),0) FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED') AS dep,
                   (SELECT COALESCE(SUM(t.amount),0) FROM transactions t WHERE t.user_id=u.id AND t.type='BONUS_CREDIT') AS bon
            FROM users u JOIN suspect s ON s.id=u.id
            WHERE EXISTS(SELECT 1 FROM transactions t WHERE t.user_id=u.id AND t.type='WITHDRAW' AND t.status='COMPLETED')
            ORDER BY saque_at
        """)
        for r in cur.fetchall():
            usr, cad, ftd, saq, min_gap, turn, dep, bon = r
            print(f"\n  {usr}")
            print(f"    cad:    {cad}")
            print(f"    ftd:    {ftd}")
            print(f"    saque:  {saq}  ({min_gap:.0f}min depois do dep)")
            print(f"    dep={int(float(dep))}  bonus={int(float(bon))}  turnover={int(float(turn))}")
            print(f"    req 75x bonus = {int(float(bon)*75)}  |  req 75x (bon+dep) = {int((float(bon)+float(dep))*75)}")
            if float(turn) >= float(bon)*75:
                print(f"    >> CUMPRIU rollover 75x*bonus")
            else:
                pct = 100*float(turn)/(float(bon)*75) if float(bon)>0 else 0
                print(f"    >> NAO CUMPRIU rollover 75x*bonus (jogou {pct:.0f}%)")

finally:
    conn.close(); tunnel.stop()
