"""Smoking gun final - comparar IPs batch vs jogadores reais + payment accounts."""
import os, sys, json
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # ====================================================================
        # 1) IPs do batch — ranges AWS confirmados?
        # ====================================================================
        print("="*90)
        print("1) IPs do BATCH (ontem + hoje, +92341374xxx) - prefixos AWS?")
        print("="*90)
        cur.execute("""
            SELECT u.username, u.phone,
                   array_agg(DISTINCT s.metadata->>'ip') AS ips
            FROM user_sessions s JOIN users u ON u.id=s.user_id
            WHERE u.phone LIKE '+92341374%'
              AND s.metadata->>'ip' IS NOT NULL
            GROUP BY u.username, u.phone
            ORDER BY u.phone
        """)
        all_ips = set()
        for r in cur.fetchall():
            for ip in r[2] or []:
                all_ips.add(ip)
            print(f"   {str(r[0])[:18]:18} {r[1]:14} ips={r[2]}")
        print(f"\n   Total IPs distintos do batch: {len(all_ips)}")

        # Top-3 octeto pra ver se sao todos AWS
        prefixos = {}
        for ip in all_ips:
            parts = ip.split('.')
            if len(parts) >= 1:
                octeto1 = parts[0]
                prefixos[octeto1] = prefixos.get(octeto1, 0) + 1
        print(f"\n   Distribuicao por primeiro octeto:")
        for octeto, qtd in sorted(prefixos.items(), key=lambda x: -x[1]):
            classifica = ""
            o = int(octeto)
            if o in (3, 13, 18, 34, 35, 44, 50, 52, 54, 98, 100, 107, 174, 184): classifica = " (AWS US)"
            elif o in (39, 110, 116, 119, 175, 182, 202, 203, 222): classifica = " (Paquistao tipico)"
            print(f"     {octeto}.x.x.x  qtd={qtd}{classifica}")

        # ====================================================================
        # 2) Jogadores reais (mirzaibrar e outros legitimos) - quais IPs?
        # ====================================================================
        print("\n" + "="*90)
        print("2) IPs de JOGADORES REAIS legitimos para comparacao")
        print("="*90)
        cur.execute("""
            SELECT u.username, u.phone,
                   array_agg(DISTINCT s.metadata->>'ip') AS ips,
                   COUNT(*) AS sessions
            FROM user_sessions s JOIN users u ON u.id=s.user_id
            WHERE u.role='USER' AND s.metadata->>'ip' IS NOT NULL
              AND u.username IN ('mirzaibrar','mehmood88','malik7','azharahmed','qamarulzaman','UsmanHaider42','azadbhi','khangkhan','newsaher')
            GROUP BY u.username, u.phone
            ORDER BY u.username
        """)
        for r in cur.fetchall():
            ips_sample = list(r[2] or [])[:3]
            octets = [ip.split('.')[0] for ip in ips_sample if ip]
            print(f"   {str(r[0])[:18]:18} {str(r[1])[:14]:14} sess={r[3]} ips={ips_sample}  octets={octets}")

        # IPs típicos de jogadores reais (pra estabelecer baseline)
        print("\n   Top primeiros octetos dos USUARIOS REAIS (universo todo, n=top 20):")
        cur.execute("""
            WITH test_ids AS (
                SELECT DISTINCT u.id FROM users u WHERE
                    (u.role != 'USER'
                     OR u.id IN (SELECT t.user_id FROM transactions t
                                 WHERE t.type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT')
                                    OR (t.type='DEPOSIT' AND t.reviewed_by IS NOT NULL)))
            )
            SELECT SPLIT_PART(s.metadata->>'ip','.',1) AS oct1,
                   COUNT(*) AS sess,
                   COUNT(DISTINCT s.user_id) AS users
            FROM user_sessions s JOIN users u ON u.id=s.user_id
            WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
              AND u.phone NOT LIKE '+92341374%'
              AND s.metadata->>'ip' IS NOT NULL
            GROUP BY 1 ORDER BY users DESC LIMIT 20
        """)
        for r in cur.fetchall():
            o = int(r[0])
            classifica = ""
            if o in (3, 13, 18, 34, 35, 44, 50, 52, 54, 98, 100, 107, 174, 184): classifica = " (AWS US)"
            elif o in (39, 110, 116, 119, 175, 182, 202, 203, 222): classifica = " (Paquistao)"
            print(f"     {r[0]:>3}.x.x.x  users={r[2]:>4}  sess={r[1]:>4}{classifica}")

        # ====================================================================
        # 3) USER_PAYMENT_ACCOUNTS - CONTA BANCARIA do farmer
        # ====================================================================
        print("\n" + "="*90)
        print("3) USER_PAYMENT_ACCOUNTS - mesma conta bancaria multipla?")
        print("="*90)
        cur.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_schema='public' AND table_name='user_payment_accounts'
            ORDER BY ordinal_position
        """)
        cols_pay = [r[0] for r in cur.fetchall()]
        print(f"   Colunas: {cols_pay}")

        # Sample
        cur.execute("""
            SELECT * FROM user_payment_accounts
            JOIN users u ON u.id=user_payment_accounts.user_id
            WHERE u.phone LIKE '+92341374%'
            LIMIT 3
        """)
        rows = cur.fetchall()
        if rows:
            cols = [d[0] for d in cur.description]
            print(f"\n   Sample do batch:")
            for row in rows:
                print(f"     ---")
                for c, v in zip(cols, row):
                    val = str(v)[:120] if v else None
                    if val: print(f"     {c}: {val}")

        # Concentracao por conta
        # CORRECAO: usar colunas reais (account_number, account_name, bank_code)
        cur.execute("""
            SELECT pa.account_number, pa.account_name, pa.bank_code,
                   COUNT(DISTINCT pa.user_id) AS users,
                   array_agg(DISTINCT u.username) AS usernames,
                   array_agg(DISTINCT u.phone) AS phones
            FROM user_payment_accounts pa
            JOIN users u ON u.id=pa.user_id
            WHERE u.phone LIKE '+92341374%'
            GROUP BY 1,2,3
            ORDER BY pa.account_number
        """)
        rows = cur.fetchall()
        print(f"\n   TODAS as contas bancarias do batch ({len(rows)} contas distintas):")
        print(f"   {'conta_jazzcash':18} {'titular':12} {'users':>5} usernames")
        seq_check = []
        for r in rows:
            flag = " <- SMOKING GUN" if r[3] >= 2 else ""
            print(f"   {str(r[0])[:18]:18} {str(r[1])[:12]:12} {r[3]:>5} {r[4]}{flag}")
            print(f"       phones_users={r[5]}")
            if r[0] and r[0].startswith('+92304720'):
                seq_check.append(r[0])
        if seq_check:
            seq_check.sort()
            print(f"\n   >> {len(seq_check)} contas Jazzcash no MESMO PREFIXO +92304720{8}xxx — SEQUENCIAL")
            print(f"   >> {seq_check}")

        # Qualquer chave alternativa em metadata
        cur.execute("""
            SELECT DISTINCT jsonb_object_keys(metadata)
            FROM user_payment_accounts WHERE metadata IS NOT NULL
        """)
        print(f"\n   Chaves no metadata de user_payment_accounts: {[r[0] for r in cur.fetchall()]}")

        # ====================================================================
        # 4) Bets — session_id concentrada?
        # ====================================================================
        print("\n" + "="*90)
        print("4) BETS.session_id - mesma sessao em multiplas contas?")
        print("="*90)
        cur.execute("""
            SELECT b.session_id, COUNT(DISTINCT b.user_id) AS users,
                   array_agg(DISTINCT u.username) FILTER (WHERE u.username IS NOT NULL) AS usernames
            FROM bets b JOIN users u ON u.id=b.user_id
            WHERE u.phone LIKE '+92341374%' AND b.session_id IS NOT NULL
            GROUP BY 1 HAVING COUNT(DISTINCT b.user_id) >= 1
            ORDER BY users DESC LIMIT 5
        """)
        for r in cur.fetchall():
            flag = " <- SMOKING GUN" if r[1] >= 2 else ""
            print(f"   session={str(r[0])[:30]:30} users={r[1]}{flag}")
            if r[1] <= 5:
                print(f"      usernames={r[2]}")

        # ====================================================================
        # 5) ADMIN_AUDIT_LOGS - alguem da casa criou essas contas?
        # ====================================================================
        print("\n" + "="*90)
        print("5) ADMIN_AUDIT_LOGS - acoes do staff sobre essas contas")
        print("="*90)
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_schema='public' AND table_name='admin_audit_logs'
        """)
        cols_aud = [r[0] for r in cur.fetchall()]
        print(f"   Colunas: {cols_aud}")
        # Busca acoes nas contas batch
        cur.execute("""
            SELECT a.*
            FROM admin_audit_logs a
            WHERE EXISTS (SELECT 1 FROM users u WHERE u.phone LIKE '+92341374%%'
                          AND (a::text LIKE '%%'||u.id::text||'%%' OR a::text LIKE '%%'||u.username||'%%'))
            LIMIT 3
        """)
        rows = cur.fetchall()
        if rows:
            cols = [d[0] for d in cur.description]
            for row in rows:
                print(f"   ---")
                for c, v in zip(cols, row):
                    if v: print(f"   {c}: {str(v)[:100]}")
        else:
            print("   Nenhuma acao admin sobre contas do batch")

finally:
    conn.close(); tunnel.stop()
