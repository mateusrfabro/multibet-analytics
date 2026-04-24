"""Monitor live - ataque ainda rolando?"""
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
        # ==============================================================
        # 1) O batch original está ativo agora?
        # ==============================================================
        print("\n" + "="*78)
        print("1) ATIVIDADE DO BATCH ORIGINAL (+92341374...) - ultimos 60min")
        print("="*78)
        cur.execute("""
            SELECT u.username, u.phone,
                   EXTRACT(EPOCH FROM (NOW() - MAX(t.created_at)))/60 AS ult_min,
                   COUNT(*) FILTER (WHERE t.created_at >= NOW() - INTERVAL '2 hours') AS tx_2h,
                   COUNT(*) FILTER (WHERE t.created_at >= NOW() - INTERVAL '1 hour') AS tx_1h,
                   COUNT(*) FILTER (WHERE t.type='WITHDRAW' AND t.created_at >= NOW() - INTERVAL '2 hours') AS saq_2h,
                   COUNT(*) FILTER (WHERE t.type='CASINO_DEBIT' AND t.created_at >= NOW() - INTERVAL '2 hours') AS bet_2h
            FROM users u JOIN transactions t ON t.user_id=u.id
            WHERE u.phone LIKE '+92341374%'
            GROUP BY u.username, u.phone
            HAVING COUNT(*) FILTER (WHERE t.created_at >= NOW() - INTERVAL '2 hours') > 0
            ORDER BY ult_min
        """)
        rows = cur.fetchall()
        if rows:
            print(f"   {'username':20} {'phone':15} {'ult':>7} {'tx_2h':>6} {'tx_1h':>6} {'saq_2h':>6} {'bet_2h':>6}")
            for r in rows:
                print(f"   {str(r[0])[:20]:20} {r[1]:15} {float(r[2]):>5.0f}m {r[3]:>6} {r[4]:>6} {r[5]:>6} {r[6]:>6}")
            print(f"\n   >> {len(rows)} de 16 contas do batch ainda ATIVAS nas ultimas 2h")
        else:
            print("   >> ZERO atividade do batch nas ultimas 2h — farmer parou OU queimou o ciclo")

        # ==============================================================
        # 2) Cadastros novos hoje PÓS-14:44 (depois da primeira análise)
        # ==============================================================
        print("\n" + "="*78)
        print("2) NOVOS CADASTROS desde a primeira analise (pos-14:44 BRT)")
        print("="*78)
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE u.role='USER') AS novos_cad,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.phone LIKE '+92341374%') AS batch_orig,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.phone LIKE '+9234137418%') AS vizinho_18,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.phone LIKE '+9234137420%') AS vizinho_20,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.phone LIKE '+9234137421%') AS vizinho_21,
                COUNT(*) FILTER (WHERE u.role='USER' AND (u.email LIKE '%@wetuns.com' OR u.email LIKE '%@whyknapp.com')) AS temp_mail_conhec,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.email IS NOT NULL
                                   AND u.email NOT LIKE '%@gmail.com' AND u.email NOT LIKE '%@yandex%'
                                   AND u.email NOT LIKE '%@hotmail%' AND u.email NOT LIKE '%@outlook%'
                                   AND u.email NOT LIKE '%@yahoo%') AS email_suspeito
            FROM users u
            WHERE u.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '3 hours')::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
               OR u.created_at >= NOW() - INTERVAL '3 hours'
        """)
        r = cur.fetchone()
        print(f"   Cadastros USER ultimas 3h: {r[0]}")
        print(f"     - batch original (+92341374): {r[1]}")
        print(f"     - prefix vizinho +9234137418: {r[2]}")
        print(f"     - prefix vizinho +9234137420: {r[3]}")
        print(f"     - prefix vizinho +9234137421: {r[4]}")
        print(f"     - temp-mail conhecido:        {r[5]}")
        print(f"     - outro email suspeito:       {r[6]}")

        # Detalhe — cadastros das últimas 3h
        cur.execute("""
            SELECT u.username, u.phone, u.email,
                   (u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::time AS cad_brt,
                   EXISTS (SELECT 1 FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED') AS fez_ftd,
                   EXISTS (SELECT 1 FROM bonus_activations b WHERE b.user_id=u.id) AS ativou_bonus
            FROM users u
            WHERE u.role='USER' AND u.created_at >= NOW() - INTERVAL '3 hours'
            ORDER BY u.created_at DESC
        """)
        rows = cur.fetchall()
        if rows:
            print(f"\n   Lista detalhada (ultimas 3h):")
            print(f"   {'username':20} {'phone':15} {'email':30} {'cad':>8} {'FTD':>4} {'BON':>4}")
            for r in rows:
                ftd = "SIM" if r[4] else "—"
                bon = "SIM" if r[5] else "—"
                print(f"   {str(r[0])[:20]:20} {str(r[1])[:15]:15} {str(r[2] or '—')[:30]:30} {str(r[3])[:8]:>8} {ftd:>4} {bon:>4}")

        # ==============================================================
        # 3) CV FTD — delta desde a primeira análise (14:44 BRT)
        # ==============================================================
        print("\n" + "="*78)
        print("3) CV FTD HOJE — valor atual vs primeira extracao (14:44 BRT)")
        print("="*78)
        cur.execute("""
            WITH test_ids AS (
                SELECT DISTINCT u.id
                FROM users u
                WHERE u.role != 'USER'
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
            SELECT
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)) AS cad,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids) AND u.phone LIKE '+92341374%') AS cad_batch,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
                                   AND EXISTS (SELECT 1 FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')) AS ftd,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
                                   AND u.phone LIKE '+92341374%'
                                   AND EXISTS (SELECT 1 FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')) AS ftd_batch,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
                                   AND u.phone NOT LIKE '+92341374%'
                                   AND u.username != 'utl2FFfrQR7Qj6qi'
                                   AND EXISTS (SELECT 1 FROM transactions t WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')) AS ftd_organ,
                COUNT(*) FILTER (WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
                                   AND u.phone NOT LIKE '+92341374%'
                                   AND u.username != 'utl2FFfrQR7Qj6qi') AS cad_organ
            FROM users u
            WHERE ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
        """)
        r = cur.fetchone()
        cad, cad_b, ftd, ftd_b, ftd_o, cad_o = r
        print(f"   CADASTRADOS hoje:  {cad}  (batch: {cad_b} | organico: {cad_o})")
        print(f"   FTDs hoje:         {ftd}  (batch: {ftd_b} | organico: {ftd_o})")
        print(f"   CV REPORTADO:      {100*ftd/max(cad,1):.1f}%  (cohort)")
        print(f"   CV ORGANICO:       {100*ftd_o/max(cad_o,1):.1f}%  (sem batch)")
        print(f"\n   DELTA vs primeira extracao (14:44): cad +{cad-81} | ftd +{ftd-32}")

        # ==============================================================
        # 4) Bonus activations hoje — granular
        # ==============================================================
        print("\n" + "="*78)
        print("4) BONUS ACTIVATIONS hoje")
        print("="*78)
        cur.execute("""
            SELECT
                DATE_TRUNC('hour', b.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora,
                COUNT(*) AS ativacoes,
                COUNT(DISTINCT b.user_id) AS users,
                SUM(CASE WHEN b.status='ACTIVE' THEN 1 ELSE 0 END) AS ativ,
                SUM(CASE WHEN b.status='CANCELLED' THEN 1 ELSE 0 END) AS canc,
                SUM(CASE WHEN b.status='COMPLETED' THEN 1 ELSE 0 END) AS compl
            FROM bonus_activations b
            WHERE b.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo')::date::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
            GROUP BY 1 ORDER BY 1
        """)
        rows = cur.fetchall()
        if rows:
            print(f"   {'hora BRT':18} {'ativ':>5} {'users':>5} {'ACTIVE':>6} {'CANC':>5} {'COMPL':>5}")
            for r in rows:
                print(f"   {str(r[0])[:16]:18} {r[1]:>5} {r[2]:>5} {r[3]:>6} {r[4]:>5} {r[5]:>5}")

        # ==============================================================
        # 5) Padrão GERAL de fraude que o scan detecta (sub-2min, dep=min)
        # ==============================================================
        print("\n" + "="*78)
        print("5) PADRAO DE FARMING em tempo real (cadastros ultimas 3h)")
        print("="*78)
        cur.execute("""
            WITH novos AS (
                SELECT u.id, u.username, u.phone, u.email, u.created_at,
                       (SELECT MIN(t.processed_at) FROM transactions t
                        WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED') AS ftd_at,
                       (SELECT SUM(t.amount) FROM transactions t
                        WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED') AS dep
                FROM users u
                WHERE u.role='USER' AND u.created_at >= NOW() - INTERVAL '3 hours'
            )
            SELECT
                COUNT(*) total,
                COUNT(*) FILTER (WHERE ftd_at IS NOT NULL) AS com_ftd,
                COUNT(*) FILTER (WHERE ftd_at IS NOT NULL AND (ftd_at - created_at) < INTERVAL '2 minutes') AS sub_2min,
                COUNT(*) FILTER (WHERE dep IN (200, 300)) AS dep_min,
                COUNT(*) FILTER (WHERE email LIKE '%wetuns.com' OR email LIKE '%whyknapp.com') AS temp_mail,
                COUNT(*) FILTER (WHERE LENGTH(username) = 16 AND username ~ '^[a-zA-Z0-9]{16}$') AS user_random_16
            FROM novos
        """)
        r = cur.fetchone()
        print(f"   Novos cad 3h: {r[0]}")
        print(f"   - com FTD: {r[1]}")
        print(f"   - FTD sub-2min: {r[2]}")
        print(f"   - dep = 200/300: {r[3]}")
        print(f"   - temp-mail conhecido: {r[4]}")
        print(f"   - username random 16 chars: {r[5]}")
        if r[0] == 0:
            print("   >> ZERO novos cadastros ha 3h — trafego paralisado")

finally:
    conn.close(); tunnel.stop()
