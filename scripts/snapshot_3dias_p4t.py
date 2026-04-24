"""Snapshot atual + comparativo 3 dias (18-19-20/04) p/ Play4Tune."""
import os, sys
from datetime import datetime
from zoneinfo import ZoneInfo
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

BRT = ZoneInfo("America/Sao_Paulo")
print(f"# Snapshot: {datetime.now(BRT).strftime('%d/%m/%Y %H:%M BRT')}")

REAL_WHITELIST = ('maharshani44377634693','muhammadrehan17657797557',
                   'rehmanzafar006972281','saimkyani15688267')

TEST_CTE = f"""
test_ids AS (
    SELECT DISTINCT u.id FROM users u WHERE
        (u.role != 'USER'
         OR LOWER(u.username) LIKE '%%test%%'
         OR LOWER(u.username) LIKE '%%demo%%'
         OR LOWER(u.username) LIKE '%%admin%%'
         OR LOWER(COALESCE(u.email, '')) LIKE '%%@karinzitta%%'
         OR LOWER(COALESCE(u.email, '')) LIKE '%%@multi.bet%%'
         OR LOWER(COALESCE(u.email, '')) LIKE '%%@grupo-pgs%%'
         OR LOWER(COALESCE(u.email, '')) LIKE '%%@supernovagaming%%'
         OR LOWER(COALESCE(u.email, '')) LIKE '%%@play4tune%%'
         OR u.id IN (SELECT DISTINCT t.user_id FROM transactions t
                     WHERE t.type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT')
                        OR (t.type='DEPOSIT' AND t.reviewed_by IS NOT NULL)))
        AND u.username NOT IN {REAL_WHITELIST}
)"""

tunnel, conn = get_supernova_bet_connection()
try:
    with conn.cursor() as cur:
        # ============================================================
        # 1) 3 DIAS: comparativo 18 / 19 / 20
        # ============================================================
        print("\n" + "="*90)
        print("1) COMPARATIVO 3 DIAS — usuarios reais (filtro oficial aplicado)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE},
            base AS (
                SELECT u.id, u.phone, u.email,
                       ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                       EXISTS(SELECT 1 FROM transactions t WHERE t.user_id=u.id
                              AND t.type='DEPOSIT' AND t.status='COMPLETED') AS fez_ftd,
                       EXISTS(SELECT 1 FROM transactions t WHERE t.user_id=u.id
                              AND t.type='WITHDRAW' AND t.status='COMPLETED') AS fez_saque
                FROM users u
                WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
                  AND u.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '4 days')::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
            )
            SELECT dia, COUNT(*) cad,
                   COUNT(*) FILTER (WHERE fez_ftd) ftd,
                   COUNT(*) FILTER (WHERE fez_saque) saq,
                   COUNT(*) FILTER (WHERE phone LIKE '+92341374%') batch_19,
                   COUNT(*) FILTER (WHERE phone LIKE '+9234137418%' OR phone LIKE '+9234137420%' OR phone LIKE '+9234137421%') viz,
                   COUNT(*) FILTER (WHERE email LIKE '%%@wetuns.com' OR email LIKE '%%@whyknapp.com') temp_mail
            FROM base GROUP BY 1 ORDER BY 1
        """)
        print(f"  {'dia':12} {'cad':>5} {'ftd':>4} {'cv%':>5} {'saq':>4} {'batch_97*':>9} {'vizinho':>7} {'tempmail':>8}")
        baselines = []
        for r in cur.fetchall():
            cv = 100*r[2]/max(r[1],1)
            if str(r[0]) not in ('2026-04-18','2026-04-19','2026-04-20'):
                baselines.append(cv)
            print(f"  {str(r[0]):12} {r[1]:>5} {r[2]:>4} {cv:>5.1f} {r[3]:>4} {r[4]:>9} {r[5]:>7} {r[6]:>8}")
        if baselines:
            print(f"\n  Baseline pre-ataque (dias antes 18/04): CV medio {sum(baselines)/len(baselines):.1f}%")

        # ============================================================
        # 2) Farmer voltou hoje? Prefixos sequenciais hoje
        # ============================================================
        print("\n" + "="*90)
        print("2) FARMER VOLTOU HOJE? Prefixos telefonicos hoje (2+ cadastros)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE}
            SELECT SUBSTRING(u.phone,1,9) AS prefix9, COUNT(*) qtd,
                   array_agg(SUBSTRING(u.phone,10,4) ORDER BY u.phone) AS sufixos
            FROM users u
            WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
              AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
              AND u.phone IS NOT NULL
            GROUP BY 1 HAVING COUNT(*) >= 2 ORDER BY qtd DESC
        """)
        rows = cur.fetchall()
        if rows:
            for r in rows:
                flag = " <- SUSPEITO" if r[1] >= 3 else ""
                print(f"    {r[0]}  qtd={r[1]}  sufixos={r[2]}{flag}")
        else:
            print("    (nenhum prefixo com 2+ cadastros hoje)")

        # ============================================================
        # 3) Novos dominios email hoje (alerta pra mudanca de tatica)
        # ============================================================
        print("\n" + "="*90)
        print("3) DOMINIOS EMAIL - hoje (novos ou conhecidos farmer?)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE},
            hoje AS (
                SELECT SPLIT_PART(email,'@',2) AS dominio, COUNT(*) AS qtd
                FROM users u
                WHERE u.role='USER' AND email IS NOT NULL
                  AND u.id NOT IN (SELECT id FROM test_ids)
                  AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                      = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                GROUP BY 1
            )
            SELECT h.dominio, h.qtd,
                   (SELECT COUNT(*) FROM users u2 WHERE u2.email LIKE '%%@'||h.dominio
                      AND u2.created_at < '2026-04-18'::date) AS pre_ataque
            FROM hoje h ORDER BY qtd DESC
        """)
        for r in cur.fetchall():
            tag = " <- TEMP-MAIL FARMER KNOWN" if r[0] in ('wetuns.com','whyknapp.com') else (
                  " <- NOVO (zero historico)" if r[2] == 0 else "")
            print(f"    {r[0]:30} hoje={r[1]:>3}  historico={r[2]:>4}{tag}")

        # ============================================================
        # 4) NOVOS JAZZCASH hoje - prefixo farmer continua?
        # ============================================================
        print("\n" + "="*90)
        print("4) JAZZCASH destinos de saque hoje (farmer conhecido: +92304720850-563)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE}
            SELECT pa.account_number, pa.account_name, pa.bank_code,
                   u.username, u.phone,
                   (pa.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::timestamp(0) AS quando
            FROM user_payment_accounts pa
            JOIN users u ON u.id=pa.user_id
            WHERE u.id NOT IN (SELECT id FROM test_ids)
              AND ((pa.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
            ORDER BY pa.created_at
        """)
        rows = cur.fetchall()
        jazz_farmer = ('+923047208500','+923047208511','+923047208512','+923047208533',
                       '+923047208563','+923006006405','+923413741900','+923413741933')
        if rows:
            print(f"  {'jazzcash_dest':18} {'titular':12} {'bank':10} {'user':20} {'phone':14} quando")
            for r in rows:
                flag = " <- FARMER KNOWN" if r[0] in jazz_farmer else (
                       " <- PREFIXO FARMER" if r[0] and r[0].startswith('+92304720850') else "")
                print(f"  {str(r[0])[:18]:18} {str(r[1])[:12]:12} {str(r[2])[:10]:10} {str(r[3])[:20]:20} {str(r[4])[:14]:14} {r[5]}{flag}")
        else:
            print("  (nenhum jazzcash adicionado hoje)")

        # ============================================================
        # 5) FTDs hoje com padrao farmer (sub-2min, dep 200/300)
        # ============================================================
        print("\n" + "="*90)
        print("5) CADASTROS SUSPEITOS HOJE (padrao farming)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE}
            SELECT u.username, u.phone, u.email,
                   (u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::time AS cad,
                   EXTRACT(EPOCH FROM (
                     (SELECT MIN(t.processed_at) FROM transactions t WHERE t.user_id=u.id
                      AND t.type='DEPOSIT' AND t.status='COMPLETED') - u.created_at))/60 AS min_gap,
                   (SELECT SUM(t.amount)::numeric(8,0) FROM transactions t WHERE t.user_id=u.id
                    AND t.type='DEPOSIT' AND t.status='COMPLETED') AS dep,
                   (SELECT SUM(t.amount)::numeric(8,0) FROM transactions t WHERE t.user_id=u.id
                    AND t.type='WITHDRAW' AND t.status='COMPLETED') AS saq
            FROM users u
            WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
              AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
              AND EXISTS(SELECT 1 FROM transactions t WHERE t.user_id=u.id
                         AND t.type='DEPOSIT' AND t.status='COMPLETED')
            ORDER BY u.created_at
        """)
        rows = cur.fetchall()
        sub2 = [r for r in rows if r[4] and float(r[4]) < 2]
        temp_mail = [r for r in rows if r[2] and ('@wetuns' in r[2] or '@whyknapp' in r[2])]
        dep_min = [r for r in rows if r[5] and float(r[5]) in (200, 300)]
        phones_97 = [r for r in rows if r[1] and r[1].startswith('+923413741')]
        print(f"  Total FTDs hoje: {len(rows)}")
        print(f"  - Sub-2min: {len(sub2)}  ({100*len(sub2)/max(len(rows),1):.0f}%)")
        print(f"  - Temp-mail wetuns/whyknapp: {len(temp_mail)}")
        print(f"  - Dep 200/300 (min bonus): {len(dep_min)}")
        print(f"  - Phone +923413741xxx: {len(phones_97)}")
        if rows:
            print(f"\n  Lista FTDs hoje:")
            print(f"  {'username':20} {'phone':14} {'email':28} {'cad':>8} {'min':>5} {'dep':>5} {'saq':>5}")
            for r in rows:
                flags = []
                if r[1] and r[1].startswith('+923413741'): flags.append('P97')
                if r[2] and ('@wetuns' in r[2] or '@whyknapp' in r[2]): flags.append('TEMP')
                if r[4] and float(r[4]) < 2: flags.append('SUB2')
                if r[5] and float(r[5]) in (200,300): flags.append('DEPmin')
                flag_str = '|'.join(flags) if flags else '-'
                print(f"  {str(r[0])[:20]:20} {str(r[1])[:14]:14} {str(r[2] or '')[:28]:28} {str(r[3])[:8]:>8} {float(r[4] or 0):>5.1f} {float(r[5] or 0):>5.0f} {float(r[6] or 0):>5.0f} {flag_str}")

        # ============================================================
        # 6) ROLLOVER - Gabriel corrigiu? (saques hoje, quantos sem cumprir?)
        # ============================================================
        print("\n" + "="*90)
        print("6) ENFORCEMENT ROLLOVER - saques HOJE cumpriram? (verificar se Gabriel corrigiu)")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE},
            saques_hoje AS (
                SELECT DISTINCT t.user_id
                FROM transactions t
                WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
                  AND ((t.processed_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                      = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                  AND t.user_id NOT IN (SELECT id FROM test_ids)
            ),
            bonus_user AS (
                SELECT s.user_id,
                       SUM(b.rollover_target) AS target,
                       SUM(b.rollover_progress) AS progress,
                       MAX(b.status) AS bonus_st
                FROM saques_hoje s
                LEFT JOIN bonus_activations b ON b.user_id=s.user_id
                GROUP BY s.user_id
            )
            SELECT
                COUNT(*) total,
                COUNT(*) FILTER (WHERE target IS NOT NULL) com_bonus,
                COUNT(*) FILTER (WHERE progress >= target) cumpriu,
                COUNT(*) FILTER (WHERE progress < target) nao_cumpriu,
                COUNT(*) FILTER (WHERE target IS NULL) sem_bonus_ativ
            FROM bonus_user
        """)
        r = cur.fetchone()
        print(f"  Total saques hoje (usuarios reais): {r[0]}")
        print(f"    - com bonus ativado: {r[1]}")
        print(f"      - CUMPRIRAM rollover: {r[2]}")
        print(f"      - NAO cumpriram: {r[3]}")
        print(f"    - sem bonus ativado: {r[4]}")
        if r[1]:
            pct_nao = 100*r[3]/r[1]
            if pct_nao < 50:
                print(f"\n  >> CONFORMIDADE EM ALTA ({pct_nao:.0f}% nao cumprindo) — possivel que Gabriel tenha atuado no enforcement!")
            else:
                print(f"\n  >> Enforcement ainda aberto: {pct_nao:.0f}% dos saques nao cumprem rollover")

        # ============================================================
        # 7) STATUS CONTAS DO BATCH - vivas ou mortas?
        # ============================================================
        print("\n" + "="*90)
        print("7) STATUS CONTAS BATCH (+92341374xxx) - vivas ou mortas?")
        print("="*90)
        cur.execute("""
            SELECT u.username, u.phone,
                   (u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date AS cad_dia,
                   (SELECT MAX(t.created_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                    FROM transactions t WHERE t.user_id=u.id) AS ult,
                   u.active, u.blocked
            FROM users u
            WHERE u.phone LIKE '+92341374%'
            ORDER BY ult DESC NULLS LAST LIMIT 30
        """)
        now_naive = datetime.now(BRT).replace(tzinfo=None)
        for r in cur.fetchall():
            ult = r[3].replace(tzinfo=None) if r[3] and r[3].tzinfo else r[3]
            atras_h = (now_naive - ult).total_seconds()/3600 if ult else None
            atras_str = f"{atras_h:.1f}h" if atras_h is not None else "nunca"
            status = ""
            if r[5]: status += " BLOCKED"
            if not r[4]: status += " INACTIVE"
            print(f"  {str(r[0])[:18]:18} {r[1]:14} cad={r[2]} ult={str(ult)[:16]:16} ({atras_str}){status}")

finally:
    conn.close(); tunnel.stop()
