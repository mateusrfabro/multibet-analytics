"""Snapshot atual + comparativo ONTEM (18/04) vs HOJE (19/04)."""
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
        # 1) COMPARATIVO ONTEM (18/04) vs HOJE (19/04)
        # ============================================================
        print("\n" + "="*90)
        print("1) COMPARATIVO ONTEM (18/04) vs HOJE (19/04) — usuarios reais")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE},
            base AS (
                SELECT u.id, u.username, u.phone, u.email,
                       ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                       EXISTS(SELECT 1 FROM transactions t WHERE t.user_id=u.id
                              AND t.type='DEPOSIT' AND t.status='COMPLETED') AS fez_ftd,
                       EXISTS(SELECT 1 FROM transactions t WHERE t.user_id=u.id
                              AND t.type='WITHDRAW' AND t.status='COMPLETED') AS fez_saque,
                       EXISTS(SELECT 1 FROM bonus_activations b WHERE b.user_id=u.id) AS ativou_bonus
                FROM users u
                WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
                  AND u.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '2 days')::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
            )
            SELECT dia,
                   COUNT(*) AS cadastros,
                   COUNT(*) FILTER (WHERE fez_ftd) AS ftds,
                   COUNT(*) FILTER (WHERE ativou_bonus) AS bonus_ativ,
                   COUNT(*) FILTER (WHERE fez_saque) AS saques,
                   COUNT(*) FILTER (WHERE phone LIKE '+92341374%') AS batch_orig,
                   COUNT(*) FILTER (WHERE phone LIKE '+9234137418%' OR phone LIKE '+9234137420%' OR phone LIKE '+9234137421%') AS batch_vizinho,
                   COUNT(*) FILTER (WHERE email LIKE '%%@wetuns.com' OR email LIKE '%%@whyknapp.com') AS temp_mail_conhec
            FROM base
            GROUP BY 1 ORDER BY 1
        """)
        print(f"  {'dia':12} {'cad':>5} {'ftd':>4} {'cv%':>5} {'bonus':>6} {'saq':>4} {'batch':>6} {'vizinho':>8} {'temp_mail':>9}")
        for r in cur.fetchall():
            cv = 100*r[2]/max(r[1],1)
            print(f"  {str(r[0]):12} {r[1]:>5} {r[2]:>4} {cv:>5.1f} {r[3]:>6} {r[4]:>4} {r[5]:>6} {r[6]:>8} {r[7]:>9}")

        # ============================================================
        # 2) NOVOS PADROES SUSPEITOS HOJE (19/04)
        # ============================================================
        print("\n" + "="*90)
        print("2) PADROES DE FARMING HOJE (19/04) — busca ativa por novos batches")
        print("="*90)

        # 2a. Prefixos contiguos hoje
        print("\n  [2a] Prefixos telefonicos com 3+ cadastros hoje:")
        cur.execute(f"""
            WITH {TEST_CTE}
            SELECT SUBSTRING(u.phone,1,9) AS prefix9, COUNT(*) AS qtd,
                   array_agg(SUBSTRING(u.phone,10,4) ORDER BY u.phone) AS sufixos
            FROM users u
            WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
              AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
              AND u.phone IS NOT NULL
            GROUP BY 1 HAVING COUNT(*) >= 2
            ORDER BY qtd DESC
        """)
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"    {r[0]}  qtd={r[1]}  sufixos={r[2]}")
        else:
            print("    (nenhum prefixo com 2+ cadastros hoje)")

        # 2b. Dominios novos hoje
        print("\n  [2b] Dominios de email novos hoje (zero historico antes de 18/04):")
        cur.execute(f"""
            WITH {TEST_CTE},
            historico AS (
                SELECT DISTINCT SPLIT_PART(email,'@',2) AS dominio
                FROM users u
                WHERE u.role='USER' AND email IS NOT NULL
                  AND u.created_at < '2026-04-18'::date
            ),
            hoje AS (
                SELECT SPLIT_PART(email,'@',2) AS dominio, COUNT(*) AS qtd
                FROM users u
                WHERE u.role='USER' AND email IS NOT NULL
                  AND u.id NOT IN (SELECT id FROM test_ids)
                  AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                      = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                GROUP BY 1
            )
            SELECT h.dominio, h.qtd
            FROM hoje h
            WHERE h.dominio NOT IN (SELECT dominio FROM historico)
            ORDER BY qtd DESC
        """)
        rows = cur.fetchall()
        if rows:
            for r in rows:
                print(f"    NOVO: {r[0]:30} qtd={r[1]}")
        else:
            print("    (nenhum dominio novo hoje)")

        # 2c. Cadastros sub-2min FTD hoje
        print("\n  [2c] Cadastros hoje com FTD em <2min (perfil bot/farmer):")
        cur.execute(f"""
            WITH {TEST_CTE}
            SELECT u.username, u.phone, u.email,
                   (u.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::time AS cad,
                   EXTRACT(EPOCH FROM (
                     (SELECT MIN(t.processed_at) FROM transactions t WHERE t.user_id=u.id
                      AND t.type='DEPOSIT' AND t.status='COMPLETED') - u.created_at))/60 AS min_gap,
                   (SELECT SUM(t.amount) FROM transactions t WHERE t.user_id=u.id
                    AND t.type='DEPOSIT' AND t.status='COMPLETED')::numeric(8,0) AS dep
            FROM users u
            WHERE u.role='USER' AND u.id NOT IN (SELECT id FROM test_ids)
              AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
              AND EXISTS(SELECT 1 FROM transactions t WHERE t.user_id=u.id
                         AND t.type='DEPOSIT' AND t.status='COMPLETED')
            ORDER BY min_gap
        """)
        rows = cur.fetchall()
        sub2 = [r for r in rows if r[4] and float(r[4]) < 2]
        print(f"    {len(sub2)} de {len(rows)} FTDs hoje foram sub-2min")
        for r in sub2[:15]:
            print(f"    {str(r[0])[:18]:18} {str(r[1])[:14]:14} {str(r[2] or '-')[:25]:25} {r[3]} dep={r[5]} ({float(r[4]):.1f}min)")

        # ============================================================
        # 3) BONUS ACTIVATIONS - hoje vs ontem
        # ============================================================
        print("\n" + "="*90)
        print("3) BONUS ACTIVATIONS - hoje vs ontem")
        print("="*90)
        cur.execute("""
            SELECT ((b.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                   COUNT(*) AS ativ,
                   COUNT(DISTINCT b.user_id) AS users,
                   COUNT(*) FILTER (WHERE b.status='COMPLETED') AS completou,
                   COUNT(*) FILTER (WHERE b.status='CANCELLED') AS cancelou,
                   COUNT(*) FILTER (WHERE b.status='ACTIVE') AS ativo
            FROM bonus_activations b
            WHERE b.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo' - INTERVAL '2 days')::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
            GROUP BY 1 ORDER BY 1
        """)
        print(f"  {'dia':12} {'ativ':>5} {'users':>5} {'COMPL':>6} {'CANC':>5} {'ACTIVE':>6}")
        for r in cur.fetchall():
            print(f"  {str(r[0]):12} {r[1]:>5} {r[2]:>5} {r[3]:>6} {r[4]:>5} {r[5]:>6}")

        # ============================================================
        # 4) NOVOS SAQUES SEM ROLLOVER hoje
        # ============================================================
        print("\n" + "="*90)
        print("4) NOVOS SAQUES feitos HOJE (19/04) - cumpriram rollover?")
        print("="*90)
        cur.execute(f"""
            WITH {TEST_CTE}
            SELECT u.username, u.phone,
                   t.amount::numeric(10,0) AS saq,
                   (t.processed_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::timestamp(0) AS quando_brt,
                   (SELECT b.rollover_progress::numeric(10,0) FROM bonus_activations b WHERE b.user_id=u.id ORDER BY b.created_at DESC LIMIT 1) AS roll_prog,
                   (SELECT b.rollover_target::numeric(10,0) FROM bonus_activations b WHERE b.user_id=u.id ORDER BY b.created_at DESC LIMIT 1) AS roll_tgt,
                   (SELECT b.status FROM bonus_activations b WHERE b.user_id=u.id ORDER BY b.created_at DESC LIMIT 1) AS bon_st
            FROM transactions t
            JOIN users u ON u.id=t.user_id
            WHERE t.type='WITHDRAW' AND t.status='COMPLETED'
              AND u.id NOT IN (SELECT id FROM test_ids)
              AND ((t.processed_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                  = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
            ORDER BY t.processed_at DESC
        """)
        rows = cur.fetchall()
        if rows:
            print(f"  {'user':22} {'phone':14} {'saq':>5} {'quando':17} {'progress':>9} {'target':>7} {'bonus':>10}")
            for r in rows:
                roll_pct = (100*float(r[4] or 0)/float(r[5])) if r[5] else None
                pct_str = f"({roll_pct:.0f}%)" if roll_pct is not None else ""
                print(f"  {str(r[0])[:22]:22} {str(r[1])[:14]:14} {r[2]:>5} {str(r[3])[:17]:17} {str(r[4] or '-'):>9} {str(r[5] or '-'):>7} {str(r[6] or '-'):>10} {pct_str}")
            print(f"\n  Total saques hoje: {len(rows)}")
        else:
            print("  (nenhum saque COMPLETED hoje ainda)")

        # ============================================================
        # 5) Status atual das contas do batch ontem (vivos ou mortos?)
        # ============================================================
        print("\n" + "="*90)
        print("5) BATCH ONTEM (+92341374xxx) - vivos ou inativos hoje?")
        print("="*90)
        cur.execute("""
            SELECT u.username, u.phone,
                   (SELECT MAX(t.created_at) AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
                    FROM transactions t WHERE t.user_id=u.id) AS ult_acao,
                   (SELECT COUNT(*) FROM transactions t WHERE t.user_id=u.id
                    AND t.created_at >= NOW() - INTERVAL '6 hours') AS tx_6h,
                   (SELECT SUM(amount) FROM transactions t WHERE t.user_id=u.id
                    AND t.type='WITHDRAW' AND t.status='COMPLETED' AND t.processed_at >= NOW() - INTERVAL '12 hours')::numeric(10,0) AS saq_12h
            FROM users u
            WHERE u.phone LIKE '+92341374%'
            ORDER BY ult_acao DESC NULLS LAST
        """)
        from datetime import timezone
        now_naive = datetime.now(BRT).replace(tzinfo=None)
        for r in cur.fetchall():
            ult = r[2].replace(tzinfo=None) if r[2] and r[2].tzinfo else r[2]
            atras = (now_naive - ult).total_seconds()/3600 if ult else None
            atras_str = f"{atras:.1f}h" if atras is not None else "n/a"
            print(f"  {str(r[0])[:18]:18} {r[1]:14} ult={str(ult)[:16]:16} ({atras_str} atras) tx_6h={r[3]} saq_12h={r[4] or 0}")

finally:
    conn.close(); tunnel.stop()
