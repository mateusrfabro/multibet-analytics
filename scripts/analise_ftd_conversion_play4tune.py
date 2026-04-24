"""
Analise FTD Conversion - Play4Tune (Super Nova Bet / Paquistao)
Demanda Castrin (Head) em 18/04/2026 via audio:
    "Taxa de conversao de FTD subiu de 22% (normal) pra 40% hoje.
     E natural, fraude, ou plataforma esta liso?
     Aonde esses caras tao clicando? Estao depositando E jogando?"

Analises executadas:
    Q1. FTD conversion diaria ultimos 10 dias (BRT) - valida salto
    Q2. Origem (UTMs) dos cadastros convertidos hoje vs baseline
    Q3. Depositaram E jogaram? (qualidade do FTD)
    Q4. Sinais de fraude: IPs, user-agents, valores padronizados, phones
    Q5. Breakdown operacional dos FTDs de hoje (user, valor, tempo reg->FTD)

Filtro test users: UNION (heuristica + logica oficial dev) - mesma
do scripts/report_jogos_play4tune_html.py. Whitelist DP/SQ aplicada.

Banco: supernova_bet | Moeda: PKR | Timezone: UTC no banco / BRT na extracao
"""
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

BRT = ZoneInfo("America/Sao_Paulo")

REAL_USERS_WHITELIST = {
    'maharshani44377634693',
    'muhammadrehan17657797557',
    'rehmanzafar006972281',
    'saimkyani15688267',
}


def get_test_ids(cur):
    cur.execute("""
        SELECT u.id, u.username
        FROM users u
        WHERE
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
                WHERE t.type IN ('ADJUSTMENT_CREDIT', 'ADJUSTMENT_DEBIT')
                   OR (t.type = 'DEPOSIT' AND t.reviewed_by IS NOT NULL)
            )
    """)
    rows = cur.fetchall()
    ids = [r[0] for r in rows if r[1] not in REAL_USERS_WHITELIST]
    return tuple(ids) if ids else ('00000000-0000-0000-0000-000000000000',)


def banner(txt):
    print("\n" + "=" * 78)
    print(txt)
    print("=" * 78)


def run():
    tunnel, conn = get_supernova_bet_connection()
    try:
        with conn.cursor() as cur:
            test_ids = get_test_ids(cur)
            print(f"# Contas de teste excluidas: {len(test_ids)}")
            print(f"# Data de referencia (BRT): {datetime.now(BRT).strftime('%d/%m/%Y %H:%M')}")

            # ============================================================
            # Q1 - FTD conversion diaria (ultimos 10 dias em BRT)
            # ============================================================
            banner("Q1 - FTD CONVERSION DIARIA (ultimos 10 dias, BRT)")
            cur.execute("""
                WITH reg AS (
                    SELECT ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                           COUNT(*) AS cadastros
                    FROM users u
                    WHERE u.role = 'USER'
                      AND u.id NOT IN %s
                      AND u.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo')::date - INTERVAL '10 days'
                    GROUP BY 1
                ),
                ftd AS (
                    SELECT ((first_dep AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia_ftd,
                           user_id,
                           first_dep
                    FROM (
                        SELECT user_id, MIN(processed_at) AS first_dep
                        FROM transactions
                        WHERE type = 'DEPOSIT' AND status = 'COMPLETED'
                          AND user_id NOT IN %s
                        GROUP BY user_id
                    ) fd
                ),
                ftd_por_dia_reg AS (
                    -- Conversao BASEADA NA COHORT DO CADASTRO:
                    -- dos que se cadastraram no dia X, quantos ja fizeram FTD?
                    SELECT ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                           COUNT(DISTINCT ftd.user_id) AS ftds_cohort
                    FROM users u
                    LEFT JOIN ftd ON ftd.user_id = u.id
                    WHERE u.role = 'USER' AND u.id NOT IN %s
                      AND u.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo')::date - INTERVAL '10 days'
                    GROUP BY 1
                ),
                ftd_event_dia AS (
                    -- FTD events que aconteceram no dia X (independente de quando se cadastrou)
                    SELECT dia_ftd AS dia, COUNT(*) AS ftds_evento
                    FROM ftd
                    WHERE dia_ftd >= (NOW() AT TIME ZONE 'America/Sao_Paulo')::date - INTERVAL '10 days'
                    GROUP BY 1
                )
                SELECT r.dia,
                       r.cadastros,
                       COALESCE(c.ftds_cohort, 0) AS ftds_cohort,
                       ROUND(100.0 * COALESCE(c.ftds_cohort, 0) / NULLIF(r.cadastros, 0), 1) AS cv_cohort_pct,
                       COALESCE(e.ftds_evento, 0) AS ftds_evento_dia,
                       ROUND(100.0 * COALESCE(e.ftds_evento, 0) / NULLIF(r.cadastros, 0), 1) AS cv_evento_pct
                FROM reg r
                LEFT JOIN ftd_por_dia_reg c ON c.dia = r.dia
                LEFT JOIN ftd_event_dia e ON e.dia = r.dia
                ORDER BY r.dia
            """, (test_ids, test_ids, test_ids))
            rows = cur.fetchall()
            print(f"{'dia':12} {'cad':>5} {'ftd_coh':>8} {'cv_coh%':>8} {'ftd_evt':>8} {'cv_evt%':>8}")
            for r in rows:
                print(f"{str(r[0]):12} {r[1]:>5} {r[2]:>8} {str(r[3] or '—'):>8} {r[4]:>8} {str(r[5] or '—'):>8}")

            # Baseline (ultimos 7 dias excluindo hoje)
            hoje = rows[-1] if rows else None
            ontem_semana = [r for r in rows[:-1] if r[1] and r[1] > 0]
            if ontem_semana:
                cv_media = sum(float(r[3] or 0) for r in ontem_semana) / len(ontem_semana)
                print(f"\n>> Baseline (9d anteriores): CV cohort media = {cv_media:.1f}%")
                if hoje:
                    delta = float(hoje[3] or 0) - cv_media
                    print(f">> Hoje ({hoje[0]}): {hoje[3]}% | cadastros={hoje[1]} | ftd_cohort={hoje[2]} | delta={delta:+.1f}pp")

            # ============================================================
            # Q2 - Origem (UTM) dos cadastros convertidos hoje
            # ============================================================
            banner("Q2 - ORIGEM (UTM) - cadastros HOJE que fizeram FTD")
            cur.execute("""
                WITH hoje_reg AS (
                    SELECT u.id, u.username, u.created_at
                    FROM users u
                    WHERE u.role = 'USER' AND u.id NOT IN %s
                      AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                          = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                ),
                hoje_ftd AS (
                    SELECT DISTINCT h.id AS user_id
                    FROM hoje_reg h
                    JOIN transactions t ON t.user_id = h.id
                    WHERE t.type='DEPOSIT' AND t.status='COMPLETED'
                ),
                -- Ultimo evento marketing do usuario ANTES do cadastro (ou proprio Register)
                ult_utm AS (
                    SELECT DISTINCT ON (e.user_id)
                           e.user_id,
                           e.utm_source, e.utm_medium, e.utm_campaign,
                           e.utm_content, e.referrer_url, e.page, e.event_type
                    FROM user_marketing_events e
                    WHERE e.user_id IN (SELECT user_id FROM hoje_ftd)
                    ORDER BY e.user_id, e.created_at DESC
                )
                SELECT
                    COALESCE(u.utm_source,'(null)') AS src,
                    COALESCE(u.utm_medium,'(null)') AS med,
                    COALESCE(u.utm_campaign,'(null)') AS camp,
                    COALESCE(u.utm_content,'(null)') AS content,
                    COUNT(*) AS ftds,
                    array_agg(SUBSTRING(COALESCE(u.referrer_url,''),1,60) ORDER BY u.user_id) FILTER (WHERE COALESCE(u.referrer_url,'')<>'') AS ref_sample
                FROM ult_utm u
                GROUP BY 1,2,3,4
                ORDER BY ftds DESC
            """, (test_ids,))
            rows = cur.fetchall()
            if not rows:
                print("(sem dados de UTM para os FTDs de hoje — marketing events pode nao estar vinculado)")
            else:
                print(f"{'source':20} {'medium':15} {'campaign':25} {'content':20} {'ftds':>5}")
                for r in rows:
                    print(f"{str(r[0])[:20]:20} {str(r[1])[:15]:15} {str(r[2])[:25]:25} {str(r[3])[:20]:20} {r[4]:>5}")
                    if r[5]:
                        for ref in list(dict.fromkeys(r[5]))[:3]:
                            print(f"    ref: {ref}")

            # Fallback: olhar user_sessions.utm (JSONB) se marketing_events vazio
            banner("Q2b - UTMs via user_sessions (fallback JSONB)")
            cur.execute("""
                WITH hoje_ftd AS (
                    SELECT DISTINCT u.id
                    FROM users u
                    JOIN transactions t ON t.user_id = u.id
                    WHERE u.role='USER' AND u.id NOT IN %s
                      AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                          = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                      AND t.type='DEPOSIT' AND t.status='COMPLETED'
                )
                SELECT s.utm->>'utm_source' AS src,
                       s.utm->>'utm_medium' AS med,
                       s.utm->>'utm_campaign' AS camp,
                       s.utm->>'utm_content' AS content,
                       s.utm->>'fbclid' AS fbclid,
                       s.utm->>'referrer' AS ref,
                       COUNT(DISTINCT s.user_id) AS users
                FROM user_sessions s
                WHERE s.user_id IN (SELECT id FROM hoje_ftd)
                GROUP BY 1,2,3,4,5,6
                ORDER BY users DESC
            """, (test_ids,))
            rows = cur.fetchall()
            if not rows:
                print("(sem dados em user_sessions.utm)")
            else:
                print(f"{'source':18} {'medium':12} {'campaign':25} {'content':15} {'fbclid':>8} {'users':>5}")
                for r in rows:
                    print(f"{str(r[0])[:18]:18} {str(r[1])[:12]:12} {str(r[2])[:25]:25} {str(r[3])[:15]:15} {('Y' if r[4] else '—'):>8} {r[6]:>5}")
                    if r[5]:
                        print(f"    ref: {str(r[5])[:70]}")

            # ============================================================
            # Q3 - Depositaram E jogaram? (qualidade)
            # ============================================================
            banner("Q3 - QUALIDADE: depositaram E jogaram? (cadastros de hoje com FTD)")
            cur.execute("""
                WITH hoje_ftd AS (
                    SELECT DISTINCT u.id AS user_id, u.username, u.created_at
                    FROM users u
                    JOIN transactions t ON t.user_id = u.id
                    WHERE u.role='USER' AND u.id NOT IN %s
                      AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                          = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                      AND t.type='DEPOSIT' AND t.status='COMPLETED'
                )
                SELECT COUNT(DISTINCT h.user_id) AS total_ftd,
                       COUNT(DISTINCT b.user_id) AS com_aposta,
                       COALESCE(SUM(b.total_bet_amount),0) AS turnover_pkr,
                       COALESCE(SUM(b.played_rounds),0) AS giros,
                       COUNT(DISTINCT CASE WHEN saque.user_id IS NOT NULL THEN h.user_id END) AS fez_saque
                FROM hoje_ftd h
                LEFT JOIN casino_user_game_metrics b ON b.user_id = h.user_id
                LEFT JOIN (
                    SELECT DISTINCT user_id FROM transactions
                    WHERE type='WITHDRAW' AND status='COMPLETED'
                ) saque ON saque.user_id = h.user_id
            """, (test_ids,))
            r = cur.fetchone()
            print(f"Total FTD (cohort hoje) : {r[0]}")
            print(f"  .. com aposta jogada  : {r[1]}  ({100*r[1]/r[0]:.0f}% se r[0]>0)" if r[0] else "")
            print(f"  .. turnover total     : Rs {float(r[2]):,.2f}")
            print(f"  .. rounds totais      : {r[3]:,}")
            print(f"  .. ja fez saque       : {r[4]}")

            # Breakdown individual
            cur.execute("""
                WITH hoje_ftd AS (
                    SELECT u.id AS user_id, u.username, u.email, u.phone, u.created_at,
                           MIN(t.processed_at) AS ftd_at,
                           SUM(t.amount) FILTER (WHERE t.type='DEPOSIT' AND t.status='COMPLETED') AS dep_total
                    FROM users u
                    JOIN transactions t ON t.user_id = u.id
                    WHERE u.role='USER' AND u.id NOT IN %s
                      AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                          = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                      AND t.type='DEPOSIT' AND t.status='COMPLETED'
                    GROUP BY 1,2,3,4,5
                )
                SELECT h.username,
                       SUBSTRING(h.email,1,28) AS email,
                       SUBSTRING(h.phone,1,14) AS phone,
                       EXTRACT(EPOCH FROM (h.ftd_at - h.created_at))/60 AS min_reg_to_ftd,
                       h.dep_total::numeric(12,2) AS dep_pkr,
                       COALESCE(SUM(b.total_bet_amount),0)::numeric(12,2) AS turnover,
                       COALESCE(SUM(b.played_rounds),0) AS giros
                FROM hoje_ftd h
                LEFT JOIN casino_user_game_metrics b ON b.user_id = h.user_id
                GROUP BY h.username, h.email, h.phone, h.created_at, h.ftd_at, h.dep_total
                ORDER BY h.ftd_at
            """, (test_ids,))
            rows = cur.fetchall()
            print(f"\nBreakdown individual ({len(rows)} FTDs hoje):")
            print(f"{'username':25} {'email':30} {'phone':16} {'reg->ftd(min)':>14} {'dep':>10} {'turnover':>10} {'giros':>6}")
            for r in rows:
                print(f"{str(r[0])[:25]:25} {str(r[1])[:30]:30} {str(r[2])[:16]:16} {float(r[3] or 0):>14.1f} {float(r[4] or 0):>10.2f} {float(r[5] or 0):>10.2f} {r[6]:>6}")

            # ============================================================
            # Q4 - Sinais de fraude
            # ============================================================
            banner("Q4 - SINAIS DE FRAUDE - FTDs de hoje")

            # 4a. Concentracao de IP
            cur.execute("""
                SELECT t.ip_address, COUNT(DISTINCT t.user_id) AS users_distintos,
                       COUNT(*) AS depositos
                FROM transactions t
                JOIN users u ON u.id = t.user_id
                WHERE u.role='USER' AND u.id NOT IN %s
                  AND t.type='DEPOSIT' AND t.status='COMPLETED'
                  AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                      = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                  AND t.ip_address IS NOT NULL
                GROUP BY 1
                HAVING COUNT(DISTINCT t.user_id) >= 1
                ORDER BY users_distintos DESC, depositos DESC
                LIMIT 20
            """, (test_ids,))
            rows = cur.fetchall()
            print("\n[4a] IPs dos FTDs de hoje (ordenado por users distintos):")
            print(f"{'ip':30} {'users':>6} {'deps':>6}")
            for r in rows:
                flag = " <- SUSPEITO" if r[1] >= 3 else ""
                print(f"{str(r[0])[:30]:30} {r[1]:>6} {r[2]:>6}{flag}")

            # 4b. User-agent concentrado
            cur.execute("""
                SELECT SUBSTRING(t.user_agent,1,80) AS ua,
                       COUNT(DISTINCT t.user_id) AS users,
                       COUNT(*) AS deps
                FROM transactions t
                JOIN users u ON u.id = t.user_id
                WHERE u.role='USER' AND u.id NOT IN %s
                  AND t.type='DEPOSIT' AND t.status='COMPLETED'
                  AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                      = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                  AND t.user_agent IS NOT NULL
                GROUP BY 1
                ORDER BY users DESC
                LIMIT 10
            """, (test_ids,))
            rows = cur.fetchall()
            print("\n[4b] User-agents dos FTDs hoje (top 10):")
            for r in rows:
                flag = " <- CONCENTRADO" if r[1] >= 3 else ""
                print(f"  users={r[1]:>3} deps={r[2]:>3}  ua={r[0]}{flag}")

            # 4c. Distribuicao de valores de deposito
            cur.execute("""
                SELECT t.amount::numeric(10,2) AS valor_pkr,
                       COUNT(DISTINCT t.user_id) AS users,
                       COUNT(*) AS qtd
                FROM transactions t
                JOIN users u ON u.id = t.user_id
                WHERE u.role='USER' AND u.id NOT IN %s
                  AND t.type='DEPOSIT' AND t.status='COMPLETED'
                  AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                      = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                GROUP BY 1
                ORDER BY qtd DESC
                LIMIT 10
            """, (test_ids,))
            rows = cur.fetchall()
            print("\n[4c] Distribuicao valores FTD hoje:")
            print(f"{'valor_pkr':>12} {'users':>6} {'qtd':>6}")
            for r in rows:
                print(f"{float(r[0]):>12.2f} {r[1]:>6} {r[2]:>6}")

            # 4d. Phones com prefixo similar (mesma operadora / batch)
            cur.execute("""
                SELECT SUBSTRING(u.phone,1,6) AS prefixo,
                       COUNT(*) AS qtd_users
                FROM users u
                WHERE u.role='USER' AND u.id NOT IN %s
                  AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                      = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                  AND u.phone IS NOT NULL
                GROUP BY 1
                ORDER BY qtd_users DESC
                LIMIT 10
            """, (test_ids,))
            rows = cur.fetchall()
            print("\n[4d] Prefixo telefonico (primeiros 6 digitos) dos cadastros hoje:")
            for r in rows:
                print(f"  {r[0]}  qtd={r[1]}")

            # 4e. Tempo reg -> FTD (muito rapido sinaliza bot/prep)
            cur.execute("""
                WITH tempos AS (
                    SELECT u.id,
                           EXTRACT(EPOCH FROM (MIN(t.processed_at) - u.created_at))/60 AS min_gap
                    FROM users u
                    JOIN transactions t ON t.user_id = u.id
                    WHERE u.role='USER' AND u.id NOT IN %s
                      AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date
                          = (NOW() AT TIME ZONE 'America/Sao_Paulo')::date
                      AND t.type='DEPOSIT' AND t.status='COMPLETED'
                    GROUP BY u.id, u.created_at
                )
                SELECT
                    SUM(CASE WHEN min_gap < 2 THEN 1 ELSE 0 END) AS sub_2min,
                    SUM(CASE WHEN min_gap BETWEEN 2 AND 10 THEN 1 ELSE 0 END) AS min_2_10,
                    SUM(CASE WHEN min_gap BETWEEN 10 AND 60 THEN 1 ELSE 0 END) AS min_10_60,
                    SUM(CASE WHEN min_gap > 60 THEN 1 ELSE 0 END) AS gt_60min,
                    ROUND(AVG(min_gap)::numeric,1) AS media,
                    ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY min_gap)::numeric,1) AS mediana
                FROM tempos
            """, (test_ids,))
            r = cur.fetchone()
            print(f"\n[4e] Tempo reg->FTD hoje: <2min={r[0]} | 2-10min={r[1]} | 10-60min={r[2]} | >60min={r[3]}")
            print(f"     media={r[4]}min | mediana={r[5]}min")

            # ============================================================
            # Q5 - Comparacao com ontem (mesma estrutura)
            # ============================================================
            banner("Q5 - COMPARATIVO HOJE vs ONTEM - UTMs dos FTDs")
            cur.execute("""
                WITH base AS (
                    SELECT u.id,
                           ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia
                    FROM users u
                    WHERE u.role='USER' AND u.id NOT IN %s
                      AND u.created_at >= (NOW() AT TIME ZONE 'America/Sao_Paulo')::date - INTERVAL '2 days'
                      AND EXISTS (SELECT 1 FROM transactions t
                                  WHERE t.user_id=u.id AND t.type='DEPOSIT' AND t.status='COMPLETED')
                ),
                utm_por_user AS (
                    SELECT DISTINCT ON (b.id)
                           b.id, b.dia,
                           COALESCE(s.utm->>'utm_source', e.utm_source, '(null)') AS src
                    FROM base b
                    LEFT JOIN user_sessions s ON s.user_id = b.id
                    LEFT JOIN user_marketing_events e ON e.user_id = b.id
                    ORDER BY b.id, s.created_at DESC NULLS LAST
                )
                SELECT dia, src, COUNT(*) AS users
                FROM utm_por_user
                GROUP BY 1,2
                ORDER BY dia DESC, users DESC
            """, (test_ids,))
            rows = cur.fetchall()
            print(f"{'dia':12} {'source':25} {'users':>5}")
            for r in rows:
                print(f"{str(r[0]):12} {str(r[1])[:25]:25} {r[2]:>5}")

    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
