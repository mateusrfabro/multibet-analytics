"""
Diagnostico Funil de Aquisicao - Play4Tune (Super Nova Bet / Paquistao)

Contexto (23/04/2026):
    - Ad Meta roda com muito clique, mas cliques nao estao convertendo em registros
    - Registros que viram FTD tem CV boa
    - 07/04 foi o melhor dia (gateway aprovou muito, registros bons, FTDs bons)
    - Mudou o modelo de gateway recentemente: 1 -> 2 gateways (quando?)
    - Hipotese principal: gateway novo rejeitando, ou split pior que unico

Objetivos (7 secoes):
    A. Inventario ATUAL: gateways, payment_methods, quando cada um entrou em operacao
    B. Funil diario 01/04 -> 22/04 (BRT): reg, tentativas, aprovados, falhas, FTDs
    C. Aprovacao por gateway/payment_method por dia
    D. Motivos de falha (rejection_reason/error_reason/failure_source)
    E. Canal de aquisicao dos registros (UTM source) por dia
    F. 07/04 deep dive: o que foi diferente
    G. Split horario (PKT) para entender janela de pico

Banco: supernova_bet | Moeda: PKR | Timezone: UTC no banco / BRT na extracao
Filtro test users: UNION heuristica + logica oficial dev (ADJUSTMENT/reviewed_by)
"""
import os
import sys
import csv
from datetime import datetime, date
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

BRT = ZoneInfo("America/Sao_Paulo")

# Janela de analise
JANELA_INI = date(2026, 4, 1)
JANELA_FIM = date(2026, 4, 22)  # D-1

# Whitelist de jogadores reais (validada 16/04 — ver project_dp_sq_whitelist_p4t.md)
REAL_USERS_WHITELIST = {
    'maharshani44377634693',
    'muhammadrehan17657797557',
    'rehmanzafar006972281',
    'saimkyani15688267',
}

# Output
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
os.makedirs(OUT_DIR, exist_ok=True)
OUT_TXT = os.path.join(OUT_DIR, "diag_funil_play4tune_2026-04-23.txt")
OUT_CSV_FUNIL = os.path.join(OUT_DIR, "diag_funil_play4tune_funil_diario.csv")
OUT_CSV_GW = os.path.join(OUT_DIR, "diag_funil_play4tune_gateway_aprovacao.csv")


def get_test_ids(cur):
    """Retorna tuple de UUIDs de usuarios teste (heuristica + oficial dev)."""
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


class Tee:
    """Espelha stdout em um arquivo."""
    def __init__(self, *streams):
        self.streams = streams
    def write(self, msg):
        for s in self.streams:
            s.write(msg)
    def flush(self):
        for s in self.streams:
            s.flush()


def banner(txt):
    print("\n" + "=" * 92)
    print(txt)
    print("=" * 92)


def run():
    txt_fh = open(OUT_TXT, "w", encoding="utf-8")
    original_stdout = sys.stdout
    sys.stdout = Tee(original_stdout, txt_fh)

    try:
        tunnel, conn = get_supernova_bet_connection()
        try:
            with conn.cursor() as cur:
                test_ids = get_test_ids(cur)
                print(f"# Diagnostico Funil Play4Tune")
                print(f"# Executado em: {datetime.now(BRT).strftime('%d/%m/%Y %H:%M BRT')}")
                print(f"# Janela: {JANELA_INI} -> {JANELA_FIM} (BRT)")
                print(f"# Test users excluidos: {len(test_ids)}")

                # ============================================================
                # A. INVENTARIO: gateways e payment_methods
                # ============================================================
                banner("A. INVENTARIO ATUAL - Gateways e Payment Methods")

                # A.1 Schema da tabela gateways (pode ter mudado)
                print("\n[A.1] Colunas da tabela `gateways`:")
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='gateways'
                    ORDER BY ordinal_position
                """)
                for r in cur.fetchall():
                    print(f"  {r[0]:30} {r[1]}")

                # A.2 Listagem
                print("\n[A.2] Gateways cadastrados:")
                cur.execute("SELECT * FROM gateways ORDER BY created_at")
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                print(f"  ({len(rows)} gateway(s) cadastrados)")
                for r in rows:
                    for c, v in zip(cols, r):
                        print(f"    {c:20}: {v}")
                    print("    " + "-" * 40)

                # A.3 Payment methods
                print("\n[A.3] Colunas da tabela `payment_methods`:")
                cur.execute("""
                    SELECT column_name, data_type
                    FROM information_schema.columns
                    WHERE table_schema='public' AND table_name='payment_methods'
                    ORDER BY ordinal_position
                """)
                pm_cols = [r[0] for r in cur.fetchall()]
                for c in pm_cols:
                    print(f"  {c}")

                print("\n[A.4] Payment methods cadastrados:")
                cur.execute("SELECT * FROM payment_methods ORDER BY created_at")
                cols = [d[0] for d in cur.description]
                for r in cur.fetchall():
                    for c, v in zip(cols, r):
                        print(f"    {c:20}: {v}")
                    print("    " + "-" * 40)

                # A.5 Primeira transacao por gateway (determina quando comecou a operar)
                print("\n[A.5] Primeira transacao por payment_method (desde quando opera):")
                cur.execute("""
                    SELECT pm.name AS pm_name,
                           pm.id AS pm_id,
                           MIN(t.created_at) AS primeira_tx,
                           MAX(t.created_at) AS ultima_tx,
                           COUNT(*) AS qtd_tx_total
                    FROM payment_methods pm
                    LEFT JOIN transactions t ON t.payment_method_id = pm.id
                         AND t.type='DEPOSIT'
                    GROUP BY pm.name, pm.id
                    ORDER BY primeira_tx NULLS LAST
                """)
                print(f"  {'payment_method':30} {'primeira_tx':22} {'ultima_tx':22} {'qtd_tx':>8}")
                for r in cur.fetchall():
                    pt = r[2].strftime('%Y-%m-%d %H:%M') if r[2] else '-'
                    ut = r[3].strftime('%Y-%m-%d %H:%M') if r[3] else '-'
                    print(f"  {str(r[0])[:30]:30} {pt:22} {ut:22} {r[4]:>8}")

                # ============================================================
                # B. FUNIL DIARIO 01/04 -> 22/04
                # ============================================================
                banner("B. FUNIL DIARIO 01/04 -> 22/04 (BRT)")

                cur.execute("""
                    WITH reg AS (
                        SELECT ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                               COUNT(*) AS cadastros
                        FROM users u
                        WHERE u.role='USER' AND u.id NOT IN %s
                          AND u.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                          AND u.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                        GROUP BY 1
                    ),
                    dep AS (
                        SELECT ((t.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                               COUNT(*) AS tentativas,
                               COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprovados,
                               COUNT(*) FILTER (WHERE t.status='FAILED')    AS falhas,
                               COUNT(*) FILTER (WHERE t.status='PENDING')   AS pendentes,
                               COUNT(*) FILTER (WHERE t.status='EXPIRED')   AS expirados,
                               COUNT(*) FILTER (WHERE t.status='CANCELLED') AS cancelados,
                               COALESCE(SUM(t.amount) FILTER (WHERE t.status='COMPLETED'), 0) AS valor_aprovado_pkr
                        FROM transactions t
                        WHERE t.type='DEPOSIT'
                          AND t.user_id NOT IN %s
                          AND t.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                          AND t.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                        GROUP BY 1
                    ),
                    ftd_map AS (
                        SELECT user_id, MIN(processed_at) AS first_dep
                        FROM transactions
                        WHERE type='DEPOSIT' AND status='COMPLETED'
                          AND user_id NOT IN %s
                        GROUP BY user_id
                    ),
                    ftd AS (
                        SELECT ((first_dep AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                               COUNT(*) AS ftds
                        FROM ftd_map
                        WHERE first_dep >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                          AND first_dep <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                        GROUP BY 1
                    ),
                    dias AS (
                        SELECT generate_series(%s::date, %s::date, INTERVAL '1 day')::date AS dia
                    )
                    SELECT d.dia,
                           COALESCE(r.cadastros,0) AS cadastros,
                           COALESCE(dep.tentativas,0) AS tentativas,
                           COALESCE(dep.aprovados,0) AS aprovados,
                           COALESCE(dep.falhas,0) AS falhas,
                           COALESCE(dep.pendentes,0) AS pendentes,
                           COALESCE(dep.expirados,0) AS expirados,
                           COALESCE(dep.cancelados,0) AS cancelados,
                           ROUND(100.0 * COALESCE(dep.aprovados,0) / NULLIF(dep.tentativas,0), 1) AS tx_aprov_pct,
                           COALESCE(f.ftds,0) AS ftds,
                           ROUND(100.0 * COALESCE(f.ftds,0) / NULLIF(r.cadastros,0), 1) AS cv_reg_ftd_pct,
                           ROUND(COALESCE(dep.valor_aprovado_pkr,0)::numeric, 2) AS valor_aprov_pkr
                    FROM dias d
                    LEFT JOIN reg r ON r.dia = d.dia
                    LEFT JOIN dep   ON dep.dia = d.dia
                    LEFT JOIN ftd f ON f.dia = d.dia
                    ORDER BY d.dia
                """, (test_ids, JANELA_INI, JANELA_FIM,
                      test_ids, JANELA_INI, JANELA_FIM,
                      test_ids,
                      JANELA_INI, JANELA_FIM,
                      JANELA_INI, JANELA_FIM))

                rows = cur.fetchall()
                print(f"\n{'dia':12} {'cad':>5} {'tent':>5} {'aprv':>5} {'fail':>5} {'pend':>5} {'expir':>5} {'canc':>5} {'aprv%':>6} {'ftd':>4} {'cv%':>5} {'val_pkr':>12}")
                for r in rows:
                    print(f"{str(r[0]):12} {r[1]:>5} {r[2]:>5} {r[3]:>5} {r[4]:>5} {r[5]:>5} {r[6]:>5} {r[7]:>5} "
                          f"{str(r[8] or '—'):>6} {r[9]:>4} {str(r[10] or '—'):>5} {float(r[11] or 0):>12,.0f}")

                # Export CSV funil
                with open(OUT_CSV_FUNIL, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["dia","cadastros","tentativas_dep","aprovados","falhas","pendentes","expirados","cancelados",
                                "tx_aprovacao_pct","ftds","cv_reg_ftd_pct","valor_aprov_pkr"])
                    for r in rows:
                        w.writerow(r)
                print(f"\n  -> CSV: {OUT_CSV_FUNIL}")

                # ============================================================
                # C. APROVACAO POR GATEWAY/PAYMENT_METHOD POR DIA
                # ============================================================
                banner("C. APROVACAO POR PAYMENT_METHOD x DIA (01/04 -> 22/04, BRT)")

                cur.execute("""
                    SELECT ((t.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                           COALESCE(pm.name, '(sem pm)') AS pm_name,
                           COUNT(*) AS tentativas,
                           COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprovados,
                           COUNT(*) FILTER (WHERE t.status='FAILED')    AS falhas,
                           COUNT(*) FILTER (WHERE t.status='PENDING')   AS pendentes,
                           COUNT(*) FILTER (WHERE t.status='EXPIRED')   AS expirados,
                           ROUND(100.0 * COUNT(*) FILTER (WHERE t.status='COMPLETED') / NULLIF(COUNT(*),0), 1) AS aprov_pct
                    FROM transactions t
                    LEFT JOIN payment_methods pm ON pm.id = t.payment_method_id
                    WHERE t.type='DEPOSIT'
                      AND t.user_id NOT IN %s
                      AND t.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                      AND t.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                """, (test_ids, JANELA_INI, JANELA_FIM))
                gw_rows = cur.fetchall()
                print(f"\n{'dia':12} {'pm':28} {'tent':>5} {'aprv':>5} {'fail':>5} {'pend':>5} {'expi':>5} {'aprov%':>7}")
                for r in gw_rows:
                    print(f"{str(r[0]):12} {str(r[1])[:28]:28} {r[2]:>5} {r[3]:>5} {r[4]:>5} {r[5]:>5} {r[6]:>5} {str(r[7] or '—'):>7}")

                with open(OUT_CSV_GW, "w", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow(["dia","payment_method","tentativas","aprovados","falhas","pendentes","expirados","aprov_pct"])
                    for r in gw_rows:
                        w.writerow(r)
                print(f"\n  -> CSV: {OUT_CSV_GW}")

                # C.2 Resumo na janela: aprovacao total por payment_method
                print("\n[C.2] Resumo aprovacao por PM na janela inteira:")
                cur.execute("""
                    SELECT COALESCE(pm.name,'(sem pm)') AS pm,
                           COUNT(*) AS tentativas,
                           COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprovados,
                           COUNT(*) FILTER (WHERE t.status='FAILED')    AS falhas,
                           ROUND(100.0 * COUNT(*) FILTER (WHERE t.status='COMPLETED') / NULLIF(COUNT(*),0), 1) AS aprov_pct,
                           MIN(t.created_at) AS primeira,
                           MAX(t.created_at) AS ultima
                    FROM transactions t
                    LEFT JOIN payment_methods pm ON pm.id = t.payment_method_id
                    WHERE t.type='DEPOSIT'
                      AND t.user_id NOT IN %s
                      AND t.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                      AND t.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                    GROUP BY 1
                    ORDER BY tentativas DESC
                """, (test_ids, JANELA_INI, JANELA_FIM))
                print(f"  {'payment_method':28} {'tent':>6} {'aprv':>6} {'fail':>6} {'aprov%':>7} {'primeira':22} {'ultima':22}")
                for r in cur.fetchall():
                    pr = r[5].strftime('%Y-%m-%d %H:%M') if r[5] else '-'
                    ul = r[6].strftime('%Y-%m-%d %H:%M') if r[6] else '-'
                    print(f"  {str(r[0])[:28]:28} {r[1]:>6} {r[2]:>6} {r[3]:>6} {str(r[4] or '—'):>7} {pr:22} {ul:22}")

                # ============================================================
                # D. MOTIVOS DE FALHA
                # ============================================================
                banner("D. TOP MOTIVOS DE FALHA NA JANELA (rejection_reason / error_reason / failure_source)")

                cur.execute("""
                    SELECT COALESCE(t.failure_source, '(null)') AS fonte,
                           COALESCE(t.rejection_reason, '(null)') AS rej,
                           COALESCE(t.error_reason, '(null)') AS err,
                           COUNT(*) AS qtd
                    FROM transactions t
                    WHERE t.type='DEPOSIT' AND t.status='FAILED'
                      AND t.user_id NOT IN %s
                      AND t.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                      AND t.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                    GROUP BY 1,2,3
                    ORDER BY qtd DESC
                    LIMIT 30
                """, (test_ids, JANELA_INI, JANELA_FIM))
                rows = cur.fetchall()
                print(f"\n  {'failure_source':20} {'rejection_reason':35} {'error_reason':35} {'qtd':>6}")
                for r in rows:
                    print(f"  {str(r[0])[:20]:20} {str(r[1])[:35]:35} {str(r[2])[:35]:35} {r[3]:>6}")

                # ============================================================
                # E. CANAL DE AQUISICAO (UTM) DOS REGISTROS POR DIA
                # ============================================================
                banner("E. CANAL DE AQUISICAO - UTM source dos cadastros por dia")

                # Tabelas disponiveis: user_marketing_events (preferencial) e user_sessions.utm (fallback JSONB)
                # Para cada cadastro, pegar a PRIMEIRA UTM_SOURCE vinculada.
                cur.execute("""
                    WITH cad AS (
                        SELECT u.id AS user_id,
                               ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia
                        FROM users u
                        WHERE u.role='USER' AND u.id NOT IN %s
                          AND u.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                          AND u.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                    ),
                    utm_evt AS (
                        SELECT DISTINCT ON (user_id)
                               user_id, utm_source
                        FROM user_marketing_events
                        WHERE user_id IN (SELECT user_id FROM cad)
                        ORDER BY user_id, created_at ASC
                    ),
                    utm_sess AS (
                        SELECT DISTINCT ON (user_id)
                               user_id, utm->>'utm_source' AS utm_source
                        FROM user_sessions
                        WHERE user_id IN (SELECT user_id FROM cad)
                        ORDER BY user_id, created_at ASC
                    ),
                    merged AS (
                        SELECT c.user_id, c.dia,
                               COALESCE(e.utm_source, s.utm_source, '(null)') AS src
                        FROM cad c
                        LEFT JOIN utm_evt  e ON e.user_id = c.user_id
                        LEFT JOIN utm_sess s ON s.user_id = c.user_id
                    )
                    SELECT dia, src, COUNT(*) AS cadastros
                    FROM merged
                    GROUP BY 1,2
                    ORDER BY 1, cadastros DESC
                """, (test_ids, JANELA_INI, JANELA_FIM))
                rows = cur.fetchall()
                print(f"\n{'dia':12} {'utm_source':25} {'cadastros':>10}")
                for r in rows:
                    print(f"{str(r[0]):12} {str(r[1])[:25]:25} {r[2]:>10}")

                # E.2 Total por source na janela
                cur.execute("""
                    WITH cad AS (
                        SELECT u.id AS user_id
                        FROM users u
                        WHERE u.role='USER' AND u.id NOT IN %s
                          AND u.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                          AND u.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                    ),
                    utm_evt AS (
                        SELECT DISTINCT ON (user_id) user_id, utm_source
                        FROM user_marketing_events
                        WHERE user_id IN (SELECT user_id FROM cad)
                        ORDER BY user_id, created_at ASC
                    ),
                    utm_sess AS (
                        SELECT DISTINCT ON (user_id) user_id, utm->>'utm_source' AS utm_source
                        FROM user_sessions
                        WHERE user_id IN (SELECT user_id FROM cad)
                        ORDER BY user_id, created_at ASC
                    ),
                    merged AS (
                        SELECT c.user_id, COALESCE(e.utm_source, s.utm_source, '(null)') AS src
                        FROM cad c
                        LEFT JOIN utm_evt  e ON e.user_id = c.user_id
                        LEFT JOIN utm_sess s ON s.user_id = c.user_id
                    )
                    SELECT src, COUNT(*) AS cadastros,
                           ROUND(100.0*COUNT(*)/SUM(COUNT(*)) OVER (), 1) AS pct
                    FROM merged
                    GROUP BY 1
                    ORDER BY cadastros DESC
                """, (test_ids, JANELA_INI, JANELA_FIM))
                print(f"\n[E.2] Total por utm_source na janela:")
                print(f"  {'utm_source':25} {'cadastros':>10} {'%':>6}")
                for r in cur.fetchall():
                    print(f"  {str(r[0])[:25]:25} {r[1]:>10} {float(r[2] or 0):>6.1f}")

                # E.3 Quantos tem fbclid (click direto de Meta)
                cur.execute("""
                    WITH cad AS (
                        SELECT u.id AS user_id
                        FROM users u
                        WHERE u.role='USER' AND u.id NOT IN %s
                          AND u.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                          AND u.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                    )
                    SELECT
                        COUNT(DISTINCT c.user_id) AS total_cad,
                        COUNT(DISTINCT CASE WHEN s.utm->>'fbclid' IS NOT NULL THEN c.user_id END) AS com_fbclid,
                        COUNT(DISTINCT CASE WHEN s.utm->>'utm_source' ILIKE '%%facebook%%' OR s.utm->>'utm_source' ILIKE '%%meta%%' THEN c.user_id END) AS utm_fb_meta
                    FROM cad c
                    LEFT JOIN user_sessions s ON s.user_id = c.user_id
                """, (test_ids, JANELA_INI, JANELA_FIM))
                r = cur.fetchone()
                print(f"\n[E.3] Tracking Meta (para todos os cadastros na janela):")
                print(f"  total cadastros   : {r[0]}")
                print(f"  com fbclid        : {r[1]} ({100*r[1]/r[0]:.1f}% do total)" if r[0] else "  (sem cadastros)")
                print(f"  utm=facebook/meta : {r[2]}")

                # ============================================================
                # F. 07/04 DEEP DIVE
                # ============================================================
                banner("F. 07/04/2026 DEEP DIVE - O que foi diferente?")

                cur.execute("""
                    SELECT COALESCE(pm.name,'(sem pm)') AS pm,
                           COUNT(*) AS tentativas,
                           COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprovados,
                           COUNT(*) FILTER (WHERE t.status='FAILED')    AS falhas,
                           ROUND(100.0*COUNT(*) FILTER (WHERE t.status='COMPLETED')/NULLIF(COUNT(*),0),1) AS aprov_pct,
                           ROUND(SUM(t.amount) FILTER (WHERE t.status='COMPLETED')::numeric, 2) AS valor_aprov
                    FROM transactions t
                    LEFT JOIN payment_methods pm ON pm.id = t.payment_method_id
                    WHERE t.type='DEPOSIT'
                      AND t.user_id NOT IN %s
                      AND ((t.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date = DATE '2026-04-07'
                    GROUP BY 1
                    ORDER BY tentativas DESC
                """, (test_ids,))
                print(f"\n[F.1] 07/04 - tentativas por payment_method:")
                print(f"  {'payment_method':28} {'tent':>5} {'aprv':>5} {'fail':>5} {'aprov%':>7} {'valor_pkr':>12}")
                for r in cur.fetchall():
                    print(f"  {str(r[0])[:28]:28} {r[1]:>5} {r[2]:>5} {r[3]:>5} {str(r[4] or '—'):>7} {float(r[5] or 0):>12,.0f}")

                # F.2 UTM source dos cadastros de 07/04
                cur.execute("""
                    WITH cad AS (
                        SELECT u.id
                        FROM users u
                        WHERE u.role='USER' AND u.id NOT IN %s
                          AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date = DATE '2026-04-07'
                    ),
                    utm AS (
                        SELECT DISTINCT ON (user_id) user_id, utm->>'utm_source' AS src, utm->>'utm_campaign' AS camp
                        FROM user_sessions
                        WHERE user_id IN (SELECT id FROM cad)
                        ORDER BY user_id, created_at ASC
                    )
                    SELECT COALESCE(u.src,'(null)') AS src,
                           COALESCE(u.camp,'(null)') AS camp,
                           COUNT(*) AS cadastros
                    FROM cad c LEFT JOIN utm u ON u.user_id = c.id
                    GROUP BY 1,2
                    ORDER BY cadastros DESC
                """, (test_ids,))
                print(f"\n[F.2] 07/04 - origem dos cadastros:")
                print(f"  {'utm_source':22} {'utm_campaign':35} {'cadastros':>10}")
                for r in cur.fetchall():
                    print(f"  {str(r[0])[:22]:22} {str(r[1])[:35]:35} {r[2]:>10}")

                # F.3 Comparativo: 07/04 vs media da janela
                cur.execute("""
                    WITH funil_dia AS (
                        SELECT ((t.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date AS dia,
                               COUNT(*) AS tent,
                               COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprv,
                               COUNT(*) FILTER (WHERE t.status='FAILED') AS fail
                        FROM transactions t
                        WHERE t.type='DEPOSIT' AND t.user_id NOT IN %s
                          AND t.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                          AND t.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                        GROUP BY 1
                    )
                    SELECT
                        ROUND(AVG(tent)::numeric, 1) AS tent_media,
                        ROUND(AVG(aprv)::numeric, 1) AS aprv_media,
                        ROUND(AVG(fail)::numeric, 1) AS fail_media,
                        ROUND(AVG(100.0*aprv/NULLIF(tent,0))::numeric, 1) AS aprov_pct_media
                    FROM funil_dia
                    WHERE dia != DATE '2026-04-07'
                """, (test_ids, JANELA_INI, JANELA_FIM))
                r = cur.fetchone()
                print(f"\n[F.3] Media diaria na janela (excluindo 07/04):")
                print(f"  tent_media={r[0]} | aprv_media={r[1]} | fail_media={r[2]} | aprov%_media={r[3]}")

                # ============================================================
                # G. SPLIT HORARIO PKT (pico de deposito)
                # ============================================================
                banner("G. SPLIT HORARIO (PKT) - aprovacao por hora na janela")

                cur.execute("""
                    SELECT EXTRACT(HOUR FROM (t.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'Asia/Karachi')::int AS h_pkt,
                           COUNT(*) AS tent,
                           COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprv,
                           ROUND(100.0*COUNT(*) FILTER (WHERE t.status='COMPLETED')/NULLIF(COUNT(*),0),1) AS aprov_pct
                    FROM transactions t
                    WHERE t.type='DEPOSIT' AND t.user_id NOT IN %s
                      AND t.created_at >= %s::timestamp AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                      AND t.created_at <  (%s::timestamp + INTERVAL '1 day') AT TIME ZONE 'America/Sao_Paulo' AT TIME ZONE 'UTC'
                    GROUP BY 1
                    ORDER BY 1
                """, (test_ids, JANELA_INI, JANELA_FIM))
                rows = cur.fetchall()
                print(f"\n{'h_PKT':>6} {'tent':>6} {'aprv':>6} {'aprov%':>7}")
                for r in rows:
                    bar = "#" * min(int(r[1]/5), 40)
                    print(f"{r[0]:>6} {r[1]:>6} {r[2]:>6} {str(r[3] or '—'):>7}  {bar}")

                print(f"\n\nOK. Arquivos:")
                print(f"  TXT: {OUT_TXT}")
                print(f"  CSV funil:    {OUT_CSV_FUNIL}")
                print(f"  CSV gateway:  {OUT_CSV_GW}")

        finally:
            conn.close()
            tunnel.stop()
    finally:
        sys.stdout = original_stdout
        txt_fh.close()


if __name__ == "__main__":
    run()
