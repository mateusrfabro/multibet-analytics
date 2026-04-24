"""
Diagnostico EXPIRED Deposits - Play4Tune (Super Nova Bet / Paquistao)

Contexto (23/04/2026):
    - Funil anterior revelou que ~50% das tentativas de deposito EXPIRAM
    - Dia 17/04: 63% (117/185) expiraram — pior dia
    - EXPIRED = jogador iniciou deposito, gateway gerou link (bctyso), mas nao confirmou pagamento no timer
    - Diferente de FAILED (118 HTTP 400): aqui o gateway aprovou gerar o boleto/link, mas o usuario desistiu

Hipoteses (em ordem de probabilidade):
    H1. Timer do gateway muito curto (expires_at - created_at < 5min)
    H2. PSP especifico trava (JazzCash vs Easypaisa tempo diferente)
    H3. Onboarding: novato expira mais que recorrente
    H4. Valor alto desanima (valor medio EXPIRED > COMPLETED)
    H5. Horario/mobile: pico madrugada = app travando/conexao ruim

Secoes:
    A. Baseline: EXPIRED vs COMPLETED no periodo (contagem, %, valor)
    B. TIMER: distribuicao diff(expires_at, created_at) em EXPIRED, processed_at analysis
    C. PSP split: JazzCash vs Easypaisa (% EXPIRED, tempo medio ate desistencia)
    D. Perfil do usuario: novato (1a tentativa) vs recorrente (>= 2a)
    E. Valor: distribuicao pkr EXPIRED vs COMPLETED (media, mediana, p75, p90)
    F. Horario PKT: split 00-23h de EXPIRED% e volume
    G. Cadeia de retries: usuario que expira tenta de novo? Quantas vezes ate concluir?

Banco: supernova_bet | Moeda: PKR | Timezone: UTC no banco / BRT na extracao
Filtro test users: OBRIGATORIO (UNION heuristica + logica oficial dev)
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

# Whitelist de jogadores reais (validada 16/04)
REAL_USERS_WHITELIST = {
    'maharshani44377634693',
    'muhammadrehan17657797557',
    'rehmanzafar006972281',
    'saimkyani15688267',
}

# Output
OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
os.makedirs(OUT_DIR, exist_ok=True)
OUT_TXT = os.path.join(OUT_DIR, "diag_expired_deposits_play4tune_2026-04-23.txt")
OUT_CSV_TIMER = os.path.join(OUT_DIR, "diag_expired_timer_pct_buckets.csv")
OUT_CSV_VALOR = os.path.join(OUT_DIR, "diag_expired_valor_buckets.csv")
OUT_CSV_RETRY = os.path.join(OUT_DIR, "diag_expired_retries_por_user.csv")


# ============================================================================
# Helper: Tee de output (print + gravar em TXT)
# ============================================================================
class Tee:
    def __init__(self, *files):
        self.files = files

    def write(self, text):
        for f in self.files:
            f.write(text)
            f.flush()

    def flush(self):
        for f in self.files:
            f.flush()


def header(title):
    print()
    print("=" * 92)
    print(title)
    print("=" * 92)


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


def main():
    tunnel, conn = get_supernova_bet_connection()
    txt = open(OUT_TXT, "w", encoding="utf-8")
    tee = Tee(sys.stdout, txt)
    _orig = sys.stdout
    sys.stdout = tee

    try:
        print(f"# Diagnostico EXPIRED Deposits - Play4Tune")
        print(f"# Executado em: {datetime.now(BRT).strftime('%d/%m/%Y %H:%M BRT')}")
        print(f"# Janela: {JANELA_INI} -> {JANELA_FIM} (BRT)")

        with conn.cursor() as cur:
            test_ids = get_test_ids(cur)
            print(f"# Test users excluidos: {len(test_ids)}")

            # ================================================================
            # A. BASELINE: EXPIRED vs COMPLETED
            # ================================================================
            header("A. BASELINE - EXPIRED vs COMPLETED vs FAILED no periodo")
            cur.execute(f"""
                SELECT
                    t.status,
                    COUNT(*) AS qtd,
                    ROUND(AVG(t.amount)::numeric, 2) AS valor_medio,
                    ROUND(
                      (PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.amount))::numeric,
                      2
                    ) AS mediana,
                    SUM(t.amount) AS valor_total
                FROM transactions t
                WHERE t.type = 'DEPOSIT'
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
                GROUP BY t.status
                ORDER BY qtd DESC
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            total = sum(r[1] for r in rows)
            print(f"\n  {'status':<14} {'qtd':>6} {'%':>6} {'valor_medio':>13} {'mediana':>10} {'total_pkr':>14}")
            for r in rows:
                pct = 100.0 * r[1] / total if total else 0
                print(f"  {r[0]:<14} {r[1]:>6} {pct:>5.1f}% {float(r[2] or 0):>13,.0f} {float(r[3] or 0):>10,.0f} {float(r[4] or 0):>14,.0f}")
            print(f"  {'TOTAL':<14} {total:>6}")

            # ================================================================
            # B. TIMER: diff(expires_at, created_at) e diff(processed_at, created_at)
            # ================================================================
            header("B. TIMER DO GATEWAY - expires_at vs created_at (EXPIRED)")

            cur.execute(f"""
                SELECT
                    COUNT(*) AS qtd,
                    ROUND(AVG(EXTRACT(EPOCH FROM (t.expires_at - t.created_at)))/60, 2) AS minutos_medio,
                    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.expires_at - t.created_at))))/60::numeric, 2) AS minutos_mediana,
                    ROUND((PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.expires_at - t.created_at))))/60::numeric, 2) AS min_p25,
                    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.expires_at - t.created_at))))/60::numeric, 2) AS min_p75,
                    ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.expires_at - t.created_at))))/60::numeric, 2) AS min_p95,
                    MIN(EXTRACT(EPOCH FROM (t.expires_at - t.created_at)))/60 AS min_min,
                    MAX(EXTRACT(EPOCH FROM (t.expires_at - t.created_at)))/60 AS min_max,
                    COUNT(*) FILTER (WHERE t.expires_at IS NULL) AS sem_expires_at
                FROM transactions t
                WHERE t.type = 'DEPOSIT' AND t.status = 'EXPIRED'
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
            """, (JANELA_INI, JANELA_FIM, test_ids))
            r = cur.fetchone()
            print(f"\n  [B.1] Estatisticas do timer (minutos ate expirar) - tx EXPIRED:")
            print(f"    qtd total EXPIRED     : {r[0]}")
            print(f"    minutos medio         : {float(r[1] or 0):>8.2f}")
            print(f"    minutos mediana (p50) : {float(r[2] or 0):>8.2f}")
            print(f"    min p25               : {float(r[3] or 0):>8.2f}")
            print(f"    min p75               : {float(r[4] or 0):>8.2f}")
            print(f"    min p95               : {float(r[5] or 0):>8.2f}")
            print(f"    min min               : {float(r[6] or 0):>8.2f}")
            print(f"    min max               : {float(r[7] or 0):>8.2f}")
            print(f"    EXPIRED sem expires_at: {r[8]}")

            # B.2: comparacao COMPLETED - quanto tempo o usuario tipicamente leva pra concluir?
            cur.execute(f"""
                SELECT
                    COUNT(*) AS qtd,
                    ROUND(AVG(EXTRACT(EPOCH FROM (t.processed_at - t.created_at)))/60::numeric, 2) AS min_medio,
                    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))))/60::numeric, 2) AS p50,
                    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))))/60::numeric, 2) AS p75,
                    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))))/60::numeric, 2) AS p90,
                    ROUND((PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (t.processed_at - t.created_at))))/60::numeric, 2) AS p95
                FROM transactions t
                WHERE t.type = 'DEPOSIT' AND t.status = 'COMPLETED'
                  AND t.processed_at IS NOT NULL
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
            """, (JANELA_INI, JANELA_FIM, test_ids))
            r = cur.fetchone()
            print(f"\n  [B.2] Tempo ate COMPLETED (processed_at - created_at) - referencia:")
            print(f"    qtd    : {r[0]}")
            print(f"    medio  : {float(r[1] or 0):>6.2f} min")
            print(f"    p50    : {float(r[2] or 0):>6.2f} min")
            print(f"    p75    : {float(r[3] or 0):>6.2f} min")
            print(f"    p90    : {float(r[4] or 0):>6.2f} min")
            print(f"    p95    : {float(r[5] or 0):>6.2f} min")

            # B.3: Buckets de timer (faixas de expires_at - created_at)
            cur.execute(f"""
                WITH expired AS (
                    SELECT EXTRACT(EPOCH FROM (t.expires_at - t.created_at))/60 AS min_timer
                    FROM transactions t
                    WHERE t.type = 'DEPOSIT' AND t.status = 'EXPIRED'
                      AND t.expires_at IS NOT NULL
                      AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                            BETWEEN %s AND %s
                      AND t.user_id NOT IN %s
                )
                SELECT
                    CASE
                        WHEN min_timer < 5 THEN '01_< 5 min'
                        WHEN min_timer < 10 THEN '02_5-10 min'
                        WHEN min_timer < 15 THEN '03_10-15 min'
                        WHEN min_timer < 30 THEN '04_15-30 min'
                        WHEN min_timer < 60 THEN '05_30-60 min'
                        ELSE '06_>= 60 min'
                    END AS bucket,
                    COUNT(*) AS qtd
                FROM expired
                GROUP BY 1
                ORDER BY 1
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows_timer = cur.fetchall()
            total_b = sum(r[1] for r in rows_timer)
            print(f"\n  [B.3] Distribuicao do TIMER (expires_at - created_at) em EXPIRED:")
            print(f"    {'bucket':<15} {'qtd':>6} {'%':>6}")
            for r in rows_timer:
                pct = 100.0 * r[1] / total_b if total_b else 0
                print(f"    {r[0]:<15} {r[1]:>6} {pct:>5.1f}%")

            with open(OUT_CSV_TIMER, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["bucket_timer", "qtd", "pct"])
                for r in rows_timer:
                    pct = 100.0 * r[1] / total_b if total_b else 0
                    w.writerow([r[0], r[1], f"{pct:.1f}"])

            # ================================================================
            # C. PSP SPLIT: JazzCash vs Easypaisa
            # ================================================================
            header("C. PSP SPLIT - JazzCash vs Easypaisa (comportamento EXPIRED)")
            cur.execute(f"""
                SELECT
                    COALESCE(pm.name, '(sem pm)') AS payment_method,
                    t.status,
                    COUNT(*) AS qtd,
                    ROUND(AVG(t.amount)::numeric, 2) AS valor_medio,
                    ROUND(AVG(EXTRACT(EPOCH FROM (t.expires_at - t.created_at)))/60::numeric, 2) AS timer_medio_min
                FROM transactions t
                LEFT JOIN payment_methods pm ON pm.id = (t.metadata->>'payment_method_id')::uuid
                WHERE t.type = 'DEPOSIT'
                  AND t.status IN ('EXPIRED','COMPLETED')
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
                GROUP BY 1,2
                ORDER BY 1,2
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            print(f"\n  {'pm':<25} {'status':<12} {'qtd':>6} {'valor_medio':>13} {'timer_med_min':>15}")
            for r in rows:
                print(f"  {r[0]:<25} {r[1]:<12} {r[2]:>6} {float(r[3] or 0):>13,.2f} {float(r[4] or 0):>15,.2f}")

            # C.2: % EXPIRED por PM
            cur.execute(f"""
                SELECT
                    COALESCE(pm.name, '(sem pm)') AS pm,
                    COUNT(*) AS tent,
                    COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprv,
                    COUNT(*) FILTER (WHERE t.status='EXPIRED') AS expir,
                    ROUND(
                        100.0 * COUNT(*) FILTER (WHERE t.status='EXPIRED') / NULLIF(COUNT(*),0)::numeric,
                        1
                    ) AS pct_expir
                FROM transactions t
                LEFT JOIN payment_methods pm ON pm.id = (t.metadata->>'payment_method_id')::uuid
                WHERE t.type = 'DEPOSIT'
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
                GROUP BY 1
                ORDER BY tent DESC
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            print(f"\n  [C.2] %EXPIRED por PM (janela total):")
            print(f"  {'pm':<25} {'tent':>6} {'aprv':>6} {'expir':>6} {'%expir':>7}")
            for r in rows:
                print(f"  {r[0]:<25} {r[1]:>6} {r[2]:>6} {r[3]:>6} {float(r[4] or 0):>6.1f}%")

            # ================================================================
            # D. PERFIL DO USUARIO: novato vs recorrente
            # ================================================================
            header("D. PERFIL USUARIO - Novato (1a tentativa) vs Recorrente")
            cur.execute(f"""
                WITH deps AS (
                    SELECT
                        t.user_id,
                        t.id AS tx_id,
                        t.status,
                        t.amount,
                        t.created_at,
                        ROW_NUMBER() OVER (PARTITION BY t.user_id ORDER BY t.created_at) AS tentativa_n
                    FROM transactions t
                    WHERE t.type = 'DEPOSIT'
                      AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                            BETWEEN %s AND %s
                      AND t.user_id NOT IN %s
                )
                SELECT
                    CASE WHEN tentativa_n = 1 THEN 'novato_1a' ELSE 'recorrente_2+' END AS perfil,
                    COUNT(*) AS tent,
                    COUNT(*) FILTER (WHERE status='COMPLETED') AS aprv,
                    COUNT(*) FILTER (WHERE status='EXPIRED') AS expir,
                    COUNT(*) FILTER (WHERE status='FAILED') AS fail,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE status='EXPIRED')/NULLIF(COUNT(*),0)::numeric, 1) AS pct_expir,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE status='COMPLETED')/NULLIF(COUNT(*),0)::numeric, 1) AS pct_aprv,
                    ROUND(AVG(amount)::numeric, 2) AS valor_medio
                FROM deps
                GROUP BY 1
                ORDER BY 1
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            print(f"\n  {'perfil':<18} {'tent':>6} {'aprv':>6} {'expir':>6} {'fail':>5} {'%expir':>7} {'%aprv':>7} {'val_med':>9}")
            for r in rows:
                print(f"  {r[0]:<18} {r[1]:>6} {r[2]:>6} {r[3]:>6} {r[4]:>5} {float(r[5] or 0):>6.1f}% {float(r[6] or 0):>6.1f}% {float(r[7] or 0):>9,.0f}")

            # ================================================================
            # E. VALOR: distribuicao EXPIRED vs COMPLETED
            # ================================================================
            header("E. VALOR PKR - EXPIRED vs COMPLETED (distribuicao)")
            cur.execute(f"""
                SELECT
                    t.status,
                    COUNT(*) AS qtd,
                    ROUND(AVG(t.amount)::numeric, 2) AS media,
                    ROUND((PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.amount))::numeric, 2) AS p50,
                    ROUND((PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY t.amount))::numeric, 2) AS p75,
                    ROUND((PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY t.amount))::numeric, 2) AS p90,
                    MIN(t.amount) AS min_,
                    MAX(t.amount) AS max_
                FROM transactions t
                WHERE t.type='DEPOSIT'
                  AND t.status IN ('EXPIRED','COMPLETED')
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
                GROUP BY 1
                ORDER BY 1
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            print(f"\n  {'status':<12} {'qtd':>6} {'media':>10} {'p50':>10} {'p75':>10} {'p90':>10} {'min':>8} {'max':>10}")
            for r in rows:
                print(f"  {r[0]:<12} {r[1]:>6} {float(r[2] or 0):>10,.0f} {float(r[3] or 0):>10,.0f} {float(r[4] or 0):>10,.0f} {float(r[5] or 0):>10,.0f} {float(r[6] or 0):>8,.0f} {float(r[7] or 0):>10,.0f}")

            # E.2: Buckets de valor
            cur.execute(f"""
                SELECT
                    CASE
                        WHEN t.amount < 200 THEN '01_< 200'
                        WHEN t.amount < 500 THEN '02_200-500'
                        WHEN t.amount < 1000 THEN '03_500-1000'
                        WHEN t.amount < 2000 THEN '04_1000-2000'
                        WHEN t.amount < 5000 THEN '05_2000-5000'
                        ELSE '06_>= 5000'
                    END AS bucket,
                    COUNT(*) FILTER (WHERE t.status='EXPIRED') AS expir,
                    COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprv,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE t.status='EXPIRED') /
                        NULLIF(COUNT(*) FILTER (WHERE t.status IN ('EXPIRED','COMPLETED')),0)::numeric, 1) AS pct_expir_no_bucket
                FROM transactions t
                WHERE t.type='DEPOSIT'
                  AND t.status IN ('EXPIRED','COMPLETED')
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
                GROUP BY 1
                ORDER BY 1
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            print(f"\n  [E.2] Taxa de abandono por faixa de valor:")
            print(f"  {'bucket':<15} {'expir':>6} {'aprv':>6} {'%expir_no_bucket':>17}")
            for r in rows:
                print(f"  {r[0]:<15} {r[1]:>6} {r[2]:>6} {float(r[3] or 0):>16.1f}%")

            with open(OUT_CSV_VALOR, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["bucket_valor", "expirados", "aprovados", "pct_expir_no_bucket"])
                for r in rows:
                    w.writerow([r[0], r[1], r[2], float(r[3] or 0)])

            # ================================================================
            # F. HORARIO PKT (UTC+5)
            # ================================================================
            header("F. HORARIO PKT - %EXPIRED por hora")
            cur.execute(f"""
                SELECT
                    EXTRACT(HOUR FROM (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Asia/Karachi'))::int AS h_pkt,
                    COUNT(*) AS tent,
                    COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprv,
                    COUNT(*) FILTER (WHERE t.status='EXPIRED') AS expir,
                    ROUND(100.0 * COUNT(*) FILTER (WHERE t.status='EXPIRED')/NULLIF(COUNT(*),0)::numeric, 1) AS pct_expir
                FROM transactions t
                WHERE t.type='DEPOSIT'
                  AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                        BETWEEN %s AND %s
                  AND t.user_id NOT IN %s
                GROUP BY 1
                ORDER BY 1
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            print(f"\n  {'h_PKT':>5} {'tent':>6} {'aprv':>6} {'expir':>6} {'%expir':>7}")
            for r in rows:
                bar = "#" * int(float(r[4] or 0) / 2)
                print(f"  {r[0]:>5} {r[1]:>6} {r[2]:>6} {r[3]:>6} {float(r[4] or 0):>6.1f}% {bar}")

            # ================================================================
            # G. CADEIA DE RETRIES
            # ================================================================
            header("G. CADEIA DE RETRIES - Usuarios que expiram tentam de novo?")
            cur.execute(f"""
                WITH user_tent AS (
                    SELECT
                        t.user_id,
                        COUNT(*) AS total_tent,
                        COUNT(*) FILTER (WHERE t.status='EXPIRED') AS expir,
                        COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprv,
                        COUNT(*) FILTER (WHERE t.status='FAILED') AS fail,
                        MIN(t.created_at) AS primeira,
                        MAX(t.created_at) AS ultima
                    FROM transactions t
                    WHERE t.type='DEPOSIT'
                      AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                            BETWEEN %s AND %s
                      AND t.user_id NOT IN %s
                    GROUP BY t.user_id
                )
                SELECT
                    COUNT(*) AS users,
                    COUNT(*) FILTER (WHERE aprv >= 1) AS users_c_aprv,
                    COUNT(*) FILTER (WHERE aprv = 0) AS users_so_falha,
                    COUNT(*) FILTER (WHERE expir >= 1) AS users_c_expir,
                    COUNT(*) FILTER (WHERE expir >= 1 AND aprv = 0) AS users_expir_nunca_conclui,
                    COUNT(*) FILTER (WHERE expir >= 1 AND aprv >= 1) AS users_expir_e_conclui,
                    ROUND(AVG(total_tent)::numeric, 2) AS media_tent_por_user,
                    ROUND(AVG(total_tent) FILTER (WHERE aprv >= 1)::numeric, 2) AS media_tent_ate_aprv
                FROM user_tent
            """, (JANELA_INI, JANELA_FIM, test_ids))
            r = cur.fetchone()
            print(f"\n  users totais (qualquer tentativa)         : {r[0]}")
            print(f"  users com pelo menos 1 COMPLETED          : {r[1]}")
            print(f"  users que NUNCA completaram               : {r[2]}")
            print(f"  users com pelo menos 1 EXPIRED            : {r[3]}")
            print(f"  users que EXPIRARAM e NUNCA concluiram    : {r[4]}   <- perda direta")
            print(f"  users que EXPIRARAM mas depois concluiram : {r[5]}   <- recuperados")
            print(f"  media de tentativas por user              : {float(r[6] or 0)}")
            print(f"  media de tentativas ate 1o COMPLETED      : {float(r[7] or 0)}")

            # G.2: Distribuicao qtd de EXPIRED por user
            cur.execute(f"""
                WITH tent AS (
                    SELECT
                        t.user_id,
                        COUNT(*) FILTER (WHERE t.status='EXPIRED') AS expir
                    FROM transactions t
                    WHERE t.type='DEPOSIT'
                      AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                            BETWEEN %s AND %s
                      AND t.user_id NOT IN %s
                    GROUP BY t.user_id
                )
                SELECT expir AS qtd_expir_por_user, COUNT(*) AS users
                FROM tent
                WHERE expir >= 1
                GROUP BY 1
                ORDER BY 1
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            print(f"\n  [G.2] Distribuicao de EXPIRED por usuario:")
            print(f"    {'qtd_expir':<12} {'users':>6}")
            for r in rows:
                print(f"    {r[0]:<12} {r[1]:>6}")

            # G.3: Amostra top users que mais expiraram (export)
            cur.execute(f"""
                WITH tent AS (
                    SELECT
                        t.user_id,
                        COUNT(*) AS total,
                        COUNT(*) FILTER (WHERE t.status='EXPIRED') AS expir,
                        COUNT(*) FILTER (WHERE t.status='COMPLETED') AS aprv,
                        COUNT(*) FILTER (WHERE t.status='FAILED') AS fail
                    FROM transactions t
                    WHERE t.type='DEPOSIT'
                      AND (t.created_at AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')::date
                            BETWEEN %s AND %s
                      AND t.user_id NOT IN %s
                    GROUP BY t.user_id
                )
                SELECT u.username, tent.total, tent.expir, tent.aprv, tent.fail
                FROM tent
                LEFT JOIN users u ON u.id = tent.user_id
                WHERE tent.expir >= 1
                ORDER BY tent.expir DESC, tent.total DESC
                LIMIT 30
            """, (JANELA_INI, JANELA_FIM, test_ids))
            rows = cur.fetchall()
            with open(OUT_CSV_RETRY, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow(["username", "total_tent", "expirados", "aprovados", "falhas"])
                for r in rows:
                    w.writerow(r)
            print(f"\n  [G.3] Top 30 users mais expirados salvos em: {OUT_CSV_RETRY}")

            print()
            print("=" * 92)
            print("ARQUIVOS GERADOS:")
            print(f"  TXT   : {OUT_TXT}")
            print(f"  CSV B : {OUT_CSV_TIMER}")
            print(f"  CSV E : {OUT_CSV_VALOR}")
            print(f"  CSV G : {OUT_CSV_RETRY}")
            print("=" * 92)

    finally:
        sys.stdout = _orig
        txt.close()
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    main()
