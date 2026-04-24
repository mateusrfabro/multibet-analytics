"""
Diagnostico Fraude Play4Tune - 07/04, 18/04, 21/04 (janela 01/04 -> 22/04)
Objetivo: Validar se os picos de CV reg->FTD foram organicos ou farming.

Contexto:
  - Baseline CV reg->FTD da janela ~19%
  - 07/04: CV 22.2% (189 cad, 82 tent, 49 aprov, 42 FTD) - ZERO falhas gateway
  - 18/04: CV 47.6% (103 cad, 137 tent, 72 aprov, 49 FTD) - farming ja documentado (+92341374xxx)
  - 21/04: CV 47.2% (36 cad, 56 tent, 31 aprov, 17 FTD) - investigar

Filtro test users: UNION heuristica + manipulacao manual (ADJUSTMENT/reviewed_by)
                  + whitelist 4 usuarios reais confirmados pelo dev (DP/SQ case).

Sinais avaliados por dia:
  S1. Concentracao de IP (N users com mesmo IP)
  S2. Concentracao de User-Agent (bot/scripting)
  S3. Prefixo telefonico contiguo (+92XXXXXXXX em sequencia)
  S4. Dominios email descartaveis (wetuns, whyknapp, temp-mail, etc)
  S5. Tempo reg->FTD muito rapido (<2min)
  S6. Valores de deposito padronizados (moda = min bonus 200/300)
  S7. % ativacao Welcome Bonus
  S8. % cadastros sem UTM (fora de canal pago)
  S9. Users do dia que ja sacaram (exfil)

Classificacao por score (0-100):
  ORGANICO        : < 30
  SUSPEITO        : 30-59
  FARMING LIKELY  : 60-79
  FARMING CONFIRM : >= 80

Banco: supernova_bet | Moeda: PKR | Host PostgreSQL via SSH tunnel
"""
import os
import sys
from datetime import date
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.supernova_bet import get_supernova_bet_connection

# --- CONFIG ---
DIAS_INVESTIGAR = ['2026-04-07', '2026-04-18', '2026-04-21']

REAL_USERS_WHITELIST = {
    'maharshani44377634693',
    'muhammadrehan17657797557',
    'rehmanzafar006972281',
    'saimkyani15688267',
}

DOMINIOS_DESCARTAVEIS_KNOWN = [
    'wetuns.com', 'whyknapp.com', 'tempmail', 'temp-mail', 'mailinator',
    'guerrillamail', 'trashmail', '10minutemail', 'yopmail', 'sharklasers',
    'mohmal', 'getnada', 'fakemail', 'throwaway', 'dispostable',
]


def get_test_ids(cur):
    """Exclui test users (heuristica + manual adjustments/reviews), preservando whitelist."""
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


def banner(txt, char='='):
    print("\n" + char * 92)
    print(txt)
    print(char * 92)


def classificar_dia(sinais):
    """Recebe dict de sinais e retorna (score, veredito, motivos)."""
    score = 0
    motivos = []

    # S1 IP concentrado
    if sinais.get('max_users_por_ip', 0) >= 5:
        score += 20
        motivos.append(f"IP concentrado: {sinais['max_users_por_ip']} users mesmo IP")
    elif sinais.get('max_users_por_ip', 0) >= 3:
        score += 10
        motivos.append(f"IP suspeito: {sinais['max_users_por_ip']} users mesmo IP")

    # S2 UA concentrado
    if sinais.get('max_users_por_ua', 0) >= 5:
        score += 10
        motivos.append(f"UA concentrado: {sinais['max_users_por_ua']} users mesmo UA")

    # S3 prefixo phone
    if sinais.get('max_prefix_10digits', 0) >= 5:
        score += 20
        motivos.append(f"Phones sequenciais: batch +{sinais['top_prefix']} ({sinais['max_prefix_10digits']} users)")
    elif sinais.get('max_prefix_10digits', 0) >= 3:
        score += 10
        motivos.append(f"Prefixo phone suspeito: {sinais['max_prefix_10digits']} users")

    # S4 email descartavel
    if sinais.get('pct_email_descartavel', 0) >= 30:
        score += 15
        motivos.append(f"{sinais['pct_email_descartavel']:.0f}% emails descartaveis")
    elif sinais.get('pct_email_descartavel', 0) >= 10:
        score += 8
        motivos.append(f"{sinais['pct_email_descartavel']:.0f}% emails suspeitos")

    # S5 tempo reg->ftd
    if sinais.get('pct_sub_2min', 0) >= 50:
        score += 15
        motivos.append(f"{sinais['pct_sub_2min']:.0f}% FTD sub-2min reg (bot)")
    elif sinais.get('pct_sub_2min', 0) >= 30:
        score += 8
        motivos.append(f"{sinais['pct_sub_2min']:.0f}% FTD sub-2min")

    # S6 valores padronizados
    if sinais.get('pct_moda_valor', 0) >= 60:
        score += 10
        motivos.append(f"{sinais['pct_moda_valor']:.0f}% deposito = Rs {sinais['moda_valor']}")

    # S7 bonus activation
    if sinais.get('pct_bonus_ativado', 0) >= 80:
        score += 10
        motivos.append(f"{sinais['pct_bonus_ativado']:.0f}% ativaram Welcome Bonus")

    # S8 sem UTM
    if sinais.get('pct_sem_utm', 0) >= 60:
        score += 5
        motivos.append(f"{sinais['pct_sem_utm']:.0f}% cadastros sem UTM")

    # S9 saque rapido
    if sinais.get('pct_ja_sacou', 0) >= 25:
        score += 15
        motivos.append(f"{sinais['pct_ja_sacou']:.0f}% ja sacaram (exfil)")

    if score >= 80:
        veredito = "FARMING CONFIRMADO"
    elif score >= 60:
        veredito = "FARMING LIKELY"
    elif score >= 30:
        veredito = "SUSPEITO"
    else:
        veredito = "ORGANICO"

    return score, veredito, motivos


def analisar_dia(cur, dia_str, test_ids):
    """Executa todas as analises para um dia especifico e retorna dict de sinais."""
    sinais = {'dia': dia_str}

    banner(f"DIA {dia_str} - DEEP DIVE SINAIS FRAUDE", '#')

    # Cohort do dia (cadastros)
    cur.execute("""
        SELECT u.id, u.username, u.email, u.phone, u.created_at
        FROM users u
        WHERE u.role='USER' AND u.id NOT IN %s
          AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date = %s::date
    """, (test_ids, dia_str))
    cohort = cur.fetchall()
    sinais['total_cadastros'] = len(cohort)
    cohort_ids = tuple([r[0] for r in cohort]) if cohort else ('00000000-0000-0000-0000-000000000000',)
    print(f"  Total cadastros do dia: {len(cohort)}")

    # Cohort que fez FTD
    cur.execute("""
        SELECT DISTINCT u.id
        FROM users u
        JOIN transactions t ON t.user_id = u.id
        WHERE u.role='USER' AND u.id NOT IN %s
          AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date = %s::date
          AND t.type='DEPOSIT' AND t.status='COMPLETED'
    """, (test_ids, dia_str))
    ftds = [r[0] for r in cur.fetchall()]
    sinais['total_ftds'] = len(ftds)
    sinais['cv_ftd'] = round(100 * len(ftds) / max(len(cohort), 1), 1)
    print(f"  Total FTD cohort:       {len(ftds)} (CV={sinais['cv_ftd']}%)")

    if not cohort:
        print("  (sem cohort neste dia)")
        return sinais

    # --- S1 concentracao IP ---
    cur.execute("""
        SELECT t.ip_address, COUNT(DISTINCT t.user_id) AS users
        FROM transactions t
        WHERE t.user_id IN %s
          AND t.type='DEPOSIT' AND t.status='COMPLETED'
          AND t.ip_address IS NOT NULL
        GROUP BY 1
        ORDER BY users DESC LIMIT 10
    """, (cohort_ids,))
    ips = cur.fetchall()
    sinais['max_users_por_ip'] = max([r[1] for r in ips], default=0)
    print(f"\n  [S1] Top IPs (max users mesmo IP = {sinais['max_users_por_ip']}):")
    for r in ips[:5]:
        flag = " <-- SUSPEITO" if r[1] >= 3 else ""
        print(f"       {str(r[0])[:30]:30} users={r[1]}{flag}")

    # --- S2 concentracao UA ---
    cur.execute("""
        SELECT SUBSTRING(t.user_agent, 1, 80) AS ua, COUNT(DISTINCT t.user_id) AS users
        FROM transactions t
        WHERE t.user_id IN %s
          AND t.type='DEPOSIT' AND t.status='COMPLETED'
          AND t.user_agent IS NOT NULL
        GROUP BY 1
        ORDER BY users DESC LIMIT 5
    """, (cohort_ids,))
    uas = cur.fetchall()
    sinais['max_users_por_ua'] = max([r[1] for r in uas], default=0)
    print(f"\n  [S2] Top User-Agents (max users mesmo UA = {sinais['max_users_por_ua']}):")
    for r in uas[:3]:
        flag = " <-- CONCENTRADO" if r[1] >= 5 else ""
        print(f"       users={r[1]:>3} ua={(r[0] or '')[:70]}{flag}")

    # --- S3 prefixo phone (10 digitos = codigo pais + operadora + faixa) ---
    phones = [r[3] for r in cohort if r[3]]
    prefix_10 = [p[:13] for p in phones if len(p) >= 13]
    cnt_prefix = Counter(prefix_10)
    top_prefix = cnt_prefix.most_common(5)
    sinais['max_prefix_10digits'] = top_prefix[0][1] if top_prefix else 0
    sinais['top_prefix'] = top_prefix[0][0] if top_prefix else None
    print(f"\n  [S3] Top prefixos phone (13 digitos) - max = {sinais['max_prefix_10digits']}:")
    for p, c in top_prefix[:5]:
        flag = " <-- BATCH SEQUENCIAL" if c >= 5 else ""
        print(f"       {p}  qtd={c}{flag}")

    # --- S4 dominios descartaveis ---
    emails = [r[2].lower() if r[2] else '' for r in cohort]
    dominios = Counter(e.split('@')[-1] for e in emails if '@' in e)
    descartaveis = 0
    for d, c in dominios.items():
        if any(k in d for k in DOMINIOS_DESCARTAVEIS_KNOWN):
            descartaveis += c
    sinais['pct_email_descartavel'] = 100 * descartaveis / max(len(cohort), 1)
    print(f"\n  [S4] Emails descartaveis: {descartaveis}/{len(cohort)} = {sinais['pct_email_descartavel']:.1f}%")
    print(f"       Top dominios:")
    for d, c in dominios.most_common(5):
        flag = " <-- DESCARTAVEL" if any(k in d for k in DOMINIOS_DESCARTAVEIS_KNOWN) else ""
        print(f"         {d}  qtd={c}{flag}")

    # --- S5 tempo reg->FTD ---
    cur.execute("""
        WITH tempos AS (
            SELECT u.id,
                   EXTRACT(EPOCH FROM (MIN(t.processed_at) - u.created_at))/60 AS min_gap
            FROM users u
            JOIN transactions t ON t.user_id=u.id
            WHERE u.id IN %s AND t.type='DEPOSIT' AND t.status='COMPLETED'
            GROUP BY u.id, u.created_at
        )
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN min_gap<2 THEN 1 ELSE 0 END) AS sub_2,
            SUM(CASE WHEN min_gap BETWEEN 2 AND 10 THEN 1 ELSE 0 END) AS min_2_10,
            ROUND(AVG(min_gap)::numeric,1) AS media,
            ROUND(percentile_cont(0.5) WITHIN GROUP (ORDER BY min_gap)::numeric,1) AS mediana
        FROM tempos
    """, (cohort_ids,))
    r = cur.fetchone()
    total_ftd = r[0] or 0
    sub_2 = r[1] or 0
    sinais['pct_sub_2min'] = 100 * sub_2 / max(total_ftd, 1)
    sinais['media_reg_ftd'] = float(r[3] or 0)
    sinais['mediana_reg_ftd'] = float(r[4] or 0)
    print(f"\n  [S5] Tempo reg->FTD: sub-2min={sub_2}/{total_ftd} ({sinais['pct_sub_2min']:.0f}%) | "
          f"2-10min={r[2]} | media={sinais['media_reg_ftd']}min | mediana={sinais['mediana_reg_ftd']}min")

    # --- S6 valores padronizados ---
    cur.execute("""
        SELECT t.amount::numeric(10,2) AS valor, COUNT(*) AS qtd
        FROM transactions t
        WHERE t.user_id IN %s AND t.type='DEPOSIT' AND t.status='COMPLETED'
        GROUP BY 1
        ORDER BY qtd DESC LIMIT 5
    """, (cohort_ids,))
    valores = cur.fetchall()
    total_deps = sum(r[1] for r in valores)
    sinais['moda_valor'] = float(valores[0][0]) if valores else 0
    sinais['pct_moda_valor'] = 100 * valores[0][1] / max(total_deps, 1) if valores else 0
    print(f"\n  [S6] Valores FTD (moda={sinais['moda_valor']:.0f} em {sinais['pct_moda_valor']:.0f}% deps):")
    for v, q in valores[:5]:
        print(f"       Rs {float(v):>8.2f}  qtd={q}")

    # --- S7 bonus activation ---
    cur.execute("""
        SELECT COUNT(DISTINCT user_id)
        FROM bonus_activations
        WHERE user_id IN %s
    """, (cohort_ids,))
    r = cur.fetchone()
    bonus_ativados = r[0] or 0
    sinais['pct_bonus_ativado'] = 100 * bonus_ativados / max(len(cohort), 1)
    print(f"\n  [S7] Bonus ativados: {bonus_ativados}/{len(cohort)} = {sinais['pct_bonus_ativado']:.0f}%")

    # --- S8 sem UTM ---
    cur.execute("""
        SELECT
          COUNT(*) FILTER (WHERE s.utm->>'utm_source' IS NULL OR s.utm->>'utm_source'='') AS sem_utm,
          COUNT(DISTINCT s.user_id) AS total
        FROM user_sessions s
        WHERE s.user_id IN %s
    """, (cohort_ids,))
    r = cur.fetchone()
    sem_utm = r[0] or 0
    total_ses = r[1] or 0
    # fallback: usar marketing_events
    cur.execute("""
        SELECT COUNT(DISTINCT user_id) FILTER (WHERE utm_source IS NULL OR utm_source=''),
               COUNT(DISTINCT user_id)
        FROM user_marketing_events
        WHERE user_id IN %s
    """, (cohort_ids,))
    r2 = cur.fetchone()
    sem_utm_me = r2[0] or 0
    total_me = r2[1] or 0
    # usar melhor fonte
    if total_me > 0:
        sinais['pct_sem_utm'] = 100 * sem_utm_me / total_me
    elif total_ses > 0:
        sinais['pct_sem_utm'] = 100 * sem_utm / total_ses
    else:
        sinais['pct_sem_utm'] = 0
    print(f"\n  [S8] Cadastros sem UTM: {sinais['pct_sem_utm']:.0f}% (marketing_events: {sem_utm_me}/{total_me} | sessions: {sem_utm}/{total_ses})")

    # --- S9 saque rapido ---
    cur.execute("""
        SELECT COUNT(DISTINCT user_id)
        FROM transactions
        WHERE user_id IN %s AND type='WITHDRAW' AND status IN ('COMPLETED','PENDING')
    """, (cohort_ids,))
    r = cur.fetchone()
    sacaram = r[0] or 0
    sinais['pct_ja_sacou'] = 100 * sacaram / max(len(cohort), 1)
    print(f"\n  [S9] Ja tentaram sacar (COMPLETED+PENDING): {sacaram}/{len(cohort)} = {sinais['pct_ja_sacou']:.0f}%")

    # --- Listar top 10 cadastros do dia para evidencia ---
    print(f"\n  [EVIDENCIA] Sample 15 users do cohort (ordenado por hora):")
    print(f"       {'username':25} {'phone':16} {'email_dom':30} {'reg_h':>6}")
    for r in cohort[:15]:
        uname, email, phone = r[1], r[2] or '', r[3] or ''
        dom = email.split('@')[-1] if '@' in email else '-'
        hora = r[4].strftime('%H:%M') if r[4] else ''
        print(f"       {str(uname)[:25]:25} {str(phone)[:16]:16} {str(dom)[:30]:30} {hora:>6}")

    return sinais


def run():
    tunnel, conn = get_supernova_bet_connection()
    try:
        with conn.cursor() as cur:
            test_ids = get_test_ids(cur)
            print(f"# Script: diag_fraude_0704_1804_play4tune.py")
            print(f"# Data execucao: {date.today()}")
            print(f"# Test users excluidos: {len(test_ids)}")
            print(f"# Dias investigados: {DIAS_INVESTIGAR}")

            resultados = {}
            for dia in DIAS_INVESTIGAR:
                sinais = analisar_dia(cur, dia, test_ids)
                score, veredito, motivos = classificar_dia(sinais)
                sinais['score'] = score
                sinais['veredito'] = veredito
                sinais['motivos'] = motivos
                resultados[dia] = sinais

            # ============================================================
            # RESUMO FINAL
            # ============================================================
            banner("RESUMO CLASSIFICACAO POR DIA", '=')
            print(f"{'dia':12} {'cad':>4} {'ftd':>4} {'cv%':>5} {'score':>5} {'veredito':>22}")
            for dia, s in resultados.items():
                print(f"{dia:12} {s.get('total_cadastros',0):>4} {s.get('total_ftds',0):>4} "
                      f"{s.get('cv_ftd',0):>5} {s.get('score',0):>5} {s.get('veredito','-'):>22}")
                for m in s.get('motivos', []):
                    print(f"             - {m}")

            # ============================================================
            # CRUZAR 18/04 COM MEMO: batch +92341374xxx ainda presente?
            # ============================================================
            banner("CROSS-CHECK: batch +92341374xxx (memo 18/04) presente em cada dia?", '=')
            for dia in DIAS_INVESTIGAR:
                cur.execute("""
                    SELECT COUNT(*) FROM users u
                    WHERE u.role='USER' AND u.id NOT IN %s
                      AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date = %s::date
                      AND u.phone LIKE '+92341374%%'
                """, (test_ids, dia))
                r = cur.fetchone()
                print(f"  {dia}: {r[0]} users com phone +92341374xxx")

            # ============================================================
            # CRUZAR PREFIXOS NOVOS DE 21/04
            # ============================================================
            banner("PREFIXOS DO DIA 21/04 - operacao nova ou continuacao?", '=')
            cur.execute("""
                SELECT SUBSTRING(u.phone,1,10) AS prefixo, COUNT(*) AS q,
                       MIN(u.created_at) AS primeira_ocorrencia_dia,
                       (SELECT COUNT(*) FROM users u2
                        WHERE SUBSTRING(u2.phone,1,10) = SUBSTRING(u.phone,1,10)
                          AND u2.created_at < '2026-04-21'::date
                          AND u2.id NOT IN %s) AS historico_antes_21_04
                FROM users u
                WHERE u.role='USER' AND u.id NOT IN %s
                  AND ((u.created_at AT TIME ZONE 'UTC') AT TIME ZONE 'America/Sao_Paulo')::date = '2026-04-21'::date
                  AND u.phone IS NOT NULL
                GROUP BY 1 ORDER BY q DESC LIMIT 10
            """, (test_ids, test_ids))
            rows = cur.fetchall()
            print(f"  {'prefixo':12} {'qtd_21/04':>10} {'hist_antes':>12}")
            for r in rows:
                flag = " <-- NOVO (sem historico)" if r[3] == 0 else ""
                print(f"  {str(r[0]):12} {r[1]:>10} {r[3]:>12}{flag}")

    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    run()
