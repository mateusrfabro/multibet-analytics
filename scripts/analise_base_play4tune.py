"""
Analise da base Play4Tune para o Castrin.
- Cada jogador: ID, telefone, depositou?, sacou?, valores, status, saldo
- Analise por operadora de telefone (prefixo PK)
- Padrao de falhas de pagamento por operadora/metodo
- Depositos PENDING (travados)

Demanda: Castrin 09/04/2026 — "puxa pra mim uma analise do play4tune:
  id usuario / telefone / conseguiu fazer dep? / conseguiu sacar?"
  + instabilidade de telefones reais no deposito/saque
  + Filipe Molon: Zong (+92370) nao reconhecido, Easypaisa saque nao processa

IMPORTANTE — tipos de transacao validados empiricamente (09/04/2026):
  DEPOSIT (COMPLETED, EXPIRED, FAILED, PENDING)
  WITHDRAW (COMPLETED, CANCELLED, FAILED, REJECTED)  ← NÃO é WITHDRAWAL
  BONUS_CREDIT, BONUS_DEBIT, BONUS_CONVERSION (COMPLETED)
  ADJUSTMENT_CREDIT, ADJUSTMENT_DEBIT (COMPLETED)
  CASINO_CREDIT, CASINO_DEBIT (COMPLETED)
"""

import os
import sys
import csv
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection


# Mapeamento de operadoras PK por prefixo
# +92 3XX = mobile numbers
PK_OPERATORS = {
    '300': 'Jazz', '301': 'Jazz', '302': 'Jazz',
    '303': 'Jazz', '304': 'Jazz', '305': 'Jazz (Warid)',
    '306': 'Jazz (Warid)', '307': 'Jazz',
    '308': 'Jazz', '309': 'Jazz (Warid)',
    '310': 'Zong', '311': 'Zong', '312': 'Zong',
    '313': 'Zong', '314': 'Zong', '315': 'Zong',
    '316': 'Zong', '317': 'Zong', '318': 'Zong',
    '319': 'Zong',
    '320': 'Jazz', '321': 'Jazz', '322': 'Jazz',
    '323': 'Jazz', '324': 'Jazz', '325': 'Jazz',
    '330': 'Ufone', '331': 'Ufone', '332': 'Ufone',
    '333': 'Ufone', '334': 'Ufone', '335': 'Ufone',
    '336': 'Ufone', '337': 'Ufone',
    '340': 'Telenor', '341': 'Telenor', '342': 'Telenor',
    '343': 'Telenor', '344': 'Telenor', '345': 'Telenor',
    '346': 'Telenor', '347': 'Telenor', '348': 'Telenor',
    '349': 'Telenor',
    '350': 'SCO', '351': 'SCO', '355': 'SCOM',
    '360': 'Ufone',
    '370': 'Zong', '371': 'Zong', '372': 'Zong',
}
# Jazzcash = Jazz/Mobilink | Easypaisa = Telenor


def get_operator(phone):
    """Extrai operadora do telefone PK."""
    if not phone or len(phone) < 6:
        return 'SEM_TELEFONE', ''
    # Normalizar: +923XXXXXXXXX ou 03XXXXXXXXX
    clean = phone.replace(' ', '').replace('-', '')
    if clean.startswith('+92'):
        prefix = clean[3:6]  # 3 digitos apos +92
    elif clean.startswith('0'):
        prefix = clean[1:4]
    else:
        return 'FORMATO_DESCONHECIDO', clean
    op = PK_OPERATORS.get(prefix, f'DESCONHECIDO ({prefix})')
    return op, prefix


QUERY_BASE_JOGADORES = """
WITH dep AS (
    SELECT
        user_id,
        COUNT(*) FILTER (WHERE status = 'COMPLETED')                  AS dep_ok,
        COUNT(*) FILTER (WHERE status = 'FAILED')                     AS dep_falhou,
        COUNT(*) FILTER (WHERE status = 'PENDING')                    AS dep_pendente,
        COUNT(*) FILTER (WHERE status = 'CANCELLED')                  AS dep_cancelado,
        COUNT(*) FILTER (WHERE status = 'EXPIRED')                    AS dep_expirado,
        COALESCE(SUM(amount) FILTER (WHERE status = 'COMPLETED'), 0)  AS dep_total,
        COALESCE(SUM(amount) FILTER (WHERE status = 'PENDING'), 0)    AS dep_pendente_valor,
        MIN(created_at) FILTER (WHERE status = 'COMPLETED')           AS ftd_data,
        (ARRAY_AGG(amount ORDER BY created_at) FILTER (WHERE status = 'COMPLETED'))[1] AS ftd_valor
    FROM transactions
    WHERE type = 'DEPOSIT'
    GROUP BY user_id
),
saq AS (
    SELECT
        user_id,
        COUNT(*) FILTER (WHERE status = 'COMPLETED')                  AS saq_ok,
        COUNT(*) FILTER (WHERE status = 'FAILED')                     AS saq_falhou,
        COUNT(*) FILTER (WHERE status = 'PENDING')                    AS saq_pendente,
        COUNT(*) FILTER (WHERE status = 'CANCELLED')                  AS saq_cancelado,
        COUNT(*) FILTER (WHERE status = 'REJECTED')                   AS saq_rejeitado,
        COALESCE(SUM(amount) FILTER (WHERE status = 'COMPLETED'), 0)  AS saq_total,
        COALESCE(SUM(amount) FILTER (WHERE status = 'PENDING'), 0)    AS saq_pendente_valor,
        COALESCE(SUM(amount) FILTER (WHERE status = 'CANCELLED'), 0)  AS saq_cancelado_valor,
        MIN(created_at) FILTER (WHERE status = 'COMPLETED')           AS primeiro_saque_data
    FROM transactions
    WHERE type = 'WITHDRAW'
    GROUP BY user_id
),
wallet AS (
    SELECT
        user_id,
        SUM(balance) FILTER (WHERE type = 'REAL')         AS saldo_real,
        SUM(balance) FILTER (WHERE type = 'BONUS')        AS saldo_bonus,
        SUM(locked_balance)                                AS saldo_bloqueado,
        bool_or(blocked)                                   AS carteira_bloqueada
    FROM wallets
    GROUP BY user_id
),
bets_summary AS (
    -- category: LOSS (aposta perdida), WIN (aposta ganha), WAGER (raro)
    -- total_apostado = LOSS + WAGER amounts (o que o jogador gastou)
    -- total_ganho = WIN win_amounts (o que o jogador recebeu)
    SELECT
        user_id,
        COUNT(*)                                            AS total_bets,
        COALESCE(SUM(amount) FILTER (WHERE category IN ('LOSS', 'WAGER')), 0)
            + COALESCE(SUM(amount) FILTER (WHERE category = 'WIN'), 0) AS total_apostado,
        COALESCE(SUM(win_amount) FILTER (WHERE category = 'WIN'), 0)   AS total_ganho,
        MAX(created_at)                                     AS ultima_aposta
    FROM bets
    GROUP BY user_id
),
pay_accounts AS (
    SELECT
        user_id,
        STRING_AGG(DISTINCT bank_code, ', ')                AS metodos_pagamento,
        COUNT(*)                                             AS qtd_contas
    FROM user_payment_accounts
    WHERE active = true
    GROUP BY user_id
)
SELECT
    u.public_id,
    u.username,
    u.phone,
    u.email,
    u.created_at                                            AS data_cadastro,
    (CURRENT_DATE - u.created_at::date)                     AS dias_conta,
    u.active                                                AS conta_ativa,
    u.blocked                                               AS conta_bloqueada,
    u.is_affiliate,
    -- Depositos
    COALESCE(d.dep_ok, 0)                                   AS dep_aprovados,
    COALESCE(d.dep_falhou, 0)                               AS dep_falhados,
    COALESCE(d.dep_pendente, 0)                              AS dep_pendentes,
    COALESCE(d.dep_cancelado, 0)                             AS dep_cancelados,
    COALESCE(d.dep_expirado, 0)                              AS dep_expirados,
    ROUND(COALESCE(d.dep_total, 0)::numeric, 2)             AS dep_total_valor,
    ROUND(COALESCE(d.dep_pendente_valor, 0)::numeric, 2)    AS dep_pendente_valor,
    d.ftd_data                                              AS ftd_data,
    ROUND(COALESCE(d.ftd_valor, 0)::numeric, 2)             AS ftd_valor,
    -- Saques
    COALESCE(s.saq_ok, 0)                                   AS saq_aprovados,
    COALESCE(s.saq_falhou, 0)                                AS saq_falhados,
    COALESCE(s.saq_pendente, 0)                              AS saq_pendentes,
    COALESCE(s.saq_cancelado, 0)                             AS saq_cancelados,
    COALESCE(s.saq_rejeitado, 0)                             AS saq_rejeitados,
    ROUND(COALESCE(s.saq_total, 0)::numeric, 2)             AS saq_total_valor,
    ROUND(COALESCE(s.saq_pendente_valor, 0)::numeric, 2)    AS saq_pendente_valor,
    ROUND(COALESCE(s.saq_cancelado_valor, 0)::numeric, 2)   AS saq_cancelado_valor,
    s.primeiro_saque_data                                    AS primeiro_saque_data,
    -- Metodos pagamento vinculados
    COALESCE(pa.metodos_pagamento, 'NENHUM')                AS metodos_pagamento,
    COALESCE(pa.qtd_contas, 0)                               AS qtd_contas_pagamento,
    -- Carteira
    ROUND(COALESCE(w.saldo_real, 0)::numeric, 2)            AS saldo_real,
    ROUND(COALESCE(w.saldo_bonus, 0)::numeric, 2)           AS saldo_bonus,
    ROUND(COALESCE(w.saldo_bloqueado, 0)::numeric, 2)       AS saldo_bloqueado,
    COALESCE(w.carteira_bloqueada, false)                    AS carteira_bloqueada,
    -- Atividade casino
    COALESCE(bs.total_bets, 0)                               AS total_apostas,
    ROUND(COALESCE(bs.total_apostado, 0)::numeric, 2)       AS total_apostado,
    ROUND(COALESCE(bs.total_ganho, 0)::numeric, 2)          AS total_ganho,
    ROUND((COALESCE(bs.total_apostado, 0) - COALESCE(bs.total_ganho, 0))::numeric, 2) AS ggr_jogador,
    bs.ultima_aposta                                         AS ultima_aposta,
    -- Net deposit
    ROUND((COALESCE(d.dep_total, 0) - COALESCE(s.saq_total, 0))::numeric, 2) AS net_deposit,
    -- Flags
    CASE
        WHEN COALESCE(d.dep_ok, 0) = 0 AND COALESCE(d.dep_falhou, 0) + COALESCE(d.dep_pendente, 0) > 0
            THEN 'TENTOU_NAO_CONSEGUIU'
        WHEN COALESCE(d.dep_ok, 0) = 0
            THEN 'NUNCA_DEPOSITOU'
        WHEN COALESCE(d.dep_ok, 0) > 0 AND COALESCE(d.dep_falhou, 0) + COALESCE(d.dep_pendente, 0) > 0
            THEN 'DEPOSITOU_COM_FALHAS'
        WHEN COALESCE(d.dep_ok, 0) > 0
            THEN 'DEPOSITOU_OK'
        ELSE 'SEM_INFO'
    END                                                      AS status_deposito,
    CASE
        WHEN COALESCE(s.saq_ok, 0) = 0 AND (COALESCE(s.saq_falhou, 0) + COALESCE(s.saq_rejeitado, 0)) > 0
            THEN 'TENTOU_NAO_CONSEGUIU'
        WHEN COALESCE(s.saq_ok, 0) = 0 AND COALESCE(s.saq_cancelado, 0) > 0
            THEN 'CANCELADO_ADMIN'
        WHEN COALESCE(s.saq_ok, 0) = 0 AND COALESCE(s.saq_pendente, 0) > 0
            THEN 'PENDENTE'
        WHEN COALESCE(s.saq_ok, 0) = 0
            THEN 'NUNCA_SACOU'
        WHEN COALESCE(s.saq_ok, 0) > 0 AND COALESCE(s.saq_cancelado, 0) > 0
            THEN 'SACOU_COM_CANCELAMENTOS'
        WHEN COALESCE(s.saq_ok, 0) > 0 AND (COALESCE(s.saq_falhou, 0) + COALESCE(s.saq_rejeitado, 0)) > 0
            THEN 'SACOU_COM_FALHAS'
        WHEN COALESCE(s.saq_ok, 0) > 0
            THEN 'SACOU_OK'
        ELSE 'SEM_INFO'
    END                                                      AS status_saque
FROM users u
LEFT JOIN dep d         ON d.user_id = u.id
LEFT JOIN saq s         ON s.user_id = u.id
LEFT JOIN wallet w      ON w.user_id = u.id
LEFT JOIN bets_summary bs ON bs.user_id = u.id
LEFT JOIN pay_accounts pa ON pa.user_id = u.id
WHERE u.role = 'USER'
ORDER BY u.created_at DESC
"""


def run_analysis():
    print("Conectando ao Super Nova Bet DB...")
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()

    ts = datetime.now().strftime("%d/%m/%Y %H:%M")
    output = []

    def log(msg=""):
        print(msg)
        output.append(msg)

    log("=" * 70)
    log(f"ANALISE BASE PLAY4TUNE — {ts}")
    log("=" * 70)

    # =========================================
    # 1. Base de jogadores completa
    # =========================================
    cur.execute(QUERY_BASE_JOGADORES)
    rows = cur.fetchall()
    col_names = [desc[0] for desc in cur.description]

    # Enriquecer com operadora
    phone_idx = col_names.index('phone')
    enriched_rows = []
    for row in rows:
        row = list(row)
        phone = row[phone_idx]
        op, prefix = get_operator(phone)
        row.append(op)
        row.append(prefix)
        enriched_rows.append(row)

    col_names_ext = col_names + ['operadora', 'prefixo']

    # =========================================
    # 2. Resumo geral
    # =========================================
    total = len(enriched_rows)
    ci = col_names.index  # shortcut

    dep_ok = sum(1 for r in enriched_rows if r[ci('dep_aprovados')] > 0)
    dep_tentou_falhou = sum(1 for r in enriched_rows if r[ci('dep_aprovados')] == 0 and (r[ci('dep_falhados')] > 0 or r[ci('dep_pendentes')] > 0))
    dep_pendentes_users = sum(1 for r in enriched_rows if r[ci('dep_pendentes')] > 0)
    dep_expirados_users = sum(1 for r in enriched_rows if r[ci('dep_expirados')] > 0)
    saq_ok = sum(1 for r in enriched_rows if r[ci('saq_aprovados')] > 0)
    saq_cancelados = sum(1 for r in enriched_rows if r[ci('saq_cancelados')] > 0)
    saq_falhou = sum(1 for r in enriched_rows if r[ci('saq_falhados')] > 0 or r[ci('saq_rejeitados')] > 0)
    saq_pendente = sum(1 for r in enriched_rows if r[ci('saq_pendentes')] > 0)
    jogaram = sum(1 for r in enriched_rows if r[ci('total_apostas')] > 0)

    total_dep_val = sum(r[ci('dep_total_valor')] or 0 for r in enriched_rows)
    total_saq_val = sum(r[ci('saq_total_valor')] or 0 for r in enriched_rows)
    total_saq_canc_val = sum(r[ci('saq_cancelado_valor')] or 0 for r in enriched_rows)
    total_saldo_real = sum(r[ci('saldo_real')] or 0 for r in enriched_rows)
    total_saldo_bonus = sum(r[ci('saldo_bonus')] or 0 for r in enriched_rows)
    total_dep_pendente_val = sum(r[ci('dep_pendente_valor')] or 0 for r in enriched_rows)

    log(f"""
RESUMO GERAL
{'='*55}
Total usuarios:                {total}
Depositaram com sucesso:       {dep_ok} ({dep_ok/total*100:.1f}%)
Tentaram dep e falharam:       {dep_tentou_falhou}
Com depositos PENDENTES:       {dep_pendentes_users}
Com depositos EXPIRADOS:       {dep_expirados_users}
Sacaram com sucesso:           {saq_ok} ({saq_ok/total*100:.1f}%)
Saques cancelados (admin):     {saq_cancelados}
Saques falhados/rejeitados:    {saq_falhou}
Jogaram casino:                {jogaram} ({jogaram/total*100:.1f}%)
{'='*55}
Total depositado (COMPLETED):  Rs {total_dep_val:,.2f}
Total dep PENDING (travado):   Rs {total_dep_pendente_val:,.2f}
Total sacado (COMPLETED):      Rs {total_saq_val:,.2f}
Total saque CANCELADO:         Rs {total_saq_canc_val:,.2f}
Net deposit:                   Rs {total_dep_val - total_saq_val:,.2f}
Saldo real em carteiras:       Rs {total_saldo_real:,.2f}
Saldo bonus em carteiras:      Rs {total_saldo_bonus:,.2f}
""")

    # =========================================
    # 3. FUNIL
    # =========================================
    log(f"FUNIL DE CONVERSAO")
    log(f"{'='*55}")
    log(f"Cadastro:     {total:>6}  (100%)")
    log(f"Depositou:    {dep_ok:>6}  ({dep_ok/total*100:.1f}%)")
    log(f"Jogou:        {jogaram:>6}  ({jogaram/total*100:.1f}%)")
    log(f"Sacou:        {saq_ok:>6}  ({saq_ok/total*100:.1f}%)")

    # =========================================
    # 4. ANALISE POR OPERADORA (CRITICO)
    # =========================================
    log(f"\n{'='*70}")
    log(f"ANALISE POR OPERADORA DE TELEFONE")
    log(f"{'='*70}")
    log(f"No Paquistao, telefone = identidade financeira (como CPF no Brasil).")
    log(f"Jazzcash = Jazz/Warid | Easypaisa = Telenor | Zong = nao suportado?")
    log(f"")

    # Agrupar por operadora
    op_stats = {}
    op_idx = col_names_ext.index('operadora')
    for r in enriched_rows:
        op = r[op_idx]
        if op not in op_stats:
            op_stats[op] = {
                'total': 0, 'dep_ok': 0, 'dep_falhou': 0, 'dep_pendente': 0, 'dep_expirado': 0,
                'saq_ok': 0, 'saq_falhou': 0, 'saq_cancelado': 0,
                'dep_val': 0, 'saq_val': 0, 'dep_pend_val': 0
            }
        s = op_stats[op]
        s['total'] += 1
        s['dep_ok'] += 1 if r[ci('dep_aprovados')] > 0 else 0
        s['dep_falhou'] += 1 if r[ci('dep_falhados')] > 0 else 0
        s['dep_pendente'] += 1 if r[ci('dep_pendentes')] > 0 else 0
        s['dep_expirado'] += 1 if r[ci('dep_expirados')] > 0 else 0
        s['saq_ok'] += 1 if r[ci('saq_aprovados')] > 0 else 0
        s['saq_falhou'] += 1 if r[ci('saq_falhados')] > 0 or r[ci('saq_rejeitados')] > 0 else 0
        s['saq_cancelado'] += 1 if r[ci('saq_cancelados')] > 0 else 0
        s['dep_val'] += r[ci('dep_total_valor')] or 0
        s['saq_val'] += r[ci('saq_total_valor')] or 0
        s['dep_pend_val'] += r[ci('dep_pendente_valor')] or 0

    log(f"{'Operadora':<20} {'Users':>6} {'DepOK':>6} {'DepFail':>8} {'DepPend':>8} {'DepExp':>7} {'SaqOK':>6} {'SaqCanc':>8} {'DepRs':>10} {'SaqRs':>10}")
    log(f"{'-'*100}")
    for op in sorted(op_stats.keys(), key=lambda x: op_stats[x]['total'], reverse=True):
        s = op_stats[op]
        if s['total'] < 2:  # filtrar operadoras com 1 user
            continue
        dep_rate = f"{s['dep_ok']/s['total']*100:.0f}%" if s['total'] > 0 else "0%"
        log(f"{op:<20} {s['total']:>6} {s['dep_ok']:>3}({dep_rate}) {s['dep_falhou']:>5} {s['dep_pendente']:>8} {s['dep_expirado']:>7} {s['saq_ok']:>6} {s['saq_cancelado']:>8} {s['dep_val']:>10,.0f} {s['saq_val']:>10,.0f}")

    # =========================================
    # 5. DEPOSITOS PENDENTES HOJE (travados)
    # =========================================
    log(f"\n{'='*70}")
    log(f"DEPOSITOS PENDENTES HOJE (09/04) — POSSIVEL INSTABILIDADE")
    log(f"{'='*70}")

    cur.execute("""
        SELECT
            u.username, u.public_id, u.phone,
            t.amount::numeric(18,2),
            t.status,
            t.created_at::timestamp(0),
            pm.name AS metodo,
            t.error_reason
        FROM transactions t
        JOIN users u ON u.id = t.user_id
        LEFT JOIN payment_methods pm ON pm.id = t.payment_method_id
        WHERE t.type = 'DEPOSIT'
          AND t.status = 'PENDING'
          AND t.created_at >= '2026-04-09'
        ORDER BY t.created_at DESC
    """)
    pendentes = cur.fetchall()

    if pendentes:
        log(f"{'Username':<20} {'PID':<10} {'Phone':<18} {'Valor':>8} {'Metodo':<15} {'Data'} {'Erro'}")
        log(f"{'-'*110}")
        for p in pendentes:
            op, _ = get_operator(p[2])
            metodo = p[6] or '?'
            erro = str(p[7] or '')[:20]
            log(f"{p[0]:<20} {p[1]:<10} {str(p[2] or ''):<18} {p[3]:>8} {metodo:<15} {p[5]}  [{op}] {erro}")
        log(f"\nTotal: {len(pendentes)} depositos pendentes hoje")
    else:
        log("Nenhum deposito pendente hoje.")

    # =========================================
    # 6. SAQUES COM PROBLEMA
    # =========================================
    log(f"\n{'='*70}")
    log(f"SAQUES FALHADOS / PENDENTES (ultimos 7 dias)")
    log(f"{'='*70}")

    cur.execute("""
        SELECT
            u.username, u.public_id, u.phone,
            t.amount::numeric(18,2),
            t.status,
            t.created_at::timestamp(0),
            pm.name AS metodo,
            t.error_reason,
            t.rejection_reason,
            pa.bank_code AS conta_banco
        FROM transactions t
        JOIN users u ON u.id = t.user_id
        LEFT JOIN payment_methods pm ON pm.id = t.payment_method_id
        LEFT JOIN user_payment_accounts pa ON pa.user_id = t.user_id AND pa.is_default = true
        WHERE t.type = 'WITHDRAWAL'
          AND t.status IN ('FAILED', 'PENDING', 'CANCELLED')
          AND t.created_at >= NOW() - INTERVAL '7 days'
        ORDER BY t.created_at DESC
    """)
    saq_problemas = cur.fetchall()

    if saq_problemas:
        log(f"{'Username':<18} {'Phone':<18} {'Valor':>8} {'Status':<12} {'Metodo':<12} {'Banco':<12} {'Data'} {'Erro'}")
        log(f"{'-'*120}")
        for s in saq_problemas:
            op, _ = get_operator(s[2])
            erro = str(s[7] or s[8] or '')[:20]
            log(f"{s[0]:<18} {str(s[2] or ''):<18} {s[3]:>8} {s[4]:<12} {str(s[6] or '?'):<12} {str(s[9] or '?'):<12} {s[5]} [{op}] {erro}")
        log(f"\nTotal: {len(saq_problemas)} saques com problema")
    else:
        log("Nenhum saque com problema nos ultimos 7 dias.")

    # =========================================
    # 7. ANALISE ESPECIFICA: Transacoes por metodo de pagamento
    # =========================================
    log(f"\n{'='*70}")
    log(f"TAXA DE SUCESSO POR METODO DE PAGAMENTO")
    log(f"{'='*70}")

    cur.execute("""
        SELECT
            pm.name AS metodo,
            t.type,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE t.status = 'COMPLETED') AS ok,
            COUNT(*) FILTER (WHERE t.status = 'FAILED') AS falhou,
            COUNT(*) FILTER (WHERE t.status = 'PENDING') AS pendente,
            COUNT(*) FILTER (WHERE t.status = 'CANCELLED') AS cancelado,
            ROUND(SUM(t.amount) FILTER (WHERE t.status = 'COMPLETED')::numeric, 0) AS valor_ok,
            ROUND(SUM(t.amount) FILTER (WHERE t.status = 'PENDING')::numeric, 0) AS valor_pend
        FROM transactions t
        LEFT JOIN payment_methods pm ON pm.id = t.payment_method_id
        WHERE t.type IN ('DEPOSIT', 'WITHDRAWAL')
        GROUP BY pm.name, t.type
        ORDER BY pm.name, t.type
    """)
    metodos = cur.fetchall()

    log(f"{'Metodo':<25} {'Tipo':<12} {'Total':>6} {'OK':>6} {'Fail':>6} {'Pend':>6} {'Canc':>6} {'Taxa OK':>8} {'Rs OK':>10} {'Rs Pend':>10}")
    log(f"{'-'*105}")
    for m in metodos:
        taxa = f"{m[3]/m[2]*100:.1f}%" if m[2] > 0 else "0%"
        log(f"{str(m[0] or 'NULL'):<25} {m[1]:<12} {m[2]:>6} {m[3]:>6} {m[4]:>6} {m[5]:>6} {m[6]:>6} {taxa:>8} {m[7] or 0:>10,.0f} {m[8] or 0:>10,.0f}")

    # =========================================
    # 8. Exportar CSV
    # =========================================
    os.makedirs("reports", exist_ok=True)
    csv_path = "reports/play4tune_base_jogadores.csv"
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(col_names_ext)
        writer.writerows(enriched_rows)
    log(f"\nCSV exportado: {csv_path} ({len(enriched_rows)} jogadores)")

    # Salvar report txt
    report_path = "reports/play4tune_analise_base.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("\n".join(output))
    log(f"Report salvo: {report_path}")

    cur.close()
    conn.close()
    tunnel.stop()


if __name__ == "__main__":
    run_analysis()
