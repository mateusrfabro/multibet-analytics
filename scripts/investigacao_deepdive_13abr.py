"""
Investigacao Deep Dive — BTR e jogadores flagados (13/abr/2026)
================================================================
Contexto: Head (Castrin) reportou BTR de R$ 54K parcial hoje.
Nossa analise as 10:30 mostrou R$ 18K. Precisamos atualizar e
identificar discrepancia, alem de deep dive nos 8 jogadores flagados.

Queries:
  Q1  — BTR atualizado hoje (c_op_type='CR')
  Q1B — BTR sem filtro c_op_type (hipotese: Castrin nao filtra)
  Q1C — BTR UTC midnight-to-midnight (hipotese: Castrin usa UTC puro)
  Q2  — Lifetime financeiro dos 8 jogadores flagados
  Q3  — Atividade casino ultimos 7 dias (jogos)
  Q4  — Bonus recebidos por campanha
  Q5  — Detalhamento campanha 20251029082323 (wager=0?)
"""

import sys
import os
import pandas as pd
from datetime import datetime

# Garantir que o diretorio raiz esta no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_rows', 100)
pd.set_option('display.float_format', lambda x: f'{x:,.2f}')

# =====================================================================
# IDs dos jogadores flagados (BIGINT — sem aspas)
# =====================================================================
ECRS = [
    336381772819042756,   # Head: sacou R$ 27.300
    840431775967363292,   # Head: registro 12/abr, dep 19.420, sacou 20.100
    423761775511458577,   # Head: registro 06/abr, 1 dep 10K, 1 saque 13K
    951121775908485591,   # Head: Renato, dep 900, sacou 8.680, BTR 6.000
    569239691792165145,   # Nos: Vitor, BTR 7.177
    910658491790198479,   # Nos: BTR 4.000
    580639611791460300,   # Nos: Lindeilson, dep 487, sacou 5.300
    900694311791710218,   # Nos: Mauricio, dep 2.008, sacou 6.323
]
ECR_LIST = ', '.join(str(e) for e in ECRS)

def run_query(label, sql, database="fund_ec2"):
    """Executa query e imprime resultado formatado."""
    sep = "=" * 80
    print(f"\n{sep}")
    print(f"  {label}")
    print(f"{sep}")
    try:
        df = query_athena(sql, database=database)
        if df.empty:
            print("  (sem resultados)")
        else:
            print(df.to_string(index=False))
        print(f"  [{len(df)} linhas retornadas]")
        return df
    except Exception as e:
        print(f"  ERRO: {e}")
        return pd.DataFrame()

# =====================================================================
# QUERY 1 — BTR atualizado hoje (BRT: 13/04 03:00 UTC a 14/04 03:00 UTC)
# Filtro: c_txn_type=20, c_op_type='CR', c_amount>0
# =====================================================================
print("\n" + "#" * 80)
print("# INVESTIGACAO BTR + DEEP DIVE JOGADORES FLAGADOS — 13/ABR/2026")
print(f"# Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} BRT")
print("#" * 80)

q1_sql = """
-- Q1: BTR hoje (BRT) com filtro c_op_type='CR'
SELECT
    COUNT(*) as qtd_btr,
    SUM(c_amount_in_ecr_ccy / 100.0) as total_btr_brl,
    COUNT(DISTINCT c_ecr_id) as jogadores
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-04-13 03:00:00'
  AND c_start_time < TIMESTAMP '2026-04-14 03:00:00'
  AND c_txn_type = 20
  AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0
"""
df_q1 = run_query("Q1 — BTR HOJE (BRT) | c_op_type='CR'", q1_sql)

# =====================================================================
# QUERY 1B — BTR SEM filtro c_op_type (talvez Castrin nao filtre)
# =====================================================================
q1b_sql = """
-- Q1B: BTR hoje (BRT) SEM filtro c_op_type
-- Hipotese: Castrin pode nao filtrar por c_op_type
SELECT
    COUNT(*) as qtd_btr,
    SUM(c_amount_in_ecr_ccy / 100.0) as total_btr_brl,
    COUNT(DISTINCT c_ecr_id) as jogadores
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-04-13 03:00:00'
  AND c_start_time < TIMESTAMP '2026-04-14 03:00:00'
  AND c_txn_type = 20
  AND c_amount_in_ecr_ccy > 0
"""
df_q1b = run_query("Q1B — BTR HOJE (BRT) | SEM filtro c_op_type", q1b_sql)

# =====================================================================
# QUERY 1C — BTR UTC midnight-to-midnight (sem ajuste BRT)
# =====================================================================
q1c_sql = """
-- Q1C: BTR UTC midnight (sem ajuste BRT)
-- Hipotese: Castrin filtra em UTC puro
SELECT
    COUNT(*) as qtd_btr,
    SUM(c_amount_in_ecr_ccy / 100.0) as total_btr_brl,
    COUNT(DISTINCT c_ecr_id) as jogadores
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-04-13 00:00:00'
  AND c_start_time < TIMESTAMP '2026-04-14 00:00:00'
  AND c_txn_type = 20
  AND c_amount_in_ecr_ccy > 0
"""
df_q1c = run_query("Q1C — BTR HOJE (UTC midnight-to-midnight) | SEM filtro c_op_type", q1c_sql)

# =====================================================================
# RESUMO COMPARATIVO BTR
# =====================================================================
print("\n" + "=" * 80)
print("  RESUMO COMPARATIVO BTR — Qual cenario bate com R$ 54K do Head?")
print("=" * 80)
labels = ["Q1 (BRT + CR)", "Q1B (BRT, sem CR)", "Q1C (UTC, sem CR)"]
dfs = [df_q1, df_q1b, df_q1c]
for lbl, df in zip(labels, dfs):
    if not df.empty and 'total_btr_brl' in df.columns:
        val = df['total_btr_brl'].iloc[0]
        qtd = df['qtd_btr'].iloc[0]
        jog = df['jogadores'].iloc[0] if 'jogadores' in df.columns else '?'
        print(f"  {lbl:30s} => R$ {val:>12,.2f}  ({qtd} txns, {jog} jogadores)")
    else:
        print(f"  {lbl:30s} => SEM DADOS")

# =====================================================================
# QUERY 2 — LIFETIME financeiro dos 8 jogadores flagados
# Nota: cashier_ec2 usa c_created_time (nao c_start_time)
#        cashier deposit usa c_confirmed_amount_in_ecr_ccy
# =====================================================================
q2_sql = f"""
-- Q2: Lifetime financeiro (dep + saq + BTR) dos jogadores flagados
-- cashier_ec2 usa c_created_time, fund_ec2 usa c_start_time
WITH deps AS (
    SELECT
        c_ecr_id,
        SUM(c_confirmed_amount_in_ecr_ccy / 100.0) as lifetime_dep,
        COUNT(*) as qtd_dep
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_txn_status = 'txn_confirmed_success'
      AND c_ecr_id IN ({ECR_LIST})
      AND c_created_time >= TIMESTAMP '2025-01-01'
    GROUP BY 1
),
saques AS (
    SELECT
        c_ecr_id,
        SUM(c_amount_in_ecr_ccy / 100.0) as lifetime_saq,
        COUNT(*) as qtd_saq
    FROM cashier_ec2.tbl_cashier_cashout
    WHERE c_txn_status = 'co_success'
      AND c_ecr_id IN ({ECR_LIST})
      AND c_created_time >= TIMESTAMP '2025-01-01'
    GROUP BY 1
),
btr AS (
    SELECT
        c_ecr_id,
        SUM(c_amount_in_ecr_ccy / 100.0) as lifetime_btr,
        COUNT(*) as qtd_btr
    FROM fund_ec2.tbl_realcash_sub_fund_txn
    WHERE c_txn_type = 20
      AND c_op_type = 'CR'
      AND c_amount_in_ecr_ccy > 0
      AND c_ecr_id IN ({ECR_LIST})
      AND c_start_time >= TIMESTAMP '2025-01-01'
    GROUP BY 1
),
info AS (
    SELECT ecr_id, external_id, is_test, registration_date
    FROM ps_bi.dim_user
    WHERE ecr_id IN ({ECR_LIST})
)
SELECT
    i.ecr_id,
    i.external_id,
    i.is_test,
    i.registration_date,
    COALESCE(d.lifetime_dep, 0) as lifetime_dep,
    COALESCE(d.qtd_dep, 0) as qtd_dep,
    COALESCE(s.lifetime_saq, 0) as lifetime_saq,
    COALESCE(s.qtd_saq, 0) as qtd_saq,
    COALESCE(b.lifetime_btr, 0) as lifetime_btr,
    COALESCE(b.qtd_btr, 0) as qtd_btr,
    COALESCE(d.lifetime_dep, 0) - COALESCE(s.lifetime_saq, 0) as net_dep_saq,
    COALESCE(d.lifetime_dep, 0) + COALESCE(b.lifetime_btr, 0) - COALESCE(s.lifetime_saq, 0) as net_com_btr
FROM info i
LEFT JOIN deps d ON i.ecr_id = d.c_ecr_id
LEFT JOIN saques s ON i.ecr_id = s.c_ecr_id
LEFT JOIN btr b ON i.ecr_id = b.c_ecr_id
ORDER BY COALESCE(s.lifetime_saq, 0) DESC
"""
df_q2 = run_query("Q2 — LIFETIME FINANCEIRO DOS 8 JOGADORES FLAGADOS", q2_sql, database="ps_bi")

# Interpretacao Q2
if not df_q2.empty:
    print("\n  --- INTERPRETACAO ---")
    for _, row in df_q2.iterrows():
        ecr = row.get('ecr_id', '?')
        dep = row.get('lifetime_dep', 0)
        saq = row.get('lifetime_saq', 0)
        btr_val = row.get('lifetime_btr', 0)
        net = row.get('net_dep_saq', 0)
        net_btr = row.get('net_com_btr', 0)
        is_test = row.get('is_test', None)
        reg = row.get('registration_date', '?')
        flag = ""
        if is_test:
            flag += " [TEST USER!]"
        if saq > dep * 2 and dep > 0:
            flag += " [SAQUE > 2x DEP]"
        if btr_val > dep and dep > 0:
            flag += " [BTR > DEP]"
        if net < -5000:
            flag += " [LOSS > R$5K]"
        print(f"  ECR {ecr}: DEP R${dep:,.0f} | SAQ R${saq:,.0f} | BTR R${btr_val:,.0f} | NET R${net:,.0f} | NET+BTR R${net_btr:,.0f} | Reg {reg}{flag}")

# =====================================================================
# QUERY 3 — Atividade casino ultimos 7 dias
# =====================================================================
q3_sql = f"""
-- Q3: Atividade casino 7 dias (06-13/abr) por jogador/jogo
-- Types: 27=bet, 45=win, 72=rollback
SELECT
    c_ecr_id,
    c_game_id,
    c_sub_vendor_id,
    SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) as total_bets,
    SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) as total_wins,
    SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END)
      - SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) as player_profit,
    COUNT(CASE WHEN c_txn_type = 27 THEN 1 END) as rodadas
FROM fund_ec2.tbl_real_fund_txn
WHERE c_start_time >= TIMESTAMP '2026-04-06 03:00:00'
  AND c_start_time < TIMESTAMP '2026-04-14 03:00:00'
  AND c_txn_type IN (27, 45, 72)
  AND c_txn_status = 'SUCCESS'
  AND c_ecr_id IN ({ECR_LIST})
GROUP BY 1, 2, 3
HAVING SUM(CASE WHEN c_txn_type IN (27, 45) THEN c_amount_in_ecr_ccy ELSE 0 END) > 0
ORDER BY c_ecr_id, player_profit DESC
"""
df_q3 = run_query("Q3 — ATIVIDADE CASINO (ULTIMOS 7 DIAS) POR JOGO", q3_sql)

# Resumo por jogador
if not df_q3.empty:
    print("\n  --- RESUMO POR JOGADOR (CASINO 7 DIAS) ---")
    for ecr_id, grp in df_q3.groupby('c_ecr_id'):
        total_bet = grp['total_bets'].sum()
        total_win = grp['total_wins'].sum()
        profit = grp['player_profit'].sum()
        n_games = len(grp)
        n_rounds = grp['rodadas'].sum()
        top_game = grp.iloc[0]['c_game_id'] if not grp.empty else '?'
        top_vendor = grp.iloc[0]['c_sub_vendor_id'] if not grp.empty else '?'
        rtp = (total_win / total_bet * 100) if total_bet > 0 else 0
        print(f"  ECR {ecr_id}: BET R${total_bet:,.0f} | WIN R${total_win:,.0f} | PROFIT R${profit:,.0f} | RTP {rtp:.1f}% | {n_rounds} rodadas, {n_games} jogos | Top: {top_game} ({top_vendor})")

# =====================================================================
# QUERY 4 — Bonus recebidos por campanha
# =====================================================================
q4_sql = f"""
-- Q4: Bonus recebidos pelos jogadores flagados
-- c_actual_issued_amount = valor convertido em real cash (centavos)
-- Nota: bonus_ec2 usa c_issue_date (nao c_created_time)
SELECT
    c_ecr_id,
    c_bonus_id,
    c_bonus_status,
    SUM(c_actual_issued_amount / 100.0) as total_bonus_brl,
    COUNT(*) as qtd
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_ecr_id IN ({ECR_LIST})
  AND c_issue_date >= TIMESTAMP '2025-01-01'
  AND c_actual_issued_amount > 0
GROUP BY 1, 2, 3
ORDER BY c_ecr_id, total_bonus_brl DESC
"""
df_q4 = run_query("Q4 — BONUS RECEBIDOS POR CAMPANHA", q4_sql, database="bonus_ec2")

# Resumo bonus por jogador
if not df_q4.empty:
    print("\n  --- RESUMO BONUS POR JOGADOR ---")
    for ecr_id, grp in df_q4.groupby('c_ecr_id'):
        total_bonus = grp['total_bonus_brl'].sum()
        n_campanhas = grp['c_bonus_id'].nunique()
        top_campanha = grp.iloc[0]['c_bonus_id']
        top_val = grp.iloc[0]['total_bonus_brl']
        print(f"  ECR {ecr_id}: TOTAL BONUS R${total_bonus:,.0f} | {n_campanhas} campanhas | Top: {top_campanha} (R${top_val:,.0f})")

# =====================================================================
# QUERY 5 — Detalhamento campanha 20251029082323 (wager=0?)
# =====================================================================
q5_sql = """
-- Q5: Detalhamento campanha 20251029082323 (hoje, BRT)
-- Verificar wager e status dos bonus emitidos
-- Nota: bonus_ec2 usa c_issue_date (nao c_created_time)
--       c_free_spin_wager_amount para wager de freespins
SELECT
    c_ecr_id,
    c_actual_issued_amount / 100.0 as bonus_brl,
    c_total_bonus_issued / 100.0 as total_issued,
    c_free_spin_wager_amount / 100.0 as wager_freespin,
    c_bonus_status,
    c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' as emissao_brt,
    c_offered_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' as oferta_brt
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_bonus_id = 20251029082323
  AND c_issue_date >= TIMESTAMP '2026-04-13 03:00:00'
  AND c_issue_date < TIMESTAMP '2026-04-14 03:00:00'
ORDER BY bonus_brl DESC
"""
df_q5 = run_query("Q5 — CAMPANHA 20251029082323 HOJE (WAGER=0?)", q5_sql, database="bonus_ec2")

if not df_q5.empty:
    total_bonus = df_q5['bonus_brl'].sum() if 'bonus_brl' in df_q5.columns else 0
    print(f"\n  --- CAMPANHA 20251029082323 ---")
    print(f"  Total emitido hoje: R$ {total_bonus:,.2f}")
    print(f"  Qtd bonus emitidos: {len(df_q5)}")
    if 'wager_freespin' in df_q5.columns:
        wager_zero = (df_q5['wager_freespin'].fillna(0) == 0).sum()
        wager_pos = (df_q5['wager_freespin'].fillna(0) > 0).sum()
        print(f"  Com wager_freespin=0: {wager_zero} bonus")
        print(f"  Com wager_freespin>0: {wager_pos} bonus")
        if wager_zero > 0:
            print(f"  [ALERTA] Bonus sem rollover de freespin detectados!")

# =====================================================================
# CONCLUSAO
# =====================================================================
print("\n" + "#" * 80)
print("# CONCLUSAO E PROXIMOS PASSOS")
print("#" * 80)
print("""
  1. Comparar Q1/Q1B/Q1C para identificar qual filtro o Head usa (R$ 54K)
  2. Se Q1B ou Q1C batem, a diferenca e c_op_type ou timezone
  3. Jogadores com SAQUE > 2x DEP ou BTR > DEP devem ser investigados
  4. Jogadores com RTP > 100% consistente indicam possivel abuso de bonus
  5. Campanha com wager=0 e risco direto de custo sem retorno
""")
print(f"# Fim da execucao: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} BRT")
print("#" * 80)
