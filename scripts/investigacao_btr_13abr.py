"""
Investigacao URGENTE: BTR (Bonus Turn to Real) em 13/04/2026 (segunda-feira)
=============================================================================
Autor: Mateus Fabro | Squad: Intelligence Engine | Data: 2026-04-13

Contexto: Head reportou que bonus ja atingiu 50K hoje. BTR ocorre quando o
jogador bate o wagering e o bonus converte em real cash -> gera saque.

Fontes:
  - fund_ec2.tbl_realcash_sub_fund_txn (BTR REAL: c_txn_type=20, c_op_type=CR)
    NOTA CRITICA: tbl_real_fund_txn.c_amount_in_ecr_ccy e SEMPRE 0 para type 20.
    O valor real do BTR esta em tbl_realcash_sub_fund_txn.
  - bonus_ec2.tbl_bonus_summary_details (validacao cruzada: c_actual_issued_amount)
  - ps_bi.dim_user (excluir test users, enriquecer jogadores)

Regras:
  - Valores fund_ec2/bonus_ec2: centavos -> dividir por 100.0
  - Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
  - 13/04 BRT = 2026-04-13 03:00 UTC a 2026-04-14 03:00 UTC
  - Sintaxe Presto/Trino (Athena)
  - Filtrar test users: is_test = false (ps_bi)

Uso:
  python scripts/investigacao_btr_13abr.py
"""

import sys
import os
import traceback
from datetime import datetime

import pandas as pd

# -- path setup --
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db.athena import query_athena

# -- pandas display --
pd.set_option("display.max_columns", 30)
pd.set_option("display.width", 220)
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# ==================================================================
# CONSTANTES — janela temporal BRT -> UTC
# ==================================================================
HOJE_UTC_START = "2026-04-13 03:00:00"
HOJE_UTC_END   = "2026-04-14 03:00:00"

WEEK_UTC_START = "2026-04-06 03:00:00"  # 7 dias atras + hoje
WEEK_UTC_END   = "2026-04-14 03:00:00"

SEP = "=" * 80
SUBSEP = "-" * 80


def fmt_brl(valor):
    """Formata valor em BRL."""
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}"


def run_query(descricao, sql, database="fund_ec2"):
    """Executa query com tratamento de erro e logging."""
    print(f"\n{SEP}")
    print(f"  {descricao}")
    print(f"{SEP}")
    try:
        df = query_athena(sql, database=database)
        if df.empty:
            print("  [VAZIO] Nenhum resultado retornado.")
        else:
            print(f"  {len(df)} linhas retornadas.\n")
            print(df.to_string(index=False))
        return df
    except Exception as e:
        print(f"  [ERRO] {e}")
        traceback.print_exc()
        return pd.DataFrame()


# ==================================================================
print(f"\n{'#' * 80}")
print(f"  INVESTIGACAO BTR (Bonus Turn to Real) -- 13/04/2026 (segunda-feira)")
print(f"  Head reportou: bonus ja atingiu 50K hoje")
print(f"  Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'#' * 80}")


# ==================================================================
# Q1: BTR DIARIO ULTIMOS 7 DIAS + HOJE
# Fonte: fund_ec2.tbl_realcash_sub_fund_txn (VALOR REAL do BTR)
# Approach: query direta na sub_fund (sem JOIN com tbl_real_fund_txn)
# Validado em: scripts/btr_by_utm_campaign.py (09/04/2026)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  Q1: BTR DIARIO — ULTIMOS 7 DIAS + HOJE")
print("  Fonte: fund_ec2.tbl_realcash_sub_fund_txn (c_txn_type=20, c_op_type=CR)")
print(f"{'#' * 80}")

sql_q1 = f"""
-- Q1: BTR diario — ultimos 7 dias + hoje
-- Fonte: tbl_realcash_sub_fund_txn (SUB-FUND, onde o valor REAL do BTR esta)
-- c_txn_type=20 = ISSUE_BONUS (wagering batido, bonus -> real cash)
-- c_op_type=CR = credito (entrada de real cash)
-- NOTA: tbl_real_fund_txn.c_amount e SEMPRE 0 para type 20
SELECT
    date(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(*) AS qtd_btr,
    ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_btr_brl,
    ROUND(AVG(c_amount_in_ecr_ccy / 100.0), 2) AS ticket_medio_btr,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(MAX(c_amount_in_ecr_ccy / 100.0), 2) AS maior_btr
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{WEEK_UTC_START}'
  AND c_start_time <  TIMESTAMP '{WEEK_UTC_END}'
  AND c_txn_type = 20
  AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0
GROUP BY 1
ORDER BY 1
"""

df_q1 = run_query("Q1: BTR diario (tbl_realcash_sub_fund_txn)", sql_q1)

# Analise comparativa
if not df_q1.empty:
    print(f"\n{SUBSEP}")
    print("  ANALISE COMPARATIVA Q1:")
    print(f"{SUBSEP}")
    hoje_row = df_q1[df_q1["dia"].astype(str) == "2026-04-13"]
    media_7d = df_q1["total_btr_brl"].mean()
    total_7d = df_q1["total_btr_brl"].sum()

    print(f"  Total BTR 7 dias: {fmt_brl(total_7d)}")
    print(f"  Media diaria:     {fmt_brl(media_7d)}")

    if not hoje_row.empty:
        total_hoje = hoje_row["total_btr_brl"].values[0]
        qtd_hoje = hoje_row["qtd_btr"].values[0]
        jogadores_hoje = hoje_row["jogadores"].values[0]
        razao = total_hoje / media_7d * 100 if media_7d > 0 else 0

        print(f"\n  HOJE (13/04):")
        print(f"    Total BTR:   {fmt_brl(total_hoje)}")
        print(f"    Quantidade:  {int(qtd_hoje)}")
        print(f"    Jogadores:   {int(jogadores_hoje)}")
        print(f"    vs Media 7d: {razao:.1f}%")

        if total_hoje >= 50000:
            print(f"\n  >>> CONFIRMADO: BTR hoje >= R$50K ({fmt_brl(total_hoje)})")
        elif total_hoje >= 40000:
            print(f"\n  >>> PROXIMO: BTR hoje esta em {fmt_brl(total_hoje)} (perto dos R$50K)")
        else:
            print(f"\n  >>> BTR hoje: {fmt_brl(total_hoje)}")

        if razao > 150:
            print("  >>> ALERTA: BTR hoje ACIMA de 150% da media!")
        elif razao > 120:
            print("  >>> ATENCAO: BTR hoje acima de 120% da media.")
    else:
        print("\n  HOJE (13/04): Dados ainda nao disponiveis ou dia incompleto.")


# ==================================================================
# Q2: TOP 30 JOGADORES POR BTR HOJE
# Fonte: fund_ec2.tbl_realcash_sub_fund_txn
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  Q2: TOP 30 JOGADORES POR BTR HOJE")
print(f"{'#' * 80}")

sql_q2 = f"""
-- Q2: Top 30 jogadores por volume BTR hoje (13/04 BRT)
-- Fonte: tbl_realcash_sub_fund_txn (valor real do BTR)
SELECT
    c_ecr_id,
    COUNT(*) AS qtd_btr,
    ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_btr_brl,
    ROUND(MAX(c_amount_in_ecr_ccy / 100.0), 2) AS maior_btr_individual,
    ROUND(AVG(c_amount_in_ecr_ccy / 100.0), 2) AS ticket_medio,
    MIN(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_btr_brt,
    MAX(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_btr_brt
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
  AND c_txn_type = 20
  AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0
GROUP BY 1
ORDER BY total_btr_brl DESC
LIMIT 30
"""

df_q2 = run_query("Q2: Top 30 jogadores BTR hoje", sql_q2)

# Enriquecer com dim_user (test user? data registro?)
if not df_q2.empty:
    ecr_ids = df_q2["c_ecr_id"].tolist()
    ecr_list = ",".join(str(int(x)) for x in ecr_ids if pd.notna(x))

    sql_enrich = f"""
    -- Enriquecer top jogadores BTR com perfil
    SELECT
        ecr_id,
        external_id,
        is_test,
        registration_date
    FROM ps_bi.dim_user
    WHERE ecr_id IN ({ecr_list})
    """
    df_enrich = run_query("Q2b: Perfil dos top 30 jogadores BTR", sql_enrich, database="ps_bi")

    if not df_enrich.empty:
        # Checar test users
        test_users = df_enrich[df_enrich["is_test"] == True]
        if not test_users.empty:
            print(f"\n  *** ATENCAO: {len(test_users)} dos top 30 sao TEST USERS! ***")
            print(f"  ECR IDs test: {test_users['ecr_id'].tolist()}")
        else:
            print("\n  OK: Nenhum dos top 30 e test user.")

        # Merge para exibir enriquecido
        df_merged = df_q2.merge(
            df_enrich[["ecr_id", "is_test", "registration_date"]],
            left_on="c_ecr_id",
            right_on="ecr_id",
            how="left",
        )
        print(f"\n{SUBSEP}")
        print("  TOP 30 BTR ENRIQUECIDO:")
        print(f"{SUBSEP}")
        cols_show = ["c_ecr_id", "qtd_btr", "total_btr_brl", "maior_btr_individual",
                     "is_test", "registration_date"]
        cols_available = [c for c in cols_show if c in df_merged.columns]
        print(df_merged[cols_available].to_string(index=False))

        # Concentracao
        total_top30 = df_q2["total_btr_brl"].sum()
        top1 = df_q2["total_btr_brl"].iloc[0] if len(df_q2) > 0 else 0
        top5 = df_q2["total_btr_brl"].head(5).sum()
        top10 = df_q2["total_btr_brl"].head(10).sum()
        print(f"\n  Concentracao BTR hoje:")
        print(f"    Top 1:  {fmt_brl(top1)} ({top1/total_top30*100:.1f}% do top 30)")
        print(f"    Top 5:  {fmt_brl(top5)} ({top5/total_top30*100:.1f}% do top 30)")
        print(f"    Top 10: {fmt_brl(top10)} ({top10/total_top30*100:.1f}% do top 30)")


# ==================================================================
# Q3: VALIDACAO CRUZADA VIA bonus_ec2
# Fonte: bonus_ec2.tbl_bonus_summary_details (c_actual_issued_amount)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  Q3: VALIDACAO CRUZADA — bonus_ec2.tbl_bonus_summary_details")
print("  (Compara BTR fund_ec2 com issued amount do bonus_ec2)")
print(f"{'#' * 80}")

sql_q3 = f"""
-- Q3: Bonus emitidos por dia via bonus_ec2 (validacao cruzada)
-- c_actual_issued_amount = valor que virou real cash (centavos BRL)
-- c_issue_date = data de emissao do bonus
SELECT
    date(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(*) AS qtd,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_issued_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    ROUND(MAX(c_actual_issued_amount / 100.0), 2) AS max_individual
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_issue_date >= TIMESTAMP '{WEEK_UTC_START}'
  AND c_issue_date <  TIMESTAMP '{WEEK_UTC_END}'
  AND c_actual_issued_amount > 0
GROUP BY 1
ORDER BY 1
"""

df_q3 = run_query("Q3: Bonus issued por dia (bonus_ec2)", sql_q3, database="bonus_ec2")

# Cruzar com Q1
if not df_q1.empty and not df_q3.empty:
    print(f"\n{SUBSEP}")
    print("  COMPARATIVO: fund_ec2 (BTR) vs bonus_ec2 (issued)")
    print(f"{SUBSEP}")
    try:
        df_cross = df_q1[["dia", "total_btr_brl"]].merge(
            df_q3[["dia", "total_issued_brl"]],
            on="dia",
            how="outer",
        ).sort_values("dia")
        df_cross["diff_brl"] = df_cross["total_btr_brl"].fillna(0) - df_cross["total_issued_brl"].fillna(0)
        df_cross["diff_pct"] = (df_cross["diff_brl"] / df_cross["total_issued_brl"].fillna(1) * 100).round(1)
        print(df_cross.to_string(index=False))
        print("\n  NOTA: Divergencia e esperada — bonus_ec2 inclui offers+issues,")
        print("  fund_ec2 sub_fund type 20 e so a conversao final (wagering batido).")
    except Exception as e:
        print(f"  Erro no cruzamento: {e}")


# ==================================================================
# Q4: BTR POR HORA HOJE (deteccao de pico)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  Q4: BTR POR HORA HOJE (deteccao de pico)")
print(f"{'#' * 80}")

sql_q4 = f"""
-- Q4: BTR por hora BRT hoje (13/04/2026)
-- Identifica picos de conversao de bonus
SELECT
    hour(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora_brt,
    COUNT(*) AS qtd,
    ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(AVG(c_amount_in_ecr_ccy / 100.0), 2) AS ticket_medio,
    ROUND(MAX(c_amount_in_ecr_ccy / 100.0), 2) AS maior_btr_hora
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
  AND c_txn_type = 20
  AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 0
GROUP BY 1
ORDER BY 1
"""

df_q4 = run_query("Q4: BTR por hora BRT hoje", sql_q4)

if not df_q4.empty:
    print(f"\n{SUBSEP}")
    print("  ANALISE DE PICO:")
    print(f"{SUBSEP}")
    pico = df_q4.loc[df_q4["total_brl"].idxmax()]
    print(f"  Hora de pico: {int(pico['hora_brt'])}h BRT")
    print(f"  Total no pico: {fmt_brl(pico['total_brl'])}")
    print(f"  Qtd no pico: {int(pico['qtd'])}")
    print(f"  Jogadores no pico: {int(pico['jogadores'])}")

    # BTR acumulado hora a hora
    df_q4["acumulado_brl"] = df_q4["total_brl"].cumsum()
    print(f"\n  Evolucao acumulada:")
    for _, row in df_q4.iterrows():
        barra = "#" * int(row["total_brl"] / max(df_q4["total_brl"].max(), 1) * 30)
        print(f"    {int(row['hora_brt']):02d}h  {fmt_brl(row['total_brl']):>14s}  acum: {fmt_brl(row['acumulado_brl']):>14s}  {barra}")


# ==================================================================
# Q5: BTR POR BONUS_ID HOJE (quais campanhas de bonus geraram mais BTR?)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  Q5: BTR POR BONUS_ID HOJE (quais bonus geraram mais conversao?)")
print(f"{'#' * 80}")

sql_q5 = f"""
-- Q5: BTR por bonus_id hoje (bonus_ec2)
-- Identifica quais campanhas/templates de bonus estao convertendo mais
SELECT
    c_bonus_id,
    c_bonus_status,
    COUNT(*) AS qtd,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_issued_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    ROUND(MAX(c_actual_issued_amount / 100.0), 2) AS max_individual
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_issue_date >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_issue_date <  TIMESTAMP '{HOJE_UTC_END}'
  AND c_actual_issued_amount > 0
GROUP BY 1, 2
ORDER BY total_issued_brl DESC
LIMIT 20
"""

df_q5 = run_query("Q5: BTR por bonus_id hoje (bonus_ec2)", sql_q5, database="bonus_ec2")

# Tentar buscar nomes dos bonus
if not df_q5.empty:
    bonus_ids = df_q5["c_bonus_id"].tolist()
    bonus_list = ",".join(str(int(x)) for x in bonus_ids if pd.notna(x))

    sql_names = f"""
    -- Nomes dos bonus via tbl_bonus_profile
    SELECT c_bonus_id, c_bonus_name
    FROM bonus_ec2.tbl_bonus_profile
    WHERE c_bonus_id IN ({bonus_list})
    """
    try:
        df_names = run_query("Q5b: Nomes dos bonus (tbl_bonus_profile)", sql_names, database="bonus_ec2")
        if not df_names.empty:
            df_q5_named = df_q5.merge(df_names, on="c_bonus_id", how="left")
            print(f"\n{SUBSEP}")
            print("  TOP BONUS COM NOME:")
            print(f"{SUBSEP}")
            cols = ["c_bonus_id", "c_bonus_name", "qtd", "total_issued_brl", "jogadores", "ticket_medio"]
            cols_avail = [c for c in cols if c in df_q5_named.columns]
            print(df_q5_named[cols_avail].to_string(index=False))
    except Exception as e:
        print(f"  [NOTA] Nao foi possivel buscar nomes: {e}")


# ==================================================================
# Q6: BTR INDIVIDUAL GRANDE HOJE (> R$500)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  Q6: BTR INDIVIDUAIS GRANDES HOJE (> R$500)")
print(f"{'#' * 80}")

sql_q6 = f"""
-- Q6: BTR individuais acima de R$500 hoje
-- Alerta para conversoes outlier
SELECT
    c_ecr_id,
    ROUND(c_amount_in_ecr_ccy / 100.0, 2) AS btr_brl,
    c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS horario_brt,
    c_sub_txn_id
FROM fund_ec2.tbl_realcash_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_start_time <  TIMESTAMP '{HOJE_UTC_END}'
  AND c_txn_type = 20
  AND c_op_type = 'CR'
  AND c_amount_in_ecr_ccy > 50000  -- > R$500 em centavos
ORDER BY c_amount_in_ecr_ccy DESC
LIMIT 30
"""

df_q6 = run_query("Q6: BTR individuais > R$500 hoje", sql_q6)


# ==================================================================
# RESUMO FINAL
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  RESUMO DA INVESTIGACAO BTR — 13/04/2026")
print(f"{'#' * 80}")

# Metricas consolidadas
if not df_q1.empty:
    hoje_row = df_q1[df_q1["dia"].astype(str) == "2026-04-13"]
    dias_anteriores = df_q1[df_q1["dia"].astype(str) != "2026-04-13"]
    media_anterior = dias_anteriores["total_btr_brl"].mean() if not dias_anteriores.empty else 0

    print(f"""
METRICAS BTR (fund_ec2.tbl_realcash_sub_fund_txn):
  Periodo analisado: 06/04 a 13/04/2026 (BRT)""")

    if not hoje_row.empty:
        total_hoje = hoje_row["total_btr_brl"].values[0]
        qtd_hoje = hoje_row["qtd_btr"].values[0]
        jogadores_hoje = hoje_row["jogadores"].values[0]
        ticket_hoje = hoje_row.get("ticket_medio_btr", pd.Series([0])).values[0]
        maior_hoje = hoje_row.get("maior_btr", pd.Series([0])).values[0]

        razao = total_hoje / media_anterior * 100 if media_anterior > 0 else 0

        print(f"""
  HOJE (13/04 BRT):
    Total BTR:         {fmt_brl(total_hoje)}
    Quantidade:        {int(qtd_hoje)} conversoes
    Jogadores unicos:  {int(jogadores_hoje)}
    Ticket medio:      {fmt_brl(ticket_hoje)}
    Maior BTR:         {fmt_brl(maior_hoje)}

  MEDIA DIAS ANTERIORES (06-12/04):
    Media diaria BTR:  {fmt_brl(media_anterior)}
    Razao hoje/media:  {razao:.1f}%""")

        if total_hoje >= 50000:
            print(f"\n  >>> CONFIRMADO: BTR ATINGIU R$50K+ HOJE ({fmt_brl(total_hoje)})")
        else:
            projecao_23h = total_hoje * (24 / max(datetime.now().hour - 0, 1)) if datetime.now().hour > 0 else total_hoje
            print(f"\n  >>> BTR ate agora: {fmt_brl(total_hoje)}")
            print(f"  >>> Projecao 24h (linear): {fmt_brl(projecao_23h)}")

if not df_q3.empty:
    hoje_bonus = df_q3[df_q3["dia"].astype(str) == "2026-04-13"]
    if not hoje_bonus.empty:
        print(f"""
  VALIDACAO CRUZADA (bonus_ec2):
    Total issued hoje: {fmt_brl(hoje_bonus['total_issued_brl'].values[0])}
    Qtd bonus issued:  {int(hoje_bonus['qtd'].values[0])}
    Jogadores:         {int(hoje_bonus['jogadores'].values[0])}""")

if not df_q4.empty:
    pico = df_q4.loc[df_q4["total_brl"].idxmax()]
    print(f"""
  PICO:
    Hora de pico BTR: {int(pico['hora_brt'])}h BRT ({fmt_brl(pico['total_brl'])})""")

if not df_q2.empty:
    top1_ecr = df_q2["c_ecr_id"].iloc[0]
    top1_val = df_q2["total_btr_brl"].iloc[0]
    top5_val = df_q2["total_btr_brl"].head(5).sum()
    print(f"""
  CONCENTRACAO:
    Top 1 jogador:  ecr_id {top1_ecr} -> {fmt_brl(top1_val)}
    Top 5 jogadores: {fmt_brl(top5_val)}""")

if not df_q6.empty:
    print(f"""
  OUTLIERS (BTR > R$500):
    Quantidade: {len(df_q6)} transacoes individuais > R$500 hoje""")

print(f"""
FONTES:
  - fund_ec2.tbl_realcash_sub_fund_txn (c_txn_type=20, c_op_type=CR, valor>0)
    -> Esta e a tabela com o VALOR REAL do BTR (tbl_real_fund_txn e sempre 0)
  - bonus_ec2.tbl_bonus_summary_details (c_actual_issued_amount, c_issue_date)
    -> Validacao cruzada
  - ps_bi.dim_user (is_test, registration_date)
    -> Enriquecimento de jogadores

ACAO RECOMENDADA:
  1. Verificar se houve campanha CRM ativa que gerou volume anormal de BTR
  2. Cruzar top jogadores BTR com depositos (BTR >> deposito = custo sem retorno)
  3. Investigar bonus_id com maior volume — desligar se necessario
  4. Monitorar hora a hora ate fim do dia
  5. Comparar com GGR do dia — BTR alto + GGR baixo = prejuizo

NOTA TECNICA:
  - BTR = c_txn_type 20 (ISSUE_BONUS) — quando jogador bate wagering
  - Valor do BTR esta SOMENTE em tbl_realcash_sub_fund_txn (sub-fund)
  - tbl_real_fund_txn.c_amount_in_ecr_ccy e SEMPRE 0 para type 20
  - Validado em: scripts/btr_by_utm_campaign.py e memory/schema_fund.md

Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")
