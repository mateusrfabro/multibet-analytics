"""
Investigacao URGENTE: Bonus anormalmente altos em 13/04/2026
Fonte principal: bonus_ec2.tbl_bonus_summary_details (c_bonus_id, c_actual_issued_amount)
Fonte secundaria: fund_ec2.tbl_bonus_sub_fund_txn (transacoes financeiras, SEM c_bonus_id)

Regras:
  - bonus_ec2: valores em centavos (dividir por 100.0)
  - fund_ec2: valores em centavos (dividir por 100.0)
  - Timezone: UTC -> BRT (13/04 BRT = 13/04 03:00 UTC a 14/04 03:00 UTC)
  - c_actual_issued_amount validado (c_total_bonus_offered NAO EXISTE)
  - c_bonus_id NAO EXISTE em fund_ec2.tbl_bonus_sub_fund_txn
  - c_bonus_name/c_bonus_amount NAO EXISTEM em tbl_ecr_bonus_details
  - Sintaxe Presto/Trino

Correcoes v2 (13/04/2026):
  - Removido c_bonus_id de queries fund_ec2 (coluna inexistente)
  - Removido c_bonus_name/c_bonus_amount de tbl_ecr_bonus_details (colunas inexistentes)
  - Breakdown por bonus_id feito via bonus_ec2.tbl_bonus_summary_details
  - Join com tbl_bonus_profile para obter nome do bonus
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.athena import query_athena
import pandas as pd
from datetime import datetime
import traceback

pd.set_option("display.max_columns", 30)
pd.set_option("display.width", 200)
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# ==================================================================
# CONSTANTES — janela temporal BRT -> UTC
# ==================================================================
HOJE_UTC_START = "2026-04-13 03:00:00"
HOJE_UTC_END   = "2026-04-14 03:00:00"

WEEK_UTC_START = "2026-04-06 03:00:00"
WEEK_UTC_END   = "2026-04-14 03:00:00"

separator = "=" * 80


def fmt_brl(valor):
    """Formata valor em BRL."""
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}"


def run_query(descricao, sql, database="bonus_ec2"):
    """Executa query com tratamento de erro e logging."""
    print(f"\n{separator}")
    print(f"  {descricao}")
    print(f"{separator}")
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
print(f"  INVESTIGACAO BONUS ALTOS v2 -- 13/04/2026 (BRT)")
print(f"  Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'#' * 80}")


# ==================================================================
# FASE 1: BONUS HOJE vs ULTIMOS 7 DIAS
# Fonte: bonus_ec2.tbl_bonus_summary_details (c_issue_date, c_actual_issued_amount)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 1: BONUS EMITIDOS POR DIA (7 DIAS) -- bonus_ec2.tbl_bonus_summary_details")
print(f"{'#' * 80}")

sql_q1 = f"""
-- Q1: Volume e valor de bonus emitidos por dia (ultimos 7 dias BRT)
-- Fonte: bonus_ec2.tbl_bonus_summary_details
-- c_issue_date = data de emissao | c_actual_issued_amount = centavos BRL
SELECT
    date(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(*) AS qtd_bonus,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_bonus_brl,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    COUNT(DISTINCT c_ecr_id) AS jogadores_distintos
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_issue_date >= TIMESTAMP '{WEEK_UTC_START}'
  AND c_issue_date < TIMESTAMP '{WEEK_UTC_END}'
  AND c_actual_issued_amount > 0
GROUP BY 1
ORDER BY 1
"""

df_q1 = run_query("Q1: Bonus emitidos por dia (tbl_bonus_summary_details)", sql_q1)


# ==================================================================
# FASE 1b: BONUS VIA FUND (tbl_bonus_sub_fund_txn) -- todos os tipos
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 1b: TRANSACOES BONUS POR DIA E TIPO (fund_ec2.tbl_bonus_sub_fund_txn)")
print(f"{'#' * 80}")

sql_q1b = f"""
-- Q1b: Transacoes financeiras de bonus por dia (fund_ec2)
-- txn_type 19 = OFFER_BONUS | 20 = ISSUE_BONUS | 80 = FREESPIN_WIN
-- NOTA: c_bonus_id NAO existe nesta tabela
SELECT
    date(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    c_txn_type,
    CASE c_txn_type
        WHEN 19 THEN 'OFFER_BONUS (concedido)'
        WHEN 20 THEN 'ISSUE_BONUS (convertido)'
        WHEN 30 THEN 'EXPIRADO'
        WHEN 37 THEN 'DROPPED'
        WHEN 80 THEN 'FREESPIN_WIN'
        WHEN 88 THEN 'ISSUE_DROP_DEBIT'
        ELSE CAST(c_txn_type AS VARCHAR)
    END AS tipo_bonus,
    COUNT(*) AS qtd,
    ROUND(SUM(c_txn_amount / 100.0), 2) AS total_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores
FROM fund_ec2.tbl_bonus_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{WEEK_UTC_START}'
  AND c_start_time < TIMESTAMP '{WEEK_UTC_END}'
  AND c_txn_amount > 0
  AND c_txn_type IN (19, 20, 30, 37, 80, 88)
GROUP BY 1, 2, 3
ORDER BY 1, 2
"""

df_q1b = run_query("Q1b: Fund bonus sub_fund por dia e tipo", sql_q1b, database="fund_ec2")

if not df_q1b.empty:
    print("\n--- Resumo diario agregado (apenas OFFER_BONUS tipo 19) ---")
    offer = df_q1b[df_q1b["c_txn_type"] == 19].copy()
    if not offer.empty:
        print(offer[["dia", "qtd", "total_brl", "jogadores"]].to_string(index=False))


# ==================================================================
# FASE 2: BONUS POR BONUS_ID HOJE (top 15)
# Fonte: bonus_ec2.tbl_bonus_summary_details (TEM c_bonus_id)
# Join com tbl_bonus_profile para nome (se existir)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 2: BONUS POR BONUS_ID HOJE (top 15)")
print(f"{'#' * 80}")

# 2a: Breakdown por bonus_id via tbl_bonus_summary_details
sql_q2a = f"""
-- Q2a: Bonus emitidos hoje agrupados por bonus_id
-- Fonte: bonus_ec2.tbl_bonus_summary_details
-- c_bonus_id identifica a campanha/template do bonus
SELECT
    bs.c_bonus_id,
    bs.c_bonus_status,
    COUNT(*) AS qtd,
    ROUND(SUM(bs.c_actual_issued_amount / 100.0), 2) AS total_brl,
    COUNT(DISTINCT bs.c_ecr_id) AS jogadores,
    ROUND(AVG(bs.c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    ROUND(MAX(bs.c_actual_issued_amount / 100.0), 2) AS max_unitario
FROM bonus_ec2.tbl_bonus_summary_details bs
WHERE bs.c_issue_date >= TIMESTAMP '{HOJE_UTC_START}'
  AND bs.c_issue_date < TIMESTAMP '{HOJE_UTC_END}'
  AND bs.c_actual_issued_amount > 0
GROUP BY 1, 2
ORDER BY total_brl DESC
LIMIT 15
"""

df_q2a = run_query("Q2a: Bonus por bonus_id HOJE (tbl_bonus_summary_details)", sql_q2a)

# 2b: Tentar buscar nomes dos bonus via tbl_bonus_profile
print("\n--- Tentando obter nomes dos bonus via tbl_bonus_profile ---")
sql_q2b_names = """
-- Q2b: Colunas de tbl_bonus_profile (para obter nomes)
SHOW COLUMNS FROM bonus_ec2.tbl_bonus_profile
"""
df_bonus_profile_cols = run_query("Q2b: Colunas tbl_bonus_profile", sql_q2b_names)

# Se temos bonus_ids, tentar buscar nomes
if not df_q2a.empty:
    bonus_ids = df_q2a["c_bonus_id"].tolist()
    bonus_list = ",".join(str(int(x)) for x in bonus_ids if pd.notna(x))

    # Tentar buscar nome via tbl_bonus_profile
    sql_names = f"""
    SELECT * FROM bonus_ec2.tbl_bonus_profile
    WHERE c_bonus_id IN ({bonus_list})
    LIMIT 20
    """
    df_names = run_query("Q2c: Nomes dos bonus (tbl_bonus_profile)", sql_names)


# ==================================================================
# FASE 3: TOP 20 JOGADORES QUE MAIS RECEBERAM BONUS HOJE
# Fonte: bonus_ec2.tbl_bonus_summary_details (TEM c_ecr_id, c_bonus_id)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 3: TOP 20 JOGADORES -- BONUS HOJE")
print(f"{'#' * 80}")

sql_q3 = f"""
-- Q3: Top 20 jogadores que mais receberam bonus hoje
-- Fonte: bonus_ec2.tbl_bonus_summary_details
SELECT
    bs.c_ecr_id,
    COUNT(*) AS qtd_bonus,
    ROUND(SUM(bs.c_actual_issued_amount / 100.0), 2) AS total_bonus_brl,
    COUNT(DISTINCT bs.c_bonus_id) AS bonus_distintos,
    ROUND(MAX(bs.c_actual_issued_amount / 100.0), 2) AS max_bonus_unico,
    ROUND(AVG(bs.c_actual_issued_amount / 100.0), 2) AS ticket_medio
FROM bonus_ec2.tbl_bonus_summary_details bs
WHERE bs.c_issue_date >= TIMESTAMP '{HOJE_UTC_START}'
  AND bs.c_issue_date < TIMESTAMP '{HOJE_UTC_END}'
  AND bs.c_actual_issued_amount > 0
GROUP BY 1
ORDER BY total_bonus_brl DESC
LIMIT 20
"""

df_q3 = run_query("Q3: Top 20 jogadores -- bonus hoje", sql_q3)

# Verificar se sao test users
if not df_q3.empty:
    ecr_ids = df_q3["c_ecr_id"].tolist()
    ecr_list = ",".join(str(int(x)) for x in ecr_ids if pd.notna(x))

    sql_test_check = f"""
    -- Verificar se top jogadores sao test users
    SELECT ecr_id, external_id, is_test, registration_date
    FROM ps_bi.dim_user
    WHERE ecr_id IN ({ecr_list})
    """
    df_test = run_query("Q3b: Verificacao test users dos top 20", sql_test_check, database="ps_bi")

    if not df_test.empty:
        test_users = df_test[df_test["is_test"] == True]["ecr_id"].tolist()
        if test_users:
            print(f"\n  *** ATENCAO: {len(test_users)} dos top 20 sao TEST USERS! ***")
            print(f"  ECR IDs test: {test_users}")
        else:
            print("\n  OK: Nenhum dos top 20 e test user.")


# ==================================================================
# FASE 4: DISTRIBUICAO POR HORA HOJE (pico?)
# Fonte: fund_ec2 (transacoes financeiras, mais granular)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 4: BONUS POR HORA HOJE (deteccao de pico)")
print(f"{'#' * 80}")

sql_q4 = f"""
-- Q4: Bonus concedidos por hora BRT hoje (fund_ec2)
-- Tipo 19 = OFFER_BONUS (sem c_bonus_id nesta tabela)
SELECT
    hour(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS hora_brt,
    COUNT(*) AS qtd_bonus,
    ROUND(SUM(c_txn_amount / 100.0), 2) AS total_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(AVG(c_txn_amount / 100.0), 2) AS ticket_medio
FROM fund_ec2.tbl_bonus_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_start_time < TIMESTAMP '{HOJE_UTC_END}'
  AND c_txn_amount > 0
  AND c_txn_type = 19
GROUP BY 1
ORDER BY 1
"""

df_q4 = run_query("Q4: Bonus por hora BRT hoje (OFFER_BONUS)", sql_q4, database="fund_ec2")


# ==================================================================
# FASE 5: COMPARATIVO — HOJE vs MEDIA 7 DIAS
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 5: COMPARATIVO -- HOJE vs MEDIA ULTIMOS 7 DIAS")
print(f"{'#' * 80}")

sql_q5 = f"""
-- Q5: Total bonus concedidos (tipo 19) -- hoje vs media 7 dias
WITH por_dia AS (
    SELECT
        date(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
        COUNT(*) AS qtd,
        SUM(c_txn_amount / 100.0) AS total_brl,
        COUNT(DISTINCT c_ecr_id) AS jogadores
    FROM fund_ec2.tbl_bonus_sub_fund_txn
    WHERE c_start_time >= TIMESTAMP '{WEEK_UTC_START}'
      AND c_start_time < TIMESTAMP '{WEEK_UTC_END}'
      AND c_txn_amount > 0
      AND c_txn_type = 19
    GROUP BY 1
)
SELECT
    dia,
    qtd,
    ROUND(total_brl, 2) AS total_brl,
    jogadores,
    ROUND(total_brl / NULLIF(AVG(total_brl) OVER (), 0) * 100, 1) AS pct_vs_media
FROM por_dia
ORDER BY dia
"""

df_q5 = run_query("Q5: Comparativo diario (% vs media)", sql_q5, database="fund_ec2")


# ==================================================================
# FASE 6: BONUS UNITARIOS GRANDES HOJE (> R$500)
# Fonte: fund_ec2 (sem c_bonus_id) -- foco no valor e jogador
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 6: BONUS UNITARIOS GRANDES HOJE (> R$500)")
print(f"{'#' * 80}")

sql_q6 = f"""
-- Q6: Bonus individuais acima de R$500 hoje
-- Tipo 19 = OFFER_BONUS | SEM c_bonus_id nesta tabela
SELECT
    c_ecr_id,
    ROUND(c_txn_amount / 100.0, 2) AS valor_brl,
    c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS horario_brt,
    c_op_type
FROM fund_ec2.tbl_bonus_sub_fund_txn
WHERE c_start_time >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_start_time < TIMESTAMP '{HOJE_UTC_END}'
  AND c_txn_amount > 50000  -- > R$500 em centavos
  AND c_txn_type = 19
ORDER BY c_txn_amount DESC
LIMIT 30
"""

df_q6 = run_query("Q6: Bonus individuais > R$500 hoje", sql_q6, database="fund_ec2")

# Se encontrou bonus grandes, verificar esses jogadores
if not df_q6.empty:
    big_ecrs = df_q6["c_ecr_id"].unique().tolist()
    big_list = ",".join(str(int(x)) for x in big_ecrs if pd.notna(x))

    sql_big_check = f"""
    -- Perfil dos jogadores com bonus > R$500
    SELECT ecr_id, external_id, is_test, registration_date
    FROM ps_bi.dim_user
    WHERE ecr_id IN ({big_list})
    """
    df_big = run_query("Q6b: Perfil jogadores com bonus > R$500", sql_big_check, database="ps_bi")


# ==================================================================
# FASE 7: BONUS POR STATUS HOJE (tbl_bonus_summary_details)
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  FASE 7: DISTRIBUICAO POR STATUS HOJE")
print(f"{'#' * 80}")

sql_q7 = f"""
-- Q7: Contagem e valor por status de bonus hoje
SELECT
    c_bonus_status,
    COUNT(*) AS qtd,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_issue_date >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_issue_date < TIMESTAMP '{HOJE_UTC_END}'
  AND c_actual_issued_amount > 0
GROUP BY 1
ORDER BY total_brl DESC
"""

df_q7 = run_query("Q7: Bonus por status hoje", sql_q7)


# ==================================================================
# RESUMO FINAL COM ANALISE
# ==================================================================
print(f"\n\n{'#' * 80}")
print("  RESUMO DA INVESTIGACAO v2")
print(f"{'#' * 80}")

# Calcular metricas de resumo
if not df_q1.empty:
    hoje = df_q1[df_q1["dia"].astype(str) == "2026-04-13"]
    media_7d = df_q1["total_bonus_brl"].mean()
    if not hoje.empty:
        total_hoje = hoje["total_bonus_brl"].values[0]
        qtd_hoje = hoje["qtd_bonus"].values[0]
        jogadores_hoje = hoje["jogadores_distintos"].values[0]
        ticket_hoje = hoje["ticket_medio"].values[0]

        print(f"""
BONUS EMITIDOS HOJE (13/04/2026 BRT) -- via tbl_bonus_summary_details:
  Quantidade: {int(qtd_hoje)}
  Total BRL: {fmt_brl(total_hoje)}
  Ticket medio: {fmt_brl(ticket_hoje)}
  Jogadores distintos: {int(jogadores_hoje)}
  Media 7 dias: {fmt_brl(media_7d)}
  Razao hoje/media: {total_hoje / media_7d * 100:.1f}%
""")

        if total_hoje > media_7d * 1.5:
            print("  >>> ALERTA: Bonus hoje ACIMA de 150% da media!")
        elif total_hoje > media_7d * 1.2:
            print("  >>> ATENCAO: Bonus hoje acima de 120% da media.")
        else:
            print("  >>> OK: Bonus hoje dentro da faixa normal.")

if not df_q4.empty:
    pico = df_q4.loc[df_q4["total_brl"].idxmax()]
    print(f"\n  Pico de bonus: {int(pico['hora_brt'])}h BRT ({fmt_brl(pico['total_brl'])}, {int(pico['qtd_bonus'])} bonus)")

print(f"""
FONTES:
  - bonus_ec2.tbl_bonus_summary_details (c_issue_date, c_actual_issued_amount, c_bonus_id)
  - fund_ec2.tbl_bonus_sub_fund_txn (c_start_time, c_txn_amount, c_txn_type)
  - ps_bi.dim_user (is_test, registration_date)

NOTA TECNICA:
  - c_bonus_id NAO EXISTE em fund_ec2.tbl_bonus_sub_fund_txn (validado 13/04)
  - c_bonus_name/c_bonus_amount NAO EXISTEM em tbl_ecr_bonus_details (validado 13/04)
  - Breakdown por campanha feito via tbl_bonus_summary_details

ACAO RECOMENDADA:
  1. Verificar se houve campanha CRM disparada hoje fora do padrao
  2. Checar bonus_id que concentra maior valor (Q2a acima)
  3. Validar top jogadores vs depositos (bonus > deposito = risco)
  4. Conferir horario do pico vs disparo de campanha
  5. HOJE E DOMINGO (13/04) -- volume menor e esperado, mas ticket medio pode estar alto

Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")
