"""
Investigacao URGENTE: Campanha de bonus 20251029082323
Bonus altos (R$600-R$4000) para jogadores — O que e? Quem recebe? Quanto gastou?

Fonte principal: bonus_ec2.tbl_bonus_summary_details
  - c_bonus_id = ID da campanha global
  - c_ecr_bonus_id = ID do bonus atribuido ao player
  - c_actual_issued_amount = valor em centavos BRL (dividir por 100.0)
  - c_issue_date = data de emissao (UTC)

Regras:
  - bonus_ec2: valores em centavos (dividir por 100.0)
  - Timezone: UTC -> BRT (AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
  - c_actual_issued_amount validado (c_total_bonus_offered NAO EXISTE)
  - Sintaxe Presto/Trino (Athena)

Executar: python scripts/investigacao_campanha_bonus_13abr.py
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.athena import query_athena
import pandas as pd
from datetime import datetime
import traceback

pd.set_option("display.max_columns", 40)
pd.set_option("display.width", 250)
pd.set_option("display.max_colwidth", 80)
pd.set_option("display.float_format", lambda x: f"{x:,.2f}")
pd.set_option("display.max_rows", 100)

# ==================================================================
# CONSTANTES
# ==================================================================
CAMPANHA_ID = "20251029082323"

# Janelas temporais BRT -> UTC
HOJE_UTC_START = "2026-04-13 03:00:00"
HOJE_UTC_END   = "2026-04-14 03:00:00"

MES_UTC_START  = "2026-03-14 03:00:00"  # Ultimos 30 dias
MES_UTC_END    = "2026-04-14 03:00:00"

WEEK_UTC_START = "2026-04-06 03:00:00"
WEEK_UTC_END   = "2026-04-14 03:00:00"

SEP = "=" * 90


def fmt_brl(valor):
    """Formata valor em BRL."""
    if pd.isna(valor):
        return "R$ 0,00"
    return f"R$ {valor:,.2f}"


def run_query(descricao, sql, database="bonus_ec2"):
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
print(f"\n{'#' * 90}")
print(f"  INVESTIGACAO CAMPANHA BONUS {CAMPANHA_ID}")
print(f"  Data: 13/04/2026 (BRT) | Executado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(f"{'#' * 90}")


# ==================================================================
# ETAPA 0: Verificar colunas disponiveis na tabela
# ==================================================================
print(f"\n\n{'#' * 90}")
print("  ETAPA 0: SCHEMA DA TABELA tbl_bonus_summary_details")
print(f"{'#' * 90}")

sql_cols = "SHOW COLUMNS FROM bonus_ec2.tbl_bonus_summary_details"
df_cols = run_query("E0: Colunas tbl_bonus_summary_details", sql_cols)


# ==================================================================
# QUERY 1: Historico completo da campanha nos ultimos 30 dias
# Objetivo: Entender volume diario, gasto total, ticket medio
# ==================================================================
print(f"\n\n{'#' * 90}")
print(f"  QUERY 1: HISTORICO CAMPANHA {CAMPANHA_ID} (ULTIMOS 30 DIAS)")
print(f"{'#' * 90}")

# Tentar primeiro com c_bonus_id como string (padrao Athena)
sql_q1 = f"""
-- Q1: Historico diario da campanha 20251029082323 nos ultimos 30 dias BRT
-- Fonte: bonus_ec2.tbl_bonus_summary_details
-- c_bonus_id = ID campanha global | c_actual_issued_amount = centavos BRL
SELECT
    date(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
    COUNT(*) AS qtd_bonus,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_issued_brl,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    ROUND(MIN(c_actual_issued_amount / 100.0), 2) AS min_bonus,
    ROUND(MAX(c_actual_issued_amount / 100.0), 2) AS max_bonus,
    COUNT(DISTINCT c_ecr_id) AS jogadores_distintos
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_bonus_id = {CAMPANHA_ID}
  AND c_issue_date >= TIMESTAMP '{MES_UTC_START}'
  AND c_issue_date < TIMESTAMP '{MES_UTC_END}'
GROUP BY 1
ORDER BY 1
"""

df_q1 = run_query(f"Q1: Historico diario campanha {CAMPANHA_ID} (30d)", sql_q1)

# Se vazio, tentar como string
if df_q1.empty:
    print("\n  -> Tentando c_bonus_id como string...")
    sql_q1_str = sql_q1.replace(
        f"c_bonus_id = {CAMPANHA_ID}",
        f"c_bonus_id = '{CAMPANHA_ID}'"
    )
    df_q1 = run_query(f"Q1 (retry string): Historico campanha {CAMPANHA_ID}", sql_q1_str)

# Se ainda vazio, tentar com c_created_time em vez de c_issue_date
if df_q1.empty:
    print("\n  -> Tentando com c_created_time como campo de data...")
    sql_q1_alt = f"""
    SELECT
        date(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia,
        COUNT(*) AS qtd_bonus,
        ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_issued_brl,
        ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
        ROUND(MIN(c_actual_issued_amount / 100.0), 2) AS min_bonus,
        ROUND(MAX(c_actual_issued_amount / 100.0), 2) AS max_bonus,
        COUNT(DISTINCT c_ecr_id) AS jogadores_distintos
    FROM bonus_ec2.tbl_bonus_summary_details
    WHERE c_bonus_id = {CAMPANHA_ID}
      AND c_created_time >= TIMESTAMP '{MES_UTC_START}'
      AND c_created_time < TIMESTAMP '{MES_UTC_END}'
    GROUP BY 1
    ORDER BY 1
    """
    df_q1 = run_query(f"Q1 (retry c_created_time): Historico campanha {CAMPANHA_ID}", sql_q1_alt)


# ==================================================================
# QUERY 2: Jogadores que receberam da campanha HOJE
# Objetivo: Lista detalhada dos que receberam hoje
# ==================================================================
print(f"\n\n{'#' * 90}")
print(f"  QUERY 2: JOGADORES QUE RECEBERAM CAMPANHA {CAMPANHA_ID} HOJE")
print(f"{'#' * 90}")

sql_q2 = f"""
-- Q2: Detalhes dos bonus emitidos hoje pela campanha 20251029082323
-- Fonte: bonus_ec2.tbl_bonus_summary_details
-- Mostra cada bonus individual emitido
SELECT
    c_ecr_id,
    c_ecr_bonus_id,
    c_bonus_id,
    c_bonus_status,
    ROUND(c_actual_issued_amount / 100.0, 2) AS valor_brl,
    c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS emissao_brt
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_bonus_id = {CAMPANHA_ID}
  AND c_issue_date >= TIMESTAMP '{HOJE_UTC_START}'
  AND c_issue_date < TIMESTAMP '{HOJE_UTC_END}'
ORDER BY c_actual_issued_amount DESC
LIMIT 50
"""

df_q2 = run_query(f"Q2: Jogadores campanha {CAMPANHA_ID} hoje", sql_q2)

# Se vazio, tentar sem filtro de data (pode ser campanha inativa hoje)
if df_q2.empty:
    print("\n  -> Campanha nao emitiu hoje. Buscando ultimos registros (sem filtro data)...")
    sql_q2_all = f"""
    SELECT
        c_ecr_id,
        c_ecr_bonus_id,
        c_bonus_id,
        c_bonus_status,
        ROUND(c_actual_issued_amount / 100.0, 2) AS valor_brl,
        c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS emissao_brt
    FROM bonus_ec2.tbl_bonus_summary_details
    WHERE c_bonus_id = {CAMPANHA_ID}
    ORDER BY c_issue_date DESC
    LIMIT 50
    """
    df_q2 = run_query(f"Q2 (sem filtro data): Ultimos bonus campanha {CAMPANHA_ID}", sql_q2_all)


# ==================================================================
# QUERY 3: Perfil/Configuracao da campanha
# Objetivo: Nome, tipo, regras, segmento alvo
# ==================================================================
print(f"\n\n{'#' * 90}")
print(f"  QUERY 3: PERFIL/CONFIGURACAO DA CAMPANHA {CAMPANHA_ID}")
print(f"{'#' * 90}")

# 3a: Listar tabelas bonus_ec2 para encontrar tabela de config
sql_q3a = "SHOW TABLES IN bonus_ec2"
df_tables = run_query("Q3a: Tabelas em bonus_ec2", sql_q3a)

# 3b: Buscar na tbl_bonus_profile
sql_q3b = f"""
-- Q3b: Configuracao da campanha via tbl_bonus_profile
-- Busca todas as colunas para entender o perfil completo
SELECT *
FROM bonus_ec2.tbl_bonus_profile
WHERE c_bonus_id = {CAMPANHA_ID}
LIMIT 5
"""
df_q3b = run_query(f"Q3b: Perfil campanha {CAMPANHA_ID} (tbl_bonus_profile)", sql_q3b)

# Se vazio com numero, tentar string
if df_q3b.empty:
    sql_q3b_str = sql_q3b.replace(
        f"c_bonus_id = {CAMPANHA_ID}",
        f"c_bonus_id = '{CAMPANHA_ID}'"
    )
    df_q3b = run_query(f"Q3b (retry string): Perfil campanha {CAMPANHA_ID}", sql_q3b_str)

# 3c: Buscar segmento da campanha (tbl_bonus_segment_details)
sql_q3c = f"""
-- Q3c: Segmento CRM vinculado a campanha
-- c_rule_name = nome do segmento (ex: 'NEW_USERS', 'VIP', etc.)
SELECT *
FROM bonus_ec2.tbl_bonus_segment_details
WHERE c_bonus_id = {CAMPANHA_ID}
LIMIT 20
"""
df_q3c = run_query(f"Q3c: Segmento da campanha {CAMPANHA_ID}", sql_q3c)

if df_q3c.empty:
    sql_q3c_str = sql_q3c.replace(
        f"c_bonus_id = {CAMPANHA_ID}",
        f"c_bonus_id = '{CAMPANHA_ID}'"
    )
    df_q3c = run_query(f"Q3c (retry string): Segmento campanha {CAMPANHA_ID}", sql_q3c_str)

# 3d: Verificar em tbl_ecr_bonus_details (bonus ativos) e tbl_ecr_bonus_details_inactive
for tbl_name in ["tbl_ecr_bonus_details", "tbl_ecr_bonus_details_inactive"]:
    sql_ecr_bonus = f"""
    -- Q3d: Bonus da campanha em {tbl_name}
    SELECT *
    FROM bonus_ec2.{tbl_name}
    WHERE c_bonus_id = {CAMPANHA_ID}
    LIMIT 10
    """
    df_ecr = run_query(f"Q3d: Campanha {CAMPANHA_ID} em {tbl_name}", sql_ecr_bonus)
    if df_ecr.empty:
        sql_ecr_str = sql_ecr_bonus.replace(
            f"c_bonus_id = {CAMPANHA_ID}",
            f"c_bonus_id = '{CAMPANHA_ID}'"
        )
        df_ecr = run_query(f"Q3d (retry string): {tbl_name}", sql_ecr_str)


# ==================================================================
# QUERY 4: TOP 20 CAMPANHAS COM MAIOR GASTO (7 DIAS)
# Objetivo: Contexto — quanto a 20251029082323 pesa vs outras
# ==================================================================
print(f"\n\n{'#' * 90}")
print("  QUERY 4: TOP 20 CAMPANHAS POR GASTO (ULTIMOS 7 DIAS)")
print(f"{'#' * 90}")

sql_q4 = f"""
-- Q4: Ranking de campanhas de bonus por gasto total (7 dias)
-- Permite comparar campanha alvo vs demais
SELECT
    c_bonus_id,
    COUNT(*) AS qtd_bonus,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    ROUND(MAX(c_actual_issued_amount / 100.0), 2) AS max_individual,
    ROUND(MIN(c_actual_issued_amount / 100.0), 2) AS min_individual
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_issue_date >= TIMESTAMP '{WEEK_UTC_START}'
  AND c_issue_date < TIMESTAMP '{WEEK_UTC_END}'
  AND c_actual_issued_amount > 0
GROUP BY 1
ORDER BY total_brl DESC
LIMIT 20
"""

df_q4 = run_query("Q4: Top 20 campanhas por gasto (7 dias)", sql_q4)


# ==================================================================
# QUERY 5: JOGADORES DA CAMPANHA — Verificacao de perfil
# Objetivo: Sao test users? Quando se registraram? Quanto depositaram?
# ==================================================================
print(f"\n\n{'#' * 90}")
print(f"  QUERY 5: PERFIL DOS JOGADORES DA CAMPANHA {CAMPANHA_ID}")
print(f"{'#' * 90}")

# Pegar ECR IDs da campanha (todos os que receberam nos ultimos 30 dias)
sql_q5_ecrs = f"""
-- Q5a: Jogadores unicos da campanha (30 dias)
SELECT DISTINCT c_ecr_id
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_bonus_id = {CAMPANHA_ID}
  AND c_issue_date >= TIMESTAMP '{MES_UTC_START}'
  AND c_issue_date < TIMESTAMP '{MES_UTC_END}'
"""

df_q5_ecrs = run_query(f"Q5a: ECR IDs da campanha {CAMPANHA_ID}", sql_q5_ecrs)

if not df_q5_ecrs.empty:
    ecr_ids = df_q5_ecrs["c_ecr_id"].tolist()
    ecr_list = ",".join(str(int(x)) for x in ecr_ids if pd.notna(x))
    total_jogadores = len(ecr_ids)
    print(f"\n  Total de jogadores unicos: {total_jogadores}")

    # Limitar a 200 para query ps_bi
    if total_jogadores > 200:
        print(f"  (Limitando a 200 para consulta de perfil)")
        ecr_list = ",".join(str(int(x)) for x in ecr_ids[:200] if pd.notna(x))

    # 5b: Perfil via ps_bi.dim_user
    sql_q5b = f"""
    -- Q5b: Perfil dos jogadores no dim_user
    -- is_test = filtro obrigatorio | registration_date = data cadastro
    SELECT
        du.ecr_id,
        du.external_id,
        du.is_test,
        du.registration_date,
        du.country
    FROM ps_bi.dim_user du
    WHERE du.ecr_id IN ({ecr_list})
    """
    df_q5b = run_query(f"Q5b: Perfil jogadores campanha {CAMPANHA_ID}", sql_q5b, database="ps_bi")

    if not df_q5b.empty:
        test_count = df_q5b[df_q5b["is_test"] == True].shape[0]
        real_count = df_q5b[df_q5b["is_test"] == False].shape[0]
        print(f"\n  Test users: {test_count}")
        print(f"  Jogadores reais: {real_count}")
        if test_count > 0:
            print(f"  *** ATENCAO: {test_count} test users recebendo bonus desta campanha! ***")

    # 5c: Quanto esses jogadores depositaram (contexto risco)
    sql_q5c = f"""
    -- Q5c: Total depositado por esses jogadores (30 dias)
    -- Para avaliar se bonus > depositos (risco)
    SELECT
        c_ecr_id,
        COUNT(*) AS qtd_depositos,
        ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) AS total_depositado_brl
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_ecr_id IN ({ecr_list})
      AND c_start_time >= TIMESTAMP '{MES_UTC_START}'
      AND c_start_time < TIMESTAMP '{MES_UTC_END}'
      AND c_txn_status = 'SUCCESS'
      AND c_txn_type = 1  -- DEPOSIT
    GROUP BY 1
    ORDER BY total_depositado_brl DESC
    """
    df_q5c = run_query(f"Q5c: Depositos dos jogadores campanha {CAMPANHA_ID}", sql_q5c, database="fund_ec2")

    # 5d: Cruzar bonus recebido vs depositos
    if not df_q5c.empty:
        # Total bonus por jogador
        sql_q5d = f"""
        SELECT
            c_ecr_id,
            COUNT(*) AS qtd_bonus,
            ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_bonus_brl
        FROM bonus_ec2.tbl_bonus_summary_details
        WHERE c_bonus_id = {CAMPANHA_ID}
          AND c_issue_date >= TIMESTAMP '{MES_UTC_START}'
          AND c_issue_date < TIMESTAMP '{MES_UTC_END}'
        GROUP BY 1
        ORDER BY total_bonus_brl DESC
        """
        df_q5d = run_query(f"Q5d: Total bonus por jogador campanha {CAMPANHA_ID}", sql_q5d)

        if not df_q5d.empty:
            # Merge bonus vs depositos
            df_merge = pd.merge(
                df_q5d, df_q5c,
                on="c_ecr_id", how="left"
            )
            df_merge["total_depositado_brl"] = df_merge["total_depositado_brl"].fillna(0)
            df_merge["razao_bonus_deposito"] = (
                df_merge["total_bonus_brl"] / df_merge["total_depositado_brl"].replace(0, float('nan'))
            ).round(2)

            print(f"\n{SEP}")
            print("  BONUS vs DEPOSITO por jogador (top 20)")
            print(f"{SEP}")
            print(df_merge.head(20).to_string(index=False))

            # Alertar jogadores com bonus > deposito
            risco = df_merge[
                (df_merge["total_bonus_brl"] > df_merge["total_depositado_brl"]) &
                (df_merge["total_depositado_brl"] > 0)
            ]
            sem_deposito = df_merge[df_merge["total_depositado_brl"] == 0]

            print(f"\n  Jogadores com bonus > deposito: {len(risco)}")
            print(f"  Jogadores SEM deposito (bonus gratis): {len(sem_deposito)}")


# ==================================================================
# QUERY 6: DISTRIBUICAO DE FAIXAS DE VALOR DA CAMPANHA
# Objetivo: Entender a distribuicao dos valores de bonus
# ==================================================================
print(f"\n\n{'#' * 90}")
print(f"  QUERY 6: DISTRIBUICAO DE FAIXAS DE VALOR — CAMPANHA {CAMPANHA_ID}")
print(f"{'#' * 90}")

sql_q6 = f"""
-- Q6: Distribuicao de bonus por faixa de valor
-- Ajuda a entender se os R$600-R$4000 sao outliers ou padrao
SELECT
    CASE
        WHEN c_actual_issued_amount / 100.0 < 10 THEN '01. < R$10'
        WHEN c_actual_issued_amount / 100.0 < 50 THEN '02. R$10-50'
        WHEN c_actual_issued_amount / 100.0 < 100 THEN '03. R$50-100'
        WHEN c_actual_issued_amount / 100.0 < 200 THEN '04. R$100-200'
        WHEN c_actual_issued_amount / 100.0 < 500 THEN '05. R$200-500'
        WHEN c_actual_issued_amount / 100.0 < 1000 THEN '06. R$500-1000'
        WHEN c_actual_issued_amount / 100.0 < 2000 THEN '07. R$1000-2000'
        WHEN c_actual_issued_amount / 100.0 < 4000 THEN '08. R$2000-4000'
        ELSE '09. > R$4000'
    END AS faixa_valor,
    COUNT(*) AS qtd,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_brl,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio,
    COUNT(DISTINCT c_ecr_id) AS jogadores
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_bonus_id = {CAMPANHA_ID}
  AND c_issue_date >= TIMESTAMP '{MES_UTC_START}'
  AND c_issue_date < TIMESTAMP '{MES_UTC_END}'
GROUP BY 1
ORDER BY 1
"""

df_q6 = run_query(f"Q6: Faixas de valor campanha {CAMPANHA_ID}", sql_q6)


# ==================================================================
# QUERY 7: VERIFICAR STATUS DOS BONUS DA CAMPANHA
# Objetivo: Quantos foram convertidos, expirados, cancelados
# ==================================================================
print(f"\n\n{'#' * 90}")
print(f"  QUERY 7: STATUS DOS BONUS — CAMPANHA {CAMPANHA_ID}")
print(f"{'#' * 90}")

sql_q7 = f"""
-- Q7: Breakdown por status dos bonus da campanha
-- BONUS_OFFER = ativo | BONUS_ISSUED_OFFER = convertido (wagering cumprido)
-- EXPIRED = expirou | DROPPED = cancelado
SELECT
    c_bonus_status,
    COUNT(*) AS qtd,
    ROUND(SUM(c_actual_issued_amount / 100.0), 2) AS total_brl,
    COUNT(DISTINCT c_ecr_id) AS jogadores,
    ROUND(AVG(c_actual_issued_amount / 100.0), 2) AS ticket_medio
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_bonus_id = {CAMPANHA_ID}
  AND c_issue_date >= TIMESTAMP '{MES_UTC_START}'
  AND c_issue_date < TIMESTAMP '{MES_UTC_END}'
GROUP BY 1
ORDER BY total_brl DESC
"""

df_q7 = run_query(f"Q7: Status dos bonus campanha {CAMPANHA_ID}", sql_q7)


# ==================================================================
# RESUMO FINAL
# ==================================================================
print(f"\n\n{'#' * 90}")
print(f"  RESUMO FINAL — INVESTIGACAO CAMPANHA {CAMPANHA_ID}")
print(f"{'#' * 90}")

# Totais acumulados
if not df_q1.empty:
    total_gasto = df_q1["total_issued_brl"].sum()
    total_qtd = df_q1["qtd_bonus"].sum()
    total_jogadores = df_q1["jogadores_distintos"].sum()  # aproximado, pode ter repeticao entre dias
    ticket_medio_geral = total_gasto / total_qtd if total_qtd > 0 else 0
    max_bonus = df_q1["max_bonus"].max()
    primeiro_dia = df_q1["dia"].min()
    ultimo_dia = df_q1["dia"].max()

    print(f"""
CAMPANHA: {CAMPANHA_ID}
Periodo analisado: {primeiro_dia} a {ultimo_dia} (30 dias BRT)

NUMEROS GERAIS:
  Total gasto: {fmt_brl(total_gasto)}
  Qtd bonus emitidos: {int(total_qtd)}
  Ticket medio geral: {fmt_brl(ticket_medio_geral)}
  Maior bonus individual: {fmt_brl(max_bonus)}
  Dias ativos no periodo: {len(df_q1)}

DISTRIBUICAO POR DIA:
""")
    for _, row in df_q1.iterrows():
        print(f"  {row['dia']} | Qtd: {int(row['qtd_bonus']):>5} | Total: {fmt_brl(row['total_issued_brl']):>14} | Ticket: {fmt_brl(row['ticket_medio']):>10} | Max: {fmt_brl(row['max_bonus']):>10} | Jogadores: {int(row['jogadores_distintos']):>5}")

    # Alertas
    print(f"\nALERTAS:")
    if max_bonus >= 4000:
        print(f"  [CRITICO] Bonus individual de ate {fmt_brl(max_bonus)} — acima de R$4.000!")
    if ticket_medio_geral >= 500:
        print(f"  [ALTO] Ticket medio de {fmt_brl(ticket_medio_geral)} — campanha de alto valor!")
    if total_gasto >= 100000:
        print(f"  [ALTO] Gasto total acima de R$100K no periodo!")

else:
    print("\n  [ATENCAO] Nenhum dado encontrado para a campanha nos ultimos 30 dias.")
    print("  Possibilidades:")
    print("  1. c_bonus_id pode ser VARCHAR, nao BIGINT — verificar tipo")
    print("  2. Campanha pode ter outro campo identificador")
    print("  3. Campanha pode estar em outra tabela")

# Status
if not df_q7.empty:
    print(f"\nSTATUS DOS BONUS:")
    for _, row in df_q7.iterrows():
        print(f"  {row['c_bonus_status']:>30} | Qtd: {int(row['qtd']):>5} | Total: {fmt_brl(row['total_brl']):>14}")

# Top campanhas comparativo
if not df_q4.empty:
    # Localizar campanha alvo no ranking
    alvo = df_q4[df_q4["c_bonus_id"].astype(str) == CAMPANHA_ID]
    if not alvo.empty:
        rank = df_q4.index[df_q4["c_bonus_id"].astype(str) == CAMPANHA_ID].tolist()[0] + 1
        total_todas = df_q4["total_brl"].sum()
        total_alvo = alvo["total_brl"].values[0]
        pct = total_alvo / total_todas * 100
        print(f"\nRANKING SEMANAL:")
        print(f"  Posicao: #{rank} de 20 campanhas com maior gasto")
        print(f"  Gasto: {fmt_brl(total_alvo)} ({pct:.1f}% do top 20)")
    else:
        print(f"\n  Campanha {CAMPANHA_ID} NAO esta no top 20 da semana.")

print(f"""
FONTES UTILIZADAS:
  - bonus_ec2.tbl_bonus_summary_details (bonus emitidos, c_actual_issued_amount)
  - bonus_ec2.tbl_bonus_profile (configuracao/perfil campanha)
  - bonus_ec2.tbl_bonus_segment_details (segmento CRM vinculado)
  - bonus_ec2.tbl_ecr_bonus_details / _inactive (bonus ativos/inativos)
  - ps_bi.dim_user (perfil jogador, is_test)
  - fund_ec2.tbl_real_fund_txn (depositos para contexto risco)

ACOES RECOMENDADAS:
  1. Identificar QUEM criou esta campanha no backoffice (nome/responsavel)
  2. Verificar se os valores R$600-R$4000 estao dentro da politica de bonus
  3. Cruzar jogadores com bonus vs depositos (bonus > deposito = risco)
  4. Se houver test users recebendo, verificar se e esperado
  5. Validar regras de wagering aplicadas (rollover minimo)
  6. Escalar para o Head (Castrin) se gasto total for >R$50K ou ticket medio >R$500

Executado em: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
""")
