"""
Investigacao URGENTE: Saques altos na MultiBet em 13/04/2026.

Objetivo: Extrair dados de saques (cashier_ec2.tbl_cashier_cashout) para
entender volume, ticket medio, top jogadores e distribuicao por faixa de valor.

Regras aplicadas:
- cashier_ec2: status = 'co_success', valor = c_amount_in_ecr_ccy / 100.0
- NOTA: c_amount_in_ecr_ccy NAO EXISTE nesta tabela. Usar c_amount_in_ecr_ccy.
- Filtro temporal para evitar full scan S3 (custo Athena)
- Timezone: UTC -> BRT (America/Sao_Paulo)
- 13/04 BRT = 13/04 03:00 UTC a 14/04 03:00 UTC
"""

import sys
import os

# Garantir que o projeto esta no path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 40)
pd.set_option('display.float_format', '{:,.2f}'.format)


def query_1_saques_7dias():
    """Query 1: Saques hoje vs ultimos 7 dias (baseline)."""
    sql = """
    -- Q1: Volume de saques por dia (ultimos 7 dias + hoje)
    -- Fonte: cashier_ec2.tbl_cashier_cashout
    -- Status confirmado: co_success
    -- Valor em centavos -> dividir por 100
    SELECT
        date(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') as dia,
        COUNT(*) as qtd_saques,
        SUM(c_amount_in_ecr_ccy / 100.0) as total_saques_brl,
        AVG(c_amount_in_ecr_ccy / 100.0) as ticket_medio_saque,
        COUNT(DISTINCT c_ecr_id) as jogadores_distintos,
        MAX(c_amount_in_ecr_ccy / 100.0) as maior_saque
    FROM cashier_ec2.tbl_cashier_cashout
    WHERE c_created_time >= TIMESTAMP '2026-04-06 03:00:00'
      AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_status = 'co_success'
    GROUP BY 1
    ORDER BY 1
    """
    print("=" * 120)
    print("QUERY 1: SAQUES DIARIOS — ULTIMOS 7 DIAS + HOJE (13/04/2026)")
    print("Fonte: cashier_ec2.tbl_cashier_cashout | Status: co_success | Valores em BRL")
    print("=" * 120)

    try:
        df = query_athena(sql, database="cashier_ec2")
        if df.empty:
            print("[AVISO] Nenhum resultado retornado.")
            return None

        # Formatar para exibicao
        print(df.to_string(index=False))
        print()

        # Calcular media dos 7 dias anteriores vs hoje
        if len(df) > 1:
            hoje = df[df['dia'] == '2026-04-13']
            anteriores = df[df['dia'] != '2026-04-13']

            if not hoje.empty and not anteriores.empty:
                media_ant = anteriores['total_saques_brl'].mean()
                total_hoje = hoje['total_saques_brl'].iloc[0]
                variacao = ((total_hoje - media_ant) / media_ant) * 100

                media_qtd_ant = anteriores['qtd_saques'].mean()
                qtd_hoje = hoje['qtd_saques'].iloc[0]
                var_qtd = ((qtd_hoje - media_qtd_ant) / media_qtd_ant) * 100

                print(f"--- ANALISE COMPARATIVA ---")
                print(f"Media diaria (7 dias anteriores): R$ {media_ant:,.2f} ({media_qtd_ant:,.0f} saques)")
                print(f"Hoje (13/04):                     R$ {total_hoje:,.2f} ({qtd_hoje:,.0f} saques)")
                print(f"Variacao valor:  {variacao:+.1f}%")
                print(f"Variacao volume: {var_qtd:+.1f}%")
                print()

        return df
    except Exception as e:
        print(f"[ERRO] Query 1 falhou: {e}")
        return None


def query_2_top_jogadores():
    """Query 2: Top 20 jogadores que mais sacaram hoje."""
    sql = """
    -- Q2: Top 20 jogadores por valor sacado hoje (13/04 BRT)
    -- Objetivo: identificar jogadores com saques anormais
    SELECT
        c_ecr_id,
        COUNT(*) as qtd_saques,
        SUM(c_amount_in_ecr_ccy / 100.0) as total_sacado_brl,
        MIN(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') as primeiro_saque,
        MAX(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') as ultimo_saque
    FROM cashier_ec2.tbl_cashier_cashout
    WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
      AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_status = 'co_success'
    GROUP BY 1
    ORDER BY total_sacado_brl DESC
    LIMIT 20
    """
    print("=" * 120)
    print("QUERY 2: TOP 20 JOGADORES — MAIOR VOLUME DE SAQUES HOJE (13/04/2026)")
    print("Fonte: cashier_ec2.tbl_cashier_cashout | Status: co_success | Valores em BRL")
    print("=" * 120)

    try:
        df = query_athena(sql, database="cashier_ec2")
        if df.empty:
            print("[AVISO] Nenhum resultado retornado.")
            return None

        print(df.to_string(index=False))
        print()

        # Resumo rapido
        total_top20 = df['total_sacado_brl'].sum()
        print(f"--- RESUMO TOP 20 ---")
        print(f"Soma total sacado (top 20): R$ {total_top20:,.2f}")
        print(f"Maior sacador:              R$ {df['total_sacado_brl'].iloc[0]:,.2f} (ecr_id: {df['c_ecr_id'].iloc[0]})")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Query 2 falhou: {e}")
        return None


def query_3_faixa_valor():
    """Query 3: Distribuicao de saques por faixa de valor hoje."""
    sql = """
    -- Q3: Distribuicao de saques por faixa de valor (13/04 BRT)
    -- Objetivo: entender se o pico e por volume de pequenos saques ou poucos saques grandes
    SELECT
        CASE
            WHEN c_amount_in_ecr_ccy/100.0 < 100 THEN 'Ate R$100'
            WHEN c_amount_in_ecr_ccy/100.0 < 500 THEN 'R$100-500'
            WHEN c_amount_in_ecr_ccy/100.0 < 1000 THEN 'R$500-1K'
            WHEN c_amount_in_ecr_ccy/100.0 < 5000 THEN 'R$1K-5K'
            WHEN c_amount_in_ecr_ccy/100.0 < 10000 THEN 'R$5K-10K'
            ELSE 'Acima R$10K'
        END as faixa_valor,
        COUNT(*) as qtd,
        SUM(c_amount_in_ecr_ccy/100.0) as total_brl,
        COUNT(DISTINCT c_ecr_id) as jogadores
    FROM cashier_ec2.tbl_cashier_cashout
    WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
      AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_status = 'co_success'
    GROUP BY 1
    ORDER BY total_brl DESC
    """
    print("=" * 120)
    print("QUERY 3: DISTRIBUICAO POR FAIXA DE VALOR — SAQUES HOJE (13/04/2026)")
    print("Fonte: cashier_ec2.tbl_cashier_cashout | Status: co_success | Valores em BRL")
    print("=" * 120)

    try:
        df = query_athena(sql, database="cashier_ec2")
        if df.empty:
            print("[AVISO] Nenhum resultado retornado.")
            return None

        print(df.to_string(index=False))
        print()

        # Percentual de concentracao
        total_geral = df['total_brl'].sum()
        if total_geral > 0:
            df['pct_valor'] = (df['total_brl'] / total_geral * 100).round(1)
            print(f"--- CONCENTRACAO POR FAIXA ---")
            for _, row in df.iterrows():
                print(f"  {row['faixa_valor']:>15s}: {row['pct_valor']:5.1f}% do valor total  ({row['qtd']:,} saques, {row['jogadores']:,} jogadores)")
            print(f"\n  TOTAL GERAL: R$ {total_geral:,.2f}")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Query 3 falhou: {e}")
        return None


if __name__ == "__main__":
    print()
    print("*" * 120)
    print("  INVESTIGACAO URGENTE: SAQUES ALTOS — MultiBet — 13/04/2026")
    print("  Executado em:", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("*" * 120)
    print()

    df1 = query_1_saques_7dias()
    df2 = query_2_top_jogadores()
    df3 = query_3_faixa_valor()

    print("=" * 120)
    print("FIM DA INVESTIGACAO")
    print("=" * 120)
