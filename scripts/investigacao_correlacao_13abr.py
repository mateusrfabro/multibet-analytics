"""
Investigacao URGENTE: Correlacao entre sacadores e bonificados — 13/04/2026.

Objetivo: Verificar se jogadores que sacaram hoje tambem receberam bonus
(possivel abuso de bonus).

Regras aplicadas:
- cashier_ec2.tbl_cashier_cashout: status = 'co_success', valor = c_amount_in_ecr_ccy / 100.0
  (CORRECAO: c_confirmed_amount_in_ecr_ccy NAO EXISTE nesta tabela — validado SHOW COLUMNS 13/04)
- cashier_ec2.tbl_cashier_deposit: status = 'txn_confirmed_success', valor = c_confirmed_amount_in_ecr_ccy / 100.0
- bonus_ec2.tbl_bonus_summary_details: valor = c_actual_issued_amount / 100.0, data = c_issue_date
  (CORRECAO: user pediu tbl_bonus, mas tabela correta eh tbl_bonus_summary_details — validado schema_bonus.md e risk_deep_dive_player.py)
- Filtro temporal UTC para BRT: 13/04 BRT = 13/04 03:00 UTC a 14/04 03:00 UTC
- Timezone: AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo'
- Sem filtro test_user pois cashier/bonus nao tem essa coluna (seria necessario JOIN com ecr_ec2)

Estimativa de custo Athena:
- cashier_ec2.tbl_cashier_cashout (1 dia): ~100-200MB
- cashier_ec2.tbl_cashier_deposit (1 dia): ~100-200MB
- bonus_ec2.tbl_bonus_summary_details (1 dia): ~50-100MB
- Total estimado: ~350-500MB (custo aceitavel)
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


def query_1_cruzamento_saque_bonus():
    """
    Query 1: Jogadores que SACARAM e RECEBERAM BONUS no mesmo dia (13/04/2026).
    Cruzamento principal — identifica possivel abuso de bonus.
    """
    sql = """
    -- Q1: Cruzamento sacadores x bonificados em 13/04/2026 (BRT)
    -- Fonte saques: cashier_ec2.tbl_cashier_cashout (co_success)
    -- Fonte bonus: bonus_ec2.tbl_bonus_summary_details (c_actual_issued_amount > 0)
    -- NOTA: bonus usa c_issue_date (nao c_created_time) — validado risk_deep_dive_player.py
    WITH sacadores AS (
        -- Jogadores que sacaram hoje com sucesso
        SELECT
            c_ecr_id,
            COUNT(*) AS qtd_saques,
            SUM(c_amount_in_ecr_ccy / 100.0) AS total_sacado
        FROM cashier_ec2.tbl_cashier_cashout
        WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
          AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
          AND c_txn_status = 'co_success'
        GROUP BY 1
    ),
    bonificados AS (
        -- Jogadores que receberam bonus hoje
        -- Tabela correta: tbl_bonus_summary_details (nao tbl_bonus)
        -- Coluna de data: c_issue_date (validado empiricamente)
        SELECT
            c_ecr_id,
            COUNT(*) AS qtd_bonus,
            SUM(c_actual_issued_amount / 100.0) AS total_bonus
        FROM bonus_ec2.tbl_bonus_summary_details
        WHERE c_issue_date >= TIMESTAMP '2026-04-13 03:00:00'
          AND c_issue_date < TIMESTAMP '2026-04-14 03:00:00'
          AND c_actual_issued_amount > 0
        GROUP BY 1
    )
    -- INNER JOIN: somente quem aparece nas DUAS listas
    SELECT
        s.c_ecr_id,
        s.qtd_saques,
        s.total_sacado,
        b.qtd_bonus,
        b.total_bonus,
        s.total_sacado - b.total_bonus AS saque_liquido
    FROM sacadores s
    INNER JOIN bonificados b ON s.c_ecr_id = b.c_ecr_id
    ORDER BY s.total_sacado DESC
    LIMIT 30
    """

    print("=" * 120)
    print("QUERY 1: CRUZAMENTO — JOGADORES QUE SACARAM E RECEBERAM BONUS HOJE (13/04/2026)")
    print("Fontes: cashier_ec2.tbl_cashier_cashout + bonus_ec2.tbl_bonus_summary_details")
    print("=" * 120)

    try:
        df = query_athena(sql, database="cashier_ec2")
        if df.empty:
            print("[AVISO] Nenhum jogador encontrado no cruzamento saque x bonus.")
            return None

        print(df.to_string(index=False))
        print()

        # Analise rapida
        total_sacado = df['total_sacado'].sum()
        total_bonus = df['total_bonus'].sum()
        ratio = (total_bonus / total_sacado * 100) if total_sacado > 0 else 0

        print(f"--- ANALISE TOP 30 SACADORES COM BONUS ---")
        print(f"Total sacado (top 30):         R$ {total_sacado:,.2f}")
        print(f"Total bonus recebido (top 30): R$ {total_bonus:,.2f}")
        print(f"Ratio bonus/saque:             {ratio:.1f}%")
        print(f"Jogadores neste cruzamento:    {len(df)}")

        # Flag de alerta
        alerta = df[df['total_bonus'] > df['total_sacado'] * 0.5]
        if not alerta.empty:
            print(f"\n[ALERTA] {len(alerta)} jogadores com bonus > 50% do valor sacado:")
            for _, row in alerta.iterrows():
                pct = (row['total_bonus'] / row['total_sacado'] * 100) if row['total_sacado'] > 0 else 0
                print(f"  ecr_id {row['c_ecr_id']}: sacou R$ {row['total_sacado']:,.2f}, bonus R$ {row['total_bonus']:,.2f} ({pct:.0f}%)")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Query 1 falhou: {e}")
        import traceback
        traceback.print_exc()
        return None


def query_2_resumo_correlacao():
    """
    Query 2: Resumo quantitativo da correlacao sacadores vs bonificados.
    """
    sql = """
    -- Q2: Resumo da correlacao sacadores x bonificados (13/04 BRT)
    -- Objetivo: quantificar sobreposicao entre quem sacou e quem recebeu bonus
    WITH sacadores AS (
        SELECT DISTINCT c_ecr_id
        FROM cashier_ec2.tbl_cashier_cashout
        WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
          AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
          AND c_txn_status = 'co_success'
    ),
    bonificados AS (
        SELECT DISTINCT c_ecr_id
        FROM bonus_ec2.tbl_bonus_summary_details
        WHERE c_issue_date >= TIMESTAMP '2026-04-13 03:00:00'
          AND c_issue_date < TIMESTAMP '2026-04-14 03:00:00'
          AND c_actual_issued_amount > 0
    )
    SELECT
        (SELECT COUNT(*) FROM sacadores) AS total_sacadores,
        (SELECT COUNT(*) FROM bonificados) AS total_bonificados,
        (SELECT COUNT(*) FROM sacadores s INNER JOIN bonificados b ON s.c_ecr_id = b.c_ecr_id) AS sacadores_com_bonus,
        ROUND(
            CAST((SELECT COUNT(*) FROM sacadores s INNER JOIN bonificados b ON s.c_ecr_id = b.c_ecr_id) AS DOUBLE) /
            NULLIF(CAST((SELECT COUNT(*) FROM sacadores) AS DOUBLE), 0) * 100, 1
        ) AS pct_sacadores_com_bonus
    """

    print("=" * 120)
    print("QUERY 2: RESUMO DA CORRELACAO — SACADORES vs BONIFICADOS (13/04/2026)")
    print("=" * 120)

    try:
        df = query_athena(sql, database="cashier_ec2")
        if df.empty:
            print("[AVISO] Nenhum resultado retornado.")
            return None

        print(df.to_string(index=False))
        print()

        # Interpretacao
        row = df.iloc[0]
        total_sac = row['total_sacadores']
        total_bon = row['total_bonificados']
        overlap = row['sacadores_com_bonus']
        pct = row['pct_sacadores_com_bonus']

        print(f"--- INTERPRETACAO ---")
        print(f"Sacadores hoje:                {total_sac:,.0f} jogadores")
        print(f"Bonificados hoje:              {total_bon:,.0f} jogadores")
        print(f"Sacadores QUE receberam bonus: {overlap:,.0f} jogadores ({pct:.1f}%)")
        print(f"Sacadores SEM bonus:           {total_sac - overlap:,.0f} jogadores ({100 - pct:.1f}%)")

        # Classificacao do risco
        if pct >= 50:
            print(f"\n[CRITICO] Mais da metade dos sacadores recebeu bonus hoje — investigar abuso!")
        elif pct >= 30:
            print(f"\n[ATENCAO] ~{pct:.0f}% dos sacadores recebeu bonus — correlacao significativa.")
        elif pct >= 15:
            print(f"\n[MODERADO] ~{pct:.0f}% dos sacadores recebeu bonus — nivel esperado para operacao CRM ativa.")
        else:
            print(f"\n[NORMAL] Apenas {pct:.0f}% dos sacadores recebeu bonus — correlacao baixa.")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Query 2 falhou: {e}")
        import traceback
        traceback.print_exc()
        return None


def query_3_perfil_completo_top20():
    """
    Query 3: Top 20 sacadores com perfil completo (saque + deposito + bonus).
    Objetivo: entender se jogador deposita pouco, ganha bonus e saca.
    """
    sql = """
    -- Q3: Top 20 sacadores — perfil completo (saque, deposito, bonus) em 13/04 BRT
    -- Objetivo: identificar padrao de abuso (deposito baixo + bonus alto + saque grande)
    WITH saques AS (
        -- Saques confirmados hoje
        SELECT
            c_ecr_id,
            SUM(c_amount_in_ecr_ccy / 100.0) AS total_saq
        FROM cashier_ec2.tbl_cashier_cashout
        WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
          AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
          AND c_txn_status = 'co_success'
        GROUP BY 1
    ),
    deps AS (
        -- Depositos confirmados hoje
        -- Coluna: c_confirmed_amount_in_ecr_ccy (validado SHOW COLUMNS)
        SELECT
            c_ecr_id,
            SUM(c_confirmed_amount_in_ecr_ccy / 100.0) AS total_dep
        FROM cashier_ec2.tbl_cashier_deposit
        WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
          AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
          AND c_txn_status = 'txn_confirmed_success'
        GROUP BY 1
    ),
    bonus AS (
        -- Bonus recebidos hoje
        -- Tabela: tbl_bonus_summary_details (corrigido de tbl_bonus)
        SELECT
            c_ecr_id,
            SUM(c_actual_issued_amount / 100.0) AS total_bonus
        FROM bonus_ec2.tbl_bonus_summary_details
        WHERE c_issue_date >= TIMESTAMP '2026-04-13 03:00:00'
          AND c_issue_date < TIMESTAMP '2026-04-14 03:00:00'
          AND c_actual_issued_amount > 0
        GROUP BY 1
    )
    SELECT
        s.c_ecr_id,
        s.total_saq AS saque_brl,
        COALESCE(d.total_dep, 0) AS deposito_brl,
        COALESCE(b.total_bonus, 0) AS bonus_brl,
        s.total_saq - COALESCE(d.total_dep, 0) AS net_saque_vs_dep,
        s.total_saq - COALESCE(d.total_dep, 0) - COALESCE(b.total_bonus, 0) AS saque_liquido_total
    FROM saques s
    LEFT JOIN deps d ON s.c_ecr_id = d.c_ecr_id
    LEFT JOIN bonus b ON s.c_ecr_id = b.c_ecr_id
    ORDER BY s.total_saq DESC
    LIMIT 20
    """

    print("=" * 120)
    print("QUERY 3: TOP 20 SACADORES — PERFIL COMPLETO (SAQUE x DEPOSITO x BONUS) — 13/04/2026")
    print("Fontes: cashier_ec2 (saques + depositos) + bonus_ec2 (bonus)")
    print("=" * 120)

    try:
        df = query_athena(sql, database="cashier_ec2")
        if df.empty:
            print("[AVISO] Nenhum resultado retornado.")
            return None

        print(df.to_string(index=False))
        print()

        # Analise detalhada
        total_saq = df['saque_brl'].sum()
        total_dep = df['deposito_brl'].sum()
        total_bon = df['bonus_brl'].sum()

        print(f"--- RESUMO TOP 20 SACADORES ---")
        print(f"Total sacado:                  R$ {total_saq:,.2f}")
        print(f"Total depositado:              R$ {total_dep:,.2f}")
        print(f"Total bonus:                   R$ {total_bon:,.2f}")
        print(f"Net (saque - deposito):        R$ {total_saq - total_dep:,.2f}")
        print(f"Net (saque - dep - bonus):     R$ {total_saq - total_dep - total_bon:,.2f}")
        print()

        # Flagging de jogadores suspeitos
        # Criterio: sacou mais do que depositou E recebeu bonus > 30% do saque
        suspeitos = df[
            (df['net_saque_vs_dep'] > 0) &
            (df['bonus_brl'] > df['saque_brl'] * 0.3) &
            (df['bonus_brl'] > 0)
        ]
        if not suspeitos.empty:
            print(f"[ALERTA] {len(suspeitos)} jogadores SUSPEITOS (sacou > depositou E bonus > 30% do saque):")
            for _, row in suspeitos.iterrows():
                bonus_pct = (row['bonus_brl'] / row['saque_brl'] * 100) if row['saque_brl'] > 0 else 0
                print(f"  ecr_id {row['c_ecr_id']}: "
                      f"sacou R$ {row['saque_brl']:,.2f} | "
                      f"dep R$ {row['deposito_brl']:,.2f} | "
                      f"bonus R$ {row['bonus_brl']:,.2f} ({bonus_pct:.0f}% do saque) | "
                      f"net R$ {row['saque_liquido_total']:,.2f}")
        else:
            print("[OK] Nenhum jogador flagado como suspeito nos top 20.")

        # Jogadores que sacaram sem depositar nada hoje
        sem_dep = df[df['deposito_brl'] == 0]
        if not sem_dep.empty:
            print(f"\n[INFO] {len(sem_dep)} jogadores sacaram SEM depositar hoje:")
            for _, row in sem_dep.iterrows():
                print(f"  ecr_id {row['c_ecr_id']}: sacou R$ {row['saque_brl']:,.2f}, bonus R$ {row['bonus_brl']:,.2f}")

        print()
        return df
    except Exception as e:
        print(f"[ERRO] Query 3 falhou: {e}")
        import traceback
        traceback.print_exc()
        return None


if __name__ == "__main__":
    print()
    print("*" * 120)
    print("  INVESTIGACAO: CORRELACAO SACADORES x BONIFICADOS — MultiBet — 13/04/2026")
    print("  Executado em:", pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S"))
    print("*" * 120)
    print()
    print("CORRECOES APLICADAS vs request original:")
    print("  1. Tabela bonus: tbl_bonus_summary_details (nao tbl_bonus — nao existe)")
    print("  2. Coluna data bonus: c_issue_date (nao c_created_time — validado empiricamente)")
    print("  3. NULLIF no divisor da pct (protecao contra divisao por zero)")
    print()

    df1 = query_1_cruzamento_saque_bonus()
    df2 = query_2_resumo_correlacao()
    df3 = query_3_perfil_completo_top20()

    print("=" * 120)
    print("FIM DA INVESTIGACAO — CORRELACAO SAQUE x BONUS")
    print("=" * 120)
