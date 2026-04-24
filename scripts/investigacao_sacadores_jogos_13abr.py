"""
Investigacao URGENTE: Top sacadores de 13/04/2026 x Atividade em jogos.
Cruzar ONDE os top sacadores ganharam dinheiro nos ultimos 3 dias.

4 Queries:
Q1 — Top 20 sacadores hoje (13/04 BRT) + total sacado
Q2 — Atividade casino dos top 20 sacadores nos ultimos 3 dias (11-13/04)
Q3 — BTR (bonus type 20) para os top sacadores
Q4 — Jogos que mais pagaram WINS nos ultimos 3 dias (geral)

Regras aplicadas:
- cashier_ec2: status = 'co_success', valor = c_amount_in_ecr_ccy / 100.0
- fund_ec2: status = 'SUCCESS', valor = c_amount_in_ecr_ccy / 100.0
- Tipos: 27=CASINO_BUYIN(bet), 45=CASINO_WIN, 72=ROLLBACK, 20=ISSUE_BONUS(BTR)
- Timezone: UTC -> BRT (13/04 BRT = 13/04 03:00 UTC a 14/04 03:00 UTC)
- BTR type 20: valor em tbl_real_fund_txn e SEMPRE 0 (real esta em tbl_realcash_sub_fund_txn)
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena
import pandas as pd

pd.set_option('display.max_columns', None)
pd.set_option('display.width', 220)
pd.set_option('display.max_colwidth', 50)
pd.set_option('display.float_format', '{:,.2f}'.format)


def query_1_top_sacadores():
    """Q1: Top 20 sacadores de hoje (13/04 BRT) com total sacado."""
    sql = """
    -- Q1: Top 20 sacadores hoje (13/04 BRT = 13/04 03:00 UTC a 14/04 03:00 UTC)
    -- Fonte: cashier_ec2.tbl_cashier_cashout
    -- Status: co_success | Valor: centavos / 100
    SELECT
        c_ecr_id,
        COUNT(*) AS qtd_saques,
        SUM(c_amount_in_ecr_ccy / 100.0) AS total_sacado_brl,
        MIN(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS primeiro_saque_brt,
        MAX(c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_saque_brt
    FROM cashier_ec2.tbl_cashier_cashout
    WHERE c_created_time >= TIMESTAMP '2026-04-13 03:00:00'
      AND c_created_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_status = 'co_success'
    GROUP BY 1
    ORDER BY total_sacado_brl DESC
    LIMIT 20
    """
    print("=" * 140)
    print("Q1: TOP 20 SACADORES HOJE (13/04/2026 BRT)")
    print("Fonte: cashier_ec2.tbl_cashier_cashout | Status: co_success | Valores em BRL")
    print("=" * 140)

    try:
        df = query_athena(sql, database="cashier_ec2")
        if df.empty:
            print("[AVISO] Nenhum resultado retornado — pode ser que ainda nao haja saques confirmados hoje.")
            return None

        print(df.to_string(index=False))
        print()

        total_top20 = df['total_sacado_brl'].sum()
        print(f"--- RESUMO TOP 20 SACADORES ---")
        print(f"Soma total sacado (top 20): R$ {total_top20:,.2f}")
        print(f"#1 sacador: R$ {df['total_sacado_brl'].iloc[0]:,.2f} (ecr_id: {df['c_ecr_id'].iloc[0]})")
        if len(df) > 1:
            print(f"#2 sacador: R$ {df['total_sacado_brl'].iloc[1]:,.2f} (ecr_id: {df['c_ecr_id'].iloc[1]})")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Q1 falhou: {e}")
        return None


def query_2_atividade_casino(ecr_ids_list):
    """Q2: Atividade casino dos top sacadores nos ultimos 3 dias (11-13/04 BRT)."""

    # Construir lista de IDs para o IN clause
    ids_str = ", ".join([f"'{eid}'" for eid in ecr_ids_list])

    sql = f"""
    -- Q2: Atividade casino dos top 20 sacadores nos ultimos 3 dias
    -- Fonte: fund_ec2.tbl_real_fund_txn
    -- Tipos: 27=BET, 45=WIN, 72=ROLLBACK | Status: SUCCESS
    -- Periodo: 11/04 03:00 UTC a 14/04 03:00 UTC (3 dias BRT)
    SELECT
        c_ecr_id,
        c_game_id,
        c_sub_vendor_id,
        SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_bets,
        SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_wins,
        SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END)
          - SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS player_ggr,
        COUNT_IF(c_txn_type = 27) AS rodadas
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= TIMESTAMP '2026-04-11 03:00:00'
      AND c_start_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_type IN (27, 45, 72)
      AND c_txn_status = 'SUCCESS'
      AND c_ecr_id IN ({ids_str})
    GROUP BY 1, 2, 3
    ORDER BY player_ggr DESC
    LIMIT 50
    """
    print("=" * 140)
    print("Q2: ATIVIDADE CASINO — TOP SACADORES NOS ULTIMOS 3 DIAS (11-13/04 BRT)")
    print("Fonte: fund_ec2.tbl_real_fund_txn | Tipos: 27=BET, 45=WIN, 72=ROLLBACK")
    print("player_ggr NEGATIVO = jogador GANHOU da casa (bom pra ele, ruim pra nos)")
    print("=" * 140)

    try:
        df = query_athena(sql, database="fund_ec2")
        if df.empty:
            print("[AVISO] Nenhuma atividade casino encontrada para esses jogadores no periodo.")
            return None

        print(df.to_string(index=False))
        print()

        # Resumo por jogador
        print("--- RESUMO POR JOGADOR (player_ggr = bets - wins, NEGATIVO = jogador lucrou) ---")
        resumo_jogador = df.groupby('c_ecr_id').agg(
            total_bets=('total_bets', 'sum'),
            total_wins=('total_wins', 'sum'),
            player_ggr=('player_ggr', 'sum'),
            jogos_distintos=('c_game_id', 'nunique'),
            total_rodadas=('rodadas', 'sum')
        ).sort_values('player_ggr', ascending=True).reset_index()

        print(resumo_jogador.to_string(index=False))
        print()

        # Resumo por jogo (entre os top sacadores)
        print("--- TOP JOGOS ONDE OS SACADORES GANHARAM (player_ggr < 0) ---")
        resumo_jogo = df[df['player_ggr'] < 0].groupby(['c_game_id', 'c_sub_vendor_id']).agg(
            total_bets=('total_bets', 'sum'),
            total_wins=('total_wins', 'sum'),
            player_ggr=('player_ggr', 'sum'),
            jogadores=('c_ecr_id', 'nunique'),
            total_rodadas=('rodadas', 'sum')
        ).sort_values('player_ggr', ascending=True).head(20).reset_index()

        if not resumo_jogo.empty:
            print(resumo_jogo.to_string(index=False))
        else:
            print("[INFO] Nenhum jogo com player_ggr negativo encontrado.")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Q2 falhou: {e}")
        return None


def query_3_btr(ecr_ids_list):
    """Q3: BTR (bonus type 20) para os top sacadores nos ultimos 3 dias."""

    ids_str = ", ".join([f"'{eid}'" for eid in ecr_ids_list])

    sql = f"""
    -- Q3: BTR (ISSUE_BONUS, type 20) para os top sacadores
    -- NOTA: valor em tbl_real_fund_txn para type 20 e SEMPRE 0
    --   O valor real do bonus esta em tbl_realcash_sub_fund_txn
    --   Aqui verificamos se HOUVE bonus — o valor precisa de consulta adicional
    SELECT
        c_ecr_id,
        COUNT(*) AS qtd_btr,
        SUM(c_amount_in_ecr_ccy / 100.0) AS btr_fund_value
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= TIMESTAMP '2026-04-11 03:00:00'
      AND c_start_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_type = 20
      AND c_txn_status = 'SUCCESS'
      AND c_ecr_id IN ({ids_str})
    GROUP BY 1
    ORDER BY btr_fund_value DESC
    """
    print("=" * 140)
    print("Q3: BTR (BONUS TYPE 20) — TOP SACADORES NOS ULTIMOS 3 DIAS (11-13/04 BRT)")
    print("Fonte: fund_ec2.tbl_real_fund_txn | Type: 20 (ISSUE_BONUS)")
    print("ATENCAO: valor type 20 em tbl_real_fund_txn e SEMPRE 0.")
    print("   Valor real do bonus esta em tbl_realcash_sub_fund_txn (consulta separada).")
    print("=" * 140)

    try:
        df = query_athena(sql, database="fund_ec2")
        if df.empty:
            print("[INFO] Nenhum BTR encontrado para esses jogadores no periodo.")
            return None

        print(df.to_string(index=False))
        print()

        # Alertar sobre o valor zero
        if (df['btr_fund_value'] == 0).all():
            print("[CONFIRMADO] Todos os btr_fund_value = 0, conforme esperado.")
            print("   O valor real deve ser consultado em tbl_realcash_sub_fund_txn.")
        else:
            print("[ATENCAO] Alguns btr_fund_value != 0 — verificar se houve mudanca no schema.")

        print(f"\nJogadores COM bonus: {len(df)} de {len(ecr_ids_list)} top sacadores")
        print(f"Jogadores SEM bonus: {len(ecr_ids_list) - len(df)}")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Q3 falhou: {e}")
        return None


def query_4_top_jogos_wins():
    """Q4: Jogos que mais pagaram WINS nos ultimos 3 dias (geral, toda a plataforma)."""
    sql = """
    -- Q4: Top 20 jogos com MAIOR player_ggr negativo (jogadores lucraram)
    -- Geral, nao restrito aos top sacadores
    -- Periodo: 11-13/04 BRT
    SELECT
        c_game_id,
        c_sub_vendor_id,
        SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_bets,
        SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS total_wins,
        SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END)
          - SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS player_ggr,
        COUNT(DISTINCT c_ecr_id) AS jogadores
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_start_time >= TIMESTAMP '2026-04-11 03:00:00'
      AND c_start_time < TIMESTAMP '2026-04-14 03:00:00'
      AND c_txn_type IN (27, 45)
      AND c_txn_status = 'SUCCESS'
    GROUP BY 1, 2
    HAVING
        SUM(CASE WHEN c_txn_type = 45 THEN c_amount_in_ecr_ccy ELSE 0 END)
        - SUM(CASE WHEN c_txn_type = 27 THEN c_amount_in_ecr_ccy ELSE 0 END) > 0
    ORDER BY player_ggr DESC
    LIMIT 20
    """
    print("=" * 140)
    print("Q4: TOP 20 JOGOS COM MAIS WINS (TODA PLATAFORMA) — ULTIMOS 3 DIAS (11-13/04 BRT)")
    print("Fonte: fund_ec2.tbl_real_fund_txn | player_ggr POSITIVO aqui = jogadores lucraram")
    print("(HAVING filtra apenas jogos onde wins > bets)")
    print("=" * 140)

    try:
        df = query_athena(sql, database="fund_ec2")
        if df.empty:
            print("[AVISO] Nenhum jogo com player_ggr positivo encontrado.")
            return None

        print(df.to_string(index=False))
        print()

        total_player_ggr = df['player_ggr'].sum()
        print(f"--- RESUMO ---")
        print(f"Total player_ggr (top 20 jogos mais 'generosos'): R$ {total_player_ggr:,.2f}")
        print(f"Jogo #1 mais 'generoso': game_id={df['c_game_id'].iloc[0]}, vendor={df['c_sub_vendor_id'].iloc[0]}, R$ {df['player_ggr'].iloc[0]:,.2f}")
        print(f"NOTA: game_id e numerico — mapear nome do jogo via bireports_ec2 se necessario.")
        print()

        return df
    except Exception as e:
        print(f"[ERRO] Q4 falhou: {e}")
        return None


def main():
    print()
    print("*" * 140)
    print("  INVESTIGACAO: TOP SACADORES 13/04/2026 x ATIVIDADE EM JOGOS")
    print(f"  Executado em: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("  Objetivo: Identificar ONDE os maiores sacadores ganharam dinheiro")
    print("*" * 140)
    print()

    # =====================================================
    # QUERY 1: Top 20 sacadores hoje
    # =====================================================
    df_sacadores = query_1_top_sacadores()

    if df_sacadores is None or df_sacadores.empty:
        print("[ABORT] Sem dados de sacadores — nao e possivel cruzar com jogos.")
        return

    # Extrair lista de ecr_ids para usar nas queries seguintes
    ecr_ids = df_sacadores['c_ecr_id'].tolist()
    print(f"[INFO] {len(ecr_ids)} ecr_ids extraidos para cruzamento com jogos.")
    print(f"[INFO] IDs: {ecr_ids[:5]}... (mostrando primeiros 5)")
    print()

    # =====================================================
    # QUERY 2: Atividade casino (bets/wins) por jogo
    # =====================================================
    df_casino = query_2_atividade_casino(ecr_ids)

    # =====================================================
    # QUERY 3: BTR (bonus)
    # =====================================================
    df_btr = query_3_btr(ecr_ids)

    # =====================================================
    # QUERY 4: Jogos mais "generosos" da plataforma
    # =====================================================
    df_jogos = query_4_top_jogos_wins()

    # =====================================================
    # CRUZAMENTO FINAL
    # =====================================================
    print("=" * 140)
    print("CRUZAMENTO FINAL: SACADORES x JOGOS x BONUS")
    print("=" * 140)

    if df_casino is not None and not df_casino.empty:
        # Jogadores que sacaram E tiveram player_ggr negativo (lucraram no casino)
        resumo = df_casino.groupby('c_ecr_id').agg(
            total_bets=('total_bets', 'sum'),
            total_wins=('total_wins', 'sum'),
            player_ggr=('player_ggr', 'sum'),
            jogos_distintos=('c_game_id', 'nunique'),
            rodadas=('rodadas', 'sum')
        ).reset_index()

        # Merge com sacadores para ter total sacado junto
        cruzamento = df_sacadores[['c_ecr_id', 'qtd_saques', 'total_sacado_brl']].merge(
            resumo, on='c_ecr_id', how='left'
        )

        # Adicionar info de BTR se disponivel
        if df_btr is not None and not df_btr.empty:
            cruzamento = cruzamento.merge(
                df_btr[['c_ecr_id', 'qtd_btr']], on='c_ecr_id', how='left'
            )
            cruzamento['qtd_btr'] = cruzamento['qtd_btr'].fillna(0).astype(int)
        else:
            cruzamento['qtd_btr'] = 0

        # Calcular ratio: wins / sacado (quanto do saque veio de wins no casino)
        cruzamento['wins_vs_sacado_pct'] = (
            cruzamento['total_wins'] / cruzamento['total_sacado_brl'] * 100
        ).round(1)

        cruzamento = cruzamento.sort_values('total_sacado_brl', ascending=False)

        print()
        print("TABELA CRUZADA: SAQUE vs ATIVIDADE CASINO (3 dias)")
        print("-" * 140)
        print(cruzamento.to_string(index=False))
        print()

        # Flags de risco
        print("--- FLAGS DE ATENCAO ---")
        for _, row in cruzamento.iterrows():
            flags = []
            if pd.notna(row.get('player_ggr')) and row['player_ggr'] < -10000:
                flags.append(f"GGR MUITO NEGATIVO (R$ {row['player_ggr']:,.2f})")
            if pd.notna(row.get('total_wins')) and row['total_wins'] > 50000:
                flags.append(f"WINS ALTOS (R$ {row['total_wins']:,.2f})")
            if pd.notna(row.get('rodadas')) and row['rodadas'] < 50:
                flags.append(f"POUCAS RODADAS ({row['rodadas']}) — possivel big win concentrado")
            if row.get('qtd_btr', 0) > 5:
                flags.append(f"MUITOS BONUS ({row['qtd_btr']} BTRs)")
            if pd.notna(row.get('total_bets')) and row['total_bets'] == 0 and row['total_sacado_brl'] > 1000:
                flags.append("SACOU SEM APOSTAR NO CASINO — verificar esportivas/bonus")
            if pd.isna(row.get('total_bets')) or (pd.notna(row.get('total_bets')) and row['total_bets'] == 0):
                flags.append("SEM ATIVIDADE CASINO NOS 3 DIAS — verificar outras fontes (esportivas?)")

            if flags:
                print(f"  ecr_id {row['c_ecr_id']}:")
                for f in flags:
                    print(f"    -> {f}")

        # Jogadores sem atividade casino
        sem_casino = cruzamento[cruzamento['total_bets'].isna() | (cruzamento['total_bets'] == 0)]
        if not sem_casino.empty:
            print(f"\n[ATENCAO] {len(sem_casino)} sacadores SEM atividade casino nos ultimos 3 dias.")
            print("  Possibilidades: apostas esportivas, bonus sem jogo, deposito de terceiros.")
            for _, row in sem_casino.iterrows():
                print(f"    ecr_id {row['c_ecr_id']}: sacou R$ {row['total_sacado_brl']:,.2f}")

    print()
    print("=" * 140)
    print("LEGENDA:")
    print("  total_bets     = soma de apostas (CASINO_BUYIN, type 27)")
    print("  total_wins     = soma de ganhos (CASINO_WIN, type 45)")
    print("  player_ggr     = bets - wins (NEGATIVO = jogador lucrou)")
    print("  rodadas        = qtd de apostas realizadas")
    print("  qtd_btr        = qtd de bonus recebidos (type 20, valor real em sub_fund)")
    print("  wins_vs_sacado = total_wins / total_sacado (cobertura)")
    print("  Fonte saques:  cashier_ec2.tbl_cashier_cashout (co_success)")
    print("  Fonte jogos:   fund_ec2.tbl_real_fund_txn (SUCCESS)")
    print("  Periodo jogos: 11-13/04/2026 BRT | Periodo saques: 13/04/2026 BRT")
    print("=" * 140)
    print("FIM DA INVESTIGACAO")
    print("=" * 140)


if __name__ == "__main__":
    main()
