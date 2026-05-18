"""
Investigacao empirica do exploit JELLY + MINES (ext_id 30311442).

Objetivo: identificar o padrao de transacoes que evidencia o exploit:
  1) Player ganhou ~140 giros free no JELLY (R$ 0,80 cada)
  2) Foi pro MINES, abriu rodada -> saldo travou em "bet pending"
  3) Voltou pro JELLY, executou giros, ganhou
  4) F5 no MINES -> estado pendente caiu -> saldo voltou

Hipotese: existe combinacao de c_txn_type que evidencia bet MINES "abortada"
sem resultado financeiro, dentro da mesma sessao de wins do JELLY.

NAO ALTERA NADA. Apenas leitura via Athena.
"""
import sys, os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
import pandas as pd
from db.athena import query_athena

EXT_ID = "30311442"


def fmt_df(df, max_rows=30):
    if df is None or df.empty:
        return "(vazio)"
    return df.head(max_rows).to_string(index=False)


# 1) Mapear ext_id -> ecr_id
print("=" * 80)
print(f"[1/6] Mapeando ext_id={EXT_ID} -> ecr_id (interno) em ps_bi.dim_user")
print("=" * 80)
sql = f"""
SELECT
    ecr_id,
    external_id,
    registration_date,
    affiliate_id,
    is_test
FROM ps_bi.dim_user
WHERE external_id = {EXT_ID}
"""
df_user = query_athena(sql, database="ps_bi")
print(fmt_df(df_user))

if df_user.empty:
    print("\n[ERRO] ext_id nao encontrado em dim_user. Abortando.")
    sys.exit(1)

ECR_ID = str(df_user.iloc[0]["ecr_id"])
REG_DATE = df_user.iloc[0]["registration_date"]
print(f"\n[i] ecr_id = {ECR_ID} | reg_date = {REG_DATE}")


# 2) Todas as transacoes do player nas ultimas 72h em fund_ec2
print("\n" + "=" * 80)
print(f"[2/6] Transacoes em fund_ec2.tbl_real_fund_txn — ultimas 72h")
print("=" * 80)
sql = f"""
SELECT
    c_txn_id,
    c_txn_type,
    c_txn_status,
    c_session_id,
    c_sub_vendor_id,
    c_amount_in_ecr_ccy / 100.0 AS amount_brl,
    c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_brt
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id = {ECR_ID}
  AND c_start_time >= CURRENT_TIMESTAMP - INTERVAL '120' HOUR
ORDER BY c_start_time DESC
LIMIT 1000
"""
df_txn = query_athena(sql, database="fund_ec2")
print(f"Total de transacoes 72h: {len(df_txn)}")
print(fmt_df(df_txn, 50))


# 3) Resumo por tipo de transacao
print("\n" + "=" * 80)
print(f"[3/6] Resumo por c_txn_type / c_txn_status (72h)")
print("=" * 80)
if not df_txn.empty:
    resumo = (
        df_txn.groupby(["c_txn_type", "c_txn_status"])
        .agg(qtd=("c_txn_id", "count"),
             total_brl=("amount_brl", "sum"))
        .reset_index()
        .sort_values("qtd", ascending=False)
    )
    print(fmt_df(resumo))


# 4) Resumo por sessao (c_session_id)
print("\n" + "=" * 80)
print(f"[4/6] Resumo por sessao (c_session_id) — 72h")
print("=" * 80)
if not df_txn.empty:
    por_sessao = (
        df_txn.groupby("c_session_id")
        .agg(
            inicio=("ts_brt", "min"),
            fim=("ts_brt", "max"),
            n_txns=("c_txn_id", "count"),
            sub_vendors=("c_sub_vendor_id", lambda s: ",".join(sorted(set(str(x) for x in s.dropna())))),
            total_brl=("amount_brl", "sum"),
        )
        .reset_index()
        .sort_values("inicio", ascending=False)
    )
    print(fmt_df(por_sessao, 20))


# 5) Tentar identificar nome dos jogos via silver.dmu_casino_bets
print("\n" + "=" * 80)
print(f"[5/6] Casino bets em silver.dmu_casino_bets — 72h (se existir)")
print("=" * 80)
try:
    sql = f"""
    SELECT
        bet_id,
        session_id,
        game_id,
        game_name,
        vendor_name,
        bet_amount,
        win_amount,
        bet_status,
        bet_ts AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS bet_ts_brt
    FROM silver.dmu_casino_bets
    WHERE ecr_id = {ECR_ID}
      AND bet_ts >= CURRENT_TIMESTAMP - INTERVAL '72' HOUR
    ORDER BY bet_ts DESC
    LIMIT 200
    """
    df_bets = query_athena(sql, database="silver")
    print(f"Total bets 72h: {len(df_bets)}")
    print(fmt_df(df_bets, 30))

    if not df_bets.empty:
        print("\n--- Resumo por game_name ---")
        resumo_jogo = (
            df_bets.groupby(["vendor_name", "game_name"])
            .agg(n_bets=("bet_id", "count"),
                 total_bet=("bet_amount", "sum"),
                 total_win=("win_amount", "sum"))
            .reset_index()
            .sort_values("n_bets", ascending=False)
        )
        print(fmt_df(resumo_jogo, 20))
except Exception as e:
    print(f"silver.dmu_casino_bets indisponivel ou schema diferente: {e}")


# 6) Cashier movimentacoes (deposito/saque)
print("\n" + "=" * 80)
print(f"[6/6] Cashier (deposito/saque) — 72h")
print("=" * 80)
try:
    sql = f"""
    SELECT 'DEPOSIT' AS tipo, c_txn_id, c_txn_status, c_initial_amount/100.0 AS amount_brl,
           c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_brt
    FROM cashier_ec2.tbl_cashier_deposit
    WHERE c_ecr_id = {ECR_ID}
      AND c_created_time >= CURRENT_TIMESTAMP - INTERVAL '72' HOUR
    UNION ALL
    SELECT 'CASHOUT' AS tipo, c_txn_id, c_txn_status, c_initial_amount/100.0 AS amount_brl,
           c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS ts_brt
    FROM cashier_ec2.tbl_cashier_cashout
    WHERE c_ecr_id = {ECR_ID}
      AND c_created_time >= CURRENT_TIMESTAMP - INTERVAL '72' HOUR
    ORDER BY ts_brt DESC
    """
    df_cash = query_athena(sql, database="cashier_ec2")
    print(fmt_df(df_cash, 30))
except Exception as e:
    print(f"Erro cashier: {e}")


print("\n" + "=" * 80)
print("INVESTIGACAO CONCLUIDA")
print("=" * 80)
