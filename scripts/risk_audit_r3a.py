"""
Investigacao profunda dos jogadores R3a (zero deposit lifetime).
Verifica TODAS as transacoes para entender de onde veio o saldo.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena
import pandas as pd

df = pd.read_csv("output/risk_fraud_alerts_2026-04-03.csv")
r3a = df[df["regras_violadas"].str.contains("R3a", na=False)].sort_values("risk_score", ascending=False)

print(f"Total R3a: {len(r3a)} jogadores")
print(f"\nTop 10 R3a:")
print(r3a.head(10)[["c_ecr_id", "risk_score", "regras_violadas", "evidencias"]].to_string(index=False))

# Investigar top 5
ids = r3a.head(5)["c_ecr_id"].tolist()

for ecr_id in ids:
    print(f"\n{'='*60}")
    print(f"INVESTIGACAO: {ecr_id}")
    print(f"{'='*60}")

    # TODAS as transacoes no periodo (sem filtro de tipo)
    df_period = query_athena(f"""
        SELECT
            c_txn_type,
            COUNT(*) as qty,
            ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) as total_brl
        FROM fund_ec2.tbl_real_fund_txn
        WHERE c_ecr_id = {ecr_id}
          AND c_start_time >= TIMESTAMP '2026-03-27'
          AND c_start_time < TIMESTAMP '2026-04-03'
          AND c_txn_status = 'SUCCESS'
        GROUP BY c_txn_type
        ORDER BY qty DESC
    """, database="fund_ec2")
    print("\n[Periodo 7d] Todas as transacoes:")
    print(df_period.to_string(index=False) if not df_period.empty else "  NENHUMA transacao!")

    # TODAS as transacoes LIFETIME
    df_life = query_athena(f"""
        SELECT
            c_txn_type,
            COUNT(*) as qty,
            ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) as total_brl
        FROM fund_ec2.tbl_real_fund_txn
        WHERE c_ecr_id = {ecr_id}
          AND c_txn_status = 'SUCCESS'
        GROUP BY c_txn_type
        ORDER BY qty DESC
    """, database="fund_ec2")
    print("\n[Lifetime] Todas as transacoes:")
    print(df_life.to_string(index=False) if not df_life.empty else "  NENHUMA!")

    # Bonus
    df_bonus = query_athena(f"""
        SELECT COUNT(*) as qty_bonus,
               SUM(COALESCE(c_total_bonus_issued, 0)) as total_issued
        FROM bonus_ec2.tbl_bonus_summary_details
        WHERE c_ecr_id = {ecr_id}
    """, database="bonus_ec2")
    print("\n[Bonus]:")
    print(df_bonus.to_string(index=False))

    # Perfil
    df_user = query_athena(f"""
        SELECT ecr_id, is_test, registration_date
        FROM ps_bi.dim_user
        WHERE ecr_id = {ecr_id}
    """, database="ps_bi")
    print("\n[Perfil]:")
    print(df_user.to_string(index=False) if not df_user.empty else "  NAO encontrado no ps_bi")

# Resumo: quantos R3a tem bets=0 AND wins=0?
print(f"\n{'='*60}")
print("RESUMO R3a")
print(f"{'='*60}")
# Os dados de bets/wins estao na evidencia, mas vamos contar direto
zero_bets = r3a[r3a["evidencias"].str.contains("bets R\\$0.00", na=False)]
print(f"Total R3a: {len(r3a)}")
print(f"R3a com bets=0 E wins=0: {len(zero_bets[zero_bets['evidencias'].str.contains('wins R\\$0.00', na=False)])}")
print(f"R3a com alguma atividade de jogo: {len(r3a) - len(zero_bets)}")
