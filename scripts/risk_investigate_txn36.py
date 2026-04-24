"""
Investigacao: o que e txn_type 36? E quais outros tipos aparecem nos R3a?
"""
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.athena import query_athena
import pandas as pd

# 1. Descobrir o que e txn_type 36
print("=" * 60)
print("  1. O que e txn_type 36?")
print("=" * 60)

# Buscar na tabela de tipos (tentar variantes de nome)
for table in ["tbl_fund_txn_type_master", "tbl_fund_txn_type_mst", "tbl_txn_type_master"]:
    try:
        df = query_athena(f"""
            SELECT * FROM fund_ec2.{table}
            WHERE c_txn_type IN (36, 2, 37, 4, 3, 19, 20, 30, 80, 89)
            ORDER BY c_txn_type
        """, database="fund_ec2")
        print(f"Tabela encontrada: {table}")
        print(df.to_string(index=False))
        break
    except Exception as e:
        print(f"  {table}: nao existe")

# 2. Buscar por nome da tabela de tipos
print("\n" + "=" * 60)
print("  2. Tabelas fund_ec2 com 'type' ou 'mst' no nome")
print("=" * 60)
df_tables = query_athena("SHOW TABLES IN fund_ec2", database="fund_ec2")
col = df_tables.columns[0]
matches = df_tables[df_tables[col].str.contains("type|mst|master", case=False, na=False)]
print(matches.to_string(index=False))

# 3. Todos os txn_types dos 39 R3a (lifetime)
print("\n" + "=" * 60)
print("  3. Todos os txn_types presentes nos 39 R3a (lifetime)")
print("=" * 60)

alerts = pd.read_csv("output/risk_fraud_alerts_2026-04-03.csv")
r3a_ids = alerts[alerts["regras_violadas"].str.contains("R3a", na=False)]["c_ecr_id"].tolist()
ids_str = ",".join([str(x) for x in r3a_ids])

df_types = query_athena(f"""
    SELECT
        c_txn_type,
        COUNT(*) as qty,
        COUNT(DISTINCT c_ecr_id) as jogadores,
        ROUND(SUM(c_amount_in_ecr_ccy / 100.0), 2) as total_brl
    FROM fund_ec2.tbl_real_fund_txn
    WHERE c_ecr_id IN ({ids_str})
      AND c_txn_status = 'SUCCESS'
    GROUP BY c_txn_type
    ORDER BY qty DESC
""", database="fund_ec2")
print(df_types.to_string(index=False))

# 4. Verificar se esses jogadores tem transacoes no SPORTSBOOK (vendor_ec2)
print("\n" + "=" * 60)
print("  4. Verificar sportsbook (txn_type 59=SB_BET, 112=SB_WIN)")
print("=" * 60)

# Tipos 59 e 112 ja estao no resultado acima, mas vamos ver os que tem
sb_types = df_types[df_types["c_txn_type"].isin([59, 112, 4, 3, 36, 37, 89])]
print(sb_types.to_string(index=False) if not sb_types.empty else "Nenhum desses tipos encontrado")

# 5. Mapeamento conhecido dos tipos
print("\n" + "=" * 60)
print("  5. Mapeamento conhecido (docs/schema_bronze)")
print("=" * 60)
known = {
    1: "REAL_CASH_DEPOSIT (CR)",
    2: "REAL_CASH_WITHDRAW (DB)",
    3: "MANUAL_CREDIT (CR) — ajuste manual pelo backoffice",
    4: "MANUAL_DEBIT (DB) — ajuste manual pelo backoffice",
    19: "RESERVE_FUNDS (DB)",
    20: "RELEASE_RESERVED_FUNDS (CR)",
    27: "CASINO_BUYIN (DB)",
    30: "BONUS_CREDIT (CR)",
    36: "???",
    37: "???",
    45: "CASINO_WIN (CR)",
    59: "SB_BUYIN (DB)",
    65: "JACKPOT_WIN (CR)",
    72: "CASINO_BUYIN_CANCEL (CR)",
    80: "CASINO_FREESPIN_WIN (CR)",
    89: "???",
    112: "SB_WIN (CR)",
}
for txn_type in sorted(df_types["c_txn_type"].unique()):
    desc = known.get(txn_type, "DESCONHECIDO")
    row = df_types[df_types["c_txn_type"] == txn_type].iloc[0]
    print(f"  {txn_type:>3} = {desc:40s} | {int(row['jogadores']):>3} jogadores, {int(row['qty']):>5} txns, R${row['total_brl']:>12,.2f}")
