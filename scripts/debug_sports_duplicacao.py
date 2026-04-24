"""Diagnostico rapido da duplicacao 2x em sports."""
import sys
sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")
import pandas as pd

df = pd.read_csv(r"reports\afiliados_marco_auditoria\sports_marco_afiliados.csv", low_memory=False)

print(f"Total linhas: {len(df):,}")
print(f"\nBreakdown por type (c_operation_type):")
print(df["type"].value_counts().to_string())

print(f"\nEstatisticas de grupos:")
print(f"  Bilhetes unicos (ext_ticket_id): {df['ext_ticket_id'].nunique():,}")
print(f"  ID unicos (c_transaction_id):    {df['id'].nunique():,}")

# Por bilhete, quantas linhas type='M' tem?
m_por_slip = df[df["type"] == "M"].groupby("ext_ticket_id").size()
print(f"\nBilhetes com type='M':")
print(f"  Bilhetes unicos com ao menos 1 M: {m_por_slip.shape[0]:,}")
print(f"  Total linhas type='M':            {(df['type']=='M').sum():,}")
print(f"  Media linhas M por bilhete:       {m_por_slip.mean():.2f}")
print(f"  Distribuicao:\n{m_por_slip.value_counts().head(10).to_string()}")

# Mesma analise pra P
p_por_slip = df[df["type"] == "P"].groupby("ext_ticket_id").size()
print(f"\nBilhetes com type='P':")
print(f"  Bilhetes unicos com ao menos 1 P: {p_por_slip.shape[0]:,}")
print(f"  Total linhas type='P':            {(df['type']=='P').sum():,}")

# Amostra de 1 bilhete com 2 M
slips_2m = m_por_slip[m_por_slip == 2].index[:3].tolist()
if slips_2m:
    print(f"\nAmostra de bilhetes com 2 linhas 'M':")
    amostra = df[df["ext_ticket_id"].isin(slips_2m) & (df["type"] == "M")][
        ["id", "ext_ticket_id", "type", "amount", "stake_amount", "source", "user_id", "created_at"]
    ].sort_values(["ext_ticket_id", "id"])
    print(amostra.to_string(index=False))
else:
    print("\nNenhum bilhete com 2+ linhas 'M'.")
