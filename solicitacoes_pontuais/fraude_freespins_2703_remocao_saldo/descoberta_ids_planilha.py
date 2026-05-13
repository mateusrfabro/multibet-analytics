"""
Descoberta empirica: como mapear os 3 tamanhos de IDs da planilha 'Remocao de Saldo - 27_03.xlsx'

Tamanhos observados:
  - 15 dig (2.517 IDs): provavel external_id Smartico
  - 8 dig  (522 IDs):   provavel ID legado / Pragmatic / outro
  - 6 dig  (61 IDs):    provavel ID antigo
  - 5/7 dig (8 IDs):    ?
  - 4 dig (1 ID):       ?

Estrategia: testar cada faixa contra colunas candidatas em ps_bi.dim_user
e em outras dimensoes possiveis.
"""
import sys
sys.path.insert(0, ".")
import pandas as pd
from db.athena import query_athena
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

INPUT = r"C:\Users\NITRO\Downloads\Remoção de Saldo - 27_03.xlsx"

# 1. Ler planilha forcando string
print("[1/5] Lendo planilha com dtype=str")
df_raw = pd.read_excel(INPUT, dtype={"ID": str})
print(f"   Total linhas: {len(df_raw)}")
print(f"   Duplicados:   {df_raw['ID'].duplicated().sum()}")
print(f"   Nulos:        {df_raw['ID'].isna().sum()}")

# 2. Limpeza
df = df_raw.copy()
df = df.dropna(subset=["ID"])
df["ID_str"] = df["ID"].astype(str).str.strip()
# IDs em notacao cientifica viram 'NaN' depois de tratar mas vamos marcar
df["eh_cientifica"] = df["ID_str"].str.contains("E\\+|E-", regex=True, na=False)
df["len_id"] = df["ID_str"].str.len()
print(f"\n[2/5] Apos limpeza:")
print(f"   Linhas validas:               {len(df)}")
print(f"   IDs em notacao cientifica:    {df['eh_cientifica'].sum()}")
print(f"   IDs unicos (validos):         {df[~df['eh_cientifica']]['ID_str'].nunique()}")
print(f"\n   Distribuicao por tamanho:")
print(df[~df["eh_cientifica"]]["len_id"].value_counts().sort_index().to_string())

# 3. Separar por faixa
df_valid = df[~df["eh_cientifica"]].copy()
ids_15 = df_valid[df_valid["len_id"] == 15]["ID_str"].drop_duplicates().tolist()
ids_8  = df_valid[df_valid["len_id"] == 8]["ID_str"].drop_duplicates().tolist()
ids_6  = df_valid[df_valid["len_id"] == 6]["ID_str"].drop_duplicates().tolist()
ids_outros = df_valid[~df_valid["len_id"].isin([15, 8, 6])]["ID_str"].drop_duplicates().tolist()
print(f"\n[3/5] Por faixa:")
print(f"   15 dig: {len(ids_15)} IDs unicos")
print(f"   8  dig: {len(ids_8)} IDs unicos")
print(f"   6  dig: {len(ids_6)} IDs unicos")
print(f"   outros: {len(ids_outros)} IDs unicos ({ids_outros})")


# 4. Testar mapping em ps_bi.dim_user para cada faixa
def quote(lst):
    return ",".join(f"'{x}'" for x in lst)

print("\n[4/5] Mapeando faixa 15 dig contra ps_bi.dim_user.external_id")
sql_15 = f"""
SELECT
    'external_id' AS coluna_match,
    COUNT(*)      AS achados,
    COUNT(DISTINCT CAST(external_id AS VARCHAR)) AS unicos_match
FROM ps_bi.dim_user
WHERE CAST(external_id AS VARCHAR) IN ({quote(ids_15[:2000])})
"""
print(f"   (sample primeiros 2000 IDs)")
r15 = query_athena(sql_15, database="ps_bi")
print(r15.to_string(index=False))
print(f"   Esperado: {len(ids_15)} | Achado: {r15.iloc[0]['achados']}")

print("\n[4.1] Mapeando faixa 8 dig — 3 hipoteses")
# 8 dig pode ser: external_id curto (smartico), ecr_id curto (improvavel), ou ID legado
# Testar em external_id (como string), em ecr_id (como string), em c_id (legado)
sql_8 = f"""
SELECT 'external_id_str' AS hipotese, COUNT(*) AS achados
FROM ps_bi.dim_user
WHERE CAST(external_id AS VARCHAR) IN ({quote(ids_8)})
UNION ALL
SELECT 'ecr_id_str' AS hipotese, COUNT(*)
FROM ps_bi.dim_user
WHERE CAST(ecr_id AS VARCHAR) IN ({quote(ids_8)})
"""
r8 = query_athena(sql_8, database="ps_bi")
print(r8.to_string(index=False))
print(f"   Esperado: {len(ids_8)}")

# Buscar alguma coluna em dim_user que tenha numeros de 8 dig
print("\n[4.2] SHOW COLUMNS ps_bi.dim_user (procurar colunas alternativas de ID)")
cols_dim = query_athena("SHOW COLUMNS FROM ps_bi.dim_user", database="ps_bi")
id_cols = cols_dim[cols_dim["field"].str.contains("id|key|external|smartico", case=False)]
print(id_cols.to_string(index=False))

print("\n[4.3] Mapeando faixa 6 dig — mesmas hipoteses + amostragem manual")
sql_6 = f"""
SELECT 'external_id_str' AS hipotese, COUNT(*) AS achados
FROM ps_bi.dim_user
WHERE CAST(external_id AS VARCHAR) IN ({quote(ids_6)})
UNION ALL
SELECT 'ecr_id_str' AS hipotese, COUNT(*)
FROM ps_bi.dim_user
WHERE CAST(ecr_id AS VARCHAR) IN ({quote(ids_6)})
"""
r6 = query_athena(sql_6, database="ps_bi")
print(r6.to_string(index=False))
print(f"   Esperado: {len(ids_6)}")

# 5. Recuperar IDs corrompidos (notacao cientifica) — talvez tenhamos linha do Excel onde da pra decodificar
print("\n[5/5] IDs em notacao cientifica (precisam de CSV original)")
corrompidos = df[df["eh_cientifica"]][["ID", "ID_str"]].drop_duplicates()
print(corrompidos.to_string(index=False))
print(f"\n   Total perdidos no formato xlsx: {len(corrompidos)}")
print("   -> Precisamos da fonte original (CSV/TXT) para recuperar")

# Salvar IDs limpos pra uso posterior
import os
os.makedirs("solicitacoes_pontuais/fraude_freespins_2703_remocao_saldo/tmp", exist_ok=True)
df_valid.to_csv(
    "solicitacoes_pontuais/fraude_freespins_2703_remocao_saldo/tmp/ids_limpos.csv",
    index=False,
    encoding="utf-8-sig"
)
print(f"\nSalvou ids_limpos.csv ({len(df_valid)} linhas)")
