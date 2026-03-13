"""
Gera amostra de contas divergentes para validacao manual no backoffice/Smartico.
Separa por grupo de divergencia com 10 contas cada (ou todas, se menor que 10).
Enriquece com email e master_id do Redshift para facilitar busca no backoffice.
Saida: CSV limpo para enviar ao Castrin.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime
from db.redshift import query_redshift

# --- Carregar dataset completo ---
df = pd.read_csv('validacoes/dataset_completo_20260311_1221.csv', dtype={'c_external_id': str, 'c_ecr_id': str})

amostras = []

# --- GRUPO 1: CRITICO - fraud no Redshift + ACTIVE na Smartico (todas) ---
g1 = df[
    (df['c_category'] == 'fraud') &
    (df['core_account_status'] == 'ACTIVE')
].copy()
g1['grupo_divergencia'] = '1_CRITICO_fraud_ativa_smartico'
amostras.append(g1)
print(f"Grupo 1 - Fraud + ACTIVE na Smartico: {len(g1)} contas (todas incluidas)")

# --- GRUPO 2: CRITICO - closed no Redshift + ACTIVE na Smartico (mudanca real) ---
g2 = df[
    (df['c_category'] == 'closed') &
    (df['core_account_status'] == 'ACTIVE') &
    (df['c_old_category'] != 'closed')
].copy()
g2['grupo_divergencia'] = '2_CRITICO_closed_real_ativa_smartico'
amostras.append(g2.sample(n=min(10, len(g2)), random_state=42))
print(f"Grupo 2 - Closed (mudanca real) + ACTIVE na Smartico: {len(g2)} contas (amostra de {min(10, len(g2))})")

# --- GRUPO 3: ALTO - closed reprocessamento SIGAP + ACTIVE na Smartico ---
g3 = df[
    (df['c_category'] == 'closed') &
    (df['core_account_status'] == 'ACTIVE') &
    (df['c_old_category'] == 'closed')
].copy()
g3['grupo_divergencia'] = '3_ALTO_closed_reprocessamento_sigap'
amostras.append(g3.sample(n=min(10, len(g3)), random_state=42))
print(f"Grupo 3 - Closed (reprocessamento SIGAP) + ACTIVE na Smartico: {len(g3)} contas (amostra de {min(10, len(g3))})")

# --- GRUPO 4: MEDIO - rg_closed com "TRUE" na Smartico ---
g4 = df[
    (df['c_category'] == 'rg_closed') &
    (df['core_external_account_status'] == 'TRUE')
].copy()
g4['grupo_divergencia'] = '4_MEDIO_rg_closed_true_smartico'
amostras.append(g4.sample(n=min(10, len(g4)), random_state=42))
print(f"Grupo 4 - rg_closed com TRUE na Smartico: {len(g4)} contas (amostra de {min(10, len(g4))})")

# --- GRUPO 5: MEDIO - rg_cool_off no Redshift + ACTIVE na Smartico ---
g5 = df[
    (df['c_category'] == 'rg_cool_off') &
    (df['core_account_status'] == 'ACTIVE')
].copy()
g5['grupo_divergencia'] = '5_MEDIO_rg_cool_off_ativa_smartico'
amostras.append(g5.sample(n=min(10, len(g5)), random_state=42))
print(f"Grupo 5 - rg_cool_off + ACTIVE na Smartico: {len(g5)} contas (amostra de {min(10, len(g5))})")

# --- Consolidar amostra ---
resultado = pd.concat(amostras, ignore_index=True)
print(f"\nTotal na amostra: {len(resultado)} contas")

# --- Enriquecer com dados do Redshift (email, master_id) ---
print("Buscando email e master_id no Redshift...")
ids_lista = resultado['c_external_id'].tolist()
ids_sql = ', '.join(ids_lista)

df_redshift = query_redshift(f"""
    SELECT
        CAST(c_external_id AS VARCHAR) AS c_external_id,
        c_master_id,
        c_email_id
    FROM ecr.tbl_ecr
    WHERE c_external_id IN ({ids_sql})
""")

# Merge para trazer email e master_id
resultado = resultado.merge(df_redshift, on='c_external_id', how='left')
print(f"Enriquecido: {resultado['c_email_id'].notna().sum()}/{len(resultado)} contas com email encontrado")

# --- Renomear colunas com origem clara ---
resultado = resultado.rename(columns={
    'c_external_id':                'redshift_external_id',
    'c_ecr_id':                     'redshift_ecr_id',
    'c_master_id':                  'redshift_master_id (buscar no backoffice)',
    'c_email_id':                   'redshift_email',
    'c_category':                   'redshift_status_atual',
    'c_old_category':               'redshift_status_anterior',
    'c_category_updated_time':      'redshift_data_mudanca_status',
    'c_category_change_source':     'redshift_origem_mudanca',
    'core_account_status':          'smartico_account_status',
    'core_external_account_status': 'smartico_external_account_status',
    'smartico_updated_at':          'smartico_ultima_atualizacao',
})

colunas_saida = [
    'grupo_divergencia',
    # --- IDs para busca ---
    'redshift_master_id (buscar no backoffice)',
    'redshift_email',
    'redshift_external_id',
    # --- Redshift (base verdade) ---
    'redshift_status_atual',
    'redshift_status_anterior',
    'redshift_data_mudanca_status',
    'redshift_origem_mudanca',
    # --- Smartico (CRM) ---
    'smartico_account_status',
    'smartico_external_account_status',
    'smartico_ultima_atualizacao',
]

colunas_existentes = [c for c in colunas_saida if c in resultado.columns]
resultado = resultado[colunas_existentes]

# Ordenar por grupo e data
resultado = resultado.sort_values(['grupo_divergencia', 'redshift_data_mudanca_status'])

# --- Salvar ---
timestamp = datetime.now().strftime('%Y%m%d_%H%M')
arquivo_saida = f'validacoes/amostra_validacao_backoffice_{timestamp}.csv'
resultado.to_csv(arquivo_saida, index=False)

print(f"\n{'='*60}")
print(f"Amostra gerada: {arquivo_saida}")
print(f"Total de contas: {len(resultado)}")
print(f"\nColunas incluidas:")
print(f"  IDENTIFICACAO (usar pra buscar):")
print(f"    - redshift_master_id = MID do jogador (buscar no backoffice PGS)")
print(f"    - redshift_email = email cadastrado")
print(f"    - redshift_external_id = c_external_id (buscar na Smartico como user_ext_id)")
print(f"  REDSHIFT (base verdade - PGS):")
print(f"    - redshift_status_atual = status atual da conta")
print(f"    - redshift_status_anterior = status antes da mudanca")
print(f"    - redshift_data_mudanca_status = quando o status mudou")
print(f"    - redshift_origem_mudanca = o que causou (SIGAP, manual, etc)")
print(f"  SMARTICO (CRM):")
print(f"    - smartico_account_status = core_account_status")
print(f"    - smartico_external_account_status = core_external_account_status")
print(f"    - smartico_ultima_atualizacao = ultima atualizacao de qualquer dado do jogador")
