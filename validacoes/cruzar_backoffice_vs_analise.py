"""
Cruza o relatorio do backoffice (AccountClosureReport) com nosso dataset de divergencias.
Gera CSV COMPLETO (todas as 12k+ contas) com status lado a lado:
    Redshift | Backoffice | Smartico
Inclui ambos IDs (c_ecr_id para Redshift/backoffice, c_external_id para Smartico).
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime

# --- Carregar arquivos ---
print("Carregando arquivos...")

bo = pd.read_csv(
    'C:/Users/NITRO/Downloads/AccountClosureReport-February-2026-1773319028403.csv',
    dtype={'Ecr ID': str}
)
bo = bo.rename(columns={'Ecr ID': 'c_ecr_id'})
print(f"Backoffice: {len(bo)} contas")

ds = pd.read_csv(
    'validacoes/dataset_completo_20260311_1221.csv',
    dtype={'c_ecr_id': str, 'c_external_id': str}
)
print(f"Dataset analise: {len(ds)} contas")

# --- Filtrar divergentes (closed no Redshift + ACTIVE na Smartico) ---
divergentes = ds[
    (ds['c_category'] == 'closed') &
    (ds['core_account_status'] == 'ACTIVE')
].copy()
print(f"Divergentes (closed Redshift + ACTIVE Smartico): {len(divergentes)} contas")

# --- Cruzar com backoffice ---
resultado = divergentes.merge(
    bo[['c_ecr_id', 'Previous Category', 'Current Category', 'Closed Reason', 'Updated By', 'Updated Time']],
    on='c_ecr_id',
    how='left',
    suffixes=('', '_backoffice')
)

# Marcar se encontrou no backoffice
resultado['encontrado_no_backoffice'] = resultado['Current Category'].notna()

encontradas = resultado['encontrado_no_backoffice'].sum()
nao_encontradas = len(resultado) - encontradas
print(f"\nEncontradas no backoffice: {encontradas} ({encontradas/len(resultado)*100:.1f}%)")
print(f"NAO encontradas: {nao_encontradas} ({nao_encontradas/len(resultado)*100:.1f}%)")

# Para quem nao foi encontrado no backoffice, preencher com "NAO CONSTA"
resultado['Current Category'] = resultado['Current Category'].fillna('NAO CONSTA NO RELATORIO')

# --- Resumo ---
print(f"\n{'='*60}")
print(f"RESUMO DO CRUZAMENTO")
print(f"{'='*60}")
print(f"\nStatus no backoffice:")
print(resultado['Current Category'].value_counts().to_string())
print(f"\nMotivo fechamento:")
print(resultado['Closed Reason'].value_counts(dropna=False).to_string())

# --- Renomear e organizar colunas ---
resultado = resultado.rename(columns={
    'c_ecr_id':                     'ecr_id (buscar no Redshift/Backoffice)',
    'c_external_id':                'external_id (buscar na Smartico como user_ext_id)',
    'c_category':                   'REDSHIFT_status',
    'c_old_category':               'REDSHIFT_status_anterior',
    'c_category_updated_time':      'REDSHIFT_data_mudanca',
    'c_category_change_source':     'REDSHIFT_origem_mudanca',
    'Current Category':             'BACKOFFICE_status',
    'Previous Category':            'BACKOFFICE_status_anterior',
    'Closed Reason':                'BACKOFFICE_motivo_fechamento',
    'Updated By':                   'BACKOFFICE_quem_fechou',
    'Updated Time':                 'BACKOFFICE_data_fechamento',
    'core_account_status':          'SMARTICO_account_status',
    'core_external_account_status': 'SMARTICO_external_account_status',
    'smartico_updated_at':          'SMARTICO_ultima_atualizacao',
})

colunas_saida = [
    # --- IDs ---
    'ecr_id (buscar no Redshift/Backoffice)',
    'external_id (buscar na Smartico como user_ext_id)',
    # --- Comparacao lado a lado ---
    'REDSHIFT_status',
    'BACKOFFICE_status',
    'SMARTICO_account_status',
    # --- Detalhes Redshift ---
    'REDSHIFT_status_anterior',
    'REDSHIFT_data_mudanca',
    'REDSHIFT_origem_mudanca',
    # --- Detalhes Backoffice ---
    'BACKOFFICE_status_anterior',
    'BACKOFFICE_motivo_fechamento',
    'BACKOFFICE_quem_fechou',
    'BACKOFFICE_data_fechamento',
    # --- Detalhes Smartico ---
    'SMARTICO_external_account_status',
    'SMARTICO_ultima_atualizacao',
    # --- Flag ---
    'encontrado_no_backoffice',
]

colunas_existentes = [c for c in colunas_saida if c in resultado.columns]
resultado = resultado[colunas_existentes]
resultado = resultado.sort_values('REDSHIFT_data_mudanca')

# --- Salvar ---
timestamp = datetime.now().strftime('%Y%m%d_%H%M')
arquivo = f'validacoes/cruzamento_completo_redshift_backoffice_smartico_{timestamp}.csv'
resultado.to_csv(arquivo, index=False)

print(f"\n{'='*60}")
print(f"CSV COMPLETO gerado: {arquivo}")
print(f"Total: {len(resultado)} contas")
print(f"{'='*60}")
print(f"\nColunas de STATUS lado a lado:")
print(f"  REDSHIFT_status | BACKOFFICE_status | SMARTICO_account_status")
print(f"\nComo validar por ID:")
print(f"  Redshift/Backoffice: usar 'ecr_id (buscar no Redshift/Backoffice)'")
print(f"  Smartico (BigQuery):  usar 'external_id (buscar na Smartico como user_ext_id)'")