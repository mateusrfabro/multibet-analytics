"""
Gera CSV completo das divergencias separando:
  Grupo A: core_external_account_status = CLOSED (correto) mas core_account_status = ACTIVE (errado)
  Grupo B: AMBOS os campos errados (ACTIVE nos dois)
Inclui cruzamento com backoffice.
Gera tambem arquivos .sql prontos para rodar no DBeaver.
"""

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from datetime import datetime

print("Carregando arquivos...")

ds = pd.read_csv('validacoes/dataset_completo_20260311_1221.csv', dtype={'c_ecr_id': str, 'c_external_id': str})
bo = pd.read_csv('C:/Users/NITRO/Downloads/AccountClosureReport-February-2026-1773319028403.csv', dtype={'Ecr ID': str})
bo = bo.rename(columns={'Ecr ID': 'c_ecr_id'})

# --- Filtrar divergentes (closed no Redshift + ACTIVE no core_account_status) ---
div = ds[
    (ds['c_category'] == 'closed') &
    (ds['core_account_status'] == 'ACTIVE')
].copy()

# Classificar em Grupo A e B
div['ext_status'] = div['core_external_account_status'].fillna('NULL').str.strip().str.upper()
div['grupo'] = div['ext_status'].apply(
    lambda x: 'A_external_CORRETO_account_ERRADO' if x == 'CLOSED'
    else 'B_AMBOS_ERRADOS'
)

print(f"\nTotal divergentes: {len(div):,}")
print(div['grupo'].value_counts().to_string())

# --- Cruzar com backoffice ---
div = div.merge(
    bo[['c_ecr_id', 'Previous Category', 'Current Category', 'Closed Reason', 'Updated By', 'Updated Time']],
    on='c_ecr_id',
    how='left'
)
div['backoffice_status'] = div['Current Category'].fillna('NAO CONSTA NO RELATORIO')

# --- Renomear colunas ---
div = div.rename(columns={
    'c_ecr_id':                     'ecr_id (Redshift/Backoffice)',
    'c_external_id':                'external_id (Smartico user_ext_id)',
    'c_category':                   'REDSHIFT_status',
    'c_old_category':               'REDSHIFT_status_anterior',
    'c_category_updated_time':      'REDSHIFT_data_mudanca',
    'c_category_change_source':     'REDSHIFT_origem_mudanca',
    'backoffice_status':            'BACKOFFICE_status',
    'Current Category':             'BACKOFFICE_status_raw',
    'Closed Reason':                'BACKOFFICE_motivo',
    'Updated By':                   'BACKOFFICE_quem_fechou',
    'Updated Time':                 'BACKOFFICE_data_fechamento',
    'core_account_status':          'SMARTICO_core_account_status',
    'core_external_account_status': 'SMARTICO_core_external_account_status',
    'smartico_updated_at':          'SMARTICO_ultima_atualizacao',
})

colunas = [
    'grupo',
    'ecr_id (Redshift/Backoffice)',
    'external_id (Smartico user_ext_id)',
    # Status lado a lado
    'REDSHIFT_status',
    'BACKOFFICE_status',
    'SMARTICO_core_account_status',
    'SMARTICO_core_external_account_status',
    # Detalhes
    'REDSHIFT_status_anterior',
    'REDSHIFT_data_mudanca',
    'REDSHIFT_origem_mudanca',
    'BACKOFFICE_motivo',
    'BACKOFFICE_quem_fechou',
    'BACKOFFICE_data_fechamento',
    'SMARTICO_ultima_atualizacao',
]

colunas_existentes = [c for c in colunas if c in div.columns]
div = div[colunas_existentes].sort_values(['grupo', 'REDSHIFT_data_mudanca'])

# --- Salvar CSV ---
timestamp = datetime.now().strftime('%Y%m%d_%H%M')
arquivo_csv = f'validacoes/divergencias_external_vs_account_{timestamp}.csv'
div.to_csv(arquivo_csv, index=False)

print(f"\nCSV gerado: {arquivo_csv}")
print(f"Total: {len(div):,} contas")

# --- Resumo ---
for grupo in ['A_external_CORRETO_account_ERRADO', 'B_AMBOS_ERRADOS']:
    sub = div[div['grupo'] == grupo]
    print(f"\n--- {grupo}: {len(sub):,} contas ---")
    print(f"  Backoffice status: {sub['BACKOFFICE_status'].value_counts().to_string()}")

# --- Gerar SQLs para DBeaver ---
# Pegar IDs para as queries
ecr_ids_a = div[div['grupo'] == 'A_external_CORRETO_account_ERRADO']['ecr_id (Redshift/Backoffice)'].tolist()
ecr_ids_b = div[div['grupo'] == 'B_AMBOS_ERRADOS']['ecr_id (Redshift/Backoffice)'].tolist()
ext_ids_a = div[div['grupo'] == 'A_external_CORRETO_account_ERRADO']['external_id (Smartico user_ext_id)'].tolist()
ext_ids_b = div[div['grupo'] == 'B_AMBOS_ERRADOS']['external_id (Smartico user_ext_id)'].tolist()

# Amostra de 20 IDs de cada grupo para as queries de validacao manual
amostra_a_ecr = ecr_ids_a[:20]
amostra_b_ecr = ecr_ids_b[:20]
amostra_a_ext = ext_ids_a[:20]
amostra_b_ext = ext_ids_b[:20]

# --- SQL Redshift ---
sql_redshift = f"""-- ============================================================================
-- VALIDACAO DE STATUS: Redshift vs Smartico vs Backoffice
-- Rodar no DBeaver conectado ao Redshift
-- Data: {datetime.now().strftime('%d/%m/%Y')}
-- ============================================================================

-- ============================================================================
-- GRUPO A: external_account_status = CLOSED (correto), account_status = ACTIVE (errado)
-- Hipotese: o campo external recebe a atualizacao, o account_status nao
-- Total no universo: {len(ecr_ids_a):,} contas
-- Amostra: 20 contas
-- ============================================================================
SELECT
    e.c_ecr_id                                                          AS ecr_id,
    e.c_external_id                                                     AS external_id,
    e.c_email_id                                                        AS email,
    b.c_category                                                        AS redshift_status_atual,
    b.c_old_category                                                    AS redshift_status_anterior,
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', b.c_category_updated_time) AS data_mudanca_brt,
    b.c_category_change_source                                          AS origem_mudanca,
    'GRUPO_A: external=CLOSED, account=ACTIVE'                          AS grupo
FROM bireports.tbl_ecr b
JOIN ecr.tbl_ecr e ON e.c_ecr_id = b.c_ecr_id
WHERE e.c_ecr_id IN ({', '.join(amostra_a_ecr)})
ORDER BY b.c_category_updated_time;


-- ============================================================================
-- GRUPO B: AMBOS os campos errados na Smartico (ACTIVE nos dois)
-- Hipotese: a integracao parou completamente para essas contas
-- Total no universo: {len(ecr_ids_b):,} contas
-- Amostra: 20 contas
-- ============================================================================
SELECT
    e.c_ecr_id                                                          AS ecr_id,
    e.c_external_id                                                     AS external_id,
    e.c_email_id                                                        AS email,
    b.c_category                                                        AS redshift_status_atual,
    b.c_old_category                                                    AS redshift_status_anterior,
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', b.c_category_updated_time) AS data_mudanca_brt,
    b.c_category_change_source                                          AS origem_mudanca,
    'GRUPO_B: AMBOS ERRADOS na Smartico'                                AS grupo
FROM bireports.tbl_ecr b
JOIN ecr.tbl_ecr e ON e.c_ecr_id = b.c_ecr_id
WHERE e.c_ecr_id IN ({', '.join(amostra_b_ecr)})
ORDER BY b.c_category_updated_time;


-- ============================================================================
-- QUERY CONSOLIDADA: todas as {len(ecr_ids_a) + len(ecr_ids_b):,} contas divergentes
-- ATENCAO: query pesada, rodar com cuidado
-- ============================================================================
SELECT
    e.c_ecr_id                                                          AS ecr_id,
    e.c_external_id                                                     AS external_id,
    e.c_email_id                                                        AS email,
    b.c_category                                                        AS redshift_status_atual,
    b.c_old_category                                                    AS redshift_status_anterior,
    CONVERT_TIMEZONE('UTC', 'America/Sao_Paulo', b.c_category_updated_time) AS data_mudanca_brt,
    b.c_category_change_source                                          AS origem_mudanca
FROM bireports.tbl_ecr b
JOIN ecr.tbl_ecr e ON e.c_ecr_id = b.c_ecr_id
WHERE b.c_category = 'closed'
  AND b.c_category_updated_time >= '2026-01-01'
  AND b.c_category_updated_time < '2026-03-12'
  AND e.c_ecr_id IN ({', '.join(ecr_ids_a + ecr_ids_b)})
ORDER BY b.c_category_updated_time;
"""

arquivo_redshift = f'validacoes/query_validacao_redshift_{timestamp}.sql'
with open(arquivo_redshift, 'w', encoding='utf-8') as f:
    f.write(sql_redshift)
print(f"\nSQL Redshift gerado: {arquivo_redshift}")

# --- SQL BigQuery (Smartico) ---
sql_bigquery = f"""-- ============================================================================
-- VALIDACAO DE STATUS: Smartico (BigQuery)
-- Rodar no DBeaver conectado ao BigQuery ou no console BigQuery
-- Data: {datetime.now().strftime('%d/%m/%Y')}
-- ============================================================================

-- ============================================================================
-- GRUPO A: external_account_status = CLOSED (correto), account_status = ACTIVE (errado)
-- Total no universo: {len(ext_ids_a):,} contas
-- Amostra: 20 contas
-- ============================================================================
SELECT
    user_ext_id                                                         AS external_id,
    core_account_status                                                 AS smartico_account_status,
    core_external_account_status                                        AS smartico_external_status,
    update_date                                                         AS smartico_ultima_atualizacao,
    'GRUPO_A: external=CLOSED, account=ACTIVE'                          AS grupo
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE user_ext_id IN ({', '.join(f"'{eid}'" for eid in amostra_a_ext)})
ORDER BY update_date;


-- ============================================================================
-- GRUPO B: AMBOS os campos errados na Smartico (ACTIVE nos dois)
-- Total no universo: {len(ext_ids_b):,} contas
-- Amostra: 20 contas
-- ============================================================================
SELECT
    user_ext_id                                                         AS external_id,
    core_account_status                                                 AS smartico_account_status,
    core_external_account_status                                        AS smartico_external_status,
    update_date                                                         AS smartico_ultima_atualizacao,
    'GRUPO_B: AMBOS ERRADOS na Smartico'                                AS grupo
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE user_ext_id IN ({', '.join(f"'{eid}'" for eid in amostra_b_ext)})
ORDER BY update_date;


-- ============================================================================
-- QUERY CONSOLIDADA: todas as {len(ext_ids_a) + len(ext_ids_b):,} contas divergentes
-- ATENCAO: query pesada, rodar com cuidado
-- ============================================================================
SELECT
    user_ext_id                                                         AS external_id,
    core_account_status                                                 AS smartico_account_status,
    core_external_account_status                                        AS smartico_external_status,
    update_date                                                         AS smartico_ultima_atualizacao,
    CASE
        WHEN core_external_account_status = 'CLOSED' AND core_account_status = 'ACTIVE'
            THEN 'GRUPO_A: external=CLOSED, account=ACTIVE'
        ELSE 'GRUPO_B: AMBOS ERRADOS'
    END                                                                 AS grupo
FROM `smartico-bq6.dwh_ext_24105.j_user`
WHERE user_ext_id IN ({', '.join(f"'{eid}'" for eid in ext_ids_a + ext_ids_b)})
ORDER BY grupo, update_date;
"""

arquivo_bigquery = f'validacoes/query_validacao_smartico_{timestamp}.sql'
with open(arquivo_bigquery, 'w', encoding='utf-8') as f:
    f.write(sql_bigquery)
print(f"SQL Smartico gerado: {arquivo_bigquery}")

print(f"\n{'='*70}")
print(f"ARQUIVOS GERADOS:")
print(f"  1. {arquivo_csv} (CSV completo, {len(div):,} contas)")
print(f"  2. {arquivo_redshift} (queries para DBeaver/Redshift)")
print(f"  3. {arquivo_bigquery} (queries para DBeaver/BigQuery)")
print(f"\nCada SQL tem 3 queries:")
print(f"  - Grupo A (amostra 20): external CORRETO, account ERRADO")
print(f"  - Grupo B (amostra 20): AMBOS errados")
print(f"  - Consolidada: todas as {len(div):,} contas")