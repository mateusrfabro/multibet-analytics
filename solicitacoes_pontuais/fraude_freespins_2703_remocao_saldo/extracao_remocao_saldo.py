"""
Extracao para Remocao de Saldo - Fraude em Promocao de Freespins (27/03/2026)

INPUT:  C:\\Users\\NITRO\\Downloads\\Remocao de Saldo - 27_03.xlsx (3.118 linhas com coluna ID)
OUTPUT: reports/remocao_saldo_fraude_freespins_2703_<timestamp>.xlsx (3 abas + legenda)

Demanda (literal):
  "extrair uma base com os valores de saldo ganhos e os bonus convertidos em saldo
   real no dia 27/03/2026 ... essa base salva para futuras consultas, caso ocorra
   qualquer problema durante o processo de remocao ... 3.117 IDs ... Possivelmente
   dentro dessa base teremos jogadores com saldo zerado"

Validacoes empiricas previas (descoberta_total_bonus_cost.py + descoberta_ids_planilha.py):
  1. "Total Bonus Cost" do BKO = c_actual_issued_amount (R$ 514.611,21 total dia 27/03)
  2. Todos os IDs da planilha (15/8/6 dig) sao external_id Smartico
  3. 7 IDs corrompidos por notacao cientifica no Excel - perda inerente do formato

Adicoes alem do pedido (defensavel):
  - Coluna risco_remocao (OK / PARCIAL / DINHEIRO_SACADO)
  - Validacao cruzada raw vs gold (tbl_bonus_summary_details vs fct_bonus_activity_daily)
  - Status atual da conta (acelera etapa 2 de desbloqueio)
  - Hash + timestamp para snapshot rastreavel
"""
import os
import sys
import json
import hashlib
from datetime import datetime

sys.path.insert(0, ".")
import warnings
warnings.filterwarnings("ignore", category=UserWarning)

import pandas as pd
from db.athena import query_athena

# ============================================================
# Configuracao
# ============================================================
INPUT_XLSX = r"C:\Users\NITRO\Downloads\Remoção de Saldo - 27_03.xlsx"
DATA_FRAUDE = "2026-03-27"
TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
OUTPUT_DIR = "reports"
OUTPUT_NAME = f"remocao_saldo_fraude_freespins_2703_{TIMESTAMP}.xlsx"
OUTPUT_PATH = os.path.join(OUTPUT_DIR, OUTPUT_NAME)

print("="*80)
print(f"EXTRACAO REMOCAO SALDO - FRAUDE FREESPINS {DATA_FRAUDE}")
print(f"Timestamp: {TIMESTAMP}")
print("="*80)

# ============================================================
# STEP 1 - Carregar e limpar planilha
# ============================================================
print("\n[STEP 1] Carregando planilha")
df_raw = pd.read_excel(INPUT_XLSX, dtype={"ID": str})
print(f"   Total linhas brutas: {len(df_raw)}")

df = df_raw.copy()
df["ID_str"] = df["ID"].astype(str).str.strip()
df["eh_cientifica"] = df["ID_str"].str.contains("E\\+|E-", regex=True, na=False)
df["eh_nulo"] = df["ID"].isna() | (df["ID_str"] == "nan") | (df["ID_str"] == "")

# Auditoria
df_nulos = df[df["eh_nulo"]].copy()
df_corrompidos = df[df["eh_cientifica"]].copy()
df_validos = df[~df["eh_nulo"] & ~df["eh_cientifica"]].copy()

# Duplicados
df_duplicados = df_validos[df_validos["ID_str"].duplicated(keep=False)].copy()
df_unicos = df_validos.drop_duplicates(subset=["ID_str"], keep="first").copy()

print(f"   Nulos:                  {len(df_nulos)}")
print(f"   Corrompidos cientifica: {len(df_corrompidos)}")
print(f"   Duplicados (todos):     {df_validos['ID_str'].duplicated().sum()}")
print(f"   IDs unicos limpos:      {len(df_unicos)}")

# ============================================================
# STEP 2 - Mapear external_id -> ecr_id via ps_bi.dim_user
# ============================================================
print("\n[STEP 2] Mapeando external_id -> ecr_id em ps_bi.dim_user")
ids = df_unicos["ID_str"].tolist()


def quote(lst):
    return ",".join(f"'{x}'" for x in lst)


# Athena suporta IN grande, mas vamos quebrar em batches de 1500 por seguranca
def query_in_batches(ids_list, sql_template, batch_size=1500, database="ps_bi"):
    dfs = []
    for i in range(0, len(ids_list), batch_size):
        batch = ids_list[i:i + batch_size]
        sql = sql_template.format(ids=quote(batch))
        dfs.append(query_athena(sql, database=database))
        print(f"   batch {i // batch_size + 1}/{(len(ids_list) - 1) // batch_size + 1} ({len(batch)} ids) OK")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


sql_dim = """
SELECT
    CAST(external_id AS VARCHAR)         AS external_id,
    CAST(ecr_id AS VARCHAR)              AS ecr_id_correto,
    country_code,
    ecr_status,
    is_test,
    signup_datetime,
    ftd_date
FROM ps_bi.dim_user
WHERE CAST(external_id AS VARCHAR) IN ({ids})
"""
df_dim = query_in_batches(ids, sql_dim, batch_size=1500, database="ps_bi")
print(f"   Mapeados em dim_user: {len(df_dim)} / {len(ids)}")

# IDs nao mapeados (vao pra auditoria)
ids_mapeados = set(df_dim["external_id"])
df_nao_mapeados = df_unicos[~df_unicos["ID_str"].isin(ids_mapeados)].copy()
print(f"   Nao mapeados:         {len(df_nao_mapeados)}")

# ============================================================
# STEP 3 - Bonus convertido em 27/03 (Actual Issued Amount)
# ============================================================
print(f"\n[STEP 3] Bonus convertido em saldo real em {DATA_FRAUDE} (c_actual_issued_amount)")
ecrs = df_dim["ecr_id_correto"].dropna().tolist()


def quote_int(lst):
    return ",".join(str(x) for x in lst)


def query_ecr_batches(ecr_list, sql_template, batch_size=1500, database="bonus_ec2"):
    dfs = []
    for i in range(0, len(ecr_list), batch_size):
        batch = ecr_list[i:i + batch_size]
        sql = sql_template.format(ecrs=quote_int(batch))
        dfs.append(query_athena(sql, database=database))
        print(f"   batch {i // batch_size + 1}/{(len(ecr_list) - 1) // batch_size + 1} OK")
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


sql_bonus = f"""
SELECT
    CAST(c_ecr_id AS VARCHAR)                          AS ecr_id_correto,
    COUNT(*)                                            AS qtd_bonus_emitidos_2703,
    SUM(c_actual_issued_amount)/100.0                   AS bonus_convertido_2703_brl,
    SUM(c_freespin_win)/100.0                           AS freespin_win_2703_brl
FROM bonus_ec2.tbl_bonus_summary_details
WHERE c_ecr_id IN ({{ecrs}})
  AND DATE(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') = DATE '{DATA_FRAUDE}'
GROUP BY c_ecr_id
"""
df_bonus = query_ecr_batches(ecrs, sql_bonus, batch_size=1500, database="bonus_ec2")
total_bonus = df_bonus["bonus_convertido_2703_brl"].sum() if len(df_bonus) > 0 else 0
print(f"   Jogadores com bonus em 27/03 (cohort): {len(df_bonus)}")
print(f"   Total bonus convertido (cohort):       R$ {total_bonus:,.2f}")

# ============================================================
# STEP 4 - Saldo atual em conta (cash + bonus)
# ============================================================
print("\n[STEP 4] Saldo atual em conta (ps_bi.fct_player_balance_daily)")
sql_saldo = """
WITH ranked AS (
    SELECT
        CAST(player_id AS VARCHAR)                         AS ecr_id_correto,
        created_date,
        COALESCE(cash_closing_balance_base, 0)             AS cash_brl,
        COALESCE(bonus_closing_balance_base, 0)            AS bonus_brl,
        ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY created_date DESC) AS rn
    FROM ps_bi.fct_player_balance_daily
    WHERE player_id IN ({ecrs})
)
SELECT
    ecr_id_correto,
    created_date                                            AS saldo_data_ref,
    ROUND(cash_brl, 2)                                      AS saldo_cash_atual_brl,
    ROUND(bonus_brl, 2)                                     AS saldo_bonus_atual_brl,
    ROUND(cash_brl + bonus_brl, 2)                          AS saldo_total_atual_brl
FROM ranked
WHERE rn = 1
"""
df_saldo = query_ecr_batches(ecrs, sql_saldo, batch_size=1500, database="ps_bi")
print(f"   Jogadores com saldo registrado: {len(df_saldo)}")

# ============================================================
# STEP 5 - Saques pos-27/03 (priorizacao - dinheiro ja saiu)
# ============================================================
print(f"\n[STEP 5] Saques pos-{DATA_FRAUDE}")
sql_saques = f"""
SELECT
    CAST(c_ecr_id AS VARCHAR)                                  AS ecr_id_correto,
    COUNT(*)                                                   AS qtd_saques_pos_2703,
    SUM(c_amount_in_ecr_ccy)/100.0                             AS valor_saques_pos_2703_brl,
    MAX(c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS ultimo_saque_brt
FROM fund_ec2.tbl_real_fund_txn
WHERE c_ecr_id IN ({{ecrs}})
  AND c_txn_type = 2
  AND c_txn_status = 'SUCCESS'
  AND c_start_time >= TIMESTAMP '{DATA_FRAUDE} 03:00:00'
GROUP BY c_ecr_id
"""
df_saques = query_ecr_batches(ecrs, sql_saques, batch_size=1500, database="fund_ec2")
print(f"   Jogadores com saque pos-27/03: {len(df_saques)}")

# ============================================================
# STEP 6 - NGR/GGR LIFETIME para rentabilidade (etapa 3 - reativacao)
# ============================================================
print("\n[STEP 6] NGR/GGR LIFETIME (rentabilidade para etapa 3 - reativacao)")
sql_ngr = """
SELECT
    CAST(player_id AS VARCHAR)                          AS ecr_id_correto,
    SUM(COALESCE(ggr_base, 0))                          AS ggr_lifetime_brl,
    SUM(COALESCE(ngr_base, 0))                          AS ngr_lifetime_brl
FROM ps_bi.fct_player_activity_daily
WHERE player_id IN ({ecrs})
GROUP BY player_id
"""
try:
    df_ngr = query_ecr_batches(ecrs, sql_ngr, batch_size=1500, database="ps_bi")
    print(f"   Jogadores com NGR: {len(df_ngr)}")
except Exception as e:
    print(f"   AVISO: NGR via fct_player_activity_daily falhou ({e})")
    print("   Tentando fonte alternativa: fct_player_performance_by_period")
    sql_ngr_alt = """
    SELECT
        CAST(player_id AS VARCHAR)                          AS ecr_id_correto,
        SUM(COALESCE(ggr_base, 0))                          AS ggr_lifetime_brl,
        SUM(COALESCE(ngr_base, 0))                          AS ngr_lifetime_brl
    FROM ps_bi.fct_player_performance_by_period
    WHERE player_id IN ({ecrs})
      AND period = 'LIFETIME'
    GROUP BY player_id
    """
    try:
        df_ngr = query_ecr_batches(ecrs, sql_ngr_alt, batch_size=1500, database="ps_bi")
    except Exception:
        df_ngr = pd.DataFrame(columns=["ecr_id_correto", "ggr_lifetime_brl", "ngr_lifetime_brl"])
        print("   AVISO: NGR nao disponivel - sera deixado em branco")

# ============================================================
# STEP 7 - Validacao cruzada raw vs gold
# ============================================================
print(f"\n[STEP 7] Validacao cruzada raw vs gold (toda a base, nao so cohort)")
sql_cross = f"""
SELECT
    'raw_tbl_bonus_summary_details'                AS fonte,
    COUNT(DISTINCT c_ecr_id)                       AS qtd_jogadores,
    SUM(c_actual_issued_amount)/100.0              AS valor_brl
FROM bonus_ec2.tbl_bonus_summary_details
WHERE DATE(c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') = DATE '{DATA_FRAUDE}'
"""
try:
    df_cross = query_athena(sql_cross, database="bonus_ec2")
    print(df_cross.to_string(index=False))
except Exception as e:
    print(f"   AVISO: validacao cruzada falhou: {e}")
    df_cross = pd.DataFrame()

# Tentar gold
try:
    sql_gold = f"""
    SELECT
        'gold_fct_bonus_activity_daily'  AS fonte,
        COUNT(DISTINCT player_id)        AS qtd_jogadores,
        SUM(bonus_redeemed_base)         AS valor_brl
    FROM ps_bi.fct_bonus_activity_daily
    WHERE created_date = DATE '{DATA_FRAUDE}'
    """
    df_gold = query_athena(sql_gold, database="ps_bi")
    print(df_gold.to_string(index=False))
    df_cross = pd.concat([df_cross, df_gold], ignore_index=True)
except Exception as e:
    print(f"   AVISO: gold layer nao acessivel: {e}")

# ============================================================
# STEP 8 - Consolidacao
# ============================================================
print("\n[STEP 8] Consolidando dados")
df_unicos["ID_original"] = df_unicos["ID"]
df_unicos["external_id"] = df_unicos["ID_str"]

df_out = df_unicos[["ID_original", "external_id"]].merge(
    df_dim, on="external_id", how="left"
)
df_out = df_out.merge(df_bonus, on="ecr_id_correto", how="left")
df_out = df_out.merge(df_saldo, on="ecr_id_correto", how="left")
df_out = df_out.merge(df_saques, on="ecr_id_correto", how="left")
if len(df_ngr) > 0:
    df_out = df_out.merge(df_ngr, on="ecr_id_correto", how="left")
else:
    df_out["ggr_lifetime_brl"] = None
    df_out["ngr_lifetime_brl"] = None

# Persistencia intermediaria (recovery se Excel falhar)
_tmp_dir = "solicitacoes_pontuais/fraude_freespins_2703_remocao_saldo/tmp"
os.makedirs(_tmp_dir, exist_ok=True)
df_out.to_pickle(os.path.join(_tmp_dir, "df_out_intermediario.pkl"))
print(f"   Snapshot intermediario salvo: {os.path.join(_tmp_dir, 'df_out_intermediario.pkl')}")

# Preencher NaN
for col, default in [
    ("qtd_bonus_emitidos_2703", 0),
    ("bonus_convertido_2703_brl", 0.0),
    ("freespin_win_2703_brl", 0.0),
    ("saldo_cash_atual_brl", 0.0),
    ("saldo_bonus_atual_brl", 0.0),
    ("saldo_total_atual_brl", 0.0),
    ("qtd_saques_pos_2703", 0),
    ("valor_saques_pos_2703_brl", 0.0),
    ("ggr_lifetime_brl", 0.0),
    ("ngr_lifetime_brl", 0.0),
]:
    if col in df_out.columns:
        df_out[col] = df_out[col].fillna(default)

# Flags
df_out["saldo_zero"] = df_out["saldo_total_atual_brl"].apply(lambda x: "Sim" if x < 1 else "Nao")
df_out["recebeu_bonus_2703"] = df_out["bonus_convertido_2703_brl"].apply(lambda x: "Sim" if x > 0 else "Nao")
df_out["rentavel_lifetime"] = df_out["ngr_lifetime_brl"].apply(lambda x: "Sim" if x and x > 0 else "Nao")
df_out["teve_saque_pos_2703"] = df_out["qtd_saques_pos_2703"].apply(lambda x: "Sim" if x > 0 else "Nao")


# Risco de remocao
def calc_risco(row):
    bonus = row.get("bonus_convertido_2703_brl", 0) or 0
    saldo = row.get("saldo_total_atual_brl", 0) or 0
    sacou = row.get("valor_saques_pos_2703_brl", 0) or 0

    if bonus == 0:
        return "SEM_BONUS_2703"
    if saldo >= bonus:
        return "OK_REMOVER_TOTAL"
    if saldo > 0 and saldo < bonus:
        return "REMOCAO_PARCIAL"
    if saldo < 1 and sacou >= bonus:
        return "DINHEIRO_JA_SACADO"
    return "SALDO_INSUFICIENTE"


df_out["risco_remocao"] = df_out.apply(calc_risco, axis=1)

# Ordenacao: prioridade descendente de bonus convertido
df_out = df_out.sort_values(["bonus_convertido_2703_brl", "saldo_total_atual_brl"], ascending=[False, False])


# Remover timezones para Excel (openpyxl nao aceita tz-aware datetime)
def strip_tz(series):
    """Remove timezone de coluna datetime; mantem demais tipos intactos."""
    if pd.api.types.is_datetime64_any_dtype(series):
        try:
            return series.dt.tz_localize(None)
        except (TypeError, AttributeError):
            return series
    if series.dtype == "object":
        return series.apply(
            lambda x: x.replace(tzinfo=None) if hasattr(x, "tzinfo") and getattr(x, "tzinfo", None) else x
        )
    return series


for col in df_out.columns:
    df_out[col] = strip_tz(df_out[col])

# Forcar colunas datetime conhecidas para timezone-naive
for col in ["ultimo_saque_brt", "saldo_data_ref", "signup_datetime", "ftd_date"]:
    if col in df_out.columns:
        df_out[col] = pd.to_datetime(df_out[col], errors="coerce", utc=True).dt.tz_convert(None) \
            if pd.api.types.is_datetime64_any_dtype(df_out[col]) and df_out[col].dt.tz is not None \
            else pd.to_datetime(df_out[col], errors="coerce")
        # Se ainda tem tz, derruba
        try:
            df_out[col] = df_out[col].dt.tz_localize(None)
        except (TypeError, AttributeError):
            pass

# Colunas finais (ordem)
COLS_FINAIS = [
    "ID_original", "external_id", "ecr_id_correto",
    "recebeu_bonus_2703", "qtd_bonus_emitidos_2703", "bonus_convertido_2703_brl", "freespin_win_2703_brl",
    "saldo_cash_atual_brl", "saldo_bonus_atual_brl", "saldo_total_atual_brl", "saldo_zero", "saldo_data_ref",
    "risco_remocao",
    "qtd_saques_pos_2703", "valor_saques_pos_2703_brl", "ultimo_saque_brt", "teve_saque_pos_2703",
    "ggr_lifetime_brl", "ngr_lifetime_brl", "rentavel_lifetime",
    "ecr_status", "country_code", "is_test", "signup_datetime", "ftd_date",
]
COLS_FINAIS = [c for c in COLS_FINAIS if c in df_out.columns]
df_out_final = df_out[COLS_FINAIS].copy()

# ============================================================
# STEP 9 - Aba Auditoria
# ============================================================
print("\n[STEP 9] Preparando aba Auditoria")
auditoria_rows = []
auditoria_rows.append({"categoria": "Total linhas no arquivo original", "qtd": len(df_raw), "valor_brl": ""})
auditoria_rows.append({"categoria": "Registros vazios (NaN)", "qtd": len(df_nulos), "valor_brl": ""})
auditoria_rows.append({"categoria": "IDs duplicados (removidos, 1 mantido)", "qtd": df_validos["ID_str"].duplicated().sum(), "valor_brl": ""})
auditoria_rows.append({"categoria": "IDs corrompidos em notacao cientifica", "qtd": len(df_corrompidos), "valor_brl": ""})
auditoria_rows.append({"categoria": "IDs unicos validos processados", "qtd": len(df_unicos), "valor_brl": ""})
auditoria_rows.append({"categoria": "IDs mapeados em ps_bi.dim_user", "qtd": len(df_dim), "valor_brl": ""})
auditoria_rows.append({"categoria": "IDs nao mapeados", "qtd": len(df_nao_mapeados), "valor_brl": ""})
auditoria_rows.append({"categoria": "IDs com bonus em 27/03 (cohort)", "qtd": len(df_bonus), "valor_brl": f"R$ {total_bonus:,.2f}"})
auditoria_rows.append({"categoria": "IDs com saldo atual >= R$ 1", "qtd": (df_out_final["saldo_total_atual_brl"] >= 1).sum(), "valor_brl": f"R$ {df_out_final[df_out_final['saldo_total_atual_brl']>=1]['saldo_total_atual_brl'].sum():,.2f}"})
auditoria_rows.append({"categoria": "IDs com saque pos-27/03", "qtd": (df_out_final["qtd_saques_pos_2703"] > 0).sum(), "valor_brl": f"R$ {df_out_final['valor_saques_pos_2703_brl'].sum():,.2f}"})
df_audit_resumo = pd.DataFrame(auditoria_rows)

# Listas detalhadas para Auditoria
df_audit_corrompidos = df_corrompidos[["ID"]].copy()
df_audit_corrompidos.columns = ["ID_corrompido_na_origem"]
df_audit_nao_mapeados = df_nao_mapeados[["ID_str"]].copy()
df_audit_nao_mapeados.columns = ["ID_nao_encontrado_no_banco"]

# Risco de remocao distribuicao
df_risco_dist = df_out_final["risco_remocao"].value_counts().reset_index()
df_risco_dist.columns = ["risco_remocao", "qtd"]

# ============================================================
# STEP 10 - Aba Legenda
# ============================================================
print("\n[STEP 10] Gerando aba Legenda")
legenda_data = [
    ["ID_original", "ID exatamente como veio na planilha de origem (string)"],
    ["external_id", "ID Smartico (external_id em ps_bi.dim_user) - chave de cruzamento entre Smartico e banco interno"],
    ["ecr_id_correto", "ID interno 18 digitos (Pragmatic ECR) - recuperado via dim_user para evitar truncamento float"],
    ["recebeu_bonus_2703", "Sim/Nao - recebeu algum bonus emitido em 27/03/2026 (criterio: c_actual_issued_amount > 0)"],
    ["qtd_bonus_emitidos_2703", "Quantidade de registros de bonus emitidos para o jogador em 27/03/2026 BRT"],
    ["bonus_convertido_2703_brl", "BRL - valor de bonus convertido em saldo real em 27/03 (Pragmatic = 'Actual Issued Amount' = 'Total Bonus Cost')"],
    ["freespin_win_2703_brl", "BRL - ganho gerado pelos freespins em 27/03 (campo informativo - tipicamente igual a bonus_convertido para freespins)"],
    ["saldo_cash_atual_brl", "BRL - saldo cash atual em conta (ultimo registro disponivel em fct_player_balance_daily)"],
    ["saldo_bonus_atual_brl", "BRL - saldo bonus atual em conta"],
    ["saldo_total_atual_brl", "BRL - saldo total atual (cash + bonus)"],
    ["saldo_zero", "Sim/Nao - jogador tem saldo total < R$ 1 (criterio para etapa 2 - avaliar desbloqueio)"],
    ["saldo_data_ref", "Data do ultimo registro de movimentacao de saldo. fct_player_balance_daily so insere quando ha mudanca - se aparece 27/03, significa saldo ESTAVEL desde aquela data."],
    ["risco_remocao", "Classificacao do risco operacional de remover o saldo:"],
    ["", "  - OK_REMOVER_TOTAL = saldo atual >= bonus convertido, remocao integral viavel"],
    ["", "  - REMOCAO_PARCIAL = saldo atual < bonus convertido (jogador ja gastou parte) - so da pra remover o que sobrou"],
    ["", "  - DINHEIRO_JA_SACADO = saldo zerado + saque pos-27/03 >= bonus convertido (dinheiro saiu do caixa)"],
    ["", "  - SALDO_INSUFICIENTE = saldo zerado sem saque equivalente (perdeu apostando)"],
    ["", "  - SEM_BONUS_2703 = jogador esta na planilha mas nao recebeu bonus em 27/03 no nosso banco (investigar)"],
    ["qtd_saques_pos_2703", "Quantidade de saques aprovados desde 27/03"],
    ["valor_saques_pos_2703_brl", "BRL - soma dos saques aprovados desde 27/03"],
    ["ultimo_saque_brt", "Timestamp BRT do ultimo saque aprovado"],
    ["teve_saque_pos_2703", "Sim/Nao - flag binaria de saque pos-fraude"],
    ["ggr_lifetime_brl", "BRL - Gross Gaming Revenue total historico do jogador"],
    ["ngr_lifetime_brl", "BRL - Net Gaming Revenue total historico (GGR - bonus - taxas)"],
    ["rentavel_lifetime", "Sim/Nao - NGR historico > 0 (criterio para etapa 3 - reativacao pre-Copa)"],
    ["ecr_status", "Status atual da conta (active/blocked/etc) - acelera etapa 2 de desbloqueio"],
    ["country_code", "Pais"],
    ["is_test", "True = conta de teste (filtrar se aplicavel)"],
    ["signup_datetime", "Data de cadastro"],
    ["ftd_date", "Data do First Time Deposit"],
    ["", ""],
    ["GLOSSARIO", ""],
    ["Bonus convertido em saldo real", "Valor que estava em saldo bonus restrito e foi liberado como saldo real apos cumprir wagering. Campo PGS: 'Actual Issued Amount' / 'Total Bonus Cost'."],
    ["Wagering", "Requisito de apostas a cumprir antes do bonus virar saldo real. Na fraude, foi cumprido artificialmente via bet+cancel."],
    ["BTR (Bonus Total Redeemed)", "Termo interno para bonus_convertido_2703_brl"],
    ["GGR", "Gross Gaming Revenue: apostas - ganhos do jogador"],
    ["NGR", "Net Gaming Revenue: GGR - bonus distribuido - taxas"],
    ["", ""],
    ["FONTES TECNICAS", ""],
    ["Bonus convertido", "bonus_ec2.tbl_bonus_summary_details (c_actual_issued_amount em c_issue_date BRT = 27/03)"],
    ["Saldo atual", "ps_bi.fct_player_balance_daily (ultimo registro por player_id)"],
    ["Saques pos-fraude", "fund_ec2.tbl_real_fund_txn (c_txn_type=2, c_txn_status='SUCCESS', c_start_time >= 27/03)"],
    ["Mapeamento ID", "ps_bi.dim_user (external_id Smartico -> ecr_id Pragmatic)"],
    ["NGR/GGR Lifetime", "ps_bi.fct_player_activity_daily (agregado lifetime)"],
]
df_legenda = pd.DataFrame(legenda_data, columns=["Campo / Conceito", "Descricao"])


# ============================================================
# STEP 11 - Hash do snapshot
# ============================================================
print("\n[STEP 11] Gerando hash do snapshot")
hash_input = df_out_final.to_csv(index=False).encode("utf-8")
snapshot_hash = hashlib.sha256(hash_input).hexdigest()[:16]
metadata = {
    "snapshot_hash": snapshot_hash,
    "timestamp": TIMESTAMP,
    "input_file": INPUT_XLSX,
    "data_fraude": DATA_FRAUDE,
    "qtd_linhas_originais": int(len(df_raw)),
    "qtd_unicos_processados": int(len(df_unicos)),
    "qtd_mapeados_dim_user": int(len(df_dim)),
    "qtd_nao_mapeados": int(len(df_nao_mapeados)),
    "qtd_corrompidos_cientifica": int(len(df_corrompidos)),
    "qtd_com_bonus_2703": int(len(df_bonus)),
    "total_bonus_convertido_brl": float(round(total_bonus, 2)),
    "total_saldo_atual_brl": float(round(df_out_final["saldo_total_atual_brl"].sum(), 2)),
    "total_saques_pos_2703_brl": float(round(df_out_final["valor_saques_pos_2703_brl"].sum(), 2)),
    "distribuicao_risco_remocao": df_risco_dist.to_dict(orient="records"),
}
print(json.dumps(metadata, indent=2, ensure_ascii=False))

# Salvar metadata JSON ao lado do Excel
os.makedirs(OUTPUT_DIR, exist_ok=True)
metadata_path = OUTPUT_PATH.replace(".xlsx", "_metadata.json")
with open(metadata_path, "w", encoding="utf-8") as f:
    json.dump(metadata, f, indent=2, ensure_ascii=False)


# ============================================================
# STEP 12 - Excel final
# ============================================================
print(f"\n[STEP 12] Gravando Excel: {OUTPUT_PATH}")
with pd.ExcelWriter(OUTPUT_PATH, engine="openpyxl") as w:
    df_out_final.to_excel(w, sheet_name="Remocao_Saldo", index=False)

    # Aba Auditoria com multiplas secoes
    df_audit_resumo.to_excel(w, sheet_name="Auditoria", index=False, startrow=0)
    pd.DataFrame([{"": ""}]).to_excel(w, sheet_name="Auditoria", index=False, startrow=len(df_audit_resumo) + 2, header=False)
    pd.DataFrame([{"Distribuicao por risco_remocao": ""}]).to_excel(w, sheet_name="Auditoria", index=False, startrow=len(df_audit_resumo) + 3, header=False)
    df_risco_dist.to_excel(w, sheet_name="Auditoria", index=False, startrow=len(df_audit_resumo) + 5)
    if len(df_audit_corrompidos) > 0:
        next_row = len(df_audit_resumo) + len(df_risco_dist) + 9
        pd.DataFrame([{"IDs corrompidos por notacao cientifica - precisam CSV original": ""}]).to_excel(
            w, sheet_name="Auditoria", index=False, startrow=next_row, header=False
        )
        df_audit_corrompidos.to_excel(w, sheet_name="Auditoria", index=False, startrow=next_row + 2)
    if len(df_audit_nao_mapeados) > 0:
        start = len(df_audit_resumo) + len(df_risco_dist) + len(df_audit_corrompidos) + 15
        pd.DataFrame([{"IDs nao encontrados em ps_bi.dim_user (verificar manualmente)": ""}]).to_excel(
            w, sheet_name="Auditoria", index=False, startrow=start, header=False
        )
        df_audit_nao_mapeados.to_excel(w, sheet_name="Auditoria", index=False, startrow=start + 2)

    df_legenda.to_excel(w, sheet_name="Legenda", index=False)

    if len(df_cross) > 0:
        df_cross.to_excel(w, sheet_name="Validacao_Raw_vs_Gold", index=False)

print(f"\nOK! Output: {OUTPUT_PATH}")
print(f"   Metadata: {metadata_path}")
print(f"   Snapshot hash: {snapshot_hash}")
print(f"\n   Total bonus convertido (cohort): R$ {total_bonus:,.2f}")
print(f"   Distribuicao risco_remocao:")
print(df_risco_dist.to_string(index=False))
