"""
Extrai 3 CSVs granulares (por transacao) para auditoria de pagamento
de afiliados referente a MARCO/2026 (01/03 a 31/03 BRT, mes fechado).

Mapeamento PDF back-office -> Athena:
  - sports_transactions    -> vendor_ec2.tbl_sports_book_bets_info (+ LEFT JOIN info)
  - t_casino_transactions  -> fund_ec2.tbl_real_fund_txn (c_product_id='CASINO')
  - t_transactions         -> fund_ec2.tbl_real_fund_txn (todas transacoes)

Vinculo com affiliate_id: transacao.user_id = ps_bi.dim_user.external_id/ecr_id
                          -> ps_bi.dim_user.affiliate_id

Regras (CLAUDE.md):
  - Athena read-only, timezone UTC -> BRT sempre
  - fund_ec2: valores em centavos (/100), status='SUCCESS'
  - vendor_ec2 sportsbook: valores em BRL real
  - Filtro test users: is_test = false (ps_bi)

Output em reports/afiliados_marco_auditoria/:
  - afiliados_consolidado.csv          (97 IDs AffiliatesBR)
  - sports_affbr_marco_afiliados.csv         + _legenda.txt
  - casino_affbr_marco_afiliados.csv         + _legenda.txt
  - geral_affbr_marco_afiliados.csv          + _legenda.txt
  - README_auditoria.md

Uso:
    python scripts/extract_afiliados_marco_auditoria.py --preview   # so counts (dimensionar)
    python scripts/extract_afiliados_marco_auditoria.py --run       # baixa os 3 CSVs
"""
import sys
import os
import argparse
import logging
from datetime import datetime

sys.path.insert(0, r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet")

import pandas as pd
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

# =====================================================================
# PARAMETROS FIXOS
# =====================================================================
# Lista AffiliatesBR (97 IDs) — lote de abril/2026 para auditoria de marco
AFFILIATE_IDS = [
    "526644", "526453", "523755", "523655", "523654", "523106", "523105",
    "522848", "522847", "522633", "511007", "508666", "506089", "506088",
    "505689", "505602", "504946", "504944", "502676", "501192", "501191",
    "500809", "500807", "489458", "489457", "489444", "489443", "489307",
    "489306", "477184", "477182", "476899", "476894", "476875", "473708",
    "473479", "472630", "471929", "471922", "458116", "457204", "452463",
    "451350", "451346", "449452", "449417", "449299", "449267", "449265",
    "449113", "449092", "447544", "447195", "447194", "447193", "446890",
    "446237", "445982", "445046", "445045", "445042", "444946", "444944",
    "444940", "444801", "442805", "442803", "442733", "442181", "442095",
    "441962", "441961", "441950", "441949", "441724", "441723", "432314",
    "432313", "432311", "432309", "431790", "431788", "431729", "431727",
    "431726", "431725", "431723", "431613", "431611", "431608", "431589",
    "431587", "427530", "427497", "427496", "427462", "427398",
]

# Janela fechada BRT: 01/03 00:00 -> 01/04 00:00 (exclusivo) = marco completo (31 dias)
# UTC equivalente: 01/03 03:00 -> 01/04 03:00
PERIOD_START_UTC = "2026-03-01 03:00:00"
PERIOD_END_UTC   = "2026-04-01 03:00:00"
PERIOD_LABEL     = "2026-03-01 a 2026-03-31 (BRT) — marco completo"

OUT_DIR = os.path.join(r"c:\Users\NITRO\OneDrive - PGX\Projetos - Super Nova\MultiBet", "reports", "afiliados_affbr_marco_auditoria")
os.makedirs(OUT_DIR, exist_ok=True)


# =====================================================================
# MAPEAMENTO DE c_txn_type (fund_ec2) -> descricao legivel
# =====================================================================
TXN_TYPE_DESC = {
    1: "REAL_CASH_DEPOSIT", 2: "REAL_CASH_WITHDRAW",
    3: "REAL_CASH_ADDITION_BY_CS", 4: "REAL_CASH_REMOVAL_BY_CS",
    6: "REAL_CASH_ADDITION_BY_CAMPAIGN",
    27: "CASINO_BUYIN", 28: "CASINO_REBUY", 29: "CASINO_LEAVE_TABLE",
    36: "REAL_CASH_CASHOUT_REVERSAL",
    41: "CASINO_TOURNAMENTS_BUYIN", 42: "CASINO_TOURNAMENTS_UN_REGISTER",
    43: "CASINO_TOURNAMENTS_REBUY", 44: "CASINO_TOURNAMENTS_PRIZE_AWARD",
    45: "CASINO_WIN", 51: "POSITIVE_ADJUSTMENT", 52: "NEGATIVE_ADJUSTMENT",
    54: "CASHOUT_FEE", 55: "INACTIVE_FEE", 56: "INACTIVE_FEE_REVERSAL",
    57: "CASHOUT_FEE_REVERSAL",
    59: "SB_BUYIN", 60: "SB_LEAVE_TABLE", 61: "SB_BUYIN_CANCEL",
    65: "JACKPOT_WIN", 68: "CASINO_TIP",
    72: "CASINO_BUYIN_CANCEL", 73: "CASINO_LEAVE_TABLE_CANCEL",
    76: "CASINO_REBUY_CANCEL", 77: "CASINO_WIN_CANCEL",
    78: "MIGRATION_TYPE", 79: "CASINO_TOURN_WIN", 80: "CASINO_FREESPIN_WIN",
    86: "CASINO_FREESPIN_WIN_CANCEL", 90: "REAL_CASH_DEBIT_FOR_USER_INACTIVITY",
    91: "CASINO_REFUND_BET", 95: "IAT_USER_DEBIT", 96: "IAT_USER_CREDIT",
    114: "JACKPOT_WIN_CANCEL", 126: "REAL_CASH_DEPOSIT_REFUND",
    129: "WIN_FEES",
    130: "CASINO_MANUAL_DEBIT", 131: "CASINO_MANUAL_CREDIT",
    132: "CASINO_FREESPIN_BUYIN", 133: "CASINO_FREESPIN_BUYIN_CANCEL",
}


def txn_type_desc(v):
    try:
        return TXN_TYPE_DESC.get(int(v), f"TYPE_{v}")
    except (ValueError, TypeError):
        return f"TYPE_{v}"


# =====================================================================
# STEP 1 — resolver players dos 12 affiliates
# =====================================================================
def resolve_players():
    ids_in = ", ".join([f"'{x}'" for x in AFFILIATE_IDS])
    sql = f"""
    SELECT
        CAST(ecr_id AS VARCHAR)       AS ecr_id,
        CAST(external_id AS VARCHAR)  AS external_id,
        CAST(affiliate_id AS VARCHAR) AS affiliate_id,
        affiliate                     AS affiliate_name
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ({ids_in})
      AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    """
    log.info("[1/4] Resolvendo players (ps_bi.dim_user is_test=false)...")
    df = query_athena(sql, database="ps_bi")
    log.info(f"  -> {len(df):,} jogadores nos {len(AFFILIATE_IDS)} afiliados")
    breakdown = df.groupby("affiliate_id").size().reset_index(name="qty_players")
    log.info("  Breakdown por affiliate_id:\n%s", breakdown.to_string(index=False))
    return df


# =====================================================================
# STEP 2 — SPORTS (vendor_ec2.tbl_sports_book_bets_info)
# =====================================================================
def query_sports(players):
    """Grao: 1 linha por bilhete (bets_info header)."""
    ext_ids = [x for x in players["external_id"].dropna().astype(str).unique() if x]
    if not ext_ids:
        return pd.DataFrame()
    chunk = ", ".join(f"'{x}'" for x in ext_ids)
    affils_in = ", ".join(f"'{x}'" for x in AFFILIATE_IDS)

    sql = f"""
    WITH affil_players AS (
        SELECT
            CAST(external_id AS VARCHAR)  AS external_id,
            CAST(affiliate_id AS VARCHAR) AS affiliate_id,
            affiliate                     AS affiliate_name
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in})
          AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    ),
    bets_header_raw AS (
        -- Header dos bilhetes criados em marco — bruto com CDC (pode ter N linhas por bet_slip)
        SELECT
            c_bet_slip_id,
            c_bet_id,
            c_total_stake,
            c_bonus_amount,
            c_total_return,
            c_total_odds,
            c_bet_type,
            c_is_free,
            c_is_live,
            c_pam_bonus_txn_id,
            c_created_time,
            c_updated_time,
            c_bet_closure_time,
            ROW_NUMBER() OVER (PARTITION BY c_bet_slip_id
                               ORDER BY c_updated_time DESC NULLS LAST,
                                        c_created_time   DESC) AS rn
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '{PERIOD_START_UTC}'
          AND c_created_time <  TIMESTAMP '{PERIOD_END_UTC}'
    ),
    bets_header AS (
        -- 1 linha por bet_slip (versao mais recente via CDC)
        SELECT * FROM bets_header_raw WHERE rn = 1
    ),
    info_dedup AS (
        -- tbl_sports_book_info tambem pode ter CDC. Dedup por c_transaction_id
        SELECT *, ROW_NUMBER() OVER (PARTITION BY c_transaction_id ORDER BY c_bet_slip_id) AS rn_info
        FROM vendor_ec2.tbl_sports_book_info
    )
    SELECT
        CAST(sbi.c_transaction_id AS VARCHAR)         AS id,
        sbi.c_vendor_id                               AS source,
        CAST(sbi.c_transaction_id AS VARCHAR)         AS ext_bet_transaction_id,
        CAST(sbi.c_bet_slip_id AS VARCHAR)            AS ext_ticket_id,
        CAST(h.c_bet_id AS VARCHAR)                   AS ext_bet_id,
        sbi.c_operation_type                          AS type,
        sbi.c_bet_slip_state                          AS status,
        CAST(sbi.c_amount AS DOUBLE)                  AS amount,
        CAST(h.c_total_stake AS DOUBLE)               AS stake_amount,
        CAST(h.c_bonus_amount AS DOUBLE)              AS freebet_amount,
        CAST(h.c_total_return AS DOUBLE)              AS gain_amount,
        CAST(sbi.c_transaction_fee_amount AS DOUBLE)  AS feature_amount,
        h.c_bet_type                                  AS bet_type,
        h.c_total_odds                                AS odds,
        CASE WHEN sbi.c_operation_type IN ('C','R')        THEN TRUE ELSE FALSE END AS is_rollover,
        CASE WHEN sbi.c_operation_type IN ('MC','MD')      THEN TRUE ELSE FALSE END AS is_resettlement,
        CASE WHEN sbi.c_operation_type IN ('P')            THEN TRUE ELSE FALSE END AS is_settlement,
        NULL                                          AS is_combo_bonus,
        h.c_is_free                                   AS is_freebet,
        CAST(h.c_pam_bonus_txn_id AS VARCHAR)         AS ext_freebet_id,
        NULL                                          AS ext_freebet_source,
        CAST(sbi.c_customer_id AS VARCHAR)            AS user_id,
        NULL                                          AS sports_token_id,
        CAST(sbi.c_bet_slip_id AS VARCHAR)            AS reference_transaction_id,
        'bet_slip'                                    AS reference_transaction_type,
        NULL                                          AS sports_event_id,
        CAST(h.c_created_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS TIMESTAMP) AS created_at,
        CAST(h.c_updated_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS TIMESTAMP) AS updated_at,
        CAST(h.c_bet_closure_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS TIMESTAMP) AS settled_at,
        sbi.c_transaction_fee_type                    AS fee_type,
        p.affiliate_id                                AS affiliate_id,
        p.affiliate_name                              AS affiliate_name
    FROM info_dedup sbi
    INNER JOIN bets_header h
        ON sbi.c_bet_slip_id = h.c_bet_slip_id
    INNER JOIN affil_players p
        ON CAST(sbi.c_customer_id AS VARCHAR) = p.external_id
    WHERE sbi.rn_info = 1
    ORDER BY created_at, id
    """
    log.info("[2/4] Baixando SPORTS (vendor_ec2.tbl_sports_book_info — por operacao)...")
    df = query_athena(sql, database="vendor_ec2")
    log.info(f"  -> {len(df):,} linhas")
    return df


# =====================================================================
# STEP 3 — CASINO (fund_ec2.tbl_real_fund_txn c_product_id='CASINO')
# =====================================================================
def query_casino():
    affils_in = ", ".join(f"'{x}'" for x in AFFILIATE_IDS)
    sql = f"""
    WITH affil_players AS (
        SELECT
            CAST(ecr_id AS VARCHAR)       AS ecr_id,
            CAST(external_id AS VARCHAR)  AS external_id,
            CAST(affiliate_id AS VARCHAR) AS affiliate_id,
            affiliate                     AS affiliate_name
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in})
          AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    )
    SELECT
        CAST(t.c_txn_id AS VARCHAR)                           AS id,
        t.c_sub_vendor_id                                     AS provider,
        CAST(t.c_txn_id AS VARCHAR)                           AS reference,
        CAST(t.c_txn_type AS INTEGER)                         AS type_id,
        t.c_txn_status                                        AS status,
        CAST(t.c_amount_in_ecr_ccy AS DOUBLE) / 100.0         AS amount,
        CAST(t.c_ecr_id AS VARCHAR)                           AS user_id,
        t.c_game_id                                           AS game_id,
        t.c_session_id                                        AS round_id,
        NULL                                                  AS round_details,
        CASE WHEN t.c_txn_type IN (80, 86, 132, 133) THEN 1 ELSE 0 END AS is_freespin,
        NULL                                                  AS reference_transaction_id,
        NULL                                                  AS reference_transaction_type,
        t.c_session_id                                        AS casino_session_id,
        CAST(to_unixtime(t.c_start_time) AS BIGINT)           AS unix_timestamp,
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS TIMESTAMP) AS created_at,
        NULL                                                  AS updated_at,
        t.c_op_type                                           AS op_type,
        t.c_channel                                           AS channel,
        p.affiliate_id                                        AS affiliate_id,
        p.affiliate_name                                      AS affiliate_name
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN affil_players p
        ON CAST(t.c_ecr_id AS VARCHAR) = p.ecr_id
    WHERE t.c_start_time >= TIMESTAMP '{PERIOD_START_UTC}'
      AND t.c_start_time <  TIMESTAMP '{PERIOD_END_UTC}'
      AND t.c_product_id = 'CASINO'
      AND t.c_txn_status = 'SUCCESS'
    ORDER BY created_at
    """
    log.info("[3/4] Baixando CASINO (fund_ec2.tbl_real_fund_txn c_product_id='CASINO')...")
    df = query_athena(sql, database="fund_ec2")
    log.info(f"  -> {len(df):,} linhas")
    if len(df):
        df["type"] = df["type_id"].apply(txn_type_desc)
    return df


# =====================================================================
# STEP 4 — GERAL (fund_ec2.tbl_real_fund_txn todas as transacoes)
# =====================================================================
def query_geral():
    affils_in = ", ".join(f"'{x}'" for x in AFFILIATE_IDS)
    sql = f"""
    WITH affil_players AS (
        SELECT
            CAST(ecr_id AS VARCHAR)       AS ecr_id,
            CAST(affiliate_id AS VARCHAR) AS affiliate_id,
            affiliate                     AS affiliate_name
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in})
          AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    )
    SELECT
        CAST(t.c_txn_id AS VARCHAR)                           AS id,
        CAST(t.c_txn_type AS INTEGER)                         AS type_id,
        CAST(t.c_amount_in_ecr_ccy AS DOUBLE) / 100.0         AS amount,
        t.c_txn_status                                        AS status,
        CAST(t.c_ecr_id AS VARCHAR)                           AS user_id,
        NULL                                                  AS wallet_id,
        t.c_product_id                                        AS src,
        t.c_sub_vendor_id                                     AS src_id,
        CAST(t.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS TIMESTAMP) AS created_at,
        NULL                                                  AS updated_at,
        NULL                                                  AS bonus_wallet_id,
        NULL                                                  AS credit_percent,
        NULL                                                  AS bonus_percent,
        NULL                                                  AS cashed_out_amount,
        NULL                                                  AS old_balance,
        NULL                                                  AS old_bonus_balance,
        t.c_op_type                                           AS op_type,
        t.c_channel                                           AS channel,
        t.c_game_id                                           AS game_id,
        t.c_session_id                                        AS session_id,
        p.affiliate_id                                        AS affiliate_id,
        p.affiliate_name                                      AS affiliate_name
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN affil_players p
        ON CAST(t.c_ecr_id AS VARCHAR) = p.ecr_id
    WHERE t.c_start_time >= TIMESTAMP '{PERIOD_START_UTC}'
      AND t.c_start_time <  TIMESTAMP '{PERIOD_END_UTC}'
      AND t.c_txn_status = 'SUCCESS'
    ORDER BY created_at
    """
    log.info("[4/4] Baixando GERAL (fund_ec2.tbl_real_fund_txn TODAS)...")
    df = query_athena(sql, database="fund_ec2")
    log.info(f"  -> {len(df):,} linhas")
    if len(df):
        df["type"] = df["type_id"].apply(txn_type_desc)
        # reordena pra batER com PDF (id, type, type_id, amount, status, user_id, wallet_id, src, src_id, created_at, ...)
        cols_pdf = [
            "id", "type", "type_id", "amount", "status", "user_id", "wallet_id",
            "src", "src_id", "created_at", "updated_at",
            "bonus_wallet_id", "credit_percent", "bonus_percent", "cashed_out_amount",
            "old_balance", "old_bonus_balance",
            "op_type", "channel", "game_id", "session_id",
            "affiliate_id", "affiliate_name",
        ]
        df = df[cols_pdf]
    return df


# =====================================================================
# PREVIEW — so counts (dimensionar volume/custo antes de baixar tudo)
# =====================================================================
def run_preview():
    affils_in = ", ".join(f"'{x}'" for x in AFFILIATE_IDS)
    log.info("=== PREVIEW — counts somente ===")

    # players
    sql_p = f"""
    SELECT COUNT(*) AS qty
    FROM ps_bi.dim_user
    WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in}) AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    """
    n_players = int(query_athena(sql_p, database="ps_bi")["qty"].iloc[0])
    log.info(f"  players (is_test=false): {n_players:,}")

    # sports — agora por operacao (tbl_sports_book_info)
    sql_s = f"""
    WITH p AS (
        SELECT CAST(external_id AS VARCHAR) AS external_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in}) AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    ),
    h AS (
        SELECT c_bet_slip_id
        FROM vendor_ec2.tbl_sports_book_bets_info
        WHERE c_created_time >= TIMESTAMP '{PERIOD_START_UTC}'
          AND c_created_time <  TIMESTAMP '{PERIOD_END_UTC}'
    )
    SELECT COUNT(*) AS qty
    FROM vendor_ec2.tbl_sports_book_info sbi
    INNER JOIN h ON sbi.c_bet_slip_id = h.c_bet_slip_id
    INNER JOIN p ON CAST(sbi.c_customer_id AS VARCHAR) = p.external_id
    """
    n_sports = int(query_athena(sql_s, database="vendor_ec2")["qty"].iloc[0])
    log.info(f"  sports (por operacao): {n_sports:,}")

    # casino
    sql_c = f"""
    WITH p AS (
        SELECT CAST(ecr_id AS VARCHAR) AS ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in}) AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    )
    SELECT COUNT(*) AS qty
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN p ON CAST(t.c_ecr_id AS VARCHAR) = p.ecr_id
    WHERE t.c_start_time >= TIMESTAMP '{PERIOD_START_UTC}'
      AND t.c_start_time <  TIMESTAMP '{PERIOD_END_UTC}'
      AND t.c_product_id = 'CASINO' AND t.c_txn_status = 'SUCCESS'
    """
    n_casino = int(query_athena(sql_c, database="fund_ec2")["qty"].iloc[0])
    log.info(f"  casino (fund CASINO SUCCESS): {n_casino:,}")

    # geral
    sql_g = f"""
    WITH p AS (
        SELECT CAST(ecr_id AS VARCHAR) AS ecr_id
        FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN ({affils_in}) AND is_test = false AND signup_datetime < TIMESTAMP '2026-04-01 03:00:00'
    )
    SELECT COUNT(*) AS qty
    FROM fund_ec2.tbl_real_fund_txn t
    INNER JOIN p ON CAST(t.c_ecr_id AS VARCHAR) = p.ecr_id
    WHERE t.c_start_time >= TIMESTAMP '{PERIOD_START_UTC}'
      AND t.c_start_time <  TIMESTAMP '{PERIOD_END_UTC}'
      AND t.c_txn_status = 'SUCCESS'
    """
    n_geral = int(query_athena(sql_g, database="fund_ec2")["qty"].iloc[0])
    log.info(f"  geral  (fund TODAS SUCCESS): {n_geral:,}")

    log.info("=== FIM PREVIEW ===")
    return {"players": n_players, "sports": n_sports, "casino": n_casino, "geral": n_geral}


# =====================================================================
# LEGENDAS / DICIONARIO
# =====================================================================
LEGENDA_SPORTS = f"""LEGENDA — sports_affbr_marco_afiliados.csv
Periodo: {PERIOD_LABEL}
Fonte: Athena vendor_ec2.tbl_sports_book_info (granular por operacao)
         + LEFT JOIN vendor_ec2.tbl_sports_book_bets_info (header do bilhete)
Grao: 1 linha por OPERACAO financeira do bilhete (commit, payout, lock, refund, rollback, manual credit/debit)
Janela: bilhetes com c_created_time em marco/2026 BRT
        (operacoes posteriores dos mesmos bilhetes tambem entram, ex: payout que ocorreu em 02/04
         de um bilhete criado em 30/03 — confirmar com auditoria se isso e desejado)
Valores em BRL real (nao centavos)

Mapeamento coluna PDF -> origem Athena:
- id                       = tbl_sports_book_info.c_transaction_id (ID da OPERACAO)
- source                   = tbl_sports_book_info.c_vendor_id (Sportradar, Altenar, PPBET)
- ext_bet_transaction_id   = c_transaction_id (da operacao)
- ext_ticket_id            = c_bet_slip_id
- ext_bet_id               = bets_info.c_bet_id
- type                     = c_operation_type (L=Lock, M=Commit, P=Payout, C=Cancel, R=Refund,
                             MC=Manual Credit, MD=Manual Debit)
- status                   = c_bet_slip_state (O=Open, C=Closed)
- amount                   = c_amount (BRL real — valor da OPERACAO)
- stake_amount             = bets_info.c_total_stake (BRL real — stake do bilhete todo)
- freebet_amount           = bets_info.c_bonus_amount
- gain_amount              = bets_info.c_total_return (NaN se bilhete aberto)
- feature_amount           = c_transaction_fee_amount (taxa da operacao)
- bet_type                 = bets_info.c_bet_type (PreLive | Live | Mixed)
- odds                     = bets_info.c_total_odds
- is_rollover              = TRUE se c_operation_type IN ('C','R')
- is_resettlement          = TRUE se c_operation_type IN ('MC','MD')
- is_settlement            = TRUE se c_operation_type = 'P'
- is_combo_bonus           = (sem equivalente direto - NULL)
- is_freebet               = bets_info.c_is_free
- ext_freebet_id           = bets_info.c_pam_bonus_txn_id
- ext_freebet_source       = (sem equivalente direto - NULL)
- user_id                  = c_customer_id (external_id = Smartico user_ext_id)
- sports_token_id          = (sem equivalente - NULL)
- reference_transaction_id = c_bet_slip_id (referencia o bilhete pai)
- reference_transaction_type = 'bet_slip'
- sports_event_id          = (disponivel via tbl_sports_book_bet_details, nao incluido neste corte)
- created_at               = bets_info.c_created_time (BRT) — criacao do BILHETE
- updated_at               = bets_info.c_updated_time (BRT)
- settled_at               = bets_info.c_bet_closure_time (BRT)
- fee_type                 = c_transaction_fee_type (NONE, DB, CR) — EXTRA
- affiliate_id             = ps_bi.dim_user.affiliate_id
- affiliate_name           = ps_bi.dim_user.affiliate

Filtros aplicados:
- bets_info.c_created_time entre {PERIOD_START_UTC} e {PERIOD_END_UTC}
  (UTC equivalente a 01/03 00h a 01/04 00h BRT)
- ps_bi.dim_user.is_test = false (jogadores de teste excluidos)
- ps_bi.dim_user.signup_datetime < 2026-04-01 00:00 BRT (players cadastrados ate 31/03)
- ps_bi.dim_user.affiliate_id IN (97 afiliados AffiliatesBR (lote grande))

Observacoes:
- 1 bilhete gera N linhas (1 por operacao). Ex: 1 bilhete comum = M (commit) + P (payout)
- stake_amount/gain_amount/odds sao REPLICADOS do header do bilhete em todas as operacoes do mesmo slip
- amount = valor da OPERACAO (diferente de stake_amount em payouts, rollbacks, etc.)
- Valores ja em BRL real - NAO dividir por 100
"""

LEGENDA_CASINO = f"""LEGENDA — casino_affbr_marco_afiliados.csv
Periodo: {PERIOD_LABEL}
Fonte: Athena fund_ec2.tbl_real_fund_txn (filtrado c_product_id='CASINO' e c_txn_status='SUCCESS')
Grao: 1 linha por transacao financeira de cassino (bet/win/rollback/etc.)
Valores em BRL real (ja convertidos de centavos -> /100)

Mapeamento coluna PDF -> origem Athena:
- id                       = c_txn_id
- provider                 = c_sub_vendor_id (pragmaticplay, hub88, pgsoft, alea_*, etc.)
- reference                = c_txn_id (proxy - nao ha ref externa isolada na tabela)
- type                     = descricao do c_txn_type (CASINO_BUYIN, CASINO_WIN, CASINO_BUYIN_CANCEL, etc.)
- type_id                  = c_txn_type (int) — codigo interno Pragmatic
- status                   = c_txn_status (filtrado SUCCESS)
- amount                   = c_amount_in_ecr_ccy / 100.0 (BRL real)
- user_id                  = c_ecr_id (ID interno 18 digitos)
- game_id                  = c_game_id
- round_id                 = c_session_id (sessao de jogo — proxy, nao ha round_id isolado)
- round_details            = (sem equivalente direto - NULL)
- is_freespin              = 1 se c_txn_type in (80=FREESPIN_WIN, 86=FREESPIN_WIN_CANCEL, 132=FREESPIN_BUYIN, 133=FREESPIN_BUYIN_CANCEL)
- reference_transaction_id = (sem equivalente direto - NULL)
- reference_transaction_type = (sem equivalente direto - NULL)
- casino_session_id        = c_session_id
- unix_timestamp           = to_unixtime(c_start_time)
- created_at               = c_start_time (BRT)
- updated_at               = NULL (coluna c_end_time nao existe em fund_ec2.tbl_real_fund_txn)
- op_type                  = c_op_type (CR=credito, DB=debito) — EXTRA, nao esta no PDF
- channel                  = c_channel (DESKTOP, MOBILE) — EXTRA
- affiliate_id             = ps_bi.dim_user.affiliate_id
- affiliate_name           = ps_bi.dim_user.affiliate

Tipos de transacao casino principais:
  27=BUYIN, 28=REBUY, 29=LEAVE_TABLE, 45=WIN, 65=JACKPOT_WIN, 72=BUYIN_CANCEL (rollback),
  77=WIN_CANCEL, 80=FREESPIN_WIN, 91=REFUND_BET, 132=FREESPIN_BUYIN, 133=FREESPIN_BUYIN_CANCEL

Filtros:
- c_start_time entre {PERIOD_START_UTC} e {PERIOD_END_UTC} (UTC = 01/03 00h a 01/04 00h BRT)
- c_product_id = 'CASINO'
- c_txn_status = 'SUCCESS'
- ps_bi.dim_user.is_test = false
- ps_bi.dim_user.signup_datetime < 2026-04-01 00:00 BRT (players cadastrados ate 31/03)
- ps_bi.dim_user.affiliate_id IN (97 afiliados AffiliatesBR (lote grande))
"""

LEGENDA_GERAL = f"""LEGENDA — geral_affbr_marco_afiliados.csv
Periodo: {PERIOD_LABEL}
Fonte: Athena fund_ec2.tbl_real_fund_txn (TODAS as transacoes financeiras, c_txn_status='SUCCESS')
Grao: 1 linha por transacao (deposito, saque, bet, win, rollback, ajuste, etc.)
Valores em BRL real (/100 de centavos)

Mapeamento coluna PDF -> origem Athena:
- id                = c_txn_id
- type              = descricao do c_txn_type (REAL_CASH_DEPOSIT, CASINO_BUYIN, SB_BUYIN, etc.)
- type_id           = c_txn_type (int)
- amount            = c_amount_in_ecr_ccy / 100.0
- status            = c_txn_status (filtrado SUCCESS)
- user_id           = c_ecr_id
- wallet_id         = (sem equivalente granular - NULL)
- src               = c_product_id (CASINO | SPORTSBOOK | vazio p/ ledger)
- src_id            = c_sub_vendor_id
- created_at        = c_start_time (BRT)
- updated_at        = NULL (coluna c_end_time nao existe em fund_ec2.tbl_real_fund_txn)
- bonus_wallet_id   = (sem equivalente granular - NULL; ver bonus_ec2 p/ detalhe)
- credit_percent    = (sem equivalente - NULL)
- bonus_percent     = (sem equivalente - NULL)
- cashed_out_amount = (sem equivalente direto aqui — ver cashier_ec2.tbl_cashier_cashout)
- old_balance       = (sem equivalente - fund_ec2 nao persiste saldo antes, usar tbl_real_fund)
- old_bonus_balance = (sem equivalente - NULL)
- op_type           = c_op_type (CR/DB) — EXTRA
- channel           = c_channel — EXTRA
- game_id           = c_game_id (se casino/sportsbook) — EXTRA
- session_id        = c_session_id — EXTRA
- affiliate_id      = ps_bi.dim_user.affiliate_id
- affiliate_name    = ps_bi.dim_user.affiliate

Categorias de c_txn_type cobertas:
  * Financeiro: 1=DEPOSIT, 2=WITHDRAW, 3=ADD_CS, 4=REMOVE_CS, 36=CASHOUT_REVERSAL,
    51=POS_ADJUSTMENT, 52=NEG_ADJUSTMENT, 54=CASHOUT_FEE, 126=DEPOSIT_REFUND
  * Casino: 27=BUYIN, 45=WIN, 65=JACKPOT, 72=BUYIN_CANCEL, 77=WIN_CANCEL, 80=FREESPIN_WIN
  * Sportsbook: 59=SB_BUYIN, 60=SB_LEAVE, 61=SB_BUYIN_CANCEL
  * Bonus relacionados: estao em bonus_ec2 separadamente — esta tabela so tem movimentos REALCASH

Filtros:
- c_start_time entre {PERIOD_START_UTC} e {PERIOD_END_UTC} (UTC = 01/03 00h a 01/04 00h BRT)
- c_txn_status = 'SUCCESS'
- ps_bi.dim_user.is_test = false
- ps_bi.dim_user.signup_datetime < 2026-04-01 00:00 BRT (players cadastrados ate 31/03)
- ps_bi.dim_user.affiliate_id IN (97 afiliados AffiliatesBR (lote grande))
"""

README = f"""# Auditoria Pagamento Afiliados — MARCO/2026

**Periodo:** {PERIOD_LABEL} (mes fechado — 31 dias completos)
**Gerado em:** {datetime.now().strftime("%Y-%m-%d %H:%M")}
**Fonte:** Athena Iceberg Data Lake (read-only)

## Arquivos

1. `afiliados_consolidado.csv` — lista dos 12 affiliate_ids alvo (duplicados consolidados)
2. `sports_affbr_marco_afiliados.csv` + `_legenda.txt` — apostas esportivas (vendor_ec2)
3. `casino_affbr_marco_afiliados.csv` + `_legenda.txt` — transacoes de cassino (fund_ec2 CASINO)
4. `geral_affbr_marco_afiliados.csv`  + `_legenda.txt` — todas transacoes financeiras (fund_ec2)

## Observacoes importantes

- As 3 tabelas dos PDFs (`sports_transactions`, `t_casino_transactions`, `t_transactions`)
  NAO existem identicamente no nosso Data Lake (Athena) — sao schemas do back-office operacional.
  Mapeamos para as tabelas equivalentes Athena e preservamos os nomes do PDF como colunas
  de saida, com legenda de-para em cada `_legenda.txt`.
- Algumas colunas do PDF nao tem equivalente direto no Athena e vieram vazias (NULL).
  Ver legenda de cada arquivo.
- Valores: sportsbook ja em BRL real; casino/geral foram convertidos de centavos (/100).
- Timezone convertido de UTC para BRT (America/Sao_Paulo) em todas as colunas timestamp.
- Test users foram excluidos (`ps_bi.dim_user.is_test = false`).
- Apenas players com `signup_datetime < 2026-04-01 00:00 BRT` (cadastrados ate 31/03).

## Lista de afiliados (97 IDs AffiliatesBR)

Duplicados consolidados conforme confirmado pelo requester:
- TP GESTAO DE MARKETING DIGITAL LTDA / TALES PERES DE MELO / [EST] Talesperes_Tiktok = **488468**
- MATHEUS MENDOCA / ADS LTDA / [EST] matheus_mendonca_rSq5aSMp = **522962**

IDs: 526453, 524476, 506920, 475425, 454861, 453598, 489203, 509759, 488468, 522962, 528138, 529063.
"""


# =====================================================================
# MAIN
# =====================================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--preview", action="store_true", help="Apenas conta linhas esperadas")
    ap.add_argument("--run", action="store_true", help="Executa e salva os 3 CSVs")
    ap.add_argument("--skip-sports", action="store_true", help="Pula sports (usa CSV existente se houver)")
    ap.add_argument("--skip-casino", action="store_true", help="Pula casino")
    ap.add_argument("--skip-geral",  action="store_true", help="Pula geral")
    args = ap.parse_args()

    if not args.preview and not args.run:
        ap.print_help()
        sys.exit(0)

    if args.preview:
        run_preview()
        return

    # RUN completo — cada etapa salva incrementalmente (resiliente a falhas)
    players = resolve_players()
    players_path = os.path.join(OUT_DIR, "players_resolvidos.csv")
    players.to_csv(players_path, index=False, encoding="utf-8-sig")
    log.info(f"  players salvos em {players_path}")

    p_sports = os.path.join(OUT_DIR, "sports_affbr_marco_afiliados.csv")
    p_casino = os.path.join(OUT_DIR, "casino_affbr_marco_afiliados.csv")
    p_geral  = os.path.join(OUT_DIR, "geral_affbr_marco_afiliados.csv")
    n_sports = n_casino = n_geral = 0

    if args.skip_sports and os.path.exists(p_sports):
        log.info(f"[2/4] SKIPPING sports — usando CSV existente ({p_sports})")
    else:
        df_sports = query_sports(players)
        df_sports.to_csv(p_sports, index=False, encoding="utf-8-sig")
        n_sports = len(df_sports)
        log.info(f"  -> {p_sports}  ({n_sports:,} linhas) SALVO")
        del df_sports

    if args.skip_casino and os.path.exists(p_casino):
        log.info(f"[3/4] SKIPPING casino — usando CSV existente ({p_casino})")
    else:
        df_casino = query_casino()
        df_casino.to_csv(p_casino, index=False, encoding="utf-8-sig")
        n_casino = len(df_casino)
        log.info(f"  -> {p_casino}  ({n_casino:,} linhas) SALVO")
        del df_casino

    if args.skip_geral and os.path.exists(p_geral):
        log.info(f"[4/4] SKIPPING geral — usando CSV existente ({p_geral})")
    else:
        df_geral = query_geral()
        df_geral.to_csv(p_geral, index=False, encoding="utf-8-sig")
        n_geral = len(df_geral)
        log.info(f"  -> {p_geral}  ({n_geral:,} linhas) SALVO")
        del df_geral

    # Salvar legendas
    with open(os.path.join(OUT_DIR, "sports_affbr_marco_afiliados_legenda.txt"), "w", encoding="utf-8") as f:
        f.write(LEGENDA_SPORTS)
    with open(os.path.join(OUT_DIR, "casino_affbr_marco_afiliados_legenda.txt"), "w", encoding="utf-8") as f:
        f.write(LEGENDA_CASINO)
    with open(os.path.join(OUT_DIR, "geral_affbr_marco_afiliados_legenda.txt"), "w", encoding="utf-8") as f:
        f.write(LEGENDA_GERAL)

    # README
    with open(os.path.join(OUT_DIR, "README_auditoria.md"), "w", encoding="utf-8") as f:
        f.write(README)
    log.info("README e legendas salvos.")

    log.info("")
    log.info("=== RESUMO ===")
    log.info(f"  Periodo: {PERIOD_LABEL}")
    log.info(f"  Afiliados: {len(AFFILIATE_IDS)} | Jogadores: {len(players):,}")
    log.info(f"  Sports: {n_sports:,} | Casino: {n_casino:,} | Geral: {n_geral:,}")
    log.info(f"  Output: {OUT_DIR}")


if __name__ == "__main__":
    main()
