"""
Extracao: CRM Report CSVs (BigQuery + Athena → data/crm_report/)
=================================================================
Gera os CSVs que alimentam o dashboard CRM Report.

FLUXO:
  1. BigQuery: campanhas + user_ext_ids por campanha
     (j_automation_rule_progress + dm_automation_rule + dm_segment)
  2. BigQuery: funil de comunicacao por campanha
     (j_communication agregado por resource_id)
  3. Athena: financeiro por user (ps_bi.fct_player_activity_daily)
     Somente para campanhas com < 50K users (RETEM, KLC, Challenge, etc.)
     DailyFS tem ~3.6M users/dia = base inteira, financeiro nao faz sentido isolar.
  4. Cruza BigQuery x Athena em Python
  5. Salva CSVs em data/crm_report/

CSVS GERADOS:
  campaigns.csv          — 1 linha por campanha logica (25 campanhas)
  campaign_financials.csv — financeiro por campanha (exceto DailyFS)
  dispatch_costs.csv     — custos de disparo por canal/provedor

CUSTOS DE DISPARO (confirmados CRM 31/03/2026):
  SMS Ligue Lead:   R$ 0,047
  SMS PushFY:       R$ 0,060
  WhatsApp Loyalty: R$ 0,160

USO:
    python scripts/extract_crm_report_csvs.py
    python scripts/extract_crm_report_csvs.py --date-from 2026-03-01 --date-to 2026-03-31

AUTOR: Mateus F. (Squad Intelligence Engine) — 01/04/2026
"""
import argparse
import re
import sys
import os
import logging
from datetime import date, timedelta

import pandas as pd

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.bigquery import query_bigquery
from db.athena import query_athena

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("extract_crm_report")

BQ_DATASET = "smartico-bq6.dwh_ext_24105"
OUTPUT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "crm_report"))

# Limite de users para cruzar com Athena (acima disso = base inteira, skip)
MAX_USERS_FOR_ATHENA = 50000

# Rules de limpeza/marcadores — excluir (nao sao campanhas CRM)
EXCLUDE_PATTERNS = [
    "LIMPAR", "LIMPEZA", "UNMARK", "MARCADOR", "RETIRAR",
    "TESTE ROLLBACK", "_UNMARK", "RESET ",
]


# =========================================================================
# AGRUPAMENTO DE CAMPANHAS
# =========================================================================
def classify_campaign_group(rule_name):
    """
    Classifica automation rule em campanha logica.
    Returns (campaign_group, campaign_type) ou None se deve ser excluida.
    """
    if not rule_name:
        return None
    upper = rule_name.upper()

    for pat in EXCLUDE_PATTERNS:
        if pat in upper:
            return None

    if upper.startswith("TESTE ") and "CASHBACK" not in upper:
        return None

    clean = upper.replace(" ", "").replace("_", "")

    if "DAILYFS" in upper or "DAILY_FS" in upper or "DAILY FS" in upper:
        if "LOWTICKET" in clean:
            return ("DailyFS Low Ticket", "DailyFS")
        if "MEDIUMTICKET" in clean:
            return ("DailyFS Medium Ticket", "DailyFS")
        if "HIGHTICKET" in clean:
            return ("DailyFS High Ticket", "DailyFS")
        return ("DailyFS Outro", "DailyFS")

    if "[RETENCAO]" in upper or "[RETEM]" in upper:
        m = re.search(r"R\$\s*(\d[\d.]*)", rule_name)
        val = m.group(1) if m else "?"
        extra = "Extra " if "EXTRA" in upper else ""
        return (f"RETEM {extra}R${val}", "RETEM")

    if "KLC" in upper and "DEP+APOST" in upper:
        m = re.search(r"(\d+)\s*$", rule_name.strip())
        val = m.group(1) if m else "?"
        return (f"KLC Fidelidade R${val}", "KLC")

    if "DESAFIO" in upper or ("PGS" in upper and "QUEST" in upper):
        for animal in ["Tiger", "Rabbit", "Ox", "Dragon", "Mouse", "Snake"]:
            if animal.upper() in upper:
                return (f"Challenge {animal}", "Challenge")
        return ("Challenge Outro", "Challenge")

    if "TELEGRAM" in upper or "MISSION" in upper:
        return ("Lifecycle Telegram", "Lifecycle")

    if "MINIGAME" in upper:
        return ("Daily Minigames", "Gamificacao")

    if "CASHBACK" in upper:
        return ("Cashback Sportsbook", "Cashback")

    return (rule_name.strip(), "Outro")


# =========================================================================
# STEP 1: BigQuery — campanhas (leve) + users (so campanhas pequenas)
# =========================================================================
def step1_campaigns_and_users(date_from, date_to):
    """
    2 queries separadas para performance:
      1A: campanhas agregadas (sem user_ext_id, rapida)
      1B: user_ext_ids apenas para campanhas com <50K users (para Athena)
    """
    log.info(f"STEP 1A: BigQuery — campanhas agregadas ({date_from} a {date_to})...")

    # --- 1A: Campanhas agregadas (rapida, sem user_ext_id) ---
    sql_camps = f"""
    SELECT
        p.automation_rule_id AS rule_id,
        r.rule_name,
        CAST(r.is_active AS STRING) AS is_active,
        r.activity_type_id,
        s.segment_name,
        COUNT(DISTINCT p.user_ext_id) AS users,
        MIN(DATE(p.dt_executed)) AS first_exec,
        MAX(DATE(p.dt_executed)) AS last_exec,
        COUNT(DISTINCT DATE(p.dt_executed)) AS dias_ativa
    FROM `{BQ_DATASET}.j_automation_rule_progress` p
    JOIN `{BQ_DATASET}.dm_automation_rule` r
        ON p.automation_rule_id = r.rule_id
    LEFT JOIN `{BQ_DATASET}.dm_segment` s
        ON r.segment_id = s.segment_id
    WHERE DATE(p.dt_executed) BETWEEN '{date_from}' AND '{date_to}'
    GROUP BY 1, 2, 3, 4, 5
    """
    df_rules = query_bigquery(sql_camps)
    log.info(f"  1A: {len(df_rules)} rules encontradas")

    if df_rules.empty:
        return pd.DataFrame(), pd.DataFrame()

    # Classificar e filtrar
    df_rules["_group"] = df_rules["rule_name"].apply(
        lambda x: classify_campaign_group(str(x) if pd.notna(x) else "")
    )
    df_rules = df_rules[df_rules["_group"].notna()].copy()
    df_rules["campaign_group"] = df_rules["_group"].apply(lambda x: x[0])
    df_rules["campaign_type"] = df_rules["_group"].apply(lambda x: x[1])
    df_rules.drop("_group", axis=1, inplace=True)

    # Mapear canal pelo activity_type_id da rule
    rule_channel_map = {504: "Popup", 200: "Minigame", 800: "Cashback Engine"}
    df_rules["channel"] = df_rules["activity_type_id"].apply(
        lambda x: rule_channel_map.get(int(x) if pd.notna(x) else 0, "Outro")
    )

    # Agregar por campanha logica
    # NOTA: users = MAX por rule (nao SUM) porque as rules da mesma campanha
    # atingem os mesmos users (ex: DailyFS Low Ticket roda 5 jogos para a mesma base)
    agg = df_rules.groupby("campaign_group").agg(
        campaign_type=("campaign_type", "first"),
        channel=("channel", "first"),
        segment_name=("segment_name", "first"),
        is_active=("is_active", lambda x: "true" in x.values),
        users=("users", "max"),
        rules_count=("rule_id", "nunique"),
        dias_ativa=("dias_ativa", "max"),
        first_exec=("first_exec", "min"),
        last_exec=("last_exec", "max"),
    ).reset_index()
    agg["status"] = agg["is_active"].apply(lambda x: "ativa" if x else "inativa")
    # Flag: campanha direcionada (<50K users) vs base inteira (>50K)
    agg["is_targeted"] = agg["users"] < MAX_USERS_FOR_ATHENA

    log.info(f"  {len(agg)} campanhas logicas "
             f"({agg['is_targeted'].sum()} direcionadas, "
             f"{(~agg['is_targeted']).sum()} base inteira)")

    # --- 1B: Users das campanhas pequenas (para Athena) ---
    small_camps = agg[agg["users"] < MAX_USERS_FOR_ATHENA]
    small_rule_ids = df_rules[
        df_rules["campaign_group"].isin(small_camps["campaign_group"])
    ]["rule_id"].unique().tolist()

    log.info(f"STEP 1B: BigQuery — user_ext_ids para {len(small_camps)} campanhas ({len(small_rule_ids)} rules)...")

    if not small_rule_ids:
        return agg, pd.DataFrame()

    ids_str = ",".join(str(int(r)) for r in small_rule_ids)
    sql_users = f"""
    SELECT DISTINCT
        p.automation_rule_id AS rule_id,
        p.user_ext_id
    FROM `{BQ_DATASET}.j_automation_rule_progress` p
    WHERE p.automation_rule_id IN ({ids_str})
      AND DATE(p.dt_executed) BETWEEN '{date_from}' AND '{date_to}'
    """
    df_users_raw = query_bigquery(sql_users)
    log.info(f"  1B: {len(df_users_raw)} linhas (rule x user)")

    if df_users_raw.empty:
        return agg, pd.DataFrame()

    # Mapear rule_id → campaign_group
    rule_to_group = df_rules.set_index("rule_id")[["campaign_group", "campaign_type"]].to_dict("index")
    df_users_raw["campaign_group"] = df_users_raw["rule_id"].map(
        lambda r: rule_to_group.get(r, {}).get("campaign_group", "?")
    )
    df_users_raw["campaign_type"] = df_users_raw["rule_id"].map(
        lambda r: rule_to_group.get(r, {}).get("campaign_type", "?")
    )

    df_users = df_users_raw[["campaign_group", "campaign_type", "user_ext_id"]].drop_duplicates()
    log.info(f"  {df_users['user_ext_id'].nunique()} users unicos para cruzar com Athena")

    return agg, df_users


# =========================================================================
# STEP 2: Athena — financeiro por user
# =========================================================================
def step2_athena_financials(df_users, date_from, date_to):
    """
    Cruza user_ext_ids com ps_bi.fct_player_activity_daily no Athena
    para obter GGR, NGR, deposits, etc.

    Somente para campanhas com < MAX_USERS_FOR_ATHENA users.
    """
    log.info("STEP 2: Athena — financeiro por campanha...")

    # Filtrar campanhas pequenas
    camp_sizes = df_users.groupby("campaign_group")["user_ext_id"].nunique()
    small_camps = camp_sizes[camp_sizes < MAX_USERS_FOR_ATHENA].index.tolist()
    big_camps = camp_sizes[camp_sizes >= MAX_USERS_FOR_ATHENA].index.tolist()

    if big_camps:
        log.info(f"  Campanhas com >50K users (sem financeiro individual): {big_camps}")

    df_small = df_users[df_users["campaign_group"].isin(small_camps)].copy()
    user_ids = df_small["user_ext_id"].unique().tolist()

    log.info(f"  {len(small_camps)} campanhas com <50K users | {len(user_ids)} user_ext_ids para Athena")

    if not user_ids:
        return pd.DataFrame()

    # Fonte: bireports_ec2 (bronze) — NAO ps_bi (gold)
    # tbl_ecr_wise_daily_bi_summary: 1 linha por c_ecr_id x c_created_date
    # Valores em CENTAVOS (/100 para BRL)
    # JOIN com tbl_ecr para pegar c_external_id (= user_ext_id do Smartico)
    batch_size = 5000
    all_financials = []

    for i in range(0, len(user_ids), batch_size):
        batch = user_ids[i:i + batch_size]
        ids_str = ",".join(f"'{uid}'" for uid in batch)
        log.info(f"  Batch {i//batch_size + 1}: {len(batch)} users...")

        sql = f"""
        SELECT
            CAST(e.c_external_id AS VARCHAR)                    AS user_ext_id,
            SUM(b.c_casino_realcash_bet_amount) / 100.0         AS casino_turnover,
            SUM(b.c_sb_realcash_bet_amount) / 100.0             AS sportsbook_turnover,
            SUM(
                b.c_casino_realcash_bet_amount - b.c_casino_realcash_win_amount
                + b.c_sb_realcash_bet_amount - b.c_sb_realcash_win_amount
            ) / 100.0                                           AS total_ggr,
            SUM(b.c_deposit_success_amount) / 100.0             AS total_deposit,
            SUM(b.c_co_success_amount) / 100.0                  AS total_withdrawal,
            SUM(b.c_deposit_success_amount - b.c_co_success_amount) / 100.0 AS net_deposit,
            SUM(b.c_bonus_issued_amount) / 100.0                AS bonus_cost,
            COUNT(DISTINCT b.c_created_date)                    AS play_days,
            SUM(b.c_login_count)                                AS login_count
        FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary b
        JOIN bireports_ec2.tbl_ecr e
            ON b.c_ecr_id = e.c_ecr_id
        LEFT JOIN ecr_ec2.tbl_ecr_flags f
            ON b.c_ecr_id = f.c_ecr_id
        WHERE CAST(e.c_external_id AS VARCHAR) IN ({ids_str})
          AND b.c_created_date >= DATE '{date_from}'
          AND b.c_created_date <= DATE '{date_to}'
          AND (f.c_test_user = false OR f.c_test_user IS NULL)
        GROUP BY CAST(e.c_external_id AS VARCHAR)
        """

        try:
            df_fin = query_athena(sql, database="bireports_ec2")
            if not df_fin.empty:
                all_financials.append(df_fin)
                log.info(f"    Athena retornou {len(df_fin)} users com atividade")
        except Exception as e:
            log.error(f"    Athena erro: {e}")

    if not all_financials:
        log.warning("  Nenhum dado financeiro retornado do Athena")
        return pd.DataFrame()

    df_fin_all = pd.concat(all_financials, ignore_index=True)

    # Cruzar: user → campanha
    df_cross = df_small.merge(df_fin_all, on="user_ext_id", how="inner")

    # Agregar por campanha
    fin_cols = ["total_ggr", "casino_turnover", "sportsbook_turnover",
                "total_deposit", "total_withdrawal", "net_deposit", "bonus_cost"]
    for c in fin_cols:
        if c in df_cross.columns:
            df_cross[c] = pd.to_numeric(df_cross[c], errors="coerce").fillna(0)

    camp_fin = df_cross.groupby("campaign_group").agg(
        fin_users=("user_ext_id", "nunique"),
        total_ggr=("total_ggr", "sum"),
        casino_turnover=("casino_turnover", "sum"),
        sportsbook_turnover=("sportsbook_turnover", "sum"),
        total_deposit=("total_deposit", "sum"),
        total_withdrawal=("total_withdrawal", "sum"),
        net_deposit=("net_deposit", "sum"),
        bonus_cost=("bonus_cost", "sum"),
        avg_play_days=("play_days", "mean"),
    ).reset_index()

    # NGR = GGR - bonus_cost (calculado, nao vem do bireports)
    camp_fin["ngr"] = camp_fin["total_ggr"] - camp_fin["bonus_cost"]

    # Metricas derivadas
    turnover_total = camp_fin["casino_turnover"] + camp_fin["sportsbook_turnover"]
    camp_fin["ggr_pct"] = (camp_fin["total_ggr"] / turnover_total.replace(0, 1) * 100).round(1)
    camp_fin["roi"] = (camp_fin["ngr"] / camp_fin["bonus_cost"].replace(0, 1)).round(1)

    log.info(f"  Financeiro calculado para {len(camp_fin)} campanhas")
    return camp_fin


# =========================================================================
# STEP 3: BigQuery — custos de disparo
# =========================================================================
def step3_dispatch_costs(date_from, date_to):
    """Extrai custos de disparo por canal."""
    log.info("STEP 3: BigQuery — custos de disparo...")

    sql = f"""
    SELECT
        activity_type_id,
        label_provider_id,
        COUNT(*) AS total_sent,
        COUNT(DISTINCT user_ext_id) AS users
    FROM `{BQ_DATASET}.j_communication`
    WHERE DATE(fact_date) BETWEEN '{date_from}' AND '{date_to}'
      AND fact_type_id = 1
    GROUP BY 1, 2
    """
    df = query_bigquery(sql)
    if df.empty:
        return pd.DataFrame()

    # Mapear canal e provedor
    channel_map = {50: "Popup", 60: "SMS", 64: "WhatsApp", 30: "Push", 40: "Push"}
    provider_map = {1536: "DisparoPro", 1545: "PushFY", 1268: "Comtele"}
    cost_map = {
        ("SMS", "DisparoPro"): 0.047,
        ("SMS", "PushFY"): 0.060,
        ("SMS", "Comtele"): 0.063,
        ("WhatsApp", "Loyalty"): 0.160,
        ("Push", "PushFY"): 0.060,
    }

    rows = []
    for _, r in df.iterrows():
        act = int(r.activity_type_id) if pd.notna(r.activity_type_id) else 0
        prov_id = int(r.label_provider_id) if pd.notna(r.label_provider_id) else 0
        ch = channel_map.get(act, "Outro")
        prov = provider_map.get(prov_id, "Smartico" if ch == "Popup" else "Loyalty" if ch == "WhatsApp" else "Desconhecido")
        custo_unit = cost_map.get((ch, prov), 0)
        total = int(r.total_sent)
        rows.append({
            "channel": ch,
            "provider": prov,
            "total_sent": total,
            "users": int(r.users),
            "custo_unitario": custo_unit,
            "custo_total_brl": round(total * custo_unit, 2),
        })

    df_disp = pd.DataFrame(rows)
    agg = df_disp.groupby(["channel", "provider"]).agg(
        total_sent=("total_sent", "sum"),
        users=("users", "sum"),
        custo_unitario=("custo_unitario", "first"),
        custo_total_brl=("custo_total_brl", "sum"),
    ).reset_index().sort_values("custo_total_brl", ascending=False)

    log.info(f"  {len(agg)} canais/provedores | custo total: R$ {agg['custo_total_brl'].sum():,.2f}")
    return agg


# =========================================================================
# STEP 4: BigQuery — funil de conversao
# =========================================================================
def step4_funnel(date_from, date_to):
    """
    Funil CRM agregado por dia.
    fact_type_id: 1=enviado, 2=entregue, 3=aberto, 4=clicado, 5=convertido
    Fonte: j_communication
    """
    log.info("STEP 4: BigQuery — funil de conversao...")

    sql = f"""
    SELECT
        DATE(fact_date) AS report_date,
        COUNTIF(fact_type_id = 1) AS enviados,
        COUNTIF(fact_type_id = 2) AS entregues,
        COUNTIF(fact_type_id = 3) AS abertos,
        COUNTIF(fact_type_id = 4) AS clicados,
        COUNTIF(fact_type_id = 5) AS convertidos,
        COUNT(DISTINCT CASE WHEN fact_type_id = 1 THEN user_ext_id END) AS users_enviados,
        COUNT(DISTINCT CASE WHEN fact_type_id = 5 THEN user_ext_id END) AS users_convertidos
    FROM `{BQ_DATASET}.j_communication`
    WHERE DATE(fact_date) BETWEEN '{date_from}' AND '{date_to}'
    GROUP BY 1
    ORDER BY 1
    """
    df = query_bigquery(sql)
    log.info(f"  Funil: {len(df)} dias | enviados total: {df['enviados'].sum() if not df.empty else 0}")
    return df


# =========================================================================
# STEP 4B: Top jogos da base impactada (Athena bireports_ec2)
# =========================================================================
def step4b_top_games(df_users, date_from, date_to, limit=15):
    """
    Top jogos por turnover da base impactada pelas campanhas CRM.
    Fonte: bireports_ec2.tbl_ecr_txn_type_wise_daily_game_play_summary
    """
    log.info("STEP 4B: Athena — top jogos da base CRM...")

    all_users = df_users["user_ext_id"].unique().tolist()
    if not all_users:
        return pd.DataFrame()

    ids_str = ",".join(f"'{uid}'" for uid in all_users)

    # fund_ec2.tbl_real_fund_txn tem c_game_id
    # Filtrar por ecr_id dos users CRM, produto CASINO, status SUCCESS
    sql = f"""
    WITH crm_users AS (
        SELECT e.c_ecr_id
        FROM bireports_ec2.tbl_ecr e
        WHERE CAST(e.c_external_id AS VARCHAR) IN ({ids_str})
    )
    SELECT
        t.c_game_id AS game_id,
        v.c_game_desc AS game_name,
        v.c_game_category AS game_category,
        COUNT(DISTINCT t.c_ecr_id) AS users,
        SUM(COALESCE(r.c_amount_in_ecr_ccy, 0)) / 100.0 AS turnover_brl,
        SUM(CASE
            WHEN m.c_op_type = 'DB' AND m.c_is_cancel_txn = false THEN COALESCE(r.c_amount_in_ecr_ccy, 0)
            WHEN m.c_op_type = 'CR' AND m.c_is_cancel_txn = false THEN -COALESCE(r.c_amount_in_ecr_ccy, 0)
            ELSE 0
        END) / 100.0 AS ggr_brl
    FROM fund_ec2.tbl_real_fund_txn t
    JOIN crm_users cu ON t.c_ecr_id = cu.c_ecr_id
    LEFT JOIN fund_ec2.tbl_realcash_sub_fund_txn r ON t.c_txn_id = r.c_fund_txn_id
    JOIN fund_ec2.tbl_real_fund_txn_type_mst m ON t.c_txn_type = m.c_txn_type
    JOIN bireports_ec2.tbl_vendor_games_mapping_data v
        ON t.c_sub_product_id = v.c_vendor_id AND t.c_game_id = v.c_game_id
    WHERE t.c_product_id = 'CASINO'
      AND t.c_txn_status = 'SUCCESS'
      AND m.c_is_gaming_txn = 'Y'
      AND t.c_game_id IS NOT NULL
      AND t.c_start_time >= TIMESTAMP '{date_from} 03:00:00'
      AND t.c_start_time < TIMESTAMP '{date_to} 03:00:00'
    GROUP BY t.c_game_id, v.c_game_desc, v.c_game_category
    ORDER BY turnover_brl DESC
    LIMIT {limit}
    """

    try:
        df = query_athena(sql, database="fund_ec2")
        log.info(f"  Top jogos: {len(df)} jogos retornados")

        # COUNT DISTINCT users que jogaram (sem duplicata entre jogos)
        sql_unique = f"""
        WITH crm_users AS (
            SELECT e.c_ecr_id
            FROM bireports_ec2.tbl_ecr e
            WHERE CAST(e.c_external_id AS VARCHAR) IN ({ids_str})
        )
        SELECT COUNT(DISTINCT t.c_ecr_id) AS unique_players
        FROM fund_ec2.tbl_real_fund_txn t
        JOIN crm_users cu ON t.c_ecr_id = cu.c_ecr_id
        WHERE t.c_product_id = 'CASINO'
          AND t.c_txn_status = 'SUCCESS'
          AND t.c_start_time >= TIMESTAMP '{date_from} 03:00:00'
          AND t.c_start_time < TIMESTAMP '{date_to} 03:00:00'
        """
        df_uniq = query_athena(sql_unique, database="fund_ec2")
        unique_players = int(df_uniq.iloc[0, 0]) if not df_uniq.empty else 0
        log.info(f"  Users unicos que jogaram: {unique_players}")

        # Salvar unique_players no CSV como metadata
        df.attrs["unique_players"] = unique_players
        return df, unique_players
    except Exception as e:
        log.error(f"  Athena erro top jogos: {e}")
        return pd.DataFrame(), 0


# =========================================================================
# STEP 5: VIP — classificar users por NGR (Athena bireports_ec2)
# =========================================================================
def step5_vip_classification(df_users, date_from, date_to):
    """
    Classifica users das campanhas CRM em grupos VIP por NGR:
      Elite:       NGR >= R$ 10.000
      Key Account: NGR >= R$ 5.000 e < R$ 10.000
      High Value:  NGR >= R$ 3.000 e < R$ 5.000
      Standard:    NGR < R$ 3.000

    Fonte: bireports_ec2.tbl_ecr_wise_daily_bi_summary (bronze)
    NGR = GGR Real - Bonus Cost = (casino_realcash_bet - casino_realcash_win
           + sb_realcash_bet - sb_realcash_win) / 100 - bonus_issued / 100

    Usa users UNICOS (deduplicados entre campanhas) para evitar sobreposicao.
    """
    log.info("STEP 5: VIP — classificacao por NGR...")

    # Deduplicar users entre campanhas
    all_users = df_users["user_ext_id"].unique().tolist()
    log.info(f"  {len(all_users)} users unicos para classificar")

    if not all_users:
        return pd.DataFrame()

    ids_str = ",".join(f"'{uid}'" for uid in all_users)

    sql = f"""
    SELECT
        CAST(e.c_external_id AS VARCHAR) AS user_ext_id,
        SUM(
            b.c_casino_realcash_bet_amount - b.c_casino_realcash_win_amount
            + b.c_sb_realcash_bet_amount - b.c_sb_realcash_win_amount
        ) / 100.0 AS ggr_brl,
        SUM(b.c_bonus_issued_amount) / 100.0 AS bonus_cost_brl,
        SUM(
            b.c_casino_realcash_bet_amount - b.c_casino_realcash_win_amount
            + b.c_sb_realcash_bet_amount - b.c_sb_realcash_win_amount
            - b.c_bonus_issued_amount
        ) / 100.0 AS ngr_brl,
        COUNT(DISTINCT b.c_created_date) AS play_days,
        SUM(b.c_deposit_success_amount) / 100.0 AS deposits_brl
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary b
    JOIN bireports_ec2.tbl_ecr e ON b.c_ecr_id = e.c_ecr_id
    LEFT JOIN ecr_ec2.tbl_ecr_flags f ON b.c_ecr_id = f.c_ecr_id
    WHERE CAST(e.c_external_id AS VARCHAR) IN ({ids_str})
      AND b.c_created_date >= DATE '{date_from}'
      AND b.c_created_date <= DATE '{date_to}'
      AND (f.c_test_user = false OR f.c_test_user IS NULL)
    GROUP BY CAST(e.c_external_id AS VARCHAR)
    """

    try:
        df_vip = query_athena(sql, database="bireports_ec2")
    except Exception as e:
        log.error(f"  Athena erro VIP: {e}")
        return pd.DataFrame()

    if df_vip.empty:
        return pd.DataFrame()

    # Converter
    for c in ["ngr_brl", "ggr_brl", "bonus_cost_brl", "deposits_brl"]:
        df_vip[c] = pd.to_numeric(df_vip[c], errors="coerce").fillna(0)
    df_vip["play_days"] = pd.to_numeric(df_vip["play_days"], errors="coerce").fillna(0)

    # Classificar VIP
    def classify_vip(ngr):
        if ngr >= 10000:
            return "Elite"
        elif ngr >= 5000:
            return "Key Account"
        elif ngr >= 3000:
            return "High Value"
        return "Standard"

    df_vip["vip_tier"] = df_vip["ngr_brl"].apply(classify_vip)

    # Agregar por tier
    agg = df_vip.groupby("vip_tier").agg(
        users=("user_ext_id", "nunique"),
        ngr_total=("ngr_brl", "sum"),
        ngr_medio=("ngr_brl", "mean"),
        ggr_total=("ggr_brl", "sum"),
        deposits_total=("deposits_brl", "sum"),
        apd=("play_days", "mean"),
    ).reset_index()

    # Ordenar por tier
    tier_order = {"Elite": 0, "Key Account": 1, "High Value": 2, "Standard": 3}
    agg["_order"] = agg["vip_tier"].map(tier_order)
    agg = agg.sort_values("_order").drop("_order", axis=1)

    log.info(f"  VIP classificado: {len(df_vip)} users")
    for _, r in agg.iterrows():
        log.info(f"    {r['vip_tier']:15s} | {int(r['users']):>5d} users | NGR R$ {r['ngr_total']:>12,.2f} | APD {r['apd']:.1f}")

    return agg, df_vip


# =========================================================================
# MAIN
# =========================================================================
def main():
    parser = argparse.ArgumentParser(description="Extracao CRM Report CSVs")
    parser.add_argument("--date-from", default="2026-03-01")
    parser.add_argument("--date-to", default="2026-03-31")
    args = parser.parse_args()

    date_from = args.date_from
    date_to = args.date_to

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    log.info(f"=== EXTRACAO CRM REPORT: {date_from} a {date_to} ===")
    log.info(f"Output: {OUTPUT_DIR}")

    # STEP 1: Campanhas + Users
    df_camps, df_users = step1_campaigns_and_users(date_from, date_to)
    if df_camps.empty:
        log.error("Nenhuma campanha encontrada. Abortando.")
        return

    # STEP 2: Financeiro (Athena)
    df_fin = step2_athena_financials(df_users, date_from, date_to)

    # STEP 3: Dispatch
    df_disp = step3_dispatch_costs(date_from, date_to)

    # STEP 4: Funil
    df_funnel = step4_funnel(date_from, date_to)

    # STEP 4B: Top jogos
    games_result = step4b_top_games(df_users, date_from, date_to)
    if isinstance(games_result, tuple):
        df_games, games_unique_players = games_result
    else:
        df_games, games_unique_players = games_result, 0

    # STEP 5: VIP
    vip_result = step5_vip_classification(df_users, date_from, date_to)
    df_vip_agg = vip_result[0] if isinstance(vip_result, tuple) and not vip_result[0].empty else pd.DataFrame()
    df_vip_detail = vip_result[1] if isinstance(vip_result, tuple) and len(vip_result) > 1 else pd.DataFrame()

    # Merge campaigns + financials
    if not df_fin.empty:
        df_merged = df_camps.merge(df_fin, on="campaign_group", how="left")
    else:
        df_merged = df_camps.copy()

    # Salvar CSVs
    log.info("Salvando CSVs...")

    campaigns_path = os.path.join(OUTPUT_DIR, "campaigns.csv")
    df_merged.to_csv(campaigns_path, index=False, sep=";", encoding="utf-8-sig")
    log.info(f"  campaigns.csv: {len(df_merged)} linhas")

    if not df_fin.empty:
        fin_path = os.path.join(OUTPUT_DIR, "campaign_financials.csv")
        df_fin.to_csv(fin_path, index=False, sep=";", encoding="utf-8-sig")
        log.info(f"  campaign_financials.csv: {len(df_fin)} linhas")

    if not df_disp.empty:
        disp_path = os.path.join(OUTPUT_DIR, "dispatch_costs.csv")
        df_disp.to_csv(disp_path, index=False, sep=";", encoding="utf-8-sig")
        log.info(f"  dispatch_costs.csv: {len(df_disp)} linhas")

    if not df_funnel.empty:
        funnel_path = os.path.join(OUTPUT_DIR, "funnel_daily.csv")
        df_funnel.to_csv(funnel_path, index=False, sep=";", encoding="utf-8-sig")
        log.info(f"  funnel_daily.csv: {len(df_funnel)} linhas")

    if not df_vip_agg.empty:
        vip_path = os.path.join(OUTPUT_DIR, "vip_summary.csv")
        df_vip_agg.to_csv(vip_path, index=False, sep=";", encoding="utf-8-sig")
        log.info(f"  vip_summary.csv: {len(df_vip_agg)} linhas")

    if not df_games.empty:
        games_path = os.path.join(OUTPUT_DIR, "top_games.csv")
        df_games.to_csv(games_path, index=False, sep=";", encoding="utf-8-sig")
        log.info(f"  top_games.csv: {len(df_games)} linhas")
        # Salvar metadata com unique_players
        import json
        meta_path = os.path.join(OUTPUT_DIR, "top_games_meta.json")
        with open(meta_path, "w") as f:
            json.dump({"unique_players": games_unique_players}, f)
        log.info(f"  top_games_meta.json: unique_players={games_unique_players}")

    # Resumo
    log.info("")
    log.info("=== RESUMO ===")
    log.info(f"Periodo: {date_from} a {date_to}")
    log.info(f"Campanhas logicas: {len(df_merged)}")
    log.info(f"Users unicos total: {df_users['user_ext_id'].nunique():,}")
    if not df_fin.empty:
        log.info(f"GGR total (campanhas com financeiro): R$ {df_fin['total_ggr'].sum():,.2f}")
        log.info(f"NGR total: R$ {df_fin['ngr'].sum():,.2f}")
        log.info(f"Net Deposit: R$ {df_fin['net_deposit'].sum():,.2f}")
    if not df_disp.empty:
        log.info(f"Custo disparos: R$ {df_disp['custo_total_brl'].sum():,.2f}")
    log.info(f"Arquivos em: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()
