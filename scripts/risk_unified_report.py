"""
Risk Unified Report — Dashboard Consolidado de Risco MultiBet
=============================================================
Gera UM unico HTML standalone combinando todas as analises de risco:

  Secao 1 — Sumario Executivo (2 jogadores flagados pelo Head)
  Secao 2 — Deep Dive Murilo & Fabiano (perfil, financeiro, bonus, timeline)
  Secao 3 — Top Players (Winners / Losers / Abusers)
  Secao 4 — Alertas R8 Free Spin Abuser
  Secao 5 — Nota de Qualidade de Dados

Fontes:
  - output/risk_deep_dive_860514_911033_2026-04-06.csv
  - output/risk_top_players_2026-04-06.csv
  - output/risk_fraud_alerts_2026-04-06.csv
  - Athena: ps_bi.dim_user, ps_bi.fct_player_activity_daily, bonus_ec2

Saida:
  - output/risk_unified_report_2026-04-06.html

Uso:
    python scripts/risk_unified_report.py

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-06
"""

import sys
import os
import json
import logging
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

TODAY = "2026-04-06"

# ECR IDs dos 2 jogadores flagados pelo Castrin
ECR_MURILO = "849167571791860514"
ECR_FABIANO = "789175681790911033"
ECR_IDS = [ECR_MURILO, ECR_FABIANO]

# Nomes para referencia (conforme demanda do Head)
PLAYER_NAMES = {
    ECR_MURILO: "Murilo",
    ECR_FABIANO: "Fabiano",
}

# ===========================================================================
# 1. LEITURA DOS CSVs EXISTENTES
# ===========================================================================

def load_csvs() -> dict:
    """Le os 3 CSVs de input e retorna dict de DataFrames."""
    data = {}

    # Deep dive
    path_dd = os.path.join(OUTPUT_DIR, f"risk_deep_dive_860514_911033_{TODAY}.csv")
    if os.path.exists(path_dd):
        data["deep_dive"] = pd.read_csv(path_dd)
        log.info(f"Deep dive CSV: {len(data['deep_dive'])} linhas")
    else:
        log.warning(f"Deep dive CSV nao encontrado: {path_dd}")
        data["deep_dive"] = pd.DataFrame()

    # Top players
    path_tp = os.path.join(OUTPUT_DIR, f"risk_top_players_{TODAY}.csv")
    if os.path.exists(path_tp):
        data["top_players"] = pd.read_csv(path_tp)
        log.info(f"Top players CSV: {len(data['top_players'])} linhas")
    else:
        log.warning(f"Top players CSV nao encontrado: {path_tp}")
        data["top_players"] = pd.DataFrame()

    # Fraud alerts
    path_fa = os.path.join(OUTPUT_DIR, f"risk_fraud_alerts_{TODAY}.csv")
    if os.path.exists(path_fa):
        data["fraud_alerts"] = pd.read_csv(path_fa)
        log.info(f"Fraud alerts CSV: {len(data['fraud_alerts'])} linhas")
    else:
        log.warning(f"Fraud alerts CSV nao encontrado: {path_fa}")
        data["fraud_alerts"] = pd.DataFrame()

    return data


# ===========================================================================
# 2. QUERIES ATHENA — DADOS COMPLEMENTARES DOS 2 JOGADORES
# ===========================================================================

def query_player_profiles() -> pd.DataFrame:
    """Perfil dos 2 jogadores via ps_bi.dim_user."""
    ids_str = ",".join(ECR_IDS)
    sql = f"""
    SELECT
        u.ecr_id,
        u.external_id,
        u.registration_date,
        u.country_code,
        u.ecr_currency,
        u.ecr_status,
        u.ftd_date,
        u.ftd_amount_inhouse,
        u.last_deposit_date,
        u.lifetime_deposit_count,
        u.lifetime_deposit_amount_inhouse,
        u.kyc_level,
        u.tier,
        u.affiliate_id,
        u.is_test,
        date_diff('day', CAST(u.registration_date AS TIMESTAMP),
                  CURRENT_TIMESTAMP) AS dias_desde_registro
    FROM ps_bi.dim_user u
    WHERE u.ecr_id IN ({ids_str})
      AND u.is_test = false
    """
    log.info("Querying dim_user para perfil dos 2 jogadores...")
    return query_athena(sql, database="ps_bi")


def query_psbi_financial() -> pd.DataFrame:
    """
    Resumo financeiro dos 2 jogadores via ps_bi.fct_player_activity_daily.
    ps_bi ja esta em BRL (nao centavos).
    """
    ids_str = ",".join(ECR_IDS)
    sql = f"""
    SELECT
        f.player_id,
        -- Lifetime
        SUM(f.deposit_success_count) AS dep_count_psbi,
        SUM(f.deposit_success_base) AS dep_total_brl,
        SUM(f.cashout_success_count) AS saque_count_psbi,
        SUM(f.cashout_success_base) AS saque_total_brl,
        SUM(f.casino_realbet_count) AS bets_count_psbi,
        SUM(f.casino_realbet_base) AS bets_total_brl,
        SUM(f.casino_real_win_base) AS wins_total_brl,
        SUM(f.ggr_base) AS ggr_total_brl,
        SUM(f.ngr_base) AS ngr_total_brl,
        SUM(f.bonus_issued_base) AS bonus_issued_brl,
        SUM(f.bonus_turnedreal_base) AS bonus_turnedreal_brl,
        -- Ultimos 7 dias
        SUM(CASE WHEN f.activity_date >= date_add('day', -7, CURRENT_DATE) THEN f.ggr_base ELSE 0 END) AS ggr_7d,
        SUM(CASE WHEN f.activity_date >= date_add('day', -7, CURRENT_DATE) THEN f.deposit_success_base ELSE 0 END) AS dep_7d,
        -- Ultimos 30 dias
        SUM(CASE WHEN f.activity_date >= date_add('day', -30, CURRENT_DATE) THEN f.ggr_base ELSE 0 END) AS ggr_30d,
        SUM(CASE WHEN f.activity_date >= date_add('day', -30, CURRENT_DATE) THEN f.deposit_success_base ELSE 0 END) AS dep_30d,
        -- Contagem de dias ativos
        COUNT(DISTINCT f.activity_date) AS dias_ativos,
        MIN(f.activity_date) AS primeira_atividade,
        MAX(f.activity_date) AS ultima_atividade
    FROM ps_bi.fct_player_activity_daily f
    WHERE f.player_id IN ({ids_str})
    GROUP BY f.player_id
    """
    log.info("Querying fct_player_activity_daily para financeiro dos 2 jogadores...")
    return query_athena(sql, database="ps_bi")


def query_psbi_daily_timeline() -> pd.DataFrame:
    """Timeline diaria dos ultimos 30 dias para graficos."""
    ids_str = ",".join(ECR_IDS)
    sql = f"""
    SELECT
        f.player_id,
        CAST(f.activity_date AS VARCHAR) AS dt,
        f.deposit_success_base AS dep_brl,
        f.cashout_success_base AS saque_brl,
        f.casino_realbet_base AS bets_brl,
        f.casino_real_win_base AS wins_brl,
        f.ggr_base AS ggr_brl,
        f.bonus_issued_base AS bonus_brl
    FROM ps_bi.fct_player_activity_daily f
    WHERE f.player_id IN ({ids_str})
      AND f.activity_date >= date_add('day', -30, CURRENT_DATE)
    ORDER BY f.player_id, f.activity_date
    """
    log.info("Querying timeline diaria (30d) dos 2 jogadores...")
    return query_athena(sql, database="ps_bi")


def query_bonus_detail() -> pd.DataFrame:
    """Detalhes de bonus dos 2 jogadores via bonus_ec2."""
    ids_str = ",".join(ECR_IDS)
    sql = f"""
    SELECT
        bs.c_ecr_id,
        bs.c_bonus_id,
        bs.c_label_id,
        bs.c_bonus_status,
        COALESCE(bs.c_actual_issued_amount, 0) / 100.0 AS bonus_issued_brl,
        COALESCE(bs.c_freespin_win, 0) / 100.0 AS freespin_win_brl,
        CAST(bs.c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR) AS issue_date_brt
    FROM bonus_ec2.tbl_bonus_summary_details bs
    WHERE bs.c_ecr_id IN ({ids_str})
    ORDER BY bs.c_ecr_id, bs.c_issue_date DESC
    """
    log.info("Querying bonus detail dos 2 jogadores...")
    return query_athena(sql, database="bonus_ec2")


def query_bonus_summary() -> pd.DataFrame:
    """Resumo de bonus agregado dos 2 jogadores."""
    ids_str = ",".join(ECR_IDS)
    sql = f"""
    SELECT
        bs.c_ecr_id,
        COUNT(*) AS total_bonus,
        COUNT_IF(bs.c_bonus_status = 'BONUS_ISSUED_OFFER') AS bonus_issued,
        COUNT_IF(bs.c_bonus_status = 'ACTIVE') AS bonus_ativos,
        COUNT_IF(bs.c_bonus_status = 'COMPLETED') AS bonus_completados,
        COUNT_IF(bs.c_bonus_status = 'EXPIRED') AS bonus_expirados,
        SUM(COALESCE(bs.c_actual_issued_amount, 0)) / 100.0 AS total_bonus_brl,
        SUM(COALESCE(bs.c_freespin_win, 0)) / 100.0 AS total_freespin_win_brl,
        COUNT(DISTINCT bs.c_bonus_id) AS bonus_ids_distintos,
        COUNT(DISTINCT date_trunc('day', bs.c_issue_date)) AS dias_com_bonus,
        COUNT_IF(bs.c_issue_date >= date_add('day', -7, CURRENT_TIMESTAMP)) AS bonus_7d,
        COUNT_IF(bs.c_issue_date >= date_add('day', -30, CURRENT_TIMESTAMP)) AS bonus_30d,
        MIN(CAST(bs.c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR)) AS primeiro_bonus,
        MAX(CAST(bs.c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS VARCHAR)) AS ultimo_bonus
    FROM bonus_ec2.tbl_bonus_summary_details bs
    WHERE bs.c_ecr_id IN ({ids_str})
    GROUP BY bs.c_ecr_id
    """
    log.info("Querying bonus summary dos 2 jogadores...")
    return query_athena(sql, database="bonus_ec2")


# ===========================================================================
# 3. HELPERS
# ===========================================================================

def fmt_brl(v) -> str:
    """Formata valor para BRL."""
    if pd.isna(v) or v is None:
        return "R$ 0,00"
    sign = "-" if v < 0 else ""
    return f"{sign}R$ {abs(v):,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


def fmt_int(v) -> str:
    """Formata inteiro com separador de milhar."""
    if pd.isna(v) or v is None:
        return "0"
    return f"{int(v):,}".replace(",", ".")


def safe_get(df, ecr_id, col, default=0):
    """Pega valor de um DataFrame filtrado por ecr_id."""
    if df.empty:
        return default
    # Tenta com coluna ecr_id
    for id_col in ["ecr_id", "player_id", "c_ecr_id"]:
        if id_col in df.columns:
            row = df[df[id_col].astype(str) == str(ecr_id)]
            if not row.empty and col in row.columns:
                val = row.iloc[0][col]
                return default if pd.isna(val) else val
    return default


# ===========================================================================
# 4. GERADOR HTML
# ===========================================================================

def generate_html(csv_data: dict, profiles: pd.DataFrame, financial: pd.DataFrame,
                  timeline: pd.DataFrame, bonus_detail: pd.DataFrame,
                  bonus_summary: pd.DataFrame) -> str:
    """Gera o HTML completo do report unificado."""

    # ========================= PREPARAR DADOS =========================

    # ---- Player cards data ----
    cards = []
    for ecr_id in ECR_IDS:
        name = PLAYER_NAMES.get(ecr_id, ecr_id)
        p = {}
        p["name"] = name
        p["ecr_id"] = ecr_id

        # Profile
        p["external_id"] = safe_get(profiles, ecr_id, "external_id", "N/A")
        p["registration_date"] = str(safe_get(profiles, ecr_id, "registration_date", "N/A"))[:10]
        p["country"] = safe_get(profiles, ecr_id, "country_code", "N/A")
        p["status"] = safe_get(profiles, ecr_id, "ecr_status", "N/A")
        p["ftd_date"] = str(safe_get(profiles, ecr_id, "ftd_date", "N/A"))[:10]
        p["ftd_amount"] = safe_get(profiles, ecr_id, "ftd_amount_inhouse", 0)
        p["kyc_level"] = safe_get(profiles, ecr_id, "kyc_level", "N/A")
        p["tier"] = safe_get(profiles, ecr_id, "tier", "N/A")
        p["affiliate_id"] = safe_get(profiles, ecr_id, "affiliate_id", "N/A")
        p["dias_registro"] = safe_get(profiles, ecr_id, "dias_desde_registro", 0)
        p["lifetime_dep_count"] = safe_get(profiles, ecr_id, "lifetime_deposit_count", 0)
        p["lifetime_dep_amount"] = safe_get(profiles, ecr_id, "lifetime_deposit_amount_inhouse", 0)

        # Financial (ps_bi)
        p["dep_count_psbi"] = safe_get(financial, ecr_id, "dep_count_psbi", 0)
        p["dep_total_brl"] = safe_get(financial, ecr_id, "dep_total_brl", 0)
        p["saque_count"] = safe_get(financial, ecr_id, "saque_count_psbi", 0)
        p["saque_total_brl"] = safe_get(financial, ecr_id, "saque_total_brl", 0)
        p["bets_count"] = safe_get(financial, ecr_id, "bets_count_psbi", 0)
        p["bets_total_brl"] = safe_get(financial, ecr_id, "bets_total_brl", 0)
        p["wins_total_brl"] = safe_get(financial, ecr_id, "wins_total_brl", 0)
        p["ggr_total"] = safe_get(financial, ecr_id, "ggr_total_brl", 0)
        p["ngr_total"] = safe_get(financial, ecr_id, "ngr_total_brl", 0)
        p["bonus_issued_brl"] = safe_get(financial, ecr_id, "bonus_issued_brl", 0)
        p["bonus_turnedreal_brl"] = safe_get(financial, ecr_id, "bonus_turnedreal_brl", 0)
        p["ggr_7d"] = safe_get(financial, ecr_id, "ggr_7d", 0)
        p["ggr_30d"] = safe_get(financial, ecr_id, "ggr_30d", 0)
        p["dep_7d"] = safe_get(financial, ecr_id, "dep_7d", 0)
        p["dep_30d"] = safe_get(financial, ecr_id, "dep_30d", 0)
        p["dias_ativos"] = safe_get(financial, ecr_id, "dias_ativos", 0)

        # Bonus summary
        p["total_bonus"] = safe_get(bonus_summary, ecr_id, "total_bonus", 0)
        p["bonus_total_brl"] = safe_get(bonus_summary, ecr_id, "total_bonus_brl", 0)
        p["freespin_win_brl"] = safe_get(bonus_summary, ecr_id, "total_freespin_win_brl", 0)
        p["bonus_7d"] = safe_get(bonus_summary, ecr_id, "bonus_7d", 0)
        p["bonus_30d"] = safe_get(bonus_summary, ecr_id, "bonus_30d", 0)
        p["bonus_ids_distintos"] = safe_get(bonus_summary, ecr_id, "bonus_ids_distintos", 0)

        # Deep dive classification
        dd = csv_data.get("deep_dive", pd.DataFrame())
        p["classification"] = safe_get(dd, ecr_id, "classification", "N/A")
        p["score_abuser"] = safe_get(dd, ecr_id, "score_abuser", 0)
        p["action"] = safe_get(dd, ecr_id, "action", "N/A")
        p["signals"] = safe_get(dd, ecr_id, "signals", "")

        # Player profit (dep - saque perspective)
        p["player_profit"] = float(p["saque_total_brl"]) - float(p["dep_total_brl"])

        cards.append(p)

    # ---- Timeline data for Chart.js ----
    timeline_json = {}
    for ecr_id in ECR_IDS:
        name = PLAYER_NAMES.get(ecr_id, ecr_id)
        if not timeline.empty:
            player_tl = timeline[timeline["player_id"].astype(str) == str(ecr_id)].sort_values("dt")
            timeline_json[name] = {
                "labels": player_tl["dt"].tolist(),
                "ggr": [float(v) if not pd.isna(v) else 0 for v in player_tl["ggr_brl"]],
                "dep": [float(v) if not pd.isna(v) else 0 for v in player_tl["dep_brl"]],
                "bets": [float(v) if not pd.isna(v) else 0 for v in player_tl["bets_brl"]],
                "wins": [float(v) if not pd.isna(v) else 0 for v in player_tl["wins_brl"]],
                "bonus": [float(v) if not pd.isna(v) else 0 for v in player_tl["bonus_brl"]],
            }
        else:
            timeline_json[name] = {"labels": [], "ggr": [], "dep": [], "bets": [], "wins": [], "bonus": []}

    # ---- Top Players data ----
    tp = csv_data.get("top_players", pd.DataFrame())
    winners = tp[tp["ranking"] == "WINNER"].head(20) if not tp.empty else pd.DataFrame()
    losers = tp[tp["ranking"] == "LOSER"].head(20) if not tp.empty else pd.DataFrame()
    abusers = tp[tp["ranking"] == "ABUSER"].head(20) if not tp.empty else pd.DataFrame()

    # Chart data: Top 10 losers GGR vs freespin wins
    losers_chart = tp[tp["ranking"] == "LOSER"].head(10) if not tp.empty else pd.DataFrame()
    losers_chart_labels = [str(eid)[-6:] for eid in losers_chart["c_ecr_id"]] if not losers_chart.empty else []
    losers_chart_ggr = [float(v) for v in losers_chart["ggr_total_brl"]] if not losers_chart.empty else []
    losers_chart_fs = [float(v) for v in losers_chart["freespin_wins_brl"]] if not losers_chart.empty else []

    # ---- Fraud alerts ----
    fa = csv_data.get("fraud_alerts", pd.DataFrame())

    # ---- Bonus detail table for the 2 players ----
    bonus_rows_html = {}
    for ecr_id in ECR_IDS:
        name = PLAYER_NAMES.get(ecr_id, ecr_id)
        if not bonus_detail.empty:
            bd = bonus_detail[bonus_detail["c_ecr_id"].astype(str) == str(ecr_id)]
            rows = ""
            for _, r in bd.iterrows():
                status = str(r.get("c_bonus_status", ""))
                status_class = "status-active" if status == "ACTIVE" else (
                    "status-completed" if status == "COMPLETED" else (
                    "status-expired" if status == "EXPIRED" else "status-other"))
                rows += f"""<tr>
                    <td class="mono">{r.get('c_bonus_id', 'N/A')}</td>
                    <td>{r.get('c_label_id', 'N/A')}</td>
                    <td><span class="badge {status_class}">{status}</span></td>
                    <td class="right">{fmt_brl(r.get('bonus_issued_brl', 0))}</td>
                    <td class="right">{fmt_brl(r.get('freespin_win_brl', 0))}</td>
                    <td>{str(r.get('issue_date_brt', 'N/A'))[:19]}</td>
                </tr>"""
            bonus_rows_html[name] = rows
        else:
            bonus_rows_html[name] = "<tr><td colspan='6' class='center'>Sem dados de bonus</td></tr>"

    # ---- Build top players rows ----
    def build_tp_rows(df, category):
        if df.empty:
            return "<tr><td colspan='10' class='center'>Sem dados</td></tr>"
        rows = ""
        for _, r in df.iterrows():
            ecr_short = str(r["c_ecr_id"])[-8:]
            ggr_val = float(r["ggr_total_brl"]) if not pd.isna(r["ggr_total_brl"]) else 0
            ggr_class = "positive" if ggr_val > 0 else "negative"
            profit = float(r["player_profit_brl"]) if not pd.isna(r["player_profit_brl"]) else 0
            profit_class = "positive" if profit > 0 else "negative"

            rows += f"""<tr>
                <td class="center">{int(r['rank_pos'])}</td>
                <td class="mono">...{ecr_short}</td>
                <td class="right">{fmt_brl(r.get('dep_brl', 0))}</td>
                <td class="right">{fmt_brl(r.get('saque_brl', 0))}</td>
                <td class="right">{fmt_brl(r.get('casino_bets_brl', 0))}</td>
                <td class="right {ggr_class}">{fmt_brl(ggr_val)}</td>
                <td class="right">{fmt_brl(r.get('freespin_wins_brl', 0))}</td>
                <td class="right">{fmt_brl(r.get('bonus_emitido_brl', 0))}</td>
                <td class="center">{int(r.get('dias_ativos', 0))}</td>
                <td class="right {profit_class}">{fmt_brl(profit)}</td>
            </tr>"""
        return rows

    winners_rows = build_tp_rows(winners, "WINNER")
    losers_rows = build_tp_rows(losers, "LOSER")
    abusers_rows = build_tp_rows(abusers, "ABUSER")

    # ---- Fraud alerts rows ----
    fraud_rows = ""
    if not fa.empty:
        for _, r in fa.iterrows():
            ecr_short = str(r["c_ecr_id"])[-8:]
            tier_class = str(r.get("risk_tier", "")).lower()
            fraud_rows += f"""<tr>
                <td class="mono">...{ecr_short}</td>
                <td><span class="badge badge-{tier_class}">{r.get('risk_tier', 'N/A')}</span></td>
                <td class="center">{r.get('risk_score', 0)}</td>
                <td>{r.get('regras_violadas', 'N/A')}</td>
                <td class="evidence">{str(r.get('evidencias', ''))[:300]}</td>
                <td>{r.get('data_deteccao', 'N/A')}</td>
            </tr>"""
    else:
        fraud_rows = "<tr><td colspan='6' class='center'>Nenhum alerta R8 detectado</td></tr>"

    # ---- KPIs ----
    total_ggr_investigated = sum(float(c["ggr_total"]) for c in cards)
    total_dep_investigated = sum(float(c["dep_total_brl"]) for c in cards)
    total_saque_investigated = sum(float(c["saque_total_brl"]) for c in cards)
    total_bonus_investigated = sum(float(c["bonus_total_brl"]) for c in cards)
    total_fraud_alerts = len(fa) if not fa.empty else 0
    total_top_players = len(tp) if not tp.empty else 0

    # ========================= HTML =========================

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Risk Unified Report — MultiBet | {TODAY}</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        :root {{
            --bg-main: #0f1923;
            --bg-card: #1a2332;
            --bg-card-alt: #1e2a3a;
            --border: #2c3e50;
            --border-light: #34495e;
            --text: #e0e6ed;
            --text-muted: #8899aa;
            --text-bright: #ffffff;
            --green: #2ecc71;
            --red: #e74c3c;
            --orange: #e67e22;
            --purple: #8b5cf6;
            --blue: #3498db;
            --yellow: #f1c40f;
            --cyan: #00bcd4;
        }}

        * {{ margin: 0; padding: 0; box-sizing: border-box; }}

        body {{
            font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
            background: var(--bg-main);
            color: var(--text);
            line-height: 1.6;
            min-height: 100vh;
        }}

        /* Navigation */
        .nav {{
            position: sticky;
            top: 0;
            z-index: 100;
            background: rgba(15, 25, 35, 0.95);
            backdrop-filter: blur(10px);
            border-bottom: 1px solid var(--border);
            padding: 0 24px;
            display: flex;
            align-items: center;
            gap: 8px;
            overflow-x: auto;
        }}
        .nav-brand {{
            font-weight: 700;
            font-size: 14px;
            color: var(--purple);
            padding: 12px 0;
            margin-right: 16px;
            white-space: nowrap;
        }}
        .nav a {{
            color: var(--text-muted);
            text-decoration: none;
            padding: 12px 16px;
            font-size: 13px;
            font-weight: 500;
            border-bottom: 2px solid transparent;
            transition: all 0.2s;
            white-space: nowrap;
        }}
        .nav a:hover, .nav a.active {{
            color: var(--text-bright);
            border-bottom-color: var(--purple);
        }}

        .container {{
            max-width: 1400px;
            margin: 0 auto;
            padding: 24px;
        }}

        /* Section headers */
        .section {{
            margin-bottom: 48px;
            scroll-margin-top: 60px;
        }}
        .section-header {{
            display: flex;
            align-items: center;
            gap: 12px;
            margin-bottom: 24px;
            padding-bottom: 12px;
            border-bottom: 1px solid var(--border);
        }}
        .section-num {{
            background: var(--purple);
            color: white;
            width: 32px;
            height: 32px;
            border-radius: 50%;
            display: flex;
            align-items: center;
            justify-content: center;
            font-weight: 700;
            font-size: 14px;
            flex-shrink: 0;
        }}
        .section-title {{
            font-size: 22px;
            font-weight: 700;
            color: var(--text-bright);
        }}
        .section-subtitle {{
            font-size: 13px;
            color: var(--text-muted);
            margin-top: 2px;
        }}

        /* KPI cards */
        .kpi-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 16px;
            margin-bottom: 32px;
        }}
        .kpi-card {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 20px;
            text-align: center;
        }}
        .kpi-value {{
            font-size: 28px;
            font-weight: 700;
            color: var(--text-bright);
            margin-bottom: 4px;
        }}
        .kpi-value.positive {{ color: var(--green); }}
        .kpi-value.negative {{ color: var(--red); }}
        .kpi-value.warning {{ color: var(--orange); }}
        .kpi-value.accent {{ color: var(--purple); }}
        .kpi-label {{
            font-size: 12px;
            color: var(--text-muted);
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        /* Player cards */
        .player-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(580px, 1fr));
            gap: 24px;
            margin-bottom: 32px;
        }}
        .player-card {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            overflow: hidden;
        }}
        .player-header {{
            padding: 20px 24px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            border-bottom: 1px solid var(--border);
        }}
        .player-name {{
            font-size: 20px;
            font-weight: 700;
            color: var(--text-bright);
        }}
        .player-ecr {{
            font-size: 11px;
            color: var(--text-muted);
            font-family: monospace;
        }}
        .player-body {{
            padding: 24px;
        }}
        .player-meta {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 12px;
            margin-bottom: 20px;
        }}
        .meta-item {{
            font-size: 12px;
        }}
        .meta-label {{
            color: var(--text-muted);
            display: block;
            margin-bottom: 2px;
        }}
        .meta-value {{
            color: var(--text-bright);
            font-weight: 600;
        }}
        .player-financials {{
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 8px;
            margin-bottom: 20px;
        }}
        .fin-item {{
            background: var(--bg-card-alt);
            border-radius: 8px;
            padding: 12px;
            text-align: center;
        }}
        .fin-value {{
            font-size: 16px;
            font-weight: 700;
            margin-bottom: 2px;
        }}
        .fin-label {{
            font-size: 11px;
            color: var(--text-muted);
        }}

        /* Classification badge */
        .classification {{
            display: inline-block;
            padding: 4px 16px;
            border-radius: 20px;
            font-weight: 700;
            font-size: 13px;
            text-transform: uppercase;
        }}
        .classification.SUSPECT, .classification.suspect {{ background: rgba(231, 76, 60, 0.2); color: var(--red); border: 1px solid var(--red); }}
        .classification.NORMAL, .classification.normal {{ background: rgba(46, 204, 113, 0.2); color: var(--green); border: 1px solid var(--green); }}
        .classification.VIP, .classification.vip {{ background: rgba(139, 92, 246, 0.2); color: var(--purple); border: 1px solid var(--purple); }}
        .classification.WARNING, .classification.warning {{ background: rgba(230, 126, 34, 0.2); color: var(--orange); border: 1px solid var(--orange); }}

        /* Signals box */
        .signals-box {{
            margin-top: 16px;
            padding: 12px 16px;
            background: rgba(231, 76, 60, 0.08);
            border: 1px solid rgba(231, 76, 60, 0.3);
            border-radius: 8px;
            font-size: 13px;
        }}
        .signals-box.ok {{
            background: rgba(46, 204, 113, 0.08);
            border-color: rgba(46, 204, 113, 0.3);
        }}
        .signals-title {{
            font-weight: 600;
            margin-bottom: 4px;
        }}

        /* Tables */
        .table-container {{
            overflow-x: auto;
            border-radius: 12px;
            border: 1px solid var(--border);
            margin-bottom: 24px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        thead {{
            background: var(--bg-card-alt);
            position: sticky;
            top: 0;
        }}
        th {{
            padding: 12px 16px;
            text-align: left;
            font-weight: 600;
            color: var(--text-muted);
            text-transform: uppercase;
            font-size: 11px;
            letter-spacing: 0.5px;
            border-bottom: 2px solid var(--border);
            white-space: nowrap;
            cursor: pointer;
        }}
        th:hover {{
            color: var(--text-bright);
        }}
        td {{
            padding: 10px 16px;
            border-bottom: 1px solid rgba(44, 62, 80, 0.5);
            vertical-align: middle;
        }}
        tr:hover {{
            background: rgba(139, 92, 246, 0.05);
        }}
        .mono {{ font-family: 'Consolas', monospace; font-size: 12px; }}
        .right {{ text-align: right; }}
        .center {{ text-align: center; }}
        .positive {{ color: var(--green); }}
        .negative {{ color: var(--red); }}
        .evidence {{
            max-width: 400px;
            font-size: 12px;
            color: var(--text-muted);
        }}

        /* Badges */
        .badge {{
            display: inline-block;
            padding: 2px 10px;
            border-radius: 12px;
            font-size: 11px;
            font-weight: 600;
            text-transform: uppercase;
        }}
        .badge-critical {{ background: rgba(231, 76, 60, 0.2); color: var(--red); }}
        .badge-high {{ background: rgba(230, 126, 34, 0.2); color: var(--orange); }}
        .badge-medium {{ background: rgba(241, 196, 15, 0.2); color: var(--yellow); }}
        .badge-low {{ background: rgba(46, 204, 113, 0.2); color: var(--green); }}
        .status-active {{ background: rgba(46, 204, 113, 0.2); color: var(--green); }}
        .status-completed {{ background: rgba(52, 152, 219, 0.2); color: var(--blue); }}
        .status-expired {{ background: rgba(136, 153, 170, 0.2); color: var(--text-muted); }}
        .status-other {{ background: rgba(241, 196, 15, 0.2); color: var(--yellow); }}

        /* Tab system for top players */
        .tabs {{
            display: flex;
            gap: 4px;
            margin-bottom: 0;
        }}
        .tab-btn {{
            padding: 10px 24px;
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-bottom: none;
            border-radius: 8px 8px 0 0;
            color: var(--text-muted);
            cursor: pointer;
            font-size: 13px;
            font-weight: 600;
            transition: all 0.2s;
        }}
        .tab-btn:hover, .tab-btn.active {{
            background: var(--bg-card-alt);
            color: var(--text-bright);
        }}
        .tab-btn.active {{
            border-bottom: 2px solid var(--purple);
        }}
        .tab-content {{
            display: none;
        }}
        .tab-content.active {{
            display: block;
        }}

        /* Charts */
        .chart-container {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
        }}
        .chart-title {{
            font-size: 15px;
            font-weight: 600;
            color: var(--text-bright);
            margin-bottom: 16px;
        }}
        .chart-wrapper {{
            position: relative;
            height: 300px;
        }}
        .charts-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 24px;
        }}

        /* Data quality note */
        .dq-note {{
            background: rgba(230, 126, 34, 0.08);
            border: 1px solid rgba(230, 126, 34, 0.3);
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
        }}
        .dq-note h4 {{
            color: var(--orange);
            margin-bottom: 12px;
        }}
        .dq-note ul {{
            margin-left: 20px;
            margin-bottom: 12px;
        }}
        .dq-note li {{
            margin-bottom: 8px;
            font-size: 14px;
        }}
        .dq-note strong {{
            color: var(--text-bright);
        }}
        .dq-recommendation {{
            background: rgba(139, 92, 246, 0.1);
            border: 1px solid rgba(139, 92, 246, 0.3);
            border-radius: 8px;
            padding: 16px;
            margin-top: 16px;
        }}
        .dq-recommendation h5 {{
            color: var(--purple);
            margin-bottom: 8px;
        }}

        /* Footer */
        .footer {{
            text-align: center;
            padding: 32px;
            margin-top: 48px;
            border-top: 1px solid var(--border);
            font-size: 12px;
            color: var(--text-muted);
        }}
        .footer strong {{
            color: var(--purple);
        }}

        /* Action box */
        .action-box {{
            background: var(--bg-card-alt);
            border-left: 4px solid var(--purple);
            padding: 16px 20px;
            border-radius: 0 8px 8px 0;
            margin-top: 12px;
            font-size: 13px;
        }}
        .action-box strong {{
            color: var(--purple);
        }}

        /* Responsive */
        @media (max-width: 768px) {{
            .player-grid {{ grid-template-columns: 1fr; }}
            .player-meta {{ grid-template-columns: repeat(2, 1fr); }}
            .player-financials {{ grid-template-columns: repeat(2, 1fr); }}
            .charts-grid {{ grid-template-columns: 1fr; }}
            .kpi-grid {{ grid-template-columns: repeat(2, 1fr); }}
        }}

        /* Legend section */
        .legend {{
            background: var(--bg-card);
            border: 1px solid var(--border);
            border-radius: 12px;
            padding: 24px;
            margin-bottom: 24px;
        }}
        .legend h4 {{
            color: var(--text-bright);
            margin-bottom: 12px;
            font-size: 15px;
        }}
        .legend-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 8px;
        }}
        .legend-item {{
            display: flex;
            gap: 8px;
            font-size: 12px;
            padding: 4px 0;
        }}
        .legend-term {{
            font-weight: 600;
            color: var(--cyan);
            min-width: 140px;
        }}
        .legend-def {{
            color: var(--text-muted);
        }}
    </style>
</head>
<body>

<!-- ================= NAVIGATION ================= -->
<nav class="nav">
    <span class="nav-brand">RISK AGENT v1.4</span>
    <a href="#section-exec" class="active">Sumario Executivo</a>
    <a href="#section-deep">Deep Dive</a>
    <a href="#section-top">Top Players</a>
    <a href="#section-r8">Alertas R8</a>
    <a href="#section-dq">Qualidade Dados</a>
    <a href="#section-legend">Legenda</a>
</nav>

<div class="container">

<!-- ================= KPI CARDS ================= -->
<div class="kpi-grid" style="margin-top: 24px;">
    <div class="kpi-card">
        <div class="kpi-value accent">2</div>
        <div class="kpi-label">Jogadores Investigados</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value {'positive' if total_ggr_investigated > 0 else 'negative'}">{fmt_brl(total_ggr_investigated)}</div>
        <div class="kpi-label">GGR Total Investigados</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value">{fmt_brl(total_dep_investigated)}</div>
        <div class="kpi-label">Depositos Total (ps_bi)</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value warning">{fmt_brl(total_bonus_investigated)}</div>
        <div class="kpi-label">Bonus Emitidos (Athena)</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value accent">{total_top_players}</div>
        <div class="kpi-label">Top Players Mapeados</div>
    </div>
    <div class="kpi-card">
        <div class="kpi-value {'negative' if total_fraud_alerts > 0 else 'positive'}">{total_fraud_alerts}</div>
        <div class="kpi-label">Alertas R8 Ativos</div>
    </div>
</div>

<!-- ================= SECTION 1: EXECUTIVE SUMMARY ================= -->
<div class="section" id="section-exec">
    <div class="section-header">
        <div class="section-num">1</div>
        <div>
            <div class="section-title">Investigacao de Risco — Jogadores Flagados pelo Head</div>
            <div class="section-subtitle">Demanda Castrin (Head of Data) em 06/04/2026 — Verificar se Murilo e Fabiano sao abusadores de bonus</div>
        </div>
    </div>

    <div class="player-grid">
"""

    # ---- Player cards ----
    for p in cards:
        clf = p["classification"]
        clf_lower = clf.lower() if isinstance(clf, str) else "normal"
        signals_class = "" if p["signals"] else "ok"
        signals_text = p["signals"] if p["signals"] else "Nenhum sinal critico detectado"
        ggr_class = "positive" if float(p["ggr_total"]) > 0 else "negative"
        profit_class = "positive" if p["player_profit"] > 0 else "negative"

        html += f"""
        <div class="player-card">
            <div class="player-header">
                <div>
                    <div class="player-name">{p['name']}</div>
                    <div class="player-ecr">ECR {p['ecr_id']}</div>
                </div>
                <span class="classification {clf_lower}">{clf}</span>
            </div>
            <div class="player-body">
                <div class="player-meta">
                    <div class="meta-item">
                        <span class="meta-label">Registro</span>
                        <span class="meta-value">{p['registration_date']}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Primeiro Deposito</span>
                        <span class="meta-value">{p['ftd_date']}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Dias Ativo</span>
                        <span class="meta-value">{fmt_int(p['dias_ativos'])}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">KYC Level</span>
                        <span class="meta-value">{p['kyc_level']}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Tier</span>
                        <span class="meta-value">{p['tier']}</span>
                    </div>
                    <div class="meta-item">
                        <span class="meta-label">Affiliate</span>
                        <span class="meta-value">{p['affiliate_id']}</span>
                    </div>
                </div>

                <div class="player-financials">
                    <div class="fin-item">
                        <div class="fin-value">{fmt_brl(p['dep_total_brl'])}</div>
                        <div class="fin-label">Depositos ({fmt_int(p['dep_count_psbi'])} txns)</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value">{fmt_brl(p['saque_total_brl'])}</div>
                        <div class="fin-label">Saques ({fmt_int(p['saque_count'])} txns)</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value {ggr_class}">{fmt_brl(p['ggr_total'])}</div>
                        <div class="fin-label">GGR Total</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value">{fmt_brl(p['bets_total_brl'])}</div>
                        <div class="fin-label">Bets Casino ({fmt_int(p['bets_count'])})</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value">{fmt_brl(p['wins_total_brl'])}</div>
                        <div class="fin-label">Wins Casino</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value {profit_class}">{fmt_brl(p['player_profit'])}</div>
                        <div class="fin-label">Lucro Jogador</div>
                    </div>
                </div>

                <div class="player-financials" style="grid-template-columns: repeat(4, 1fr);">
                    <div class="fin-item">
                        <div class="fin-value" style="color: var(--orange);">{fmt_brl(p['bonus_total_brl'])}</div>
                        <div class="fin-label">Bonus (Athena)</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value">{fmt_int(p['total_bonus'])}</div>
                        <div class="fin-label">Bonus Recebidos</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value">{fmt_brl(p['freespin_win_brl'])}</div>
                        <div class="fin-label">Free Spin Wins</div>
                    </div>
                    <div class="fin-item">
                        <div class="fin-value">{fmt_brl(p['bonus_issued_brl'])}</div>
                        <div class="fin-label">Bonus Issued (ps_bi)</div>
                    </div>
                </div>

                <div class="signals-box {signals_class}">
                    <div class="signals-title">Sinais de Risco</div>
                    {signals_text}
                </div>

                <div class="action-box">
                    <strong>Acao Recomendada:</strong> {p['action']}
                </div>
            </div>
        </div>
"""

    html += """
    </div> <!-- end player-grid -->

    <div class="dq-note">
        <h4>Nota Importante — Divergencia de Dados</h4>
        <ul>
            <li><strong>Depositos:</strong> Os valores no ps_bi.fct_player_activity_daily mostram ~3x mais depositos que o fund_ec2.tbl_real_fund_txn. Isso indica que a camada <code>fund_ec2</code> pode estar incompleta para depositos.</li>
            <li><strong>Bonus:</strong> Os valores de bonus no Athena (bonus_ec2) sao ~100x menores que os reportados pelo Head no back-office da Pragmatic. Isso sugere que a tabela bonus_ec2 nao captura todos os tipos de bonus.</li>
            <li><strong>Recomendacao:</strong> Solicitar ao Head a fonte exata de dados que ele utiliza (back-office Pragmatic) para comparacao cruzada.</li>
        </ul>
    </div>
</div> <!-- end section-exec -->

<!-- ================= SECTION 2: DEEP DIVE ================= -->
<div class="section" id="section-deep">
    <div class="section-header">
        <div class="section-num">2</div>
        <div>
            <div class="section-title">Deep Dive — Perfil Completo dos 2 Jogadores</div>
            <div class="section-subtitle">Timeline 30 dias, historico de bonus, breakdown financeiro via ps_bi</div>
        </div>
    </div>

    <!-- Timeline Charts -->
    <div class="charts-grid">
"""

    # ---- Timeline charts for each player ----
    for ecr_id in ECR_IDS:
        name = PLAYER_NAMES.get(ecr_id, ecr_id)
        html += f"""
        <div class="chart-container">
            <div class="chart-title">Timeline 30 Dias — {name} (ECR ...{ecr_id[-6:]})</div>
            <div class="chart-wrapper">
                <canvas id="chart-timeline-{name}"></canvas>
            </div>
        </div>
"""

    html += """
    </div> <!-- end charts-grid -->
"""

    # ---- Bonus detail tables ----
    for ecr_id in ECR_IDS:
        name = PLAYER_NAMES.get(ecr_id, ecr_id)
        p = next((c for c in cards if c["ecr_id"] == ecr_id), {})

        html += f"""
    <h3 style="margin: 24px 0 12px; color: var(--text-bright);">Historico de Bonus — {name}</h3>
    <div style="display: flex; gap: 16px; margin-bottom: 16px; flex-wrap: wrap;">
        <div class="fin-item" style="background: var(--bg-card); padding: 12px 20px;">
            <div class="fin-value" style="font-size: 14px;">{fmt_int(p.get('total_bonus', 0))}</div>
            <div class="fin-label">Total Recebidos</div>
        </div>
        <div class="fin-item" style="background: var(--bg-card); padding: 12px 20px;">
            <div class="fin-value" style="font-size: 14px;">{fmt_brl(p.get('bonus_total_brl', 0))}</div>
            <div class="fin-label">Total Emitido (Athena)</div>
        </div>
        <div class="fin-item" style="background: var(--bg-card); padding: 12px 20px;">
            <div class="fin-value" style="font-size: 14px;">{fmt_brl(p.get('freespin_win_brl', 0))}</div>
            <div class="fin-label">Free Spin Wins</div>
        </div>
        <div class="fin-item" style="background: var(--bg-card); padding: 12px 20px;">
            <div class="fin-value" style="font-size: 14px;">{fmt_int(p.get('bonus_7d', 0))}</div>
            <div class="fin-label">Bonus 7d</div>
        </div>
        <div class="fin-item" style="background: var(--bg-card); padding: 12px 20px;">
            <div class="fin-value" style="font-size: 14px;">{fmt_int(p.get('bonus_30d', 0))}</div>
            <div class="fin-label">Bonus 30d</div>
        </div>
    </div>
    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th>Bonus ID</th>
                    <th>Label ID</th>
                    <th>Status</th>
                    <th>Valor Emitido</th>
                    <th>Free Spin Win</th>
                    <th>Data Emissao (BRT)</th>
                </tr>
            </thead>
            <tbody>
                {bonus_rows_html.get(name, '')}
            </tbody>
        </table>
    </div>
"""

    html += """
</div> <!-- end section-deep -->

<!-- ================= SECTION 3: TOP PLAYERS ================= -->
<div class="section" id="section-top">
    <div class="section-header">
        <div class="section-num">3</div>
        <div>
            <div class="section-title">Top Players — Winners, Losers & Abusers</div>
            <div class="section-subtitle">Ranking dos 50 jogadores mais relevantes em cada categoria (ultimos 30 dias)</div>
        </div>
    </div>

    <!-- Tabs -->
    <div class="tabs">
        <button class="tab-btn active" onclick="showTab('winners')">Winners (Top GGR)</button>
        <button class="tab-btn" onclick="showTab('losers')">Losers (Pior GGR)</button>
        <button class="tab-btn" onclick="showTab('abusers')">Abusers (Suspeitos)</button>
    </div>
"""

    # Table template for each category
    table_header = """
        <table>
            <thead>
                <tr>
                    <th>#</th>
                    <th>ECR ID</th>
                    <th>Depositos</th>
                    <th>Saques</th>
                    <th>Bets Casino</th>
                    <th>GGR Total</th>
                    <th>FS Wins</th>
                    <th>Bonus</th>
                    <th>Dias</th>
                    <th>Lucro Jogador</th>
                </tr>
            </thead>
    """

    html += f"""
    <div id="tab-winners" class="tab-content active">
        <div class="table-container" style="border-radius: 0 12px 12px 12px;">
            {table_header}
            <tbody>{winners_rows}</tbody>
            </table>
        </div>
    </div>

    <div id="tab-losers" class="tab-content">
        <div class="table-container" style="border-radius: 0 12px 12px 12px;">
            {table_header}
            <tbody>{losers_rows}</tbody>
            </table>
        </div>
    </div>

    <div id="tab-abusers" class="tab-content">
        <div class="table-container" style="border-radius: 0 12px 12px 12px;">
            {table_header}
            <tbody>{abusers_rows}</tbody>
            </table>
        </div>
    </div>

    <!-- Chart: Top 10 Losers -->
    <div class="chart-container" style="margin-top: 24px;">
        <div class="chart-title">Top 10 Losers — GGR vs Free Spin Wins</div>
        <div class="chart-wrapper">
            <canvas id="chart-losers"></canvas>
        </div>
    </div>
</div> <!-- end section-top -->

<!-- ================= SECTION 4: R8 ALERTS ================= -->
<div class="section" id="section-r8">
    <div class="section-header">
        <div class="section-num">4</div>
        <div>
            <div class="section-title">Alertas R8 — Free Spin Abuser Detection</div>
            <div class="section-subtitle">Jogadores flagados pela regra R8 do Risk Agent (revenue negativo + bonus alto + freespin wins)</div>
        </div>
    </div>

    <div style="background: var(--bg-card); border: 1px solid var(--border); border-radius: 12px; padding: 20px; margin-bottom: 24px;">
        <h4 style="color: var(--text-bright); margin-bottom: 12px;">Como funciona a Regra R8</h4>
        <p style="font-size: 14px; color: var(--text-muted); margin-bottom: 8px;">
            A regra R8 identifica jogadores que podem estar abusando de free spins. Criterios:
        </p>
        <ul style="margin-left: 20px; font-size: 13px; color: var(--text-muted);">
            <li><strong>Revenue negativo</strong> — o jogador gera prejuizo para a casa</li>
            <li><strong>Bonus alto vs depositos</strong> — ratio bonus/deposito acima de 0.5x</li>
            <li><strong>Free spin wins relevantes</strong> — ganhos via free spins maiores que depositos</li>
            <li><strong>Padrao repetitivo</strong> — multiplos bonus em poucos dias</li>
        </ul>
    </div>

    <div class="table-container">
        <table>
            <thead>
                <tr>
                    <th>ECR ID</th>
                    <th>Tier</th>
                    <th>Score</th>
                    <th>Regras</th>
                    <th>Evidencias</th>
                    <th>Data Deteccao</th>
                </tr>
            </thead>
            <tbody>
                {fraud_rows}
            </tbody>
        </table>
    </div>

    <div style="text-align: center; font-size: 13px; color: var(--text-muted); padding: 12px;">
        Total de alertas R8: <strong style="color: var(--orange);">{total_fraud_alerts}</strong> jogadores flagados
    </div>
</div> <!-- end section-r8 -->

<!-- ================= SECTION 5: DATA QUALITY ================= -->
<div class="section" id="section-dq">
    <div class="section-header">
        <div class="section-num">5</div>
        <div>
            <div class="section-title">Nota de Qualidade de Dados</div>
            <div class="section-subtitle">Divergencias identificadas entre fontes — requer alinhamento com o Head</div>
        </div>
    </div>

    <div class="dq-note">
        <h4>1. Depositos: fund_ec2 vs ps_bi</h4>
        <ul>
            <li>A tabela <code>fund_ec2.tbl_real_fund_txn</code> (c_txn_type = 1, status = SUCCESS) retorna <strong>menos depositos</strong> que <code>ps_bi.fct_player_activity_daily.deposit_success_count</code>.</li>
            <li>No caso dos 2 jogadores investigados, ps_bi mostra aproximadamente <strong>3x mais depositos</strong> que fund_ec2.</li>
            <li><strong>Hipotese:</strong> fund_ec2 pode nao capturar todos os metodos de deposito (ex: Pix via gateway alternativo) ou os dados estao em defasagem no Iceberg.</li>
            <li><strong>Impacto:</strong> Analises usando fund_ec2 para depositos podem subestimar o volume real.</li>
        </ul>
    </div>

    <div class="dq-note">
        <h4>2. Bonus: Athena vs Back-office Pragmatic</h4>
        <ul>
            <li>Os valores de bonus no Athena (<code>bonus_ec2.tbl_bonus_summary_details</code>) sao <strong>~100x menores</strong> que os reportados pelo Head via back-office.</li>
            <li><strong>Hipotese:</strong> O campo <code>c_actual_issued_amount</code> pode representar apenas uma fracao do bonus (ex: parte em centavos nao convertida, ou apenas bonus de deposito, excluindo free spins e creditos manuais).</li>
            <li><strong>Impacto:</strong> Classificacoes de abuso de bonus podem ser imprecisas se baseadas exclusivamente no Athena.</li>
        </ul>
    </div>

    <div class="dq-note">
        <h4>3. GGR: Consistencia entre camadas</h4>
        <ul>
            <li>O GGR calculado via <code>ps_bi.fct_player_activity_daily.ggr_base</code> pode diferir do calculo manual via fund_ec2 (bets - wins - rollbacks).</li>
            <li>ps_bi e a camada BI mart (dbt) e deve ser <strong>preferida para analises de negocio</strong>.</li>
            <li>fund_ec2 e util para <strong>auditoria granular</strong> (transacao a transacao).</li>
        </ul>
    </div>

    <div class="dq-recommendation">
        <h5>Recomendacao ao Head</h5>
        <p style="font-size: 14px;">
            Para classificar corretamente Murilo e Fabiano como <strong>VIP vs Abuser</strong>, precisamos:
        </p>
        <ol style="margin-left: 20px; font-size: 14px; margin-top: 8px;">
            <li>Confirmar a <strong>fonte de dados do back-office</strong> que mostra os valores maiores de bonus</li>
            <li>Exportar um <strong>snapshot de bonus detalhado</strong> para os 2 ECR IDs diretamente do Pragmatic</li>
            <li>Validar se o campo <code>c_actual_issued_amount</code> esta em centavos ou BRL no bonus_ec2</li>
            <li>Definir <strong>thresholds claros</strong> para classificar VIP vs Abuser (ex: ratio bonus/deposito > 2x = suspect)</li>
        </ol>
    </div>
</div> <!-- end section-dq -->

<!-- ================= SECTION 6: LEGEND ================= -->
<div class="section" id="section-legend">
    <div class="section-header">
        <div class="section-num">L</div>
        <div>
            <div class="section-title">Legenda e Glossario</div>
            <div class="section-subtitle">Como ler este relatorio — definicoes, fontes e metodologia</div>
        </div>
    </div>

    <div class="legend">
        <h4>Glossario de Termos</h4>
        <div class="legend-grid">
            <div class="legend-item">
                <span class="legend-term">GGR</span>
                <span class="legend-def">Gross Gaming Revenue — receita bruta (bets - wins do jogador)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">NGR</span>
                <span class="legend-def">Net Gaming Revenue — GGR menos bonus e comissoes</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Free Spin Win</span>
                <span class="legend-def">Ganho obtido via rodada gratuita (bonus)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Rollback</span>
                <span class="legend-def">Cancelamento de aposta (devolucao ao jogador)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">FTD</span>
                <span class="legend-def">First Time Deposit — primeiro deposito do jogador</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">ECR ID</span>
                <span class="legend-def">ID interno do jogador no sistema Pragmatic (18 digitos)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">KYC Level</span>
                <span class="legend-def">Nivel de verificacao de identidade (Know Your Customer)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Tier</span>
                <span class="legend-def">Classificacao VIP do jogador (Bronze, Silver, Gold, etc.)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Abuse Score</span>
                <span class="legend-def">Pontuacao de abuso (0-100) baseada em regras heuristicas</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">R8</span>
                <span class="legend-def">Regra de deteccao de abuso de Free Spin do Risk Agent</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Lucro Jogador</span>
                <span class="legend-def">Saques - Depositos (positivo = jogador lucrando)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Bonus Issued (ps_bi)</span>
                <span class="legend-def">Bonus emitido conforme camada BI mart (dbt)</span>
            </div>
        </div>
    </div>

    <div class="legend">
        <h4>Fontes de Dados</h4>
        <div class="legend-grid">
            <div class="legend-item">
                <span class="legend-term">ps_bi.dim_user</span>
                <span class="legend-def">Perfil do jogador (registro, KYC, tier, lifetime)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">ps_bi.fct_player_*</span>
                <span class="legend-def">Atividade diaria agregada (depositos, bets, GGR) — BRL</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">fund_ec2.tbl_real_fund_txn</span>
                <span class="legend-def">Transacoes brutas (centavos) — usado para auditoria</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">bonus_ec2.tbl_bonus_*</span>
                <span class="legend-def">Detalhes de bonus (centavos) — campo c_actual_issued_amount</span>
            </div>
        </div>
    </div>

    <div class="legend">
        <h4>Periodo e Metodologia</h4>
        <div class="legend-grid">
            <div class="legend-item">
                <span class="legend-term">Periodo</span>
                <span class="legend-def">Dados ate D-1 ({TODAY}). Valores lifetime + janelas 7d/30d.</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Timezone</span>
                <span class="legend-def">Dados convertidos de UTC para BRT (America/Sao_Paulo)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Moeda</span>
                <span class="legend-def">Todos os valores em BRL (R$)</span>
            </div>
            <div class="legend-item">
                <span class="legend-term">Filtros</span>
                <span class="legend-def">is_test = false, c_product_id = CASINO (quando aplicavel)</span>
            </div>
        </div>
    </div>
</div>

</div> <!-- end container -->

<!-- ================= FOOTER ================= -->
<div class="footer">
    <strong>Squad 3 — Intelligence Engine</strong> | MultiBet Risk Agent v1.4<br>
    Gerado em {datetime.now().strftime('%d/%m/%Y %H:%M')} | Dados ate {TODAY} (D-1)<br>
    Fonte: AWS Athena (Iceberg) — ps_bi, fund_ec2, bonus_ec2
</div>

<!-- ================= JAVASCRIPT ================= -->
<script>
// ---- Navigation active state ----
document.querySelectorAll('.nav a').forEach(link => {{
    link.addEventListener('click', function() {{
        document.querySelectorAll('.nav a').forEach(l => l.classList.remove('active'));
        this.classList.add('active');
    }});
}});

// Scroll spy
window.addEventListener('scroll', function() {{
    const sections = document.querySelectorAll('.section');
    let current = '';
    sections.forEach(section => {{
        const top = section.offsetTop - 80;
        if (window.pageYOffset >= top) {{
            current = section.getAttribute('id');
        }}
    }});
    document.querySelectorAll('.nav a').forEach(link => {{
        link.classList.remove('active');
        if (link.getAttribute('href') === '#' + current) {{
            link.classList.add('active');
        }}
    }});
}});

// ---- Tab system ----
function showTab(tab) {{
    document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
    document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
    document.getElementById('tab-' + tab).classList.add('active');
    event.target.classList.add('active');
}}

// ---- Chart.js defaults ----
Chart.defaults.color = '#8899aa';
Chart.defaults.borderColor = 'rgba(44, 62, 80, 0.5)';
Chart.defaults.font.family = "'Segoe UI', system-ui, sans-serif";

// ---- Timeline Charts ----
const timelineData = {json.dumps(timeline_json)};

Object.keys(timelineData).forEach(name => {{
    const ctx = document.getElementById('chart-timeline-' + name);
    if (!ctx) return;
    const d = timelineData[name];
    new Chart(ctx, {{
        type: 'line',
        data: {{
            labels: d.labels,
            datasets: [
                {{
                    label: 'GGR',
                    data: d.ggr,
                    borderColor: '#8b5cf6',
                    backgroundColor: 'rgba(139, 92, 246, 0.1)',
                    fill: true,
                    tension: 0.3,
                    borderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 6
                }},
                {{
                    label: 'Depositos',
                    data: d.dep,
                    borderColor: '#2ecc71',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 2,
                    pointHoverRadius: 5
                }},
                {{
                    label: 'Bets',
                    data: d.bets,
                    borderColor: '#3498db',
                    borderWidth: 1.5,
                    tension: 0.3,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    borderDash: [4, 4]
                }},
                {{
                    label: 'Wins',
                    data: d.wins,
                    borderColor: '#e74c3c',
                    borderWidth: 1.5,
                    tension: 0.3,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                    borderDash: [4, 4]
                }},
                {{
                    label: 'Bonus',
                    data: d.bonus,
                    borderColor: '#e67e22',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 2,
                    pointHoverRadius: 5
                }}
            ]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            interaction: {{
                mode: 'index',
                intersect: false
            }},
            plugins: {{
                legend: {{
                    position: 'top',
                    labels: {{ usePointStyle: true, padding: 16 }}
                }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            return ctx.dataset.label + ': R$ ' + ctx.parsed.y.toLocaleString('pt-BR', {{minimumFractionDigits: 2}});
                        }}
                    }}
                }}
            }},
            scales: {{
                x: {{
                    grid: {{ display: false }},
                    ticks: {{ maxRotation: 45 }}
                }},
                y: {{
                    grid: {{ color: 'rgba(44, 62, 80, 0.3)' }},
                    ticks: {{
                        callback: function(v) {{ return 'R$ ' + (v/1000).toFixed(0) + 'k'; }}
                    }}
                }}
            }}
        }}
    }});
}});

// ---- Losers Chart ----
const losersCtx = document.getElementById('chart-losers');
if (losersCtx) {{
    new Chart(losersCtx, {{
        type: 'bar',
        data: {{
            labels: {json.dumps(losers_chart_labels)},
            datasets: [
                {{
                    label: 'GGR (negativo = casa perde)',
                    data: {json.dumps(losers_chart_ggr)},
                    backgroundColor: 'rgba(231, 76, 60, 0.7)',
                    borderColor: '#e74c3c',
                    borderWidth: 1
                }},
                {{
                    label: 'Free Spin Wins',
                    data: {json.dumps(losers_chart_fs)},
                    backgroundColor: 'rgba(230, 126, 34, 0.7)',
                    borderColor: '#e67e22',
                    borderWidth: 1
                }}
            ]
        }},
        options: {{
            responsive: true,
            maintainAspectRatio: false,
            plugins: {{
                legend: {{
                    position: 'top',
                    labels: {{ usePointStyle: true, padding: 16 }}
                }},
                tooltip: {{
                    callbacks: {{
                        label: function(ctx) {{
                            return ctx.dataset.label + ': R$ ' + ctx.parsed.y.toLocaleString('pt-BR', {{minimumFractionDigits: 2}});
                        }}
                    }}
                }}
            }},
            scales: {{
                x: {{
                    grid: {{ display: false }}
                }},
                y: {{
                    grid: {{ color: 'rgba(44, 62, 80, 0.3)' }},
                    ticks: {{
                        callback: function(v) {{
                            if (Math.abs(v) >= 1000) return 'R$ ' + (v/1000).toFixed(0) + 'k';
                            return 'R$ ' + v.toFixed(0);
                        }}
                    }}
                }}
            }}
        }}
    }});
}}

// ---- Table sorting ----
document.querySelectorAll('th').forEach(th => {{
    th.addEventListener('click', function() {{
        const table = this.closest('table');
        const tbody = table.querySelector('tbody');
        const rows = Array.from(tbody.querySelectorAll('tr'));
        const idx = Array.from(this.parentElement.children).indexOf(this);
        const asc = this.dataset.sort !== 'asc';
        this.dataset.sort = asc ? 'asc' : 'desc';

        rows.sort((a, b) => {{
            let va = a.children[idx]?.textContent.trim() || '';
            let vb = b.children[idx]?.textContent.trim() || '';
            // Try numeric
            const na = parseFloat(va.replace(/[R$\\s.]/g, '').replace(',', '.'));
            const nb = parseFloat(vb.replace(/[R$\\s.]/g, '').replace(',', '.'));
            if (!isNaN(na) && !isNaN(nb)) {{
                return asc ? na - nb : nb - na;
            }}
            return asc ? va.localeCompare(vb) : vb.localeCompare(va);
        }});

        rows.forEach(r => tbody.appendChild(r));
    }});
}});
</script>

</body>
</html>"""

    return html


# ===========================================================================
# 5. MAIN
# ===========================================================================

def main():
    log.info("=" * 60)
    log.info("RISK UNIFIED REPORT — MultiBet")
    log.info("=" * 60)

    # Step 1: Load CSVs
    log.info("Step 1: Carregando CSVs existentes...")
    csv_data = load_csvs()

    # Step 2: Query Athena
    log.info("Step 2: Consultando Athena para dados complementares...")

    try:
        profiles = query_player_profiles()
        log.info(f"  dim_user: {len(profiles)} jogadores")
    except Exception as e:
        log.error(f"  Erro ao consultar dim_user: {e}")
        profiles = pd.DataFrame()

    try:
        financial = query_psbi_financial()
        log.info(f"  fct_player_activity_daily: {len(financial)} jogadores")
    except Exception as e:
        log.error(f"  Erro ao consultar fct_player_activity_daily: {e}")
        financial = pd.DataFrame()

    try:
        timeline = query_psbi_daily_timeline()
        log.info(f"  Timeline diaria: {len(timeline)} registros")
    except Exception as e:
        log.error(f"  Erro ao consultar timeline: {e}")
        timeline = pd.DataFrame()

    try:
        bonus_detail = query_bonus_detail()
        log.info(f"  Bonus detail: {len(bonus_detail)} registros")
    except Exception as e:
        log.error(f"  Erro ao consultar bonus detail: {e}")
        bonus_detail = pd.DataFrame()

    try:
        bonus_summary = query_bonus_summary()
        log.info(f"  Bonus summary: {len(bonus_summary)} registros")
    except Exception as e:
        log.error(f"  Erro ao consultar bonus summary: {e}")
        bonus_summary = pd.DataFrame()

    # Step 3: Generate HTML
    log.info("Step 3: Gerando HTML unificado...")
    html = generate_html(csv_data, profiles, financial, timeline, bonus_detail, bonus_summary)

    # Step 4: Save
    output_path = os.path.join(OUTPUT_DIR, f"risk_unified_report_{TODAY}.html")
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    file_size = os.path.getsize(output_path)
    log.info(f"Report salvo: {output_path} ({file_size / 1024:.1f} KB)")
    log.info("=" * 60)
    log.info("CONCLUIDO — Abra o HTML no navegador para visualizar")
    log.info("=" * 60)

    return output_path


if __name__ == "__main__":
    main()
