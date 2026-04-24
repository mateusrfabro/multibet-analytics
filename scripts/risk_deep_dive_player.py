"""
Deep Dive de Jogador — Investigacao completa de perfil de risco
================================================================
Gera perfil financeiro, historico de bonus, fonte dos free spins,
timeline de atividade e classificacao VIP vs Abuser.

Uso:
    python scripts/risk_deep_dive_player.py --ecr 849167571791860514
    python scripts/risk_deep_dive_player.py --ecr 849167571791860514,789175681790911033
    python scripts/risk_deep_dive_player.py --ecr 849167571791860514 --html

Saida:
    output/risk_deep_dive_{ecr_id}_{date}.csv
    output/risk_deep_dive_{ecr_id}_{date}.html  (com --html)

Autor: Squad 3 — Intelligence Engine
Data: 2026-04-06
"""

import sys
import os
import argparse
import logging
import json
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from db.athena import query_athena

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Mapeamento de c_txn_type (validado empiricamente 03/04/2026)
TXN_TYPES = {
    1: "DEPOSIT",
    2: "WITHDRAW",
    3: "CS_MANUAL_CREDIT",
    19: "BONUS_CREDIT",
    20: "BONUS_DEBIT",
    27: "CASINO_BET",
    28: "CASINO_REBUY",
    36: "CASHOUT_REVERSAL",
    37: "BONUS_DROPPED",
    45: "CASINO_WIN",
    59: "SB_BET",
    65: "JACKPOT_WIN",
    72: "ROLLBACK",
    80: "FREESPIN_WIN",
    89: "SB_CANCEL",
    112: "SB_WIN",
}


def query_player_profile(ecr_ids: list[str]) -> pd.DataFrame:
    """Busca perfil do jogador no dim_user (colunas validadas 24/03/2026)."""
    ids_str = ",".join([str(eid) for eid in ecr_ids])
    sql = f"""
    -- Perfil do jogador (dim_user) — colunas validadas
    SELECT
        u.ecr_id,
        u.external_id,
        u.registration_date,
        u.country_code,
        u.ecr_currency,
        u.is_test,
        u.ecr_status,
        u.ftd_date,
        u.ftd_amount_inhouse,
        u.last_deposit_date,
        u.auth_last_login_time,
        u.lifetime_deposit_count,
        u.lifetime_deposit_amount_inhouse,
        u.kyc_level,
        u.tier,
        u.affiliate_id,
        -- Dias desde registro
        date_diff('day', CAST(u.registration_date AS TIMESTAMP),
                  CURRENT_TIMESTAMP) AS dias_desde_registro
    FROM ps_bi.dim_user u
    WHERE u.ecr_id IN ({ids_str})
    """
    return query_athena(sql, database="ps_bi")


def query_financial_summary(ecr_ids: list[str]) -> pd.DataFrame:
    """
    Resumo financeiro LIFETIME do jogador.
    Separa por tipo de transacao para visao completa.
    """
    ids_str = ",".join([str(eid) for eid in ecr_ids])
    sql = f"""
    -- Resumo financeiro LIFETIME por tipo de transacao
    SELECT
        f.c_ecr_id,
        -- Depositos reais
        SUM(CASE WHEN f.c_txn_type = 1 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS dep_lifetime_brl,
        COUNT_IF(f.c_txn_type = 1) AS dep_qty,
        -- Saques
        SUM(CASE WHEN f.c_txn_type = 2 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS saque_lifetime_brl,
        COUNT_IF(f.c_txn_type = 2) AS saque_qty,
        -- Casino bets
        SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bets_casino_brl,
        COUNT_IF(f.c_txn_type = 27) AS bets_casino_qty,
        -- Casino wins (real)
        SUM(CASE WHEN f.c_txn_type = 45 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS wins_casino_brl,
        COUNT_IF(f.c_txn_type = 45) AS wins_casino_qty,
        -- Free Spin wins
        SUM(CASE WHEN f.c_txn_type = 80 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS freespin_wins_brl,
        COUNT_IF(f.c_txn_type = 80) AS freespin_wins_qty,
        -- Jackpot wins
        SUM(CASE WHEN f.c_txn_type = 65 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS jackpot_wins_brl,
        COUNT_IF(f.c_txn_type = 65) AS jackpot_wins_qty,
        -- SB bets + wins
        SUM(CASE WHEN f.c_txn_type = 59 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS sb_bets_brl,
        SUM(CASE WHEN f.c_txn_type = 112 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS sb_wins_brl,
        -- Rollbacks
        SUM(CASE WHEN f.c_txn_type = 72 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS rollbacks_brl,
        COUNT_IF(f.c_txn_type = 72) AS rollbacks_qty,
        -- Credito manual CS
        SUM(CASE WHEN f.c_txn_type = 3 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS cs_credit_brl,
        COUNT_IF(f.c_txn_type = 3) AS cs_credit_qty,
        -- Bonus credit/debit
        SUM(CASE WHEN f.c_txn_type = 19 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bonus_credit_brl,
        SUM(CASE WHEN f.c_txn_type = 20 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bonus_debit_brl,
        -- Estorno saque
        SUM(CASE WHEN f.c_txn_type = 36 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS cashout_reversal_brl,
        COUNT_IF(f.c_txn_type = 36) AS cashout_reversal_qty,
        -- Revenue windows
        SUM(CASE WHEN f.c_txn_type = 27 AND f.c_start_time >= date_add('day', -7, CURRENT_TIMESTAMP)
                 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bets_7d_brl,
        SUM(CASE WHEN f.c_txn_type IN (45, 80, 65) AND f.c_start_time >= date_add('day', -7, CURRENT_TIMESTAMP)
                 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS wins_7d_brl,
        SUM(CASE WHEN f.c_txn_type = 27 AND f.c_start_time >= date_add('day', -30, CURRENT_TIMESTAMP)
                 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bets_30d_brl,
        SUM(CASE WHEN f.c_txn_type IN (45, 80, 65) AND f.c_start_time >= date_add('day', -30, CURRENT_TIMESTAMP)
                 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS wins_30d_brl,
        -- Primeiro e ultimo deposito via fund
        MIN(CASE WHEN f.c_txn_type = 1 THEN f.c_start_time END) AS first_deposit_time,
        MAX(CASE WHEN f.c_txn_type = 1 THEN f.c_start_time END) AS last_deposit_time,
        -- Primeira e ultima atividade
        MIN(f.c_start_time) AS first_activity,
        MAX(f.c_start_time) AS last_activity
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_ecr_id IN ({ids_str})
      AND f.c_txn_status = 'SUCCESS'
    GROUP BY f.c_ecr_id
    """
    return query_athena(sql, database="fund_ec2")


def query_bonus_history(ecr_ids: list[str]) -> pd.DataFrame:
    """
    Historico detalhado de bonus do jogador.
    Fonte: bonus_ec2.tbl_bonus_summary_details
    Colunas validadas: c_total_bonus_issued, c_freespin_win, c_bonus_status (06/04/2026)
    """
    ids_str = ",".join([str(eid) for eid in ecr_ids])
    sql = f"""
    -- Historico de bonus por jogador
    -- Fonte: bonus_ec2.tbl_bonus_summary_details
    SELECT
        bs.c_ecr_id,
        COUNT(*) AS total_bonus_recebidos,
        -- Por status
        COUNT_IF(bs.c_bonus_status = 'BONUS_ISSUED_OFFER') AS bonus_issued,
        COUNT_IF(bs.c_bonus_status = 'ACTIVE') AS bonus_ativos,
        COUNT_IF(bs.c_bonus_status = 'COMPLETED') AS bonus_completados,
        COUNT_IF(bs.c_bonus_status = 'EXPIRED') AS bonus_expirados,
        -- Valores (centavos) — c_actual_issued_amount validado 06/04
        SUM(COALESCE(bs.c_actual_issued_amount, 0)) AS total_bonus_issued_centavos,
        -- Freespin wins (centavos)
        SUM(COALESCE(bs.c_freespin_win, 0)) AS total_freespin_win_centavos,
        -- Bonus distintos
        COUNT(DISTINCT bs.c_bonus_id) AS bonus_ids_distintos,
        -- Pct conversao (issued > 0 vs total)
        ROUND(100.0 * COUNT_IF(bs.c_total_bonus_issued > 0) / NULLIF(COUNT(*), 0), 1) AS pct_conversao,
        -- Datas
        MIN(bs.c_issue_date) AS primeiro_bonus,
        MAX(bs.c_issue_date) AS ultimo_bonus,
        -- Dias distintos com bonus
        COUNT(DISTINCT date_trunc('day', bs.c_issue_date)) AS dias_com_bonus,
        -- Ultimos 7 dias
        COUNT_IF(bs.c_issue_date >= date_add('day', -7, CURRENT_TIMESTAMP)) AS bonus_7d,
        -- Ultimos 30 dias
        COUNT_IF(bs.c_issue_date >= date_add('day', -30, CURRENT_TIMESTAMP)) AS bonus_30d
    FROM bonus_ec2.tbl_bonus_summary_details bs
    WHERE bs.c_ecr_id IN ({ids_str})
    GROUP BY bs.c_ecr_id
    """
    return query_athena(sql, database="bonus_ec2")


def query_bonus_detail(ecr_ids: list[str]) -> pd.DataFrame:
    """
    Detalhe dos bonus individuais — para rastrear FONTE dos free spins.
    Traz os ultimos 100 bonus de cada jogador.
    Colunas validadas 06/04/2026.
    """
    ids_str = ",".join([str(eid) for eid in ecr_ids])
    sql = f"""
    -- Detalhe dos bonus individuais (ultimos 100)
    -- Objetivo: rastrear de onde vem os free spins
    SELECT
        bs.c_ecr_id,
        bs.c_bonus_id,
        bs.c_label_id,
        bs.c_bonus_status,
        bs.c_issue_date AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS issue_date_brt,
        COALESCE(bs.c_total_bonus_issued, 0) AS bonus_issued_centavos,
        COALESCE(bs.c_freespin_win, 0) AS freespin_win_centavos,
        bs.c_comments
    FROM bonus_ec2.tbl_bonus_summary_details bs
    WHERE bs.c_ecr_id IN ({ids_str})
    ORDER BY bs.c_issue_date DESC
    LIMIT 100
    """
    return query_athena(sql, database="bonus_ec2")


def query_bonus_config(bonus_ids: list) -> pd.DataFrame:
    """
    Busca configuracao de bonus (pre_offer) para verificar flag exclude_abusers.
    Fonte: bonus_ec2.tbl_bonus_pre_offer
    Colunas validadas 06/04/2026.
    """
    # tbl_bonus_pre_offer usa c_pre_offer_id, nao bonus_id
    # Trazer config geral dos bonus ativos
    sql = """
    -- Config de bonus pre-offer — verificar flag exclude_abusers
    SELECT
        c_pre_offer_id,
        c_product_id,
        c_exclude_bonus_abusers,
        c_max_bonus_amount,
        c_is_active,
        c_amount_type,
        c_abs_amount,
        c_campaign_start_date,
        c_campaign_end_date
    FROM bonus_ec2.tbl_bonus_pre_offer
    WHERE c_is_active = true
    ORDER BY c_campaign_start_date DESC
    LIMIT 20
    """
    return query_athena(sql, database="bonus_ec2")


def query_daily_activity(ecr_ids: list[str], days: int = 30) -> pd.DataFrame:
    """Timeline de atividade diaria (ultimos N dias)."""
    ids_str = ",".join([str(eid) for eid in ecr_ids])
    sql = f"""
    -- Timeline de atividade diaria
    SELECT
        f.c_ecr_id,
        date_trunc('day', f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo') AS dia_brt,
        -- Depositos
        SUM(CASE WHEN f.c_txn_type = 1 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS dep_brl,
        COUNT_IF(f.c_txn_type = 1) AS dep_qty,
        -- Saques
        SUM(CASE WHEN f.c_txn_type = 2 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS saque_brl,
        -- Bets
        SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS bets_brl,
        COUNT_IF(f.c_txn_type = 27) AS bets_qty,
        -- Wins (real + freespin)
        SUM(CASE WHEN f.c_txn_type IN (45, 80, 65) THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS wins_brl,
        -- Freespin wins separado
        SUM(CASE WHEN f.c_txn_type = 80 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS freespin_brl,
        COUNT_IF(f.c_txn_type = 80) AS freespin_qty,
        -- GGR do dia (bets - wins, perspectiva casa)
        SUM(CASE WHEN f.c_txn_type = 27 THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END)
        - SUM(CASE WHEN f.c_txn_type IN (45, 80, 65) THEN f.c_amount_in_ecr_ccy / 100.0 ELSE 0 END) AS ggr_dia_brl
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_ecr_id IN ({ids_str})
      AND f.c_start_time >= date_add('day', -{days}, CURRENT_TIMESTAMP)
      AND f.c_txn_status = 'SUCCESS'
    GROUP BY f.c_ecr_id, date_trunc('day', f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo')
    ORDER BY dia_brt
    """
    return query_athena(sql, database="fund_ec2")


def classify_player(fin: pd.Series, bonus: pd.Series) -> dict:
    """
    Classifica jogador como VIP, Abuser, ou Normal.
    Retorna dict com classificacao e justificativa.
    """
    signals = []
    score_vip = 0
    score_abuser = 0

    dep = fin.get("dep_lifetime_brl", 0) or 0
    saque = fin.get("saque_lifetime_brl", 0) or 0
    bets = fin.get("bets_casino_brl", 0) or 0
    wins_total = (fin.get("wins_casino_brl", 0) or 0) + (fin.get("freespin_wins_brl", 0) or 0)
    freespin_wins = fin.get("freespin_wins_brl", 0) or 0
    bonus_issued = (bonus.get("total_bonus_issued_centavos", 0) or 0) / 100.0
    bonus_conv = bonus.get("pct_conversao", 0) or 0
    ggr_lifetime = bets - wins_total  # positivo = casa ganha
    revenue_7d = (fin.get("bets_7d_brl", 0) or 0) - (fin.get("wins_7d_brl", 0) or 0)
    revenue_30d = (fin.get("bets_30d_brl", 0) or 0) - (fin.get("wins_30d_brl", 0) or 0)

    # --- Sinais de VIP ---
    if dep > 50000:
        score_vip += 30
        signals.append(f"Depositos altos: R${dep:,.0f}")
    if bets > 500000:
        score_vip += 20
        signals.append(f"Volume alto de bets: R${bets:,.0f}")
    if ggr_lifetime > 10000:
        score_vip += 25
        signals.append(f"GGR positivo para casa: R${ggr_lifetime:,.0f}")

    # --- Sinais de ABUSER ---
    if bonus_issued > 0 and dep > 0 and bonus_issued / dep > 0.5:
        score_abuser += 25
        signals.append(f"Bonus/Deposito ratio alto: {bonus_issued/dep:.1f}x")
    elif bonus_issued > 0 and dep == 0:
        score_abuser += 35
        signals.append("Bonus alto com ZERO depositos")

    if freespin_wins > 0 and dep > 0 and freespin_wins / dep > 1.0:
        score_abuser += 20
        signals.append(f"Freespin wins > depositos: R${freespin_wins:,.0f} vs R${dep:,.0f}")

    if revenue_30d < -10000:
        score_abuser += 30
        signals.append(f"Revenue 30d muito negativo: R${revenue_30d:,.0f}")
    elif revenue_30d < -1000:
        score_abuser += 15
        signals.append(f"Revenue 30d negativo: R${revenue_30d:,.0f}")

    if bonus_conv >= 90 and (bonus.get("total_bonus_recebidos", 0) or 0) > 10:
        score_abuser += 15
        signals.append(f"Conversao de bonus {bonus_conv:.0f}% com {int(bonus.get('total_bonus_recebidos', 0))} bonus")

    if revenue_7d < -50000:
        score_abuser += 20
        signals.append(f"Revenue 7d catastrofico: R${revenue_7d:,.0f}")

    # --- Classificacao final ---
    if score_abuser >= 50:
        classification = "ABUSER"
        action = "Bloquear bonus imediatamente, revisar campanha CRM, avaliar bloqueio de conta"
    elif score_abuser >= 30 and score_vip < 30:
        classification = "SUSPECT"
        action = "Suspender bonus, investigar manualmente, monitorar 7 dias"
    elif score_vip >= 40 and score_abuser < 20:
        classification = "VIP"
        action = "Manter beneficios, acompanhar com account manager"
    elif score_vip >= 30 and score_abuser >= 20:
        classification = "VIP_AT_RISK"
        action = "Jogador relevante mas com sinais de abuso — revisar limites de bonus"
    else:
        classification = "NORMAL"
        action = "Monitorar normalmente"

    return {
        "classification": classification,
        "score_vip": score_vip,
        "score_abuser": score_abuser,
        "signals": signals,
        "action": action,
        "ggr_lifetime": ggr_lifetime,
        "revenue_7d": revenue_7d,
        "revenue_30d": revenue_30d,
    }


def fmt_brl(val):
    """Formata valor em BRL."""
    if pd.isna(val) or val is None:
        return "R$ 0"
    val = float(val)
    if val < 0:
        return f"-R$ {abs(val):,.2f}"
    return f"R$ {val:,.2f}"


def generate_html_report(players_data: list[dict]) -> str:
    """Gera HTML report de deep dive."""

    cards_html = ""
    for p in players_data:
        fin = p["financial"]
        bonus = p["bonus"]
        profile = p["profile"]
        cls = p["classification"]
        timeline = p["timeline"]
        bonus_detail = p["bonus_detail"]
        templates = p["templates"]

        ecr_id = p["ecr_id"]

        # Badge de classificacao
        badge_colors = {
            "ABUSER": "#e74c3c",
            "SUSPECT": "#e67e22",
            "VIP": "#2ecc71",
            "VIP_AT_RISK": "#f39c12",
            "NORMAL": "#95a5a6",
        }
        badge_color = badge_colors.get(cls["classification"], "#95a5a6")

        # Dados financeiros
        dep = fin.get("dep_lifetime_brl", 0) or 0
        saque = fin.get("saque_lifetime_brl", 0) or 0
        bets = fin.get("bets_casino_brl", 0) or 0
        wins = (fin.get("wins_casino_brl", 0) or 0) + (fin.get("freespin_wins_brl", 0) or 0)
        fs_wins = fin.get("freespin_wins_brl", 0) or 0
        bonus_total = (bonus.get("total_bonus_issued_centavos", 0) or 0) / 100.0

        # Timeline chart data
        tl_labels = []
        tl_ggr = []
        tl_deps = []
        tl_fs = []
        if not timeline.empty:
            tl_player = timeline[timeline["c_ecr_id"] == ecr_id] if "c_ecr_id" in timeline.columns else timeline
            for _, row in tl_player.iterrows():
                tl_labels.append(str(row.get("dia_brt", ""))[:10])
                tl_ggr.append(round(float(row.get("ggr_dia_brl", 0) or 0), 2))
                tl_deps.append(round(float(row.get("dep_brl", 0) or 0), 2))
                tl_fs.append(round(float(row.get("freespin_brl", 0) or 0), 2))

        # Bonus detail table
        bonus_rows = ""
        if not bonus_detail.empty:
            bd_player = bonus_detail[bonus_detail["c_ecr_id"] == ecr_id] if "c_ecr_id" in bonus_detail.columns else bonus_detail
            for _, row in bd_player.head(20).iterrows():
                fs_win = (row.get('freespin_win_centavos', 0) or 0) / 100.0
                fs_color = "color:#e67e22" if fs_win > 0 else ""
                bonus_rows += f"""<tr>
                    <td>{str(row.get('issue_date_brt', ''))[:16]}</td>
                    <td>{row.get('c_bonus_id', 'N/A')}</td>
                    <td>{fmt_brl((row.get('bonus_issued_centavos', 0) or 0) / 100.0)}</td>
                    <td style="{fs_color}">{fmt_brl(fs_win)}</td>
                    <td>{row.get('c_bonus_status', 'N/A')}</td>
                    <td style="font-size:11px;max-width:200px;overflow:hidden;text-overflow:ellipsis">{row.get('c_comments', 'N/A')}</td>
                </tr>"""

        # Template info (bonus pre_offer config)
        template_info = ""
        if not templates.empty:
            for _, row in templates.iterrows():
                exclude_flag = row.get("c_exclude_bonus_abusers", "N/A")
                flag_class = "color:#e74c3c;font-weight:bold" if str(exclude_flag).lower() == "false" else "color:#2ecc71"
                template_info += f"""<div style="background:#2c3e50;padding:8px 12px;border-radius:4px;margin:4px 0;font-size:13px">
                    Pre-Offer <b>{row.get('c_pre_offer_id', '?')}</b> |
                    Produto: {row.get('c_product_id', '?')} |
                    Max: {fmt_brl((row.get('c_max_bonus_amount', 0) or 0) / 100.0)} |
                    Exclude Abusers: <span style="{flag_class}">{exclude_flag}</span> |
                    Ativo: {row.get('c_is_active', '?')}
                </div>"""

        # Sinais de classificacao
        signals_html = "".join([f'<li style="margin:3px 0">{s}</li>' for s in cls["signals"]])

        # Registro info
        if not profile.empty and "registration_date" in profile.columns:
            reg_date = profile.iloc[0]["registration_date"]
        else:
            reg_date = "N/A"

        cards_html += f"""
        <div class="player-card">
            <div class="player-header">
                <div>
                    <h2>ECR: {ecr_id}</h2>
                    <p style="color:#aaa;margin:0">Registro: {reg_date}</p>
                </div>
                <span class="badge" style="background:{badge_color}">{cls['classification']}</span>
            </div>

            <div class="kpi-row">
                <div class="kpi"><div class="kpi-label">Depositos Lifetime</div><div class="kpi-value">{fmt_brl(dep)}</div><div class="kpi-sub">{int(fin.get('dep_qty', 0) or 0)} depositos</div></div>
                <div class="kpi"><div class="kpi-label">Saques Lifetime</div><div class="kpi-value">{fmt_brl(saque)}</div><div class="kpi-sub">{int(fin.get('saque_qty', 0) or 0)} saques</div></div>
                <div class="kpi"><div class="kpi-label">Bonus Emitidos</div><div class="kpi-value" style="color:#e67e22">{fmt_brl(bonus_total)}</div><div class="kpi-sub">{int(bonus.get('total_bonus_recebidos', 0) or 0)} bonus ({bonus.get('pct_conversao', 0):.0f}% conv)</div></div>
                <div class="kpi"><div class="kpi-label">Free Spin Wins</div><div class="kpi-value" style="color:#e74c3c">{fmt_brl(fs_wins)}</div><div class="kpi-sub">{int(fin.get('freespin_wins_qty', 0) or 0)} wins de FS</div></div>
            </div>

            <div class="kpi-row">
                <div class="kpi"><div class="kpi-label">GGR Lifetime</div><div class="kpi-value" style="color:{'#2ecc71' if cls['ggr_lifetime'] > 0 else '#e74c3c'}">{fmt_brl(cls['ggr_lifetime'])}</div><div class="kpi-sub">bets - wins (perspectiva casa)</div></div>
                <div class="kpi"><div class="kpi-label">Revenue 7d</div><div class="kpi-value" style="color:{'#2ecc71' if cls['revenue_7d'] > 0 else '#e74c3c'}">{fmt_brl(cls['revenue_7d'])}</div></div>
                <div class="kpi"><div class="kpi-label">Revenue 30d</div><div class="kpi-value" style="color:{'#2ecc71' if cls['revenue_30d'] > 0 else '#e74c3c'}">{fmt_brl(cls['revenue_30d'])}</div></div>
                <div class="kpi"><div class="kpi-label">Score Abuser</div><div class="kpi-value" style="color:{'#e74c3c' if cls['score_abuser'] >= 50 else '#f39c12' if cls['score_abuser'] >= 30 else '#2ecc71'}">{cls['score_abuser']}</div><div class="kpi-sub">VIP: {cls['score_vip']}</div></div>
            </div>

            <div class="section">
                <h3>Classificacao: {cls['classification']}</h3>
                <ul style="margin:5px 0">{signals_html}</ul>
                <div class="action-box"><b>Acao sugerida:</b> {cls['action']}</div>
            </div>

            <div class="section">
                <h3>Timeline 30 dias (GGR diario)</h3>
                <canvas id="chart_{ecr_id}" height="200"></canvas>
                <script>
                new Chart(document.getElementById('chart_{ecr_id}'), {{
                    type: 'bar',
                    data: {{
                        labels: {json.dumps(tl_labels)},
                        datasets: [
                            {{label: 'GGR (casa)', data: {json.dumps(tl_ggr)}, backgroundColor: {json.dumps(tl_ggr)}.map(v => v >= 0 ? 'rgba(46,204,113,0.7)' : 'rgba(231,76,60,0.7)'), borderWidth: 0}},
                            {{label: 'Free Spin Wins', data: {json.dumps(tl_fs)}, type: 'line', borderColor: '#e67e22', backgroundColor: 'transparent', pointRadius: 2, borderWidth: 2}}
                        ]
                    }},
                    options: {{
                        responsive: true,
                        plugins: {{legend: {{labels: {{color: '#ccc'}}}}}},
                        scales: {{
                            x: {{ticks: {{color: '#aaa', maxRotation: 45}}}},
                            y: {{ticks: {{color: '#aaa', callback: v => 'R$ ' + v.toLocaleString()}}}}
                        }}
                    }}
                }});
                </script>
            </div>

            <div class="section">
                <h3>Ultimos 20 Bonus (fonte dos free spins)</h3>
                <div style="overflow-x:auto">
                <table>
                    <thead><tr><th>Data</th><th>Bonus ID</th><th>Valor Issued</th><th>FS Win</th><th>Status</th><th>Comentario</th></tr></thead>
                    <tbody>{bonus_rows if bonus_rows else '<tr><td colspan="6" style="text-align:center">Sem bonus registrados</td></tr>'}</tbody>
                </table>
                </div>
            </div>

            <div class="section">
                <h3>Templates de Bonus (config da campanha)</h3>
                {template_info if template_info else '<p style="color:#aaa">Nenhum template encontrado</p>'}
                <p style="color:#e67e22;font-size:12px;margin-top:8px">
                    <b>ATENCAO:</b> Se "Exclude Abusers" = <span style="color:#e74c3c">false</span>,
                    a campanha NAO filtra abusers — possivel falha de configuracao.
                </p>
            </div>
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Deep Dive — Investigacao de Jogadores | MultiBet Risk</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
<style>
* {{ margin: 0; padding: 0; box-sizing: border-box; }}
body {{ font-family: 'Segoe UI', system-ui, sans-serif; background: #0f1923; color: #e0e0e0; padding: 20px; }}
h1 {{ color: #fff; margin-bottom: 5px; }}
.subtitle {{ color: #aaa; margin-bottom: 30px; }}
.player-card {{ background: #1a2332; border-radius: 12px; padding: 24px; margin-bottom: 30px; border: 1px solid #2c3e50; }}
.player-header {{ display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; border-bottom: 1px solid #2c3e50; padding-bottom: 15px; }}
.player-header h2 {{ color: #fff; font-size: 18px; }}
.badge {{ padding: 6px 16px; border-radius: 20px; font-weight: bold; font-size: 14px; color: #fff; }}
.kpi-row {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-bottom: 16px; }}
.kpi {{ background: #0f1923; border-radius: 8px; padding: 14px; text-align: center; border: 1px solid #2c3e50; }}
.kpi-label {{ font-size: 11px; color: #888; text-transform: uppercase; letter-spacing: 0.5px; }}
.kpi-value {{ font-size: 20px; font-weight: bold; margin: 4px 0; }}
.kpi-sub {{ font-size: 11px; color: #666; }}
.section {{ margin-top: 20px; padding-top: 16px; border-top: 1px solid #2c3e50; }}
.section h3 {{ color: #fff; font-size: 14px; margin-bottom: 10px; }}
.action-box {{ background: #2c3e50; padding: 10px 14px; border-radius: 6px; margin-top: 10px; font-size: 13px; border-left: 4px solid #e67e22; }}
table {{ width: 100%; border-collapse: collapse; font-size: 12px; }}
th {{ background: #2c3e50; color: #fff; padding: 8px 10px; text-align: left; }}
td {{ padding: 6px 10px; border-bottom: 1px solid #2c3e50; }}
tr:hover {{ background: #1e2d3d; }}
.footer {{ text-align: center; color: #555; font-size: 11px; margin-top: 40px; padding-top: 20px; border-top: 1px solid #2c3e50; }}
</style>
</head>
<body>
<h1>Deep Dive — Investigacao de Jogadores</h1>
<p class="subtitle">MultiBet Risk Agent v1.3 | Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')} | Squad 3 — Intelligence Engine</p>
{cards_html}
<div class="footer">
    <p>MultiBet Risk Intelligence | Dados: Athena (fund_ec2, bonus_ec2, ps_bi) | Timezone: BRT</p>
    <p>Classificacao automatica — requer validacao manual antes de acao.</p>
</div>
</body>
</html>"""
    return html


def deep_dive(ecr_ids: list[str], generate_html: bool = True) -> list[dict]:
    """Executa deep dive completo em uma lista de ECR IDs."""

    log.info(f"Deep Dive: {len(ecr_ids)} jogador(es): {ecr_ids}")

    # 1. Perfil
    log.info("Buscando perfil (dim_user)...")
    df_profile = query_player_profile(ecr_ids)

    # 2. Financeiro lifetime
    log.info("Buscando resumo financeiro LIFETIME...")
    df_financial = query_financial_summary(ecr_ids)

    # 3. Bonus historico (agregado)
    log.info("Buscando historico de bonus...")
    df_bonus = query_bonus_history(ecr_ids)

    # 4. Bonus detalhe (individuais)
    log.info("Buscando detalhe de bonus individuais...")
    df_bonus_detail = query_bonus_detail(ecr_ids)

    # 5. Config de bonus (pre_offer — exclude_abusers flag)
    log.info("Buscando config de bonus (pre_offer)...")
    df_templates = query_bonus_config([])

    # 6. Timeline diaria (30d)
    log.info("Buscando timeline 30 dias...")
    df_timeline = query_daily_activity(ecr_ids, days=30)

    # 7. Montar resultado por jogador
    players_data = []
    for ecr_id in ecr_ids:
        ecr_id_val = int(ecr_id) if str(ecr_id).isdigit() else ecr_id

        # Filtrar dados deste jogador
        profile = df_profile[df_profile["ecr_id"] == ecr_id_val] if not df_profile.empty else pd.DataFrame()
        fin_row = df_financial[df_financial["c_ecr_id"] == ecr_id_val].iloc[0] if not df_financial.empty and ecr_id_val in df_financial["c_ecr_id"].values else pd.Series()
        bonus_row = df_bonus[df_bonus["c_ecr_id"] == ecr_id_val].iloc[0] if not df_bonus.empty and ecr_id_val in df_bonus["c_ecr_id"].values else pd.Series()

        # Classificar
        cls = classify_player(fin_row, bonus_row)

        log.info(f"  ECR {ecr_id}: {cls['classification']} (VIP={cls['score_vip']}, ABUSER={cls['score_abuser']})")
        for sig in cls["signals"]:
            log.info(f"    - {sig}")

        players_data.append({
            "ecr_id": ecr_id,
            "profile": profile,
            "financial": fin_row,
            "bonus": bonus_row,
            "classification": cls,
            "timeline": df_timeline,
            "bonus_detail": df_bonus_detail,
            "templates": df_templates,
        })

    # 8. Gerar outputs
    today = datetime.now().strftime("%Y-%m-%d")
    ids_label = "_".join([str(eid)[-6:] for eid in ecr_ids])

    # CSV resumo
    csv_rows = []
    for p in players_data:
        cls = p["classification"]
        fin = p["financial"]
        bonus = p["bonus"]
        csv_rows.append({
            "ecr_id": p["ecr_id"],
            "classification": cls["classification"],
            "score_vip": cls["score_vip"],
            "score_abuser": cls["score_abuser"],
            "dep_lifetime_brl": fin.get("dep_lifetime_brl", 0),
            "saque_lifetime_brl": fin.get("saque_lifetime_brl", 0),
            "bets_casino_brl": fin.get("bets_casino_brl", 0),
            "freespin_wins_brl": fin.get("freespin_wins_brl", 0),
            "bonus_emitidos_brl": (bonus.get("total_bonus_issued_centavos", 0) or 0) / 100.0,
            "ggr_lifetime": cls["ggr_lifetime"],
            "revenue_7d": cls["revenue_7d"],
            "revenue_30d": cls["revenue_30d"],
            "total_bonus": bonus.get("total_bonus_recebidos", 0),
            "pct_conversao_bonus": bonus.get("pct_conversao", 0),
            "action": cls["action"],
            "signals": " | ".join(cls["signals"]),
        })
    df_csv = pd.DataFrame(csv_rows)
    csv_path = os.path.join(OUTPUT_DIR, f"risk_deep_dive_{ids_label}_{today}.csv")
    df_csv.to_csv(csv_path, index=False)
    log.info(f"CSV salvo: {csv_path}")

    # HTML
    if generate_html:
        html = generate_html_report(players_data)
        html_path = os.path.join(OUTPUT_DIR, f"risk_deep_dive_{ids_label}_{today}.html")
        with open(html_path, "w", encoding="utf-8") as f:
            f.write(html)
        log.info(f"HTML salvo: {html_path}")

    return players_data


def main():
    parser = argparse.ArgumentParser(description="Deep Dive — Investigacao completa de jogador")
    parser.add_argument("--ecr", type=str, required=True, help="ECR ID(s), separados por virgula")
    parser.add_argument("--html", action="store_true", default=True, help="Gerar HTML report (default: true)")
    parser.add_argument("--no-html", action="store_true", help="Nao gerar HTML")
    args = parser.parse_args()

    ecr_ids = [eid.strip() for eid in args.ecr.split(",")]
    generate_html = not args.no_html

    deep_dive(ecr_ids, generate_html=generate_html)


if __name__ == "__main__":
    main()
