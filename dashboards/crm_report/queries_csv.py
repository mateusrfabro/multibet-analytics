"""
Queries para Dashboard CRM Report v2 — Performance de Campanhas MultiBet
=========================================================================
Fonte: Super Nova DB (PostgreSQL, schema multibet)
Tabelas: alimentadas por pipelines/crm_report_daily_v3_agent.py
Schema: pipelines/ddl_crm_report.py (8 tabelas, prefixo crm_)

Reescrito do zero em 31/03/2026.

CONTRATO (importado por app.py):
    get_all_dashboard_data(...)      -> dict com todas as secoes do dashboard
    get_campaigns_for_export(...)    -> list[dict] para export CSV
    clear_cache()                    -> None

TABELAS CONSULTADAS:
    1. crm_campaign_daily            — principal (1 linha x campanha x dia)
    2. crm_campaign_segment_daily    — quebra por segmento/produto/ticket
    3. crm_campaign_game_daily       — top jogos por campanha
    4. crm_campaign_comparison       — antes/durante/depois
    5. crm_dispatch_budget           — orcamento de disparos
    6. crm_vip_group_daily           — analise VIP (Elite/Key/High)
    7. crm_recovery_daily            — recuperacao de inativos

SCHEMA REFERENCIA (crm_campaign_daily — DDL oficial):
    report_date, campaign_id, campaign_name, campaign_type, channel,
    segment_name, status, segmentados, msg_entregues, msg_abertos,
    msg_clicados, convertidos, apostaram, cumpriram_condicao,
    turnover_total_brl, ggr_brl, ngr_brl, net_deposit_brl,
    depositos_brl, saques_brl, turnover_casino_brl, ggr_casino_brl,
    turnover_sports_brl, ggr_sports_brl, custo_bonus_brl,
    custo_disparos_brl, custo_total_brl, roi

CUSTOS DE DISPARO (confirmados CRM 31/03/2026):
    SMS Ligue Lead:   R$ 0,047
    SMS PushFY:       R$ 0,060
    WhatsApp Loyalty: R$ 0,160
    Outros canais:    ignorar custo
"""
import sys
import os
import logging
from time import time
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))

from db.supernova import execute_supernova
from dashboards.crm_report.config import (
    CACHE_TTL_SECONDS, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE,
)

log = logging.getLogger(__name__)


# =========================================================================
# CACHE em memoria (TTL configuravel via config.py)
# =========================================================================
_cache = {}


def _cached(key, fn):
    """Executa fn() e cacheia resultado por CACHE_TTL_SECONDS."""
    now = time()
    if key in _cache:
        result, ts = _cache[key]
        if now - ts < CACHE_TTL_SECONDS:
            return result
    result = fn()
    _cache[key] = (result, now)
    return result


def clear_cache():
    """Limpa cache manualmente (chamado por POST /api/refresh)."""
    _cache.clear()
    log.info("Cache CRM limpo")


# =========================================================================
# HELPERS
# =========================================================================
def _f(val):
    """Converte para float seguro (Decimal, None, str)."""
    if val is None:
        return 0.0
    if isinstance(val, Decimal):
        return float(val)
    try:
        return float(val)
    except (TypeError, ValueError):
        return 0.0


def _i(val):
    """Converte para int seguro."""
    if val is None:
        return 0
    try:
        return int(val)
    except (TypeError, ValueError):
        return 0


def _s(val):
    """Converte para str seguro."""
    return str(val) if val is not None else ""


def _safe_div(a, b, default=0.0):
    """Divisao segura — retorna default se denominador e zero/nulo."""
    try:
        if b and float(b) != 0:
            return float(a) / float(b)
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return default


def _build_where(date_from, date_to, campaign_type=None, channel=None):
    """Constroi WHERE clause + params para filtros comuns do dashboard."""
    where = ["report_date BETWEEN %s AND %s"]
    params = [date_from, date_to]
    if campaign_type and campaign_type != "all":
        where.append("campaign_type = %s")
        params.append(campaign_type)
    if channel and channel != "all":
        where.append("channel = %s")
        params.append(channel)
    return " AND ".join(where), params


def _q(sql, params=None):
    """Executa query no Super Nova DB. Retorna rows ou lista vazia."""
    try:
        return execute_supernova(sql, params=params, fetch=True) or []
    except Exception as e:
        log.error(f"Query erro: {e}")
        return []


# =========================================================================
# 1. KPIs CONSOLIDADOS (header do dashboard)
# =========================================================================
def get_kpis(date_from, date_to, campaign_type=None, channel=None):
    """
    KPIs do header: campanhas ativas, players, GGR, ROI, custos.

    Retorna dict com:
        campanhas_ativas, players_ativos, ggr_total, ngr_total,
        turnover_total, depositos_total, saques_total, net_deposit_total,
        custo_bonus_total, custo_disparo_total, custo_crm_total,
        roi_medio, arpu
    """
    where, params = _build_where(date_from, date_to, campaign_type, channel)

    sql = f"""
    SELECT
        COUNT(DISTINCT campaign_id),
        COALESCE(SUM(segmentados), 0),
        COALESCE(SUM(apostaram), 0),
        COALESCE(SUM(ggr_brl), 0),
        COALESCE(SUM(ngr_brl), 0),
        COALESCE(SUM(turnover_total_brl), 0),
        COALESCE(SUM(depositos_brl), 0),
        COALESCE(SUM(saques_brl), 0),
        COALESCE(SUM(net_deposit_brl), 0),
        COALESCE(SUM(custo_bonus_brl), 0),
        COALESCE(SUM(custo_disparos_brl), 0)
    FROM multibet.crm_campaign_daily
    WHERE {where}
    """

    rows = _q(sql, params)
    empty = {
        "campanhas_ativas": 0, "players_ativos": 0, "ggr_total": 0,
        "ngr_total": 0, "turnover_total": 0, "depositos_total": 0,
        "saques_total": 0, "net_deposit_total": 0, "custo_bonus_total": 0,
        "custo_disparo_total": 0, "custo_crm_total": 0, "roi_medio": 0,
        "arpu": 0,
    }

    if not rows or not rows[0] or _i(rows[0][0]) == 0:
        return empty

    r = rows[0]
    custo_b = _f(r[9])
    custo_d = _f(r[10])
    custo_crm = custo_b + custo_d
    ggr = _f(r[3])
    players = _i(r[2])

    return {
        "campanhas_ativas": _i(r[0]),
        "players_ativos": players,
        "ggr_total": ggr,
        "ngr_total": _f(r[4]),
        "turnover_total": _f(r[5]),
        "depositos_total": _f(r[6]),
        "saques_total": _f(r[7]),
        "net_deposit_total": _f(r[8]),
        "custo_bonus_total": custo_b,
        "custo_disparo_total": custo_d,
        "custo_crm_total": custo_crm,
        "roi_medio": round(_safe_div(ggr, custo_crm), 1),
        "arpu": round(_safe_div(ggr, players), 2),
    }


# =========================================================================
# 2. TABELA PRINCIPAL DE CAMPANHAS (paginada, agregada por campaign_id)
# =========================================================================
# Mapa sort_by frontend -> expressao SQL no contexto do GROUP BY
_SORT_MAP = {
    "report_date":       "dias_ativa",
    "campaign_name":     "campaign_name",
    "campaign_type":     "campaign_type",
    "channel":           "channel",
    "segmentados":       "oferecidos",
    "oferecidos":        "oferecidos",
    "completaram":       "completaram",
    "cumpriram_condicao": "completaram",
    "ggr_brl":           "total_ggr",
    "total_ggr":         "total_ggr",
    "custo_bonus_brl":   "custo_bonus_brl",
    "roi":               "roi",
    "apostaram":         "coorte_users",
    "turnover_total_brl": "turnover_brl",
    "depositos_brl":     "depositos_brl",
    "net_deposit_brl":   "net_deposit",
    "status":            "status",
}


def get_campaigns(date_from, date_to, campaign_type=None, channel=None,
                  page=1, page_size=DEFAULT_PAGE_SIZE,
                  sort_by="ggr_brl", sort_dir="DESC"):
    """
    Tabela paginada de campanhas, agregada por campaign_id no periodo.
    Cada linha = 1 campanha com metricas acumuladas.

    Retorna: {data: [...], total, page, page_size, total_pages}

    Campos por campanha (alinham com dashboard.html renderTable):
        report_date (str "X dias"), campaign_id, campaign_name,
        campaign_type, channel, segment_name, status,
        oferecidos, completaram, total_ggr, custo_bonus_brl,
        depositos_brl, net_deposit, roi
    """
    page_size = min(page_size, MAX_PAGE_SIZE)
    offset = (page - 1) * page_size
    where, params = _build_where(date_from, date_to, campaign_type, channel)

    sort_col = _SORT_MAP.get(sort_by, "total_ggr")
    if sort_dir.upper() not in ("ASC", "DESC"):
        sort_dir = "DESC"

    # Total de campanhas distintas (para paginacao)
    count_sql = f"""
    SELECT COUNT(DISTINCT campaign_id)
    FROM multibet.crm_campaign_daily
    WHERE {where}
    """
    count_rows = _q(count_sql, list(params))
    total = _i(count_rows[0][0]) if count_rows else 0
    total_pages = max(1, -(-total // page_size))

    if total == 0:
        return {"data": [], "total": 0, "page": 1,
                "page_size": page_size, "total_pages": 1}

    # Dados agregados por campanha
    sql = f"""
    SELECT
        campaign_id,
        MAX(campaign_name)                              AS campaign_name,
        MAX(campaign_type)                              AS campaign_type,
        MAX(channel)                                    AS channel,
        MAX(segment_name)                               AS segment_name,
        MAX(status)                                     AS status,
        COUNT(DISTINCT report_date)                     AS dias_ativa,
        COALESCE(SUM(segmentados), 0)                   AS oferecidos,
        COALESCE(SUM(apostaram), 0)                     AS coorte_users,
        COALESCE(SUM(cumpriram_condicao), 0)            AS completaram,
        COALESCE(SUM(ggr_brl), 0)                       AS total_ggr,
        COALESCE(SUM(custo_bonus_brl), 0)               AS custo_bonus_brl,
        COALESCE(SUM(depositos_brl), 0)                 AS depositos_brl,
        COALESCE(SUM(net_deposit_brl), 0)               AS net_deposit,
        COALESCE(SUM(turnover_total_brl), 0)            AS turnover_brl,
        CASE WHEN SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)) > 0
             THEN ROUND(
                SUM(ggr_brl)::NUMERIC /
                NULLIF(SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)), 0),
             1)
             ELSE 0
        END                                             AS roi
    FROM multibet.crm_campaign_daily
    WHERE {where}
    GROUP BY campaign_id
    ORDER BY {sort_col} {sort_dir} NULLS LAST
    LIMIT %s OFFSET %s
    """
    params_data = list(params) + [page_size, offset]
    rows = _q(sql, params_data)

    data = []
    for r in rows:
        data.append({
            "report_date": f"{_i(r[6])} dias",
            "campaign_id": _s(r[0]),
            "campaign_name": _s(r[1]),
            "campaign_type": _s(r[2]),
            "channel": _s(r[3]),
            "segment_name": _s(r[4]),
            "status": _s(r[5]),
            "oferecidos": _i(r[7]),
            "completaram": _i(r[9]),
            "total_ggr": _f(r[10]),
            "custo_bonus_brl": _f(r[11]),
            "depositos_brl": _f(r[12]),
            "net_deposit": _f(r[13]),
            "roi": _f(r[15]),
        })

    return {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": total_pages,
    }


# =========================================================================
# 3. FUNIL DE CONVERSAO CRM (agregado)
# =========================================================================
def get_funnel(date_from, date_to, campaign_type=None, channel=None):
    """
    Funil de conversao agregado.

    Retorna campos para AMBOS os formatos:
    - v1 (dashboard.html atual): impactados, com_financeiro, ativados,
      monetizados + pct_*
    - CRM spec (funil completo): segmentados, entregues, abertos,
      clicados, convertidos, completaram + pct_*
    """
    where, params = _build_where(date_from, date_to, campaign_type, channel)

    sql = f"""
    SELECT
        COALESCE(SUM(segmentados), 0),
        COALESCE(SUM(msg_entregues), 0),
        COALESCE(SUM(msg_abertos), 0),
        COALESCE(SUM(msg_clicados), 0),
        COALESCE(SUM(convertidos), 0),
        COALESCE(SUM(apostaram), 0),
        COALESCE(SUM(cumpriram_condicao), 0)
    FROM multibet.crm_campaign_daily
    WHERE {where}
    """

    empty = {
        # v1 (backward compat com dashboard.html)
        "impactados": 0, "com_financeiro": 0, "ativados": 0,
        "monetizados": 0, "pct_financeiro": 0, "pct_ativados": 0,
        "pct_monetizados": 0,
        # CRM spec (funil completo)
        "segmentados": 0, "entregues": 0, "abertos": 0,
        "clicados": 0, "convertidos": 0, "completaram": 0,
        "pct_entregues": 0, "pct_abertos": 0, "pct_clicados": 0,
        "pct_convertidos": 0, "pct_completaram": 0,
    }

    rows = _q(sql, params)
    if not rows or not rows[0]:
        return empty

    r = rows[0]
    seg = _i(r[0])
    entregues = _i(r[1])
    abertos = _i(r[2])
    clicados = _i(r[3])
    convertidos = _i(r[4])
    apostaram = _i(r[5])
    completaram = _i(r[6])
    base = seg if seg > 0 else 1

    return {
        # v1 — Impactados > Com Financeiro > Ativados > Monetizados
        "impactados": seg,
        "com_financeiro": apostaram,
        "ativados": convertidos,
        "monetizados": completaram,
        "pct_financeiro": round(apostaram / base * 100, 1),
        "pct_ativados": round(convertidos / base * 100, 1),
        "pct_monetizados": round(completaram / base * 100, 1),
        # CRM spec — Segmentados > Entregues > Abertos > Clicados >
        #            Convertidos > Completaram
        "segmentados": seg,
        "entregues": entregues,
        "abertos": abertos,
        "clicados": clicados,
        "convertidos": convertidos,
        "completaram": completaram,
        "pct_entregues": round(entregues / base * 100, 1),
        "pct_abertos": round(abertos / base * 100, 1),
        "pct_clicados": round(clicados / base * 100, 1),
        "pct_convertidos": round(convertidos / base * 100, 1),
        "pct_completaram": round(completaram / base * 100, 1),
    }


# =========================================================================
# 4. FUNIL POR TIPO DE CAMPANHA (grafico de barras)
# =========================================================================
def get_funnel_by_type(date_from, date_to):
    """
    Funil agrupado por campaign_type.
    Retorna: [{campaign_type, oferecidos, completaram, campanhas, taxa_conversao}]
    """
    sql = """
    SELECT
        campaign_type,
        COALESCE(SUM(segmentados), 0)          AS oferecidos,
        COALESCE(SUM(convertidos), 0)          AS convertidos,
        COALESCE(SUM(cumpriram_condicao), 0)   AS completaram,
        COUNT(DISTINCT campaign_id)            AS campanhas
    FROM multibet.crm_campaign_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY campaign_type
    ORDER BY oferecidos DESC
    """
    rows = _q(sql, [date_from, date_to])
    return [
        {
            "campaign_type": _s(r[0]) or "Outro",
            "oferecidos": _i(r[1]),
            "completaram": _i(r[3]),
            "campanhas": _i(r[4]),
            "taxa_conversao": round(_safe_div(r[3], r[1]) * 100, 1),
        }
        for r in rows
    ]


# =========================================================================
# 5. VOLUME DIARIO (grafico de linhas)
# =========================================================================
def get_daily_volume(date_from, date_to, campaign_type=None):
    """
    Volume diario por etapa do funil.
    Retorna: [{date, segmentados/oferecidos, convertidos, completaram}]
    """
    where = ["report_date BETWEEN %s AND %s"]
    params = [date_from, date_to]
    if campaign_type and campaign_type != "all":
        where.append("campaign_type = %s")
        params.append(campaign_type)
    where_sql = " AND ".join(where)

    sql = f"""
    SELECT
        report_date,
        COALESCE(SUM(segmentados), 0),
        COALESCE(SUM(convertidos), 0),
        COALESCE(SUM(cumpriram_condicao), 0)
    FROM multibet.crm_campaign_daily
    WHERE {where_sql}
    GROUP BY report_date
    ORDER BY report_date
    """
    rows = _q(sql, params)
    return [
        {
            "date": str(r[0]),
            "segmentados": _i(r[1]),
            "oferecidos": _i(r[1]),
            "convertidos": _i(r[2]),
            "completaram": _i(r[3]),
        }
        for r in rows
    ]


# =========================================================================
# 6. TOP JOGOS — base impactada pelas campanhas CRM
# =========================================================================
def get_top_games(date_from, date_to, limit=10):
    """
    Top jogos por turnover na base impactada pelo CRM.
    Retorna: [{game_name, game_id, users, turnover_brl, ggr_brl, rtp_pct}]
    """
    sql = """
    SELECT
        game_name,
        game_id,
        COALESCE(SUM(users), 0)        AS users,
        COALESCE(SUM(turnover_brl), 0) AS turnover_brl,
        COALESCE(SUM(ggr_brl), 0)      AS ggr_brl,
        CASE WHEN SUM(turnover_brl) > 0
             THEN ROUND((1.0 - SUM(ggr_brl)::NUMERIC / SUM(turnover_brl)) * 100, 1)
             ELSE 0
        END                            AS rtp_pct
    FROM multibet.crm_campaign_game_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY game_name, game_id
    ORDER BY turnover_brl DESC
    LIMIT %s
    """
    rows = _q(sql, [date_from, date_to, limit])
    return [
        {
            "game_name": _s(r[0]) or "Desconhecido",
            "game_id": _s(r[1]),
            "users": _i(r[2]),
            "turnover_brl": _f(r[3]),
            "ggr_brl": _f(r[4]),
            "rtp_pct": _f(r[5]),
        }
        for r in rows
    ]


# =========================================================================
# 7. ANALISE VIP (Elite / Key Account / High Value)
# =========================================================================
def get_vip_analysis(date_from, date_to):
    """
    Metricas por grupo VIP.
    Retorna: [{vip_tier, users, ngr_total, ngr_medio, apd}]
    """
    sql = """
    SELECT
        vip_group,
        COALESCE(SUM(users), 0)   AS users,
        COALESCE(SUM(ngr_brl), 0) AS ngr_total,
        CASE WHEN SUM(users) > 0
             THEN ROUND(SUM(ngr_brl)::NUMERIC / SUM(users), 2)
             ELSE 0
        END                       AS ngr_medio,
        COALESCE(AVG(apd), 0)     AS apd
    FROM multibet.crm_vip_group_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY vip_group
    ORDER BY ngr_total DESC
    """
    rows = _q(sql, [date_from, date_to])
    return [
        {
            "vip_tier": _s(r[0]) or "Standard",
            "users": _i(r[1]),
            "ngr_total": _f(r[2]),
            "ngr_medio": round(_f(r[3]), 2),
            "apd": round(_f(r[4]), 1),
        }
        for r in rows
    ]


# =========================================================================
# 8. ORCAMENTO DE DISPAROS (por canal/provedor)
# =========================================================================
def get_dispatch_budget(date_from, date_to):
    """
    Orcamento de disparos agrupado por canal e provedor.

    Retorna: [{channel, provider, custo_unitario, total_sent,
               custo_total_brl, budget_monthly_brl, budget_pct_used,
               projection_eom_brl}]
    """
    sql = """
    SELECT
        channel,
        provider,
        COALESCE(AVG(cost_per_unit), 0)    AS custo_unitario,
        COALESCE(SUM(total_sent), 0)       AS total_sent,
        COALESCE(SUM(total_cost_brl), 0)   AS custo_total_brl,
        MAX(budget_monthly_brl)            AS budget_monthly_brl,
        MAX(budget_pct_used)               AS budget_pct_used,
        MAX(projection_eom_brl)            AS projection_eom_brl
    FROM multibet.crm_dispatch_budget
    WHERE month_ref BETWEEN DATE_TRUNC('month', %s::date) AND DATE_TRUNC('month', %s::date)
    GROUP BY channel, provider
    ORDER BY custo_total_brl DESC
    """
    rows = _q(sql, [date_from, date_to])
    return [
        {
            "channel": _s(r[0]) or "outro",
            "provider": _s(r[1]) or "desconhecido",
            "custo_unitario": round(_f(r[2]), 4),
            "total_sent": _i(r[3]),
            "custo_total_brl": round(_f(r[4]), 2),
            "budget_monthly_brl": _f(r[5]) if r[5] else None,
            "budget_pct_used": _f(r[6]) if r[6] else None,
            "projection_eom_brl": _f(r[7]) if r[7] else None,
        }
        for r in rows
    ]


# =========================================================================
# 9. ROI POR TIPO DE CAMPANHA (grafico)
# =========================================================================
def get_roi_by_type(date_from, date_to):
    """
    ROI e custos agrupados por tipo de campanha.
    Retorna: [{campaign_type, roi, custo_total, ggr_total, taxa_conversao}]
    """
    sql = """
    SELECT
        campaign_type,
        CASE WHEN SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)) > 0
             THEN ROUND(
                SUM(ggr_brl)::NUMERIC /
                NULLIF(SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)), 0),
             1)
             ELSE 0
        END                                                                 AS roi,
        COALESCE(SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)), 0) AS custo_total,
        COALESCE(SUM(ggr_brl), 0)                                          AS ggr_total,
        COALESCE(SUM(segmentados), 0)                                      AS oferecidos,
        COALESCE(SUM(cumpriram_condicao), 0)                               AS completaram
    FROM multibet.crm_campaign_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY campaign_type
    ORDER BY ggr_total DESC
    """
    rows = _q(sql, [date_from, date_to])
    return [
        {
            "campaign_type": _s(r[0]) or "Outro",
            "roi": _f(r[1]),
            "custo_total": _f(r[2]),
            "ggr_total": _f(r[3]),
            "taxa_conversao": round(_safe_div(r[5], r[4]) * 100, 1),
        }
        for r in rows
    ]


# =========================================================================
# 10. COMPARATIVO ANTES / DURANTE / DEPOIS
# =========================================================================
def get_comparison(campaign_type="RETEM"):
    """
    Comparativo de metricas em 3 janelas de tempo:
    - Antes: mesmo intervalo do mes anterior (baseline com sazonalidade)
    - Durante: periodo de vigencia da campanha
    - Depois: D+1 a D+3 apos encerramento

    Retorna: [{period, period_start, period_end, users, ggr, deposit,
               ngr, sessions, apd}]
    """
    sql = """
    SELECT
        period, period_start, period_end,
        users, ggr_brl, depositos_brl, ngr_brl,
        sessoes, apd
    FROM multibet.crm_campaign_comparison
    WHERE campaign_id IN (
        SELECT DISTINCT campaign_id
        FROM multibet.crm_campaign_daily
        WHERE campaign_type = %s
        LIMIT 1
    )
    ORDER BY period_start
    """
    rows = _q(sql, [campaign_type])
    return [
        {
            "period": _s(r[0]),
            "period_start": str(r[1]) if r[1] else None,
            "period_end": str(r[2]) if r[2] else None,
            "users": _i(r[3]),
            "ggr": _f(r[4]),
            "deposit": _f(r[5]),
            "ngr": _f(r[6]),
            "sessions": _i(r[7]),
            "apd": _f(r[8]),
        }
        for r in rows
    ]


# =========================================================================
# 11. RECUPERACAO DE INATIVOS
# =========================================================================
def get_recovery(date_from, date_to):
    """
    Metricas de recuperacao por canal.

    Retorna: [{channel, inativos_impactados, reengajados, depositaram,
               depositos_brl, tempo_medio, churn_d7_pct}]
    """
    sql = """
    SELECT
        channel,
        COALESCE(SUM(inativos_impactados), 0)            AS inativos_impactados,
        COALESCE(SUM(reengajados), 0)                    AS reengajados,
        COALESCE(SUM(depositaram), 0)                    AS depositaram,
        COALESCE(SUM(depositos_brl), 0)                  AS depositos_brl,
        COALESCE(AVG(tempo_medio_reengajamento_horas), 0) AS tempo_medio,
        COALESCE(AVG(churn_d7_pct), 0)                   AS churn_d7_pct
    FROM multibet.crm_recovery_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY channel
    ORDER BY reengajados DESC
    """
    rows = _q(sql, [date_from, date_to])
    return [
        {
            "channel": _s(r[0]) or "outro",
            "inativos_impactados": _i(r[1]),
            "reengajados": _i(r[2]),
            "depositaram": _i(r[3]),
            "depositos_brl": _f(r[4]),
            "tempo_medio": round(_f(r[5]), 1),
            "churn_d7_pct": round(_f(r[6]), 1),
        }
        for r in rows
    ]


# =========================================================================
# 12. SEGMENTACAO DA BASE (CRM spec secao 3 — nova secao)
# =========================================================================
def get_segmentation(date_from, date_to, campaign_type=None):
    """
    Quebra por tipo de segmento, preferencia de produto e ticket tier.

    Retorna: [{segment_type, product_preference, ticket_tier,
               users, apostaram, turnover_brl, ggr_brl, depositos_brl}]
    """
    where = ["report_date BETWEEN %s AND %s"]
    params = [date_from, date_to]
    if campaign_type and campaign_type != "all":
        where.append("""campaign_id IN (
            SELECT DISTINCT campaign_id
            FROM multibet.crm_campaign_daily
            WHERE campaign_type = %s AND report_date BETWEEN %s AND %s
        )""")
        params.extend([campaign_type, date_from, date_to])
    where_sql = " AND ".join(where)

    sql = f"""
    SELECT
        segment_type,
        product_preference,
        ticket_tier,
        COALESCE(SUM(users), 0)        AS users,
        COALESCE(SUM(apostaram), 0)    AS apostaram,
        COALESCE(SUM(turnover_brl), 0) AS turnover_brl,
        COALESCE(SUM(ggr_brl), 0)      AS ggr_brl,
        COALESCE(SUM(depositos_brl), 0) AS depositos_brl
    FROM multibet.crm_campaign_segment_daily
    WHERE {where_sql}
    GROUP BY segment_type, product_preference, ticket_tier
    ORDER BY ggr_brl DESC
    """
    rows = _q(sql, params)
    return [
        {
            "segment_type": _s(r[0]) or "Outro",
            "product_preference": _s(r[1]) or "Misto",
            "ticket_tier": _s(r[2]) or "Standard",
            "users": _i(r[3]),
            "apostaram": _i(r[4]),
            "turnover_brl": _f(r[5]),
            "ggr_brl": _f(r[6]),
            "depositos_brl": _f(r[7]),
        }
        for r in rows
    ]


# =========================================================================
# 13. FINANCEIRO POR CORTE (CRM spec secao 4 — 6 cortes)
# =========================================================================
def get_financial_cuts(date_from, date_to, campaign_type=None, channel=None):
    """
    Resultado financeiro em 6 cortes:
    1. Geral (consolidado)
    2. Casino
    3. Sportsbook
    4. Por segmento (via crm_campaign_segment_daily)
    5. Por jogo (via crm_campaign_game_daily — top 10)
    6. Por campanha (via crm_campaign_daily — top 10 por GGR)

    Retorna dict com as 6 chaves.
    """
    where, params = _build_where(date_from, date_to, campaign_type, channel)

    # Corte 1/2/3: Geral + Casino + Sportsbook (da tabela principal)
    sql_main = f"""
    SELECT
        COALESCE(SUM(turnover_total_brl), 0)   AS turnover,
        COALESCE(SUM(ggr_brl), 0)              AS ggr,
        COALESCE(SUM(ngr_brl), 0)              AS ngr,
        COALESCE(SUM(net_deposit_brl), 0)      AS net_deposit,
        COALESCE(SUM(turnover_casino_brl), 0)  AS turnover_casino,
        COALESCE(SUM(ggr_casino_brl), 0)       AS ggr_casino,
        COALESCE(SUM(turnover_sports_brl), 0)  AS turnover_sports,
        COALESCE(SUM(ggr_sports_brl), 0)       AS ggr_sports
    FROM multibet.crm_campaign_daily
    WHERE {where}
    """
    rows = _q(sql_main, params)
    if rows and rows[0]:
        r = rows[0]
        geral = {
            "turnover_brl": _f(r[0]), "ggr_brl": _f(r[1]),
            "ngr_brl": _f(r[2]), "net_deposit_brl": _f(r[3]),
            "ggr_pct": round(_safe_div(r[1], r[0]) * 100, 1),
        }
        casino = {
            "turnover_brl": _f(r[4]), "ggr_brl": _f(r[5]),
            "ggr_pct": round(_safe_div(r[5], r[4]) * 100, 1),
        }
        sports = {
            "turnover_brl": _f(r[6]), "ggr_brl": _f(r[7]),
            "ggr_pct": round(_safe_div(r[7], r[6]) * 100, 1),
        }
    else:
        geral = {"turnover_brl": 0, "ggr_brl": 0, "ngr_brl": 0,
                 "net_deposit_brl": 0, "ggr_pct": 0}
        casino = {"turnover_brl": 0, "ggr_brl": 0, "ggr_pct": 0}
        sports = {"turnover_brl": 0, "ggr_brl": 0, "ggr_pct": 0}

    return {
        "geral": geral,
        "casino": casino,
        "sportsbook": sports,
        "por_segmento": get_segmentation(date_from, date_to, campaign_type),
        "por_jogo": get_top_games(date_from, date_to, limit=10),
        "por_campanha": _top_campaigns_by_ggr(date_from, date_to, campaign_type, channel),
    }


def _top_campaigns_by_ggr(date_from, date_to, campaign_type=None,
                           channel=None, limit=10):
    """Top campanhas por GGR (corte 6 — por campanha)."""
    where, params = _build_where(date_from, date_to, campaign_type, channel)

    sql = f"""
    SELECT
        campaign_id,
        MAX(campaign_name)                         AS campaign_name,
        MAX(campaign_type)                         AS campaign_type,
        COALESCE(SUM(turnover_total_brl), 0)       AS turnover_brl,
        COALESCE(SUM(ggr_brl), 0)                  AS ggr_brl,
        COALESCE(SUM(ngr_brl), 0)                  AS ngr_brl,
        COALESCE(SUM(net_deposit_brl), 0)          AS net_deposit_brl
    FROM multibet.crm_campaign_daily
    WHERE {where}
    GROUP BY campaign_id
    ORDER BY ggr_brl DESC
    LIMIT %s
    """
    params.append(limit)
    rows = _q(sql, params)
    return [
        {
            "campaign_id": _s(r[0]),
            "campaign_name": _s(r[1]),
            "campaign_type": _s(r[2]),
            "turnover_brl": _f(r[3]),
            "ggr_brl": _f(r[4]),
            "ngr_brl": _f(r[5]),
            "net_deposit_brl": _f(r[6]),
        }
        for r in rows
    ]


# =========================================================================
# CONSOLIDADO — todos os dados do dashboard em uma unica chamada
# =========================================================================
def get_all_dashboard_data(date_from, date_to, campaign_type=None,
                           channel=None, page=1, page_size=DEFAULT_PAGE_SIZE,
                           sort_by="ggr_brl", sort_dir="DESC"):
    """
    Retorna todos os dados do dashboard em unica chamada.
    Usado pelo endpoint GET /api/data.
    Cache por TTL configuravel em config.py.
    """
    cache_key = (
        f"all_{date_from}_{date_to}_{campaign_type}_{channel}"
        f"_{page}_{page_size}_{sort_by}_{sort_dir}"
    )

    def _fetch():
        return {
            "kpis": get_kpis(date_from, date_to, campaign_type, channel),
            "campaigns": get_campaigns(
                date_from, date_to, campaign_type, channel,
                page, page_size, sort_by, sort_dir,
            ),
            "funnel": get_funnel(date_from, date_to, campaign_type, channel),
            "funnel_by_type": get_funnel_by_type(date_from, date_to),
            "daily_volume": get_daily_volume(date_from, date_to, campaign_type),
            "top_games": get_top_games(date_from, date_to),
            "vip_analysis": get_vip_analysis(date_from, date_to),
            "dispatch_budget": get_dispatch_budget(date_from, date_to),
            "roi_by_type": get_roi_by_type(date_from, date_to),
            "comparison": get_comparison(campaign_type or "RETEM"),
            "recovery": get_recovery(date_from, date_to),
            "segmentation": get_segmentation(
                date_from, date_to, campaign_type,
            ),
            "financial_cuts": get_financial_cuts(
                date_from, date_to, campaign_type, channel,
            ),
        }

    return _cached(cache_key, _fetch)


# =========================================================================
# EXPORT CSV — todas as campanhas sem paginacao (detalhe por dia)
# =========================================================================
def get_campaigns_for_export(date_from, date_to, campaign_type=None,
                              channel=None):
    """
    Retorna todas as campanhas do periodo para export CSV.
    1 linha por campanha x dia (sem agregacao, sem paginacao).

    Campos alinhados com o CSV writer em app.py:
        rule_id, rule_name, campaign_type, channel, segment_name,
        is_active, enviados, entregues, abertos, clicados, convertidos,
        cumpriram_condicao, custo_bonus_brl, coorte_users,
        casino_ggr, sportsbook_ggr, total_ggr,
        total_deposit, total_withdrawal, net_deposit,
        casino_turnover, sportsbook_turnover,
        custo_disparo_brl, roi
    """
    where, params = _build_where(date_from, date_to, campaign_type, channel)

    sql = f"""
    SELECT
        report_date,
        campaign_id,
        campaign_name,
        campaign_type,
        channel,
        segment_name,
        status,
        segmentados,
        msg_entregues,
        msg_abertos,
        msg_clicados,
        convertidos,
        cumpriram_condicao,
        custo_bonus_brl,
        apostaram,
        ggr_casino_brl,
        ggr_sports_brl,
        ggr_brl,
        depositos_brl,
        saques_brl,
        net_deposit_brl,
        turnover_casino_brl,
        turnover_sports_brl,
        custo_disparos_brl,
        roi
    FROM multibet.crm_campaign_daily
    WHERE {where}
    ORDER BY report_date DESC, ggr_brl DESC NULLS LAST
    """
    rows = _q(sql, params)
    return [
        {
            "report_date": str(r[0]) if r[0] else "",
            "rule_id": _s(r[1]),
            "rule_name": _s(r[2]),
            "campaign_type": _s(r[3]),
            "channel": _s(r[4]),
            "segment_name": _s(r[5]),
            "is_active": _s(r[6]).lower() in ("ativa", "true", "1"),
            "enviados": _i(r[7]),
            "entregues": _i(r[8]),
            "abertos": _i(r[9]),
            "clicados": _i(r[10]),
            "convertidos": _i(r[11]),
            "cumpriram_condicao": _i(r[12]),
            "custo_bonus_brl": _f(r[13]),
            "coorte_users": _i(r[14]),
            "casino_ggr": _f(r[15]),
            "sportsbook_ggr": _f(r[16]),
            "total_ggr": _f(r[17]),
            "total_deposit": _f(r[18]),
            "total_withdrawal": _f(r[19]),
            "net_deposit": _f(r[20]),
            "casino_turnover": _f(r[21]),
            "sportsbook_turnover": _f(r[22]),
            "custo_disparo_brl": _f(r[23]),
            "roi": _f(r[24]),
        }
        for r in rows
    ]
