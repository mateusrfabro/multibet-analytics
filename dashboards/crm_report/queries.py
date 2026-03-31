"""
Queries Super Nova DB para o Dashboard CRM Report.

Todas as queries consultam o schema multibet no Super Nova DB (PostgreSQL),
que e alimentado pela pipeline crm_report_daily.py (v1).

MAPEAMENTO DE COLUNAS (DDL v1 — tabela real):
  campaign_id     = ID da campanha (varchar)
  campaign_name   = Nome da campanha
  campaign_type   = Tipo (RETEM, DailyFS, Cashback, etc.)
  channel         = Canal de disparo (popup, SMS, WhatsApp, push)
  segmentados     = Total segmentados (enviados)
  msg_entregues   = Mensagens entregues
  msg_abertos     = Mensagens abertas
  msg_clicados    = Mensagens clicadas
  convertidos     = Converteram (opt-in)
  cumpriram_condicao = Completaram a condicao do bonus
  ggr_brl         = GGR total (BRL)
  ggr_casino_brl  = GGR cassino
  ggr_sports_brl  = GGR sportsbook
  turnover_total_brl = Turnover total
  turnover_casino_brl = Turnover cassino
  turnover_sports_brl = Turnover sportsbook
  depositos_brl   = Depositos
  saques_brl      = Saques
  net_deposit_brl = Net deposit
  custo_bonus_brl = Custo de bonus
  custo_disparos_brl = Custo de disparos
  roi             = ROI calculado
"""
import sys
import os
import logging
from datetime import date, timedelta
from time import time
from decimal import Decimal

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from db.supernova import execute_supernova
from dashboards.crm_report.config import CACHE_TTL_SECONDS, DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE

log = logging.getLogger(__name__)


# =========================================================================
# CACHE simples em memoria (TTL configuravel)
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
    """Limpa cache manualmente."""
    _cache.clear()
    log.info("Cache CRM limpo")


# =========================================================================
# HELPERS
# =========================================================================

def _to_float(val):
    """Converte Decimal para float."""
    if isinstance(val, Decimal):
        return float(val)
    return val


def _rows_to_dicts(rows, columns):
    """Converte lista de tuples + colunas para lista de dicts."""
    if not rows:
        return []
    return [
        {col: _to_float(val) for col, val in zip(columns, row)}
        for row in rows
    ]


def _safe_div(a, b, default=0.0):
    """Divisao segura."""
    try:
        if b and float(b) != 0:
            return float(a) / float(b)
    except (TypeError, ValueError, ZeroDivisionError):
        pass
    return default


def _build_where(date_from, date_to, campaign_type=None, channel=None):
    """Constroi WHERE clause + params para filtros do dashboard."""
    where = ["report_date BETWEEN %s AND %s"]
    params = [date_from, date_to]
    if campaign_type and campaign_type != "all":
        where.append("campaign_type = %s")
        params.append(campaign_type)
    if channel and channel != "all":
        where.append("channel = %s")
        params.append(channel)
    return " AND ".join(where), params


# =========================================================================
# QUERY 1: KPIs consolidados (header do dashboard)
# =========================================================================

def get_kpis(date_from: str, date_to: str, campaign_type: str = None, channel: str = None) -> dict:
    """Retorna KPIs consolidados para o periodo filtrado."""
    where_sql, params = _build_where(date_from, date_to, campaign_type, channel)

    sql = f"""
    SELECT
        COUNT(DISTINCT campaign_id)                   AS campanhas_ativas,
        COALESCE(SUM(segmentados), 0)                 AS usuarios_impactados,
        CASE WHEN SUM(segmentados) > 0
             THEN ROUND(SUM(convertidos)::NUMERIC / SUM(segmentados) * 100, 1)
             ELSE 0 END                               AS taxa_conversao,
        COALESCE(SUM(ggr_brl), 0)                     AS ggr_total,
        CASE WHEN SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)) > 0
             THEN ROUND(SUM(ggr_brl)::NUMERIC / NULLIF(SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)), 0), 1)
             ELSE 0 END                               AS roi_medio,
        COALESCE(SUM(turnover_total_brl), 0)          AS turnover_total,
        COALESCE(SUM(depositos_brl), 0)               AS depositos_total,
        COALESCE(SUM(custo_bonus_brl), 0)             AS custo_bonus_total,
        COALESCE(SUM(custo_disparos_brl), 0)          AS custo_disparo_total,
        COALESCE(SUM(net_deposit_brl), 0)             AS net_deposit_total
    FROM multibet.crm_campaign_daily
    WHERE {where_sql}
    """

    try:
        rows = execute_supernova(sql, params=params, fetch=True)
        if rows and rows[0]:
            r = rows[0]
            return {
                "campanhas_ativas": int(r[0] or 0),
                "usuarios_impactados": int(r[1] or 0),
                "taxa_conversao": float(r[2] or 0),
                "ggr_total": float(r[3] or 0),
                "roi_medio": float(r[4] or 0),
                "turnover_total": float(r[5] or 0),
                "depositos_total": float(r[6] or 0),
                "custo_bonus_total": float(r[7] or 0),
                "custo_disparo_total": float(r[8] or 0),
                "net_deposit_total": float(r[9] or 0),
            }
    except Exception as e:
        log.error(f"Erro get_kpis: {e}")

    return {
        "campanhas_ativas": 0, "usuarios_impactados": 0, "taxa_conversao": 0,
        "ggr_total": 0, "roi_medio": 0, "turnover_total": 0,
        "depositos_total": 0, "custo_bonus_total": 0, "custo_disparo_total": 0,
        "net_deposit_total": 0,
    }


# =========================================================================
# QUERY 2: Tabela principal de campanhas (com paginacao)
# =========================================================================

def get_campaigns(
    date_from: str, date_to: str,
    campaign_type: str = None, channel: str = None,
    page: int = 1, page_size: int = DEFAULT_PAGE_SIZE,
    sort_by: str = "ggr_brl", sort_dir: str = "DESC",
) -> dict:
    """Retorna lista paginada de campanhas com metricas."""
    page_size = min(page_size, MAX_PAGE_SIZE)
    offset = (page - 1) * page_size
    where_sql, params = _build_where(date_from, date_to, campaign_type, channel)

    # Whitelist de colunas para sort (previne SQL injection)
    allowed_sorts = {
        "report_date", "campaign_name", "campaign_type", "channel",
        "segmentados", "msg_entregues", "convertidos", "cumpriram_condicao",
        "ggr_brl", "custo_bonus_brl", "roi",
    }
    if sort_by not in allowed_sorts:
        sort_by = "ggr_brl"
    if sort_dir.upper() not in ("ASC", "DESC"):
        sort_dir = "DESC"

    count_sql = f"SELECT COUNT(*) FROM multibet.crm_campaign_daily WHERE {where_sql}"
    count_params = list(params)

    sql = f"""
    SELECT
        report_date, campaign_id, campaign_name, campaign_type, channel,
        segment_name, status,
        segmentados, msg_entregues, msg_abertos, msg_clicados, convertidos,
        cumpriram_condicao, custo_bonus_brl,
        apostaram, ggr_casino_brl, ggr_sports_brl, ggr_brl,
        depositos_brl, saques_brl, net_deposit_brl,
        turnover_casino_brl, turnover_sports_brl,
        custo_disparos_brl, roi
    FROM multibet.crm_campaign_daily
    WHERE {where_sql}
    ORDER BY {sort_by} {sort_dir} NULLS LAST
    LIMIT %s OFFSET %s
    """
    params.extend([page_size, offset])

    try:
        total_rows = execute_supernova(count_sql, params=count_params, fetch=True)
        total = int(total_rows[0][0]) if total_rows else 0

        rows = execute_supernova(sql, params=params, fetch=True)
        columns = [
            "report_date", "campaign_id", "campaign_name", "campaign_type", "channel",
            "segment_name", "status",
            "enviados", "entregues", "abertos", "clicados", "convertidos",
            "cumpriram_condicao", "custo_bonus_brl",
            "apostaram", "casino_ggr", "sportsbook_ggr", "total_ggr",
            "depositos", "saques", "net_deposit",
            "casino_turnover", "sportsbook_turnover",
            "custo_disparo_brl", "roi",
        ]
        data = _rows_to_dicts(rows, columns)

        for row in data:
            if row.get("report_date"):
                row["report_date"] = str(row["report_date"])

        return {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": max(1, -(-total // page_size)),
        }
    except Exception as e:
        log.error(f"Erro get_campaigns: {e}")
        return {"data": [], "total": 0, "page": 1, "page_size": page_size, "total_pages": 1}


# =========================================================================
# QUERY 3: Funil de conversao agregado
# =========================================================================

def get_funnel(date_from: str, date_to: str, campaign_type: str = None, channel: str = None) -> dict:
    """Retorna funil de conversao agregado."""
    where_sql, params = _build_where(date_from, date_to, campaign_type, channel)

    sql = f"""
    SELECT
        COALESCE(SUM(segmentados), 0)          AS segmentados,
        COALESCE(SUM(msg_entregues), 0)        AS entregues,
        COALESCE(SUM(msg_abertos), 0)          AS abertos,
        COALESCE(SUM(msg_clicados), 0)         AS clicados,
        COALESCE(SUM(convertidos), 0)          AS convertidos,
        COALESCE(SUM(cumpriram_condicao), 0)   AS completaram
    FROM multibet.crm_campaign_daily
    WHERE {where_sql}
    """

    try:
        rows = execute_supernova(sql, params=params, fetch=True)
        if rows and rows[0]:
            r = rows[0]
            seg = int(r[0] or 0)
            return {
                "segmentados": seg,
                "entregues": int(r[1] or 0),
                "abertos": int(r[2] or 0),
                "clicados": int(r[3] or 0),
                "convertidos": int(r[4] or 0),
                "completaram": int(r[5] or 0),
                "pct_entregues": round(_safe_div(r[1], seg) * 100, 1),
                "pct_abertos": round(_safe_div(r[2], seg) * 100, 1),
                "pct_clicados": round(_safe_div(r[3], seg) * 100, 1),
                "pct_convertidos": round(_safe_div(r[4], seg) * 100, 1),
                "pct_completaram": round(_safe_div(r[5], seg) * 100, 1),
            }
    except Exception as e:
        log.error(f"Erro get_funnel: {e}")

    return {
        "segmentados": 0, "entregues": 0, "abertos": 0, "clicados": 0,
        "convertidos": 0, "completaram": 0,
        "pct_entregues": 0, "pct_abertos": 0, "pct_clicados": 0,
        "pct_convertidos": 0, "pct_completaram": 0,
    }


# =========================================================================
# QUERY 4: Funil por tipo de campanha (para grafico de barras)
# =========================================================================

def get_funnel_by_type(date_from: str, date_to: str) -> list:
    """Retorna funil agrupado por campaign_type."""
    sql = """
    SELECT
        campaign_type,
        COALESCE(SUM(segmentados), 0)          AS segmentados,
        COALESCE(SUM(convertidos), 0)          AS convertidos,
        COALESCE(SUM(cumpriram_condicao), 0)   AS completaram
    FROM multibet.crm_campaign_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY campaign_type
    ORDER BY segmentados DESC
    """
    try:
        rows = execute_supernova(sql, params=[date_from, date_to], fetch=True)
        return [
            {
                "campaign_type": r[0] or "Outro",
                "segmentados": int(r[1] or 0),
                "convertidos": int(r[2] or 0),
                "completaram": int(r[3] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_funnel_by_type: {e}")
        return []


# =========================================================================
# QUERY 5: Volume diario (para grafico de linhas)
# =========================================================================

def get_daily_volume(date_from: str, date_to: str, campaign_type: str = None) -> list:
    """Retorna volume diario por etapa do funil."""
    where = ["report_date BETWEEN %s AND %s"]
    params = [date_from, date_to]
    if campaign_type and campaign_type != "all":
        where.append("campaign_type = %s")
        params.append(campaign_type)
    where_sql = " AND ".join(where)

    sql = f"""
    SELECT
        report_date,
        COALESCE(SUM(segmentados), 0)          AS segmentados,
        COALESCE(SUM(convertidos), 0)          AS convertidos,
        COALESCE(SUM(cumpriram_condicao), 0)   AS completaram
    FROM multibet.crm_campaign_daily
    WHERE {where_sql}
    GROUP BY report_date
    ORDER BY report_date
    """
    try:
        rows = execute_supernova(sql, params=params, fetch=True)
        return [
            {
                "date": str(r[0]),
                "segmentados": int(r[1] or 0),
                "convertidos": int(r[2] or 0),
                "completaram": int(r[3] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_daily_volume: {e}")
        return []


# =========================================================================
# QUERY 6: Top jogos da coorte CRM
# =========================================================================

def get_top_games(date_from: str, date_to: str, limit: int = 10) -> list:
    """Retorna top jogos por turnover na base impactada pelo CRM."""
    sql = """
    SELECT
        game_name, game_id,
        COALESCE(SUM(users), 0)        AS users,
        COALESCE(SUM(turnover_brl), 0) AS turnover_brl,
        COALESCE(SUM(ggr_brl), 0)      AS ggr_brl,
        CASE WHEN SUM(turnover_brl) > 0
             THEN ROUND((1 - SUM(ggr_brl) / SUM(turnover_brl)) * 100, 1)
             ELSE 0 END                AS rtp_pct
    FROM multibet.crm_campaign_game_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY game_name, game_id
    ORDER BY turnover_brl DESC
    LIMIT %s
    """
    try:
        rows = execute_supernova(sql, params=[date_from, date_to, limit], fetch=True)
        return [
            {
                "game_name": r[0] or "Desconhecido",
                "game_id": r[1],
                "users": int(r[2] or 0),
                "turnover_brl": float(r[3] or 0),
                "ggr_brl": float(r[4] or 0),
                "rtp_pct": float(r[5] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_top_games: {e}")
        return []


# =========================================================================
# QUERY 7: Analise VIP
# =========================================================================

def get_vip_analysis(date_from: str, date_to: str) -> list:
    """Retorna metricas por grupo VIP."""
    sql = """
    SELECT
        vip_group,
        COALESCE(SUM(users), 0)   AS users,
        COALESCE(SUM(ngr_brl), 0) AS ngr_total,
        CASE WHEN SUM(users) > 0
             THEN ROUND(SUM(ngr_brl) / SUM(users), 2)
             ELSE 0 END           AS ngr_medio,
        COALESCE(AVG(apd), 0)     AS apd
    FROM multibet.crm_vip_group_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY vip_group
    ORDER BY ngr_total DESC
    """
    try:
        rows = execute_supernova(sql, params=[date_from, date_to], fetch=True)
        return [
            {
                "vip_tier": r[0] or "Standard",
                "users": int(r[1] or 0),
                "ngr_total": float(r[2] or 0),
                "ngr_medio": float(r[3] or 0),
                "apd": float(r[4] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_vip_analysis: {e}")
        return []


# =========================================================================
# QUERY 8: Orcamento de disparos
# =========================================================================

def get_dispatch_budget(date_from: str, date_to: str) -> list:
    """Retorna orcamento de disparos agrupado por canal/provedor."""
    sql = """
    SELECT
        channel, provider,
        COALESCE(AVG(cost_per_unit), 0) AS custo_unitario,
        COALESCE(SUM(total_sent), 0)    AS total_sent,
        COALESCE(SUM(total_cost_brl), 0) AS custo_total_brl
    FROM multibet.crm_dispatch_budget
    WHERE month_ref BETWEEN %s AND %s
    GROUP BY channel, provider
    ORDER BY custo_total_brl DESC
    """
    try:
        rows = execute_supernova(sql, params=[date_from, date_to], fetch=True)
        return [
            {
                "channel": r[0] or "outro",
                "provider": r[1] or "desconhecido",
                "custo_unitario": float(r[2] or 0),
                "total_sent": int(r[3] or 0),
                "custo_total_brl": float(r[4] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_dispatch_budget: {e}")
        return []


# =========================================================================
# QUERY 9: Comparativo antes/durante/depois
# =========================================================================

def get_comparison(campaign_type: str = "RETEM") -> list:
    """Retorna comparativo antes/durante/depois para um tipo de campanha."""
    sql = """
    SELECT
        period, period_start, period_end,
        users, ggr_brl, depositos_brl, ngr_brl,
        sessoes, apd
    FROM multibet.crm_campaign_comparison
    WHERE campaign_id IN (
        SELECT DISTINCT campaign_id FROM multibet.crm_campaign_daily
        WHERE campaign_type = %s
        LIMIT 1
    )
    ORDER BY period_start
    """
    try:
        rows = execute_supernova(sql, params=[campaign_type], fetch=True)
        return [
            {
                "period": r[0],
                "period_start": str(r[1]) if r[1] else None,
                "period_end": str(r[2]) if r[2] else None,
                "users": int(r[3] or 0),
                "ggr": float(r[4] or 0),
                "deposit": float(r[5] or 0),
                "ngr": float(r[6] or 0),
                "sessions": int(r[7] or 0),
                "apd": float(r[8] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_comparison: {e}")
        return []


# =========================================================================
# QUERY 10: Recuperacao
# =========================================================================

def get_recovery(date_from: str, date_to: str) -> list:
    """Retorna metricas de recuperacao por canal."""
    sql = """
    SELECT
        channel,
        COALESCE(SUM(inativos_impactados), 0)   AS inativos_impactados,
        COALESCE(SUM(reengajados), 0)            AS reengajados,
        COALESCE(SUM(depositaram), 0)            AS depositaram,
        COALESCE(AVG(tempo_medio_reengajamento_horas), 0) AS tempo_medio,
        COALESCE(AVG(churn_d7_pct), 0)           AS churn_d7_pct
    FROM multibet.crm_recovery_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY channel
    ORDER BY reengajados DESC
    """
    try:
        rows = execute_supernova(sql, params=[date_from, date_to], fetch=True)
        return [
            {
                "channel": r[0] or "outro",
                "inativos_impactados": int(r[1] or 0),
                "reengajados": int(r[2] or 0),
                "depositaram": int(r[3] or 0),
                "tempo_medio": float(r[4] or 0),
                "churn_d7_pct": float(r[5] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_recovery: {e}")
        return []


# =========================================================================
# QUERY 11: ROI por tipo de campanha (para grafico)
# =========================================================================

def get_roi_by_type(date_from: str, date_to: str) -> list:
    """Retorna ROI agrupado por tipo de campanha."""
    sql = """
    SELECT
        campaign_type,
        CASE WHEN SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)) > 0
             THEN ROUND(SUM(ggr_brl)::NUMERIC / NULLIF(SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)), 0), 1)
             ELSE 0 END AS roi,
        COALESCE(SUM(COALESCE(custo_bonus_brl, 0) + COALESCE(custo_disparos_brl, 0)), 0) AS custo_total,
        COALESCE(SUM(ggr_brl), 0) AS ggr_total
    FROM multibet.crm_campaign_daily
    WHERE report_date BETWEEN %s AND %s
    GROUP BY campaign_type
    ORDER BY ggr_total DESC
    """
    try:
        rows = execute_supernova(sql, params=[date_from, date_to], fetch=True)
        return [
            {
                "campaign_type": r[0] or "Outro",
                "roi": float(r[1] or 0),
                "custo_total": float(r[2] or 0),
                "ggr_total": float(r[3] or 0),
            }
            for r in (rows or [])
        ]
    except Exception as e:
        log.error(f"Erro get_roi_by_type: {e}")
        return []


# =========================================================================
# EXPORTAR TODAS AS METRICAS (endpoint consolidado)
# =========================================================================

def get_all_dashboard_data(
    date_from: str, date_to: str,
    campaign_type: str = None, channel: str = None,
    page: int = 1, page_size: int = DEFAULT_PAGE_SIZE,
    sort_by: str = "ggr_brl", sort_dir: str = "DESC",
) -> dict:
    """Retorna todos os dados do dashboard em uma unica chamada."""
    cache_key = f"all_{date_from}_{date_to}_{campaign_type}_{channel}_{page}_{page_size}_{sort_by}_{sort_dir}"

    def _fetch():
        return {
            "kpis": get_kpis(date_from, date_to, campaign_type, channel),
            "campaigns": get_campaigns(date_from, date_to, campaign_type, channel, page, page_size, sort_by, sort_dir),
            "funnel": get_funnel(date_from, date_to, campaign_type, channel),
            "funnel_by_type": get_funnel_by_type(date_from, date_to),
            "daily_volume": get_daily_volume(date_from, date_to, campaign_type),
            "top_games": get_top_games(date_from, date_to),
            "vip_analysis": get_vip_analysis(date_from, date_to),
            "dispatch_budget": get_dispatch_budget(date_from, date_to),
            "roi_by_type": get_roi_by_type(date_from, date_to),
            "recovery": get_recovery(date_from, date_to),
        }

    return _cached(cache_key, _fetch)


# =========================================================================
# EXPORT CSV — retorna todas as campanhas sem paginacao
# =========================================================================

def get_campaigns_for_export(
    date_from: str, date_to: str,
    campaign_type: str = None, channel: str = None,
) -> list:
    """Retorna todas as campanhas sem paginacao para export CSV."""
    where_sql, params = _build_where(date_from, date_to, campaign_type, channel)

    sql = f"""
    SELECT
        report_date, campaign_id, campaign_name, campaign_type, channel,
        segment_name, status,
        segmentados, msg_entregues, msg_abertos, msg_clicados, convertidos,
        cumpriram_condicao, custo_bonus_brl,
        apostaram, ggr_casino_brl, ggr_sports_brl, ggr_brl,
        depositos_brl, saques_brl, net_deposit_brl,
        turnover_casino_brl, turnover_sports_brl,
        custo_disparos_brl, roi
    FROM multibet.crm_campaign_daily
    WHERE {where_sql}
    ORDER BY report_date DESC, ggr_brl DESC NULLS LAST
    """

    try:
        rows = execute_supernova(sql, params=params, fetch=True)
        columns = [
            "report_date", "campaign_id", "campaign_name", "campaign_type", "channel",
            "segment_name", "status",
            "enviados", "entregues", "abertos", "clicados", "convertidos",
            "cumpriram_condicao", "custo_bonus_brl",
            "apostaram", "casino_ggr", "sportsbook_ggr", "total_ggr",
            "depositos", "saques", "net_deposit",
            "casino_turnover", "sportsbook_turnover",
            "custo_disparo_brl", "roi",
        ]
        data = _rows_to_dicts(rows, columns)
        for row in data:
            if row.get("report_date"):
                row["report_date"] = str(row["report_date"])
        return data
    except Exception as e:
        log.error(f"Erro get_campaigns_for_export: {e}")
        return []
