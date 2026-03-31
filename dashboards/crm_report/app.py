"""
Dashboard CRM Report — Performance de Campanhas MultiBet

Produto self-service para acompanhamento de campanhas CRM.
Dados alimentados pela pipeline crm_report_daily_v3_agent.py
e persistidos no Super Nova DB (schema multibet).

Uso:
    python dashboards/crm_report/app.py

Acesso:
    http://localhost:5051  (local)

Padrao: mesmo do dashboard google_ads (Flask + API JSON + template HTML).
"""
import csv
import io
import os
import sys
import logging
from datetime import datetime, date, timedelta

from flask import (
    Flask, render_template, jsonify, request,
    redirect, url_for, session, Response,
)
from functools import wraps

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from dashboards.crm_report.config import (
    FLASK_HOST, FLASK_PORT, FLASK_DEBUG, SECRET_KEY,
    DASHBOARD_USER, DASHBOARD_PASS, RATE_LIMIT,
    CAMPAIGN_TYPES, CHANNELS, DEFAULT_PAGE_SIZE,
)
from dashboards.crm_report.queries_csv import (
    get_all_dashboard_data, get_campaigns_for_export, clear_cache,
)

# =========================================================================
# SETUP
# =========================================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger(__name__)

app = Flask(
    __name__,
    template_folder="templates",
    static_folder="static",
)
app.secret_key = SECRET_KEY

# Rate limiting (opcional)
try:
    from flask_limiter import Limiter
    from flask_limiter.util import get_remote_address
    limiter = Limiter(
        get_remote_address,
        app=app,
        default_limits=[RATE_LIMIT],
        storage_uri="memory://",
    )
    log.info(f"Rate limiting ativo: {RATE_LIMIT}")
except ImportError:
    log.warning("flask-limiter nao instalado — rate limiting desabilitado")
    limiter = None


# =========================================================================
# AUTENTICACAO — login simples com sessao
# =========================================================================
def login_required(f):
    """Decorator: redireciona para login se nao autenticado."""
    @wraps(f)
    def decorated(*args, **kwargs):
        if not session.get("authenticated"):
            return redirect(url_for("login", next=request.url))
        return f(*args, **kwargs)
    return decorated


@app.route("/login", methods=["GET", "POST"])
def login():
    """Tela de login simples."""
    error = None
    if request.method == "POST":
        user = request.form.get("username", "")
        pwd = request.form.get("password", "")
        if user == DASHBOARD_USER and pwd == DASHBOARD_PASS:
            session["authenticated"] = True
            session["login_time"] = datetime.now().isoformat()
            log.info(f"Login OK de {request.remote_addr}")
            next_url = request.args.get("next", url_for("dashboard"))
            return redirect(next_url)
        else:
            error = "Usuario ou senha incorretos"
            log.warning(f"Login FALHOU de {request.remote_addr}")
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    """Encerra sessao."""
    session.clear()
    return redirect(url_for("login"))


# =========================================================================
# ROTAS — Dashboard
# =========================================================================
@app.route("/")
@login_required
def dashboard():
    """Pagina principal do dashboard CRM."""
    # Defaults: ultimo mes ate D-1
    d1 = (date.today() - timedelta(days=1)).isoformat()
    d_from = (date.today() - timedelta(days=30)).isoformat()
    return render_template(
        "dashboard.html",
        campaign_types=CAMPAIGN_TYPES,
        channels=CHANNELS,
        default_date_from=d_from,
        default_date_to=d1,
        now=datetime.now().strftime("%d/%m/%Y %H:%M"),
    )


# =========================================================================
# API — Dados do Dashboard (JSON)
# =========================================================================
@app.route("/api/data")
@login_required
def api_data():
    """
    Endpoint principal — retorna todos os dados do dashboard em JSON.

    Query params:
        date_from: YYYY-MM-DD (default: 30 dias atras)
        date_to: YYYY-MM-DD (default: D-1)
        campaign_type: tipo de campanha ou 'all'
        channel: canal ou 'all'
        page: numero da pagina (default: 1)
        page_size: itens por pagina (default: 25)
        sort_by: coluna para ordenar (default: total_ggr)
        sort_dir: ASC ou DESC (default: DESC)
    """
    d1 = (date.today() - timedelta(days=1)).isoformat()
    d_from = (date.today() - timedelta(days=30)).isoformat()

    date_from = request.args.get("date_from", d_from)
    date_to = request.args.get("date_to", d1)
    campaign_type = request.args.get("campaign_type", "all")
    channel = request.args.get("channel", "all")
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", DEFAULT_PAGE_SIZE))
    sort_by = request.args.get("sort_by", "ggr_brl")
    sort_dir = request.args.get("sort_dir", "DESC")

    try:
        data = get_all_dashboard_data(
            date_from=date_from,
            date_to=date_to,
            campaign_type=campaign_type,
            channel=channel,
            page=page,
            page_size=page_size,
            sort_by=sort_by,
            sort_dir=sort_dir,
        )
        data["filters"] = {
            "date_from": date_from,
            "date_to": date_to,
            "campaign_type": campaign_type,
            "channel": channel,
        }
        data["updated_at"] = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        return jsonify(data)
    except Exception as e:
        log.error(f"Erro api_data: {e}", exc_info=True)
        return jsonify({"error": "Erro ao consultar dados. Tente novamente."}), 500


# =========================================================================
# API — Export CSV
# =========================================================================
@app.route("/api/export/csv")
@login_required
def api_export_csv():
    """
    Exporta todas as campanhas filtradas como CSV (sem paginacao).
    Retorna arquivo para download direto no browser.
    """
    d1 = (date.today() - timedelta(days=1)).isoformat()
    d_from = (date.today() - timedelta(days=30)).isoformat()

    date_from = request.args.get("date_from", d_from)
    date_to = request.args.get("date_to", d1)
    campaign_type = request.args.get("campaign_type", "all")
    channel = request.args.get("channel", "all")

    try:
        rows = get_campaigns_for_export(date_from, date_to, campaign_type, channel)

        output = io.StringIO()
        output.write('\ufeff')  # BOM para Excel
        writer = csv.writer(output, delimiter=';')

        # Header
        header = [
            "Data", "Rule ID", "Campanha", "Tipo", "Canal", "Segmento", "Ativa",
            "Enviados", "Entregues", "Abertos", "Clicados", "Convertidos",
            "Completaram", "Custo Bonus (R$)",
            "Coorte Users", "GGR Casino (R$)", "GGR Sports (R$)", "GGR Total (R$)",
            "Depositos (R$)", "Saques (R$)", "Net Deposit (R$)",
            "Turnover Casino (R$)", "Turnover Sports (R$)",
            "Custo Disparo (R$)", "ROI",
        ]
        writer.writerow(header)

        for r in rows:
            writer.writerow([
                r.get("report_date", ""),
                r.get("rule_id", ""),
                r.get("rule_name", ""),
                r.get("campaign_type", ""),
                r.get("channel", ""),
                r.get("segment_name", ""),
                "Sim" if r.get("is_active") else "Nao",
                r.get("enviados", 0),
                r.get("entregues", 0),
                r.get("abertos", 0),
                r.get("clicados", 0),
                r.get("convertidos", 0),
                r.get("cumpriram_condicao", 0),
                r.get("custo_bonus_brl", 0),
                r.get("coorte_users", 0),
                r.get("casino_ggr", 0),
                r.get("sportsbook_ggr", 0),
                r.get("total_ggr", 0),
                r.get("total_deposit", 0),
                r.get("total_withdrawal", 0),
                r.get("net_deposit", 0),
                r.get("casino_turnover", 0),
                r.get("sportsbook_turnover", 0),
                r.get("custo_disparo_brl", 0),
                r.get("roi", 0),
            ])

        filename = f"crm_performance_{date_from}_a_{date_to}.csv"
        return Response(
            output.getvalue(),
            mimetype="text/csv",
            headers={"Content-Disposition": f"attachment; filename={filename}"},
        )
    except Exception as e:
        log.error(f"Erro export CSV: {e}", exc_info=True)
        return jsonify({"error": "Erro ao exportar CSV."}), 500


# =========================================================================
# API — Refresh cache
# =========================================================================
@app.route("/api/refresh", methods=["POST"])
@login_required
def api_refresh():
    """Limpa o cache e forca recarregamento dos dados."""
    clear_cache()
    log.info(f"Cache CRM limpo por {request.remote_addr}")
    return jsonify({"status": "ok", "message": "Cache limpo. Recarregue a pagina."})


# =========================================================================
# HEALTH CHECK (sem auth — para monitoramento)
# =========================================================================
@app.route("/health")
def health():
    """Health check simples."""
    return jsonify({
        "status": "ok",
        "service": "crm-report-dashboard",
        "timestamp": datetime.now().isoformat(),
    })


# =========================================================================
# LOG de acesso
# =========================================================================
@app.after_request
def log_request(response):
    """Loga cada request para auditoria."""
    if request.path != "/health":
        log.info(
            f"{request.remote_addr} {request.method} {request.path} "
            f"-> {response.status_code}"
        )
    return response


# =========================================================================
# MAIN
# =========================================================================
if __name__ == "__main__":
    log.info(f"Dashboard CRM Report iniciando em {FLASK_HOST}:{FLASK_PORT}")
    log.info(f"Debug: {FLASK_DEBUG}")
    app.run(
        host=FLASK_HOST,
        port=FLASK_PORT,
        debug=FLASK_DEBUG,
    )
