"""
Configuracao do Dashboard CRM Report — Performance de Campanhas.

Altere aqui os parametros sem mexer no codigo principal.
Segue o mesmo padrao do dashboard google_ads.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# =====================================================================
# FLASK
# =====================================================================
FLASK_HOST = os.getenv("CRM_DASHBOARD_HOST", "0.0.0.0")
FLASK_PORT = int(os.getenv("CRM_DASHBOARD_PORT", "5051"))
FLASK_DEBUG = os.getenv("CRM_DASHBOARD_DEBUG", "false").lower() == "true"
SECRET_KEY = os.getenv("CRM_DASHBOARD_SECRET_KEY", os.urandom(32).hex())

# =====================================================================
# AUTENTICACAO — credenciais via .env (NUNCA hardcodar)
# =====================================================================
DASHBOARD_USER = os.getenv("DASHBOARD_USER", "multibet")
DASHBOARD_PASS = os.getenv("DASHBOARD_PASS", "mb2026")

# =====================================================================
# RATE LIMITING
# =====================================================================
RATE_LIMIT = "120 per minute"

# =====================================================================
# CACHE — evita consultas repetidas ao Super Nova DB
# =====================================================================
CACHE_TTL_SECONDS = 60 * 5  # 5 minutos (dados vem do PostgreSQL, nao Athena)

# =====================================================================
# TIPOS DE CAMPANHA — usados nos filtros do dashboard
# =====================================================================
CAMPAIGN_TYPES = [
    "Challenge",
    "Cashback_VIP",
    "DailyFS",
    "RETEM",
    "Lifecycle",
    "Gamificacao",
    "CrossSell_Sports",
    "Bonus_Generico",
    "Reativacao_FTD",
    "CX_Recovery",
    "FreeSpins",
    "Sem_Classificacao",
]

# =====================================================================
# CANAIS DE DISPARO
# =====================================================================
CHANNELS = [
    "popup",
    "SMS",
    "WhatsApp",
    "push",
    "push_notification",
    "inbox",
]

# =====================================================================
# PAGINACAO
# =====================================================================
DEFAULT_PAGE_SIZE = 25
MAX_PAGE_SIZE = 100

# =====================================================================
# VIP TIERS — faixas de classificacao
# =====================================================================
VIP_TIERS = {
    "Elite": {"min_ngr": 10000, "label": "Elite (NGR >= R$ 10.000)"},
    "Key Account": {"min_ngr": 5000, "label": "Key Account (R$ 5K-10K)"},
    "High Value": {"min_ngr": 3000, "label": "High Value (R$ 3K-5K)"},
    "Standard": {"min_ngr": 0, "label": "Standard"},
}

# =====================================================================
# CUSTOS DE DISPARO POR CANAL/PROVEDOR (confirmados CRM 31/03/2026)
# Canais nao listados = sem custo associado (ignorar)
# =====================================================================
DISPATCH_COSTS = {
    "sms_ligue_lead": 0.047,
    "sms_pushfy":     0.060,
    "whatsapp_loyalty": 0.160,
}
