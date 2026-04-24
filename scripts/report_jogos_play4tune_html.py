"""
Report de Jogos Play4Tune (P4T) — HTML para Diretoria
3 Rankings: GGR, Turnover, Giros + detalhamento por jogador, concentracao, tendencia.

Arquitetura modular:
- fetch_data(cur)          -> dict com resultados das 7 queries SQL
- compute_metrics(data)    -> dict com metricas derivadas (totais, concentracoes, etc)
- sanity_checks(data, m)   -> asserts de integridade (pre-build)
- build_html(data, m, ts)  -> string HTML final
- save_report(html)        -> salva _FINAL.html + copia versionada com data
- run_report()             -> orquestrador

Banco: supernova_bet (PostgreSQL 15.14) | Moeda: PKR | Timezone: UTC no banco, BRT extracao
"""

import os
import sys
import json
import logging
import urllib.request
from datetime import datetime
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db.supernova_bet import get_supernova_bet_connection

BRT = ZoneInfo("America/Sao_Paulo")

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("play4tune_report")


# =============================================
# TAXAS DE CAMBIO — fetch dinamico com fallback
# =============================================
FALLBACK_PKR_TO_BRL = 0.017881
FALLBACK_PKR_TO_USD = 0.003582
FALLBACK_FX_DATE = "16/04/2026"


# =============================================
# WHITELIST — usuarios REAIS que a logica do dev sinaliza errado
# =============================================
# Contas que TERIAM sido excluidas pelo filtro de manipulacao manual,
# mas o dev validou manualmente como usuarios reais (caso DP/SQ — deposito/saque
# operacional ajustado manualmente pelo suporte).
# Atualizar aqui quando o dev sinalizar novos casos.
REAL_USERS_WHITELIST = {
    # Confirmados reais pelo dev em 16/04/2026 (caso DP/SQ):
    'maharshani44377634693',
    'muhammadrehan17657797557',
    'rehmanzafar006972281',       # email: rehmanzafar006@gmail.com
    'saimkyani15688267',
}


def fetch_fx_rates():
    """Busca PKR->BRL e PKR->USD via open.er-api.com. Fallback se API falhar."""
    try:
        req = urllib.request.Request(
            "https://open.er-api.com/v6/latest/PKR",
            headers={"User-Agent": "Mozilla/5.0"},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            payload = json.loads(resp.read())
        if payload.get("result") == "success":
            rates = payload["rates"]
            pkr_brl = float(rates["BRL"])
            pkr_usd = float(rates["USD"])
            date_str = datetime.now(BRT).strftime("%d/%m/%Y")
            log.info(f"Taxas FX via API: 1 PKR = R$ {pkr_brl:.6f} | $ {pkr_usd:.6f}")
            return pkr_brl, pkr_usd, date_str, "api (open.er-api.com)"
    except Exception as e:
        log.warning(f"Falha API FX: {e}. Usando fallback {FALLBACK_FX_DATE}")
    return FALLBACK_PKR_TO_BRL, FALLBACK_PKR_TO_USD, FALLBACK_FX_DATE, "fallback (hardcoded)"


PKR_TO_BRL, PKR_TO_USD, FX_DATE, FX_SOURCE = fetch_fx_rates()


# =============================================
# FORMATADORES (usam constantes FX module-level)
# =============================================
def fmt_pkr(v):
    if v is None:
        return "—"
    return f"Rs {float(v):,.2f}"


def fmt_brl(v):
    if v is None:
        return "—"
    return f"R$ {float(v) * PKR_TO_BRL:,.2f}"


def fmt_usd(v):
    if v is None:
        return "—"
    return f"$ {float(v) * PKR_TO_USD:,.2f}"


def fmt_pct(v):
    if v is None:
        return "—"
    return f"{float(v):.2f}%"


def fmt_int(v):
    if v is None:
        return "—"
    return f"{int(v):,}"


def css_class_ggr(v):
    if v is None:
        return ""
    return "positive" if float(v) >= 0 else "negative"


# =============================================
# TOOLTIPS (centralizados — 1 lugar pra atualizar)
# =============================================
TOOLTIPS = {
    "ggr": "GGR = Gross Gaming Revenue = Apostado - Pago. Receita bruta da casa. Positivo = casa ganhou; negativo = casa perdeu.",
    "ggr_brl": "GGR convertido para Real brasileiro na cotacao do dia.",
    "ggr_usd": "GGR convertido para Dolar americano na cotacao do dia.",
    "turnover": "Turnover = volume total apostado pelos jogadores. Soma de todas as bets no periodo.",
    "turnover_brl": "Turnover convertido para Real brasileiro.",
    "turnover_usd": "Turnover convertido para Dolar americano.",
    "giros": "Total de rodadas (spins/bets) jogadas. Indica engajamento e popularidade.",
    "hold": "Hold% = GGR / Turnover x 100. Margem da casa. Saudavel: 2-5% em slots. Negativo = casa perdeu.",
    "rtp": "RTP = Return to Player. % que o jogo devolve ao jogador em teoria (configurado pelo provider).",
    "jogadores": "Quantidade de jogadores distintos que apostaram no periodo (ja filtrando contas de teste).",
    "dias": "Quantidade de dias com pelo menos 1 aposta registrada no periodo.",
    "pid": "Public ID — identificador curto e publico do jogador (9 caracteres).",
    "ticket": "Ticket Medio = Turnover / Giros = valor medio apostado por rodada. Indica perfil de stake.",
    "pct_total": "Percentual que este item representa do total agregado no periodo.",
    "risco": "CRITICO = 1 jogador unico | ALTO = 2 jogadores | MEDIO = 3 jogadores. Turnover > Rs 1.000.",
    "concentracao_top": "% do turnover total que os top N jogadores representam. Top 1 > 30% = risco critico.",
    "ggr_neg_count": "Quantidade de jogos onde a casa perdeu dinheiro no periodo (jogadores ganharam mais que apostaram).",
    "jogos_ativos": "Jogos com pelo menos 1 aposta registrada no dia.",
    "jogadores_ativos": "Jogadores distintos que apostaram no dia (filtro teste aplicado).",
    "username": "Nome de usuario publico do jogador no Play4Tune.",
    "jogos_distintos": "Quantidade de jogos diferentes que o jogador apostou no periodo.",
}


def _add_tooltips(html):
    """Injeta title=... em labels/headers chave. Centraliza tooltips num unico dict."""
    replacements = {
        # KPI cards
        '<div class="label">GGR Total</div>':
            f'<div class="label" title="{TOOLTIPS["ggr"]}">GGR Total</div>',
        '<div class="label">Turnover Total</div>':
            f'<div class="label" title="{TOOLTIPS["turnover"]}">Turnover Total</div>',
        '<div class="label">Giros Totais</div>':
            f'<div class="label" title="{TOOLTIPS["giros"]}">Giros Totais</div>',
        '<div class="label">Hold% Geral</div>':
            f'<div class="label" title="{TOOLTIPS["hold"]}">Hold% Geral</div>',
        '<div class="label">Concentracao Top 1</div>':
            f'<div class="label" title="{TOOLTIPS["concentracao_top"]}">Concentracao Top 1</div>',
        '<div class="label">Jogos GGR Negativo</div>':
            f'<div class="label" title="{TOOLTIPS["ggr_neg_count"]}">Jogos GGR Negativo</div>',

        # Table headers (repetem em varias tabelas — replace acerta todas)
        '<th class="r">GGR (PKR)</th>':
            f'<th class="r" title="{TOOLTIPS["ggr"]}">GGR (PKR)</th>',
        '<th class="r">GGR (BRL)</th>':
            f'<th class="r" title="{TOOLTIPS["ggr_brl"]}">GGR (BRL)</th>',
        '<th class="r">GGR (USD)</th>':
            f'<th class="r" title="{TOOLTIPS["ggr_usd"]}">GGR (USD)</th>',
        '<th class="r">Turnover (PKR)</th>':
            f'<th class="r" title="{TOOLTIPS["turnover"]}">Turnover (PKR)</th>',
        '<th class="r">Turnover (BRL)</th>':
            f'<th class="r" title="{TOOLTIPS["turnover_brl"]}">Turnover (BRL)</th>',
        '<th class="r">Turnover (USD)</th>':
            f'<th class="r" title="{TOOLTIPS["turnover_usd"]}">Turnover (USD)</th>',
        '<th class="r">Giros</th>':
            f'<th class="r" title="{TOOLTIPS["giros"]}">Giros</th>',
        '<th class="r">Hold%</th>':
            f'<th class="r" title="{TOOLTIPS["hold"]}">Hold%</th>',
        '<th class="r">RTP Cat</th>':
            f'<th class="r" title="{TOOLTIPS["rtp"]}">RTP Cat</th>',
        '<th class="r">Jogadores</th>':
            f'<th class="r" title="{TOOLTIPS["jogadores"]}">Jogadores</th>',
        '<th class="r">Dias</th>':
            f'<th class="r" title="{TOOLTIPS["dias"]}">Dias</th>',
        '<th class="r">% Total</th>':
            f'<th class="r" title="{TOOLTIPS["pct_total"]}">% Total</th>',
        '<th class="r">Ticket Med (PKR)</th>':
            f'<th class="r" title="{TOOLTIPS["ticket"]}">Ticket Med (PKR)</th>',
        '<th class="r">Ticket Med</th>':
            f'<th class="r" title="{TOOLTIPS["ticket"]}">Ticket Med</th>',
        '<th class="r">Ticket</th>':
            f'<th class="r" title="{TOOLTIPS["ticket"]}">Ticket</th>',
        '<th class="r">Risco</th>':
            f'<th class="r" title="{TOOLTIPS["risco"]}">Risco</th>',
        '<th class="r">Jogos</th>':
            f'<th class="r" title="{TOOLTIPS["jogos_distintos"]}">Jogos</th>',
        '<th>Username</th>':
            f'<th title="{TOOLTIPS["username"]}">Username</th>',
        '<th>PID</th>':
            f'<th title="{TOOLTIPS["pid"]}">PID</th>',
    }
    for old, new in replacements.items():
        html = html.replace(old, new)
    return html


# =============================================
# CONEXAO
# =============================================
def _safe_close(tunnel, conn, cur):
    """Fecha recursos garantidamente (ideal em finally)."""
    for name, res in (("cur", cur), ("conn", conn), ("tunnel", tunnel)):
        if res is None:
            continue
        try:
            res.stop() if name == "tunnel" else res.close()
        except Exception as e:
            log.warning(f"Erro fechando {name}: {e}")


def connect():
    """Abre tunnel SSH + conn Postgres read-only. Retorna (tunnel, conn, cur)."""
    log.info("Conectando ao Super Nova Bet DB...")
    tunnel, conn = get_supernova_bet_connection()
    conn.set_session(readonly=True, autocommit=True)
    cur = conn.cursor()
    return tunnel, conn, cur


# =============================================
# QUERIES (uma por funcao — facilita teste/reuso)
# =============================================
def query_test_users(cur):
    """Identifica contas de teste por 2 regras combinadas (UNION):
       (a) HEURISTICA: role != USER OR username/email padrao test/teste/demo/admin/internal
       (b) LOGICA OFICIAL DO DEV (16/04/2026): user teve manipulacao manual de saldo:
           - transactions.type IN ('ADJUSTMENT_CREDIT','ADJUSTMENT_DEBIT'), OU
           - transactions.type='DEPOSIT' AND reviewed_by IS NOT NULL (confirmacao manual)
       Retorna (lista_contas, tupla_ids_pra_WHERE_NOT_IN)."""
    cur.execute("""
        SELECT u.id, u.username, u.public_id, u.role, u.email
        FROM users u
        WHERE
           -- (a) HEURISTICA: padroes de username/email/role
           u.role != 'USER'
           OR LOWER(u.username) LIKE '%%test%%'
           OR LOWER(u.username) LIKE '%%teste%%'
           -- %qa% REMOVIDO: gera falso positivo em nomes paquistaneses comuns
           -- (Qadir, Qamar, Qasim, Qasem). Contas QA legitimas sao capturadas
           -- por 'test'/'teste' (convencao do time). Flagged pelo auditor 16/04.
           OR LOWER(u.username) LIKE '%%demo%%'
           OR LOWER(u.username) LIKE '%%admin%%'
           OR LOWER(COALESCE(u.email, '')) LIKE '%%@karinzitta%%'
           OR LOWER(COALESCE(u.email, '')) LIKE '%%@multi.bet%%'
           OR LOWER(COALESCE(u.email, '')) LIKE '%%@grupo-pgs%%'
           OR LOWER(COALESCE(u.email, '')) LIKE '%%@supernovagaming%%'
           OR LOWER(COALESCE(u.email, '')) LIKE '%%@play4tune%%'
           -- (b) LOGICA OFICIAL DEV: qualquer manipulacao manual de saldo
           OR u.id IN (
               SELECT DISTINCT t.user_id
               FROM transactions t
               WHERE t.type IN ('ADJUSTMENT_CREDIT', 'ADJUSTMENT_DEBIT')
                  OR (t.type = 'DEPOSIT' AND t.reviewed_by IS NOT NULL)
           )
        ORDER BY u.role, u.username
    """)
    accounts = cur.fetchall()

    # Aplica whitelist: usuarios confirmados reais pelo dev saem da lista
    whitelisted_found = [a for a in accounts if a[1] in REAL_USERS_WHITELIST]
    accounts = [a for a in accounts if a[1] not in REAL_USERS_WHITELIST]

    ids = [r[0] for r in accounts]
    ids_sql = tuple(ids) if ids else ('00000000-0000-0000-0000-000000000000',)
    log.info(f"{len(accounts)} contas de teste identificadas (heuristica + logica dev)")
    if whitelisted_found:
        log.info(f"{len(whitelisted_found)} conta(s) devolvida(s) ao report via whitelist: "
                 f"{', '.join(a[1] for a in whitelisted_found)}")
    return accounts, ids_sql, whitelisted_found


def query_periodo(cur, test_ids_sql):
    """Primeiro dia, ultimo dia e total de dias com atividade (ja filtrando teste)."""
    cur.execute("""
        SELECT MIN(m.date), MAX(m.date), COUNT(DISTINCT m.date)
        FROM casino_user_game_metrics m
        WHERE m.user_id NOT IN %s
    """, (test_ids_sql,))
    return cur.fetchone()


def query_jogos(cur, test_ids_sql):
    """Ranking de jogos: GGR, turnover, giros, hold%, RTP, jogadores, dias ativos."""
    cur.execute("""
        SELECT
            g.name AS jogo,
            g.rtp AS rtp_catalogo,
            ROUND(SUM(m.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(m.total_win_amount)::numeric, 2) AS pago,
            ROUND(SUM(m.net_revenue)::numeric, 2) AS ggr,
            SUM(m.played_rounds) AS giros,
            COUNT(DISTINCT m.date) AS dias_ativo,
            CASE WHEN SUM(m.total_bet_amount) > 0
                 THEN ROUND((SUM(m.net_revenue) / SUM(m.total_bet_amount) * 100)::numeric, 2)
                 ELSE 0 END AS hold_pct,
            COUNT(DISTINCT m.user_id) AS jogadores
        FROM casino_user_game_metrics m
        JOIN casino_games g ON g.id = m.game_id
        WHERE m.user_id NOT IN %s
        GROUP BY g.name, g.rtp
        HAVING SUM(m.total_bet_amount) > 0
        ORDER BY SUM(m.net_revenue) DESC
    """, (test_ids_sql,))
    return cur.fetchall()


def query_top_players(cur, test_ids_sql, limit=20):
    """Top N jogadores por turnover."""
    cur.execute("""
        SELECT
            u.username,
            u.public_id,
            u.phone,
            ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
            SUM(um.played_rounds) AS giros,
            COUNT(DISTINCT um.game_id) AS jogos_distintos,
            COUNT(DISTINCT um.date) AS dias_ativo,
            ROUND((SUM(um.total_bet_amount) / NULLIF(SUM(um.played_rounds), 0))::numeric, 2) AS ticket_medio
        FROM casino_user_game_metrics um
        JOIN users u ON u.id = um.user_id
        WHERE um.user_id NOT IN %s
        GROUP BY u.username, u.public_id, u.phone
        ORDER BY SUM(um.total_bet_amount) DESC
        LIMIT %s
    """, (test_ids_sql, limit))
    return cur.fetchall()


def query_detalhe_por_jogo(cur, jogos_names, test_ids_sql, order_by):
    """Detalhamento por jogador nos jogos listados. order_by: 'ggr'|'turnover'|'giros'."""
    if order_by == 'ggr':
        sql = """
            SELECT
                g.name AS jogo, u.username, u.public_id,
                ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
                ROUND(SUM(um.total_win_amount)::numeric, 2) AS pago,
                ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
                SUM(um.played_rounds) AS giros,
                COUNT(DISTINCT um.date) AS dias,
                ROUND((SUM(um.total_bet_amount) / NULLIF(SUM(um.played_rounds), 0))::numeric, 2) AS ticket
            FROM casino_user_game_metrics um
            JOIN casino_games g ON g.id = um.game_id
            JOIN users u ON u.id = um.user_id
            WHERE g.name = ANY(%s) AND um.user_id NOT IN %s
            GROUP BY g.name, u.username, u.public_id
            ORDER BY g.name, SUM(um.net_revenue) DESC
        """
    elif order_by == 'turnover':
        sql = """
            SELECT
                g.name AS jogo, u.username, u.public_id,
                ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
                ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
                SUM(um.played_rounds) AS giros,
                COUNT(DISTINCT um.date) AS dias,
                ROUND((SUM(um.total_bet_amount) / NULLIF(SUM(um.played_rounds), 0))::numeric, 2) AS ticket
            FROM casino_user_game_metrics um
            JOIN casino_games g ON g.id = um.game_id
            JOIN users u ON u.id = um.user_id
            WHERE g.name = ANY(%s) AND um.user_id NOT IN %s
            GROUP BY g.name, u.username, u.public_id
            ORDER BY g.name, SUM(um.total_bet_amount) DESC
        """
    else:  # giros
        sql = """
            SELECT
                g.name AS jogo, u.username, u.public_id,
                SUM(um.played_rounds) AS giros,
                ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
                ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr,
                COUNT(DISTINCT um.date) AS dias,
                ROUND((SUM(um.total_bet_amount) / NULLIF(SUM(um.played_rounds), 0))::numeric, 2) AS ticket
            FROM casino_user_game_metrics um
            JOIN casino_games g ON g.id = um.game_id
            JOIN users u ON u.id = um.user_id
            WHERE g.name = ANY(%s) AND um.user_id NOT IN %s
            GROUP BY g.name, u.username, u.public_id
            ORDER BY g.name, SUM(um.played_rounds) DESC
        """
    cur.execute(sql, (jogos_names, test_ids_sql))
    return cur.fetchall()


def query_jogos_concentrados(cur, test_ids_sql):
    """Jogos com risco de dependencia (poucos jogadores e turnover > 1000)."""
    cur.execute("""
        SELECT g.name, COUNT(DISTINCT um.user_id) AS jogadores,
               ROUND(SUM(um.total_bet_amount)::numeric, 2) AS turnover,
               ROUND(SUM(um.net_revenue)::numeric, 2) AS ggr
        FROM casino_user_game_metrics um
        JOIN casino_games g ON g.id = um.game_id
        WHERE um.user_id NOT IN %s
        GROUP BY g.name
        HAVING SUM(um.total_bet_amount) > 1000
        ORDER BY COUNT(DISTINCT um.user_id), SUM(um.total_bet_amount) DESC
    """, (test_ids_sql,))
    return cur.fetchall()


def query_tendencia(cur, test_ids_sql):
    """Tendencia diaria: turnover, GGR, giros, jogadores ativos, jogos ativos."""
    cur.execute("""
        SELECT
            m.date,
            ROUND(SUM(m.total_bet_amount)::numeric, 2) AS turnover,
            ROUND(SUM(m.net_revenue)::numeric, 2) AS ggr,
            SUM(m.played_rounds) AS giros,
            COUNT(DISTINCT m.user_id) AS jogadores_ativos,
            COUNT(DISTINCT m.game_id) AS jogos_ativos
        FROM casino_user_game_metrics m
        WHERE m.user_id NOT IN %s
        GROUP BY m.date
        ORDER BY m.date
    """, (test_ids_sql,))
    return cur.fetchall()


# =============================================
# FETCH DATA (orquestra queries)
# =============================================
def fetch_data(cur):
    """Responsabilidade UNICA: rodar todas as queries e devolver dict estruturado."""
    test_accounts, test_ids_sql, whitelisted = query_test_users(cur)
    periodo = query_periodo(cur, test_ids_sql)
    jogos_all = query_jogos(cur, test_ids_sql)
    top_players = query_top_players(cur, test_ids_sql)
    jogos_concentrados = query_jogos_concentrados(cur, test_ids_sql)
    tendencia = query_tendencia(cur, test_ids_sql)

    top10_ggr_names = [j[0] for j in jogos_all[:10]]
    top10_turn_names = [j[0] for j in sorted(jogos_all, key=lambda x: x[2], reverse=True)[:10]]
    top10_giros_names = [j[0] for j in sorted(jogos_all, key=lambda x: x[5], reverse=True)[:10]]

    detalhe_ggr = query_detalhe_por_jogo(cur, top10_ggr_names, test_ids_sql, 'ggr')
    detalhe_turn = query_detalhe_por_jogo(cur, top10_turn_names, test_ids_sql, 'turnover')
    detalhe_giros = query_detalhe_por_jogo(cur, top10_giros_names, test_ids_sql, 'giros')

    return {
        "test_accounts": test_accounts,
        "whitelisted": whitelisted,
        "periodo": periodo,
        "jogos_all": jogos_all,
        "top_players": top_players,
        "jogos_concentrados": jogos_concentrados,
        "tendencia": tendencia,
        "detalhe_ggr": detalhe_ggr,
        "detalhe_turn": detalhe_turn,
        "detalhe_giros": detalhe_giros,
    }


# =============================================
# COMPUTE METRICS (logica de negocio pura)
# =============================================
def compute_metrics(data):
    """Responsabilidade UNICA: derivar totais, concentracoes, ordenacoes alternativas."""
    jogos_all = data["jogos_all"]
    top_players = data["top_players"]

    total_ggr = float(sum(j[4] for j in jogos_all))
    total_turn = float(sum(j[2] for j in jogos_all))
    total_giros = int(sum(j[5] for j in jogos_all))

    jogos_by_turn = sorted(jogos_all, key=lambda x: x[2], reverse=True)
    jogos_by_giros = sorted(jogos_all, key=lambda x: x[5], reverse=True)
    jogos_ggr_neg = [j for j in jogos_all if float(j[4]) < 0]

    top1_pct = (float(top_players[0][3]) / total_turn * 100) if top_players and total_turn > 0 else 0
    top3_pct = (float(sum(p[3] for p in top_players[:3])) / total_turn * 100) if top_players and total_turn > 0 else 0
    top5_pct = (float(sum(p[3] for p in top_players[:5])) / total_turn * 100) if top_players and total_turn > 0 else 0

    top3_turn_jogos = (float(sum(j[2] for j in jogos_by_turn[:3])) / total_turn * 100) if total_turn > 0 else 0
    top3_giros_jogos = (float(sum(j[5] for j in jogos_by_giros[:3])) / total_giros * 100) if total_giros > 0 else 0

    hold_pct = (total_ggr / total_turn * 100) if total_turn > 0 else 0
    crash2 = next((j for j in jogos_all if j[0] == 'CRASH II'), None)

    return {
        "total_ggr": total_ggr,
        "total_turn": total_turn,
        "total_giros": total_giros,
        "hold_pct": hold_pct,
        "jogos_by_turn": jogos_by_turn,
        "jogos_by_giros": jogos_by_giros,
        "jogos_ggr_neg": jogos_ggr_neg,
        "top1_pct": top1_pct,
        "top3_pct": top3_pct,
        "top5_pct": top5_pct,
        "top3_turn_jogos": top3_turn_jogos,
        "top3_giros_jogos": top3_giros_jogos,
        "crash2": crash2,
    }


# =============================================
# SANITY CHECKS (pre-build)
# =============================================
def sanity_checks(data, metrics):
    """Falha cedo se dados estao fora do esperado — evita report vazio/corrompido."""
    periodo = data["periodo"]
    assert periodo and periodo[0] is not None, "Sem dados no periodo"
    assert periodo[2] > 0, f"Periodo invalido (0 dias): {periodo}"
    assert len(data["jogos_all"]) > 0, "Nenhum jogo com turnover > 0"
    assert len(data["top_players"]) > 0, "Nenhum jogador apos filtro de teste"
    assert len(data["tendencia"]) > 0, "Tendencia diaria vazia"
    assert metrics["total_turn"] > 0, "Turnover total = 0"
    assert 0 <= metrics["top1_pct"] <= 100, f"top1_pct fora da faixa: {metrics['top1_pct']}"

    data_mais_recente = max(t[0] for t in data["tendencia"])
    lag = (datetime.now(BRT).date() - data_mais_recente).days
    if lag > 2:
        log.warning(f"ALERTA: dados defasados — ultimo dia com atividade = {data_mais_recente} ({lag}d atras)")
    else:
        log.info(f"Dados atualizados: ultimo dia = {data_mais_recente} ({lag}d lag)")


# =============================================
# HTML BUILDERS
# =============================================
DP_SQ_TOOLTIP = ("Ajuste DP/SQ operacional: deposito do jogador nao entrou no caixa da casa "
                 "e o suporte creditou o valor manualmente. Jogador real, confirmado pelo dev (16/04/2026).")


def _flag_dpsq(username, whitelist_set):
    """Retorna o HTML de flag DP/SQ se o username esta na whitelist, senao string vazia."""
    if username in whitelist_set:
        return f' <span class="dp-sq-flag" title="{DP_SQ_TOOLTIP}">DP/SQ</span>'
    return ''


def _build_game_detail_html(detalhe_rows, value_col_name, whitelist_set=None):
    """Gera tabelas de detalhamento por jogador para cada jogo (usado nos 3 rankings)."""
    whitelist_set = whitelist_set or set()
    html = ""
    current_game = None
    for d in detalhe_rows:
        if d[0] != current_game:
            if current_game is not None:
                html += "</tbody></table></div>\n"
            current_game = d[0]
            html += f"""
            <div class="game-detail">
                <h4>{current_game}</h4>
                <table class="detail-table">
                <thead><tr>
                    <th>Username</th><th>PID</th>
                    <th class="r">{value_col_name} (PKR)</th>
                    <th class="r">{value_col_name} (BRL)</th>
                    <th class="r">GGR (PKR)</th>
                    <th class="r">Turnover (PKR)</th>
                    <th class="r">Giros</th>
                    <th class="r">Ticket Med</th>
                    <th class="r">Dias</th>
                </tr></thead>
                <tbody>
            """
        if value_col_name == "GGR":
            turnover, ggr, giros, dias, ticket = d[3], d[5], d[6], d[7], d[8]
            main_val = ggr
        elif value_col_name == "Turnover":
            turnover, ggr, giros, dias, ticket = d[3], d[4], d[5], d[6], d[7]
            main_val = turnover
        else:  # Giros
            giros, turnover, ggr, dias, ticket = d[3], d[4], d[5], d[6], d[7]
            main_val = giros

        ggr_class = css_class_ggr(ggr)

        if value_col_name == "Giros":
            main_display = fmt_int(main_val)
            main_brl = fmt_int(main_val)  # giros nao converte
        else:
            main_display = fmt_pkr(main_val)
            main_brl = fmt_brl(main_val)

        flag = _flag_dpsq(d[1], whitelist_set)
        row_cls = ' class="dp-sq-row"' if d[1] in whitelist_set else ''
        html += f"""<tr{row_cls}>
            <td>{d[1]}{flag}</td><td class="mono">{d[2]}</td>
            <td class="r {ggr_class if value_col_name == 'GGR' else ''}">{main_display}</td>
            <td class="r">{main_brl}</td>
            <td class="r {ggr_class}">{fmt_pkr(ggr)}</td>
            <td class="r">{fmt_pkr(turnover)}</td>
            <td class="r">{fmt_int(giros)}</td>
            <td class="r">{fmt_pkr(ticket) if ticket else '—'}</td>
            <td class="r">{dias}</td>
        </tr>"""

    if current_game is not None:
        html += "</tbody></table></div>\n"
    return html


_CSS = """
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', -apple-system, BlinkMacSystemFont, sans-serif;
            background: #f5f6fa;
            color: #2c3e50;
            line-height: 1.5;
            padding: 20px;
        }
        .container { max-width: 1400px; margin: 0 auto; }

        .header {
            background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
            color: white;
            padding: 30px 40px;
            border-radius: 12px;
            margin-bottom: 24px;
        }
        .header h1 { font-size: 28px; margin-bottom: 8px; }
        .header .subtitle { font-size: 14px; opacity: 0.85; }
        .header .meta { display: flex; gap: 30px; margin-top: 16px; font-size: 13px; opacity: 0.75; }

        .kpi-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 16px; margin-bottom: 24px; }
        .kpi-card { background: white; border-radius: 10px; padding: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .kpi-card .label { font-size: 12px; color: #7f8c8d; text-transform: uppercase; letter-spacing: 0.5px; }
        .kpi-card .value { font-size: 22px; font-weight: 700; margin: 4px 0; color: #2c3e50; }
        .kpi-card .sub { font-size: 11px; color: #95a5a6; }
        .kpi-card.alert { border-left: 4px solid #e74c3c; }
        .kpi-card.success { border-left: 4px solid #27ae60; }
        .kpi-card.info { border-left: 4px solid #3498db; }

        .section { background: white; border-radius: 10px; padding: 24px 28px; margin-bottom: 20px; box-shadow: 0 2px 8px rgba(0,0,0,0.06); }
        .section h2 { font-size: 18px; color: #1a1a2e; margin-bottom: 4px; padding-bottom: 10px; border-bottom: 2px solid #f0f0f0; }
        .section h2 span { font-weight: 400; color: #7f8c8d; font-size: 14px; }
        .section h3 { font-size: 15px; color: #34495e; margin: 16px 0 8px; }

        table { width: 100%; border-collapse: collapse; font-size: 13px; margin-top: 12px; }
        thead th { background: #f8f9fa; color: #5a6c7d; font-weight: 600; text-transform: uppercase; font-size: 11px; letter-spacing: 0.5px; padding: 10px 8px; border-bottom: 2px solid #e9ecef; position: sticky; top: 0; }
        tbody td { padding: 8px; border-bottom: 1px solid #f0f0f0; }
        tbody tr:hover { background: #f8f9fa; }
        .r { text-align: right; }
        .c { text-align: center; }
        .mono { font-family: 'Consolas', 'Courier New', monospace; font-size: 11px; color: #7f8c8d; }
        .positive { color: #27ae60; font-weight: 600; }
        .negative { color: #e74c3c; font-weight: 600; }
        .muted { color: #95a5a6; }

        .game-detail { margin: 12px 0; padding: 12px 16px; background: #fafbfc; border-radius: 8px; border-left: 3px solid #3498db; }
        .game-detail h4 { font-size: 14px; color: #2c3e50; margin-bottom: 8px; }
        .detail-table { font-size: 12px; }
        .detail-table thead th { font-size: 10px; padding: 6px 6px; }
        .detail-table tbody td { padding: 5px 6px; }

        .concentracao-bar { height: 24px; background: #ecf0f1; border-radius: 12px; overflow: hidden; margin: 8px 0; }
        .concentracao-fill { height: 100%; border-radius: 12px; display: flex; align-items: center; padding: 0 10px; font-size: 11px; font-weight: 600; color: white; }
        .fill-danger { background: linear-gradient(90deg, #e74c3c, #c0392b); }
        .fill-warning { background: linear-gradient(90deg, #f39c12, #e67e22); }
        .fill-ok { background: linear-gradient(90deg, #27ae60, #2ecc71); }

        .alert-box { padding: 14px 18px; border-radius: 8px; margin: 12px 0; font-size: 13px; }
        .alert-danger { background: #fdf0f0; border-left: 4px solid #e74c3c; color: #721c24; }
        .alert-warning { background: #fef9e7; border-left: 4px solid #f39c12; color: #856404; }
        .alert-info { background: #eaf4fe; border-left: 4px solid #3498db; color: #0c5460; }

        .trend-bar { display: inline-block; height: 14px; border-radius: 3px; vertical-align: middle; }

        .footer { text-align: center; font-size: 11px; color: #95a5a6; padding: 20px; margin-top: 20px; }

        .cambio-note { font-size: 11px; color: #7f8c8d; font-style: italic; margin-top: 8px; }

        .test-list { columns: 3; font-size: 11px; color: #7f8c8d; margin-top: 8px; }
        .test-list li { margin-bottom: 2px; }

        /* Tooltips: cursor help + sublinhado sutil em elementos com title */
        th[title], .kpi-card .label[title] {
            cursor: help;
            border-bottom: 1px dotted #95a5a6;
        }
        thead th[title]:hover { background: #e8ecef; color: #2c3e50; }

        /* Flag DP/SQ — usuarios com ajuste manual de saldo (whitelist) */
        .dp-sq-flag {
            display: inline-block;
            margin-left: 6px;
            padding: 1px 6px;
            font-size: 10px;
            font-weight: 700;
            background: #fff3cd;
            color: #856404;
            border: 1px solid #ffc107;
            border-radius: 10px;
            cursor: help;
            vertical-align: middle;
        }
        tr.dp-sq-row td:first-child { border-left: 3px solid #ffc107; }

        @media print {
            body { background: white; padding: 10px; }
            .section { box-shadow: none; border: 1px solid #e9ecef; }
            thead th { position: static; }
        }
"""


def build_html(data, metrics, ts):
    """Responsabilidade UNICA: gerar HTML final a partir de data+metrics+timestamp."""
    from datetime import date as date_cls

    periodo = data["periodo"]
    test_accounts = data["test_accounts"]
    jogos_all = data["jogos_all"]
    top_players = data["top_players"]
    tendencia = data["tendencia"]
    jogos_concentrados = data["jogos_concentrados"]
    whitelist_set = {w[1] for w in data.get("whitelisted", [])}

    total_ggr = metrics["total_ggr"]
    total_turn = metrics["total_turn"]
    total_giros = metrics["total_giros"]
    jogos_by_turn = metrics["jogos_by_turn"]
    jogos_by_giros = metrics["jogos_by_giros"]
    jogos_ggr_neg = metrics["jogos_ggr_neg"]
    top1_pct = metrics["top1_pct"]
    top3_pct = metrics["top3_pct"]
    top5_pct = metrics["top5_pct"]
    top3_turn_jogos = metrics["top3_turn_jogos"]
    top3_giros_jogos = metrics["top3_giros_jogos"]
    hold_pct = metrics["hold_pct"]
    crash2 = metrics["crash2"]

    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Report Jogos Play4Tune — {ts}</title>
    <style>{_CSS}</style>
</head>
<body>
<div class="container">

    <!-- HEADER -->
    <div class="header">
        <h1>Report de Jogos — Play4Tune (P4T)</h1>
        <div class="subtitle">Performance dos jogos de casino por GGR, Turnover e Giros com detalhamento por jogador</div>
        <div class="meta">
            <span>Data: {ts}</span>
            <span>Periodo: {periodo[0]} a {periodo[1]} ({periodo[2]} dias)</span>
            <span>Moeda: PKR (Rupia Paquistanesa)</span>
            <span>Provider: 2J Games</span>
            <span>Contas de teste excluidas: {len(test_accounts)}</span>
        </div>
    </div>

    <!-- GLOSSARIO -->
    <div class="section" id="glossario" style="background:#f8f9fa; border:1px solid #e9ecef;">
        <h2>Como Ler Este Report</h2>
        <table class="detail-table">
        <thead><tr><th style="width:180px;">Termo</th><th>Descricao</th></tr></thead>
        <tbody>
            <tr><td><strong>GGR</strong></td><td>Gross Gaming Revenue = Apostado - Pago ao jogador. Receita bruta da casa. Positivo = casa ganhou. Negativo = casa perdeu.</td></tr>
            <tr><td><strong>Turnover</strong></td><td>Volume total apostado pelos jogadores (soma de todas as bets). Quanto maior, mais engajamento financeiro.</td></tr>
            <tr><td><strong>Giros</strong></td><td>Total de rodadas jogadas. Indica popularidade e engajamento no jogo.</td></tr>
            <tr><td><strong>Hold%</strong></td><td>GGR / Turnover x 100. Margem da casa. Saudavel: 2-5% para slots. Negativo = casa perdeu.</td></tr>
            <tr><td><strong>RTP Cat(alogo)</strong></td><td>Return to Player configurado pelo provider (2J Games). % que o jogo devolve ao jogador em teoria.</td></tr>
            <tr><td><strong>Ticket Medio</strong></td><td>Turnover / Giros = valor medio apostado por rodada. Indica o perfil de stake.</td></tr>
            <tr><td><strong>Jogadores</strong></td><td>Quantidade de jogadores distintos que apostaram naquele jogo no periodo.</td></tr>
            <tr><td><strong>Dias</strong></td><td>Quantidade de dias com pelo menos 1 aposta registrada naquele jogo.</td></tr>
            <tr><td><strong>Concentracao</strong></td><td>% do total que os top N jogadores/jogos representam. Acima de 30% para 1 jogador = risco critico.</td></tr>
            <tr><td><strong>PID</strong></td><td>Public ID — identificador curto do jogador (9 caracteres).</td></tr>
        </tbody></table>
        <p class="cambio-note" style="margin-top:10px;">
            Conversao: 1 PKR = R$ {PKR_TO_BRL:.6f} (BRL) | 1 PKR = $ {PKR_TO_USD:.6f} (USD) | Cotacao {FX_DATE} — fonte: {FX_SOURCE}
            &nbsp;|&nbsp; Nota: este report mostra GGR (antes de bonus/impostos). NGR (Net Gaming Revenue) = GGR - custos de bonus, nao incluido nesta analise.
        </p>
    </div>

    <!-- KPIs -->
    <div class="kpi-grid">
        <div class="kpi-card success">
            <div class="label">GGR Total</div>
            <div class="value">{fmt_pkr(total_ggr)}</div>
            <div class="sub">{fmt_brl(total_ggr)} | {fmt_usd(total_ggr)}</div>
        </div>
        <div class="kpi-card info">
            <div class="label">Turnover Total</div>
            <div class="value">{fmt_pkr(total_turn)}</div>
            <div class="sub">{fmt_brl(total_turn)} | {fmt_usd(total_turn)}</div>
        </div>
        <div class="kpi-card info">
            <div class="label">Giros Totais</div>
            <div class="value">{fmt_int(total_giros)}</div>
            <div class="sub">{len(jogos_all)} jogos com atividade</div>
        </div>
        <div class="kpi-card {'alert' if hold_pct < 3 else 'success'}">
            <div class="label">Hold% Geral</div>
            <div class="value">{hold_pct:.2f}%</div>
            <div class="sub">Saudavel: 2-5% (slots)</div>
        </div>
        <div class="kpi-card alert">
            <div class="label">Concentracao Top 1</div>
            <div class="value">{top1_pct:.1f}%</div>
            <div class="sub">{top_players[0][0] if top_players else '—'}</div>
        </div>
        <div class="kpi-card {'alert' if len(jogos_ggr_neg) > 20 else 'info'}">
            <div class="label">Jogos GGR Negativo</div>
            <div class="value">{len(jogos_ggr_neg)}</div>
            <div class="sub">Casa perdeu dinheiro</div>
        </div>
    </div>

    <!-- RESUMO EXECUTIVO -->
    <div class="section" style="border-left: 4px solid #1a1a2e;">
        <h2>Resumo Executivo</h2>
        <div style="font-size:14px; line-height:1.8; color:#2c3e50;">
            <p>A operacao <strong>Play4Tune</strong> (Paquistao, 100% casino, provider 2J Games) gerou
            <strong>{fmt_pkr(total_ggr)}</strong> de receita bruta ({fmt_brl(total_ggr)}) em <strong>{periodo[2]} dias</strong>
            de operacao, com {fmt_int(total_giros)} rodadas jogadas em {len(jogos_all)} jogos ativos.</p>

            <p><strong>Resultado positivo, porem com riscos criticos de concentracao:</strong></p>
            <ul style="margin:8px 0 8px 20px;">
                <li>Um unico jogador (<strong>{top_players[0][0]}</strong>) responde por <strong>{top1_pct:.1f}%</strong> do volume total
                    de apostas ({fmt_pkr(top_players[0][3])}). Se este jogador parar, a operacao perde mais de 1/3 do movimento.</li>
                <li>O jogo <strong>CRASH II</strong> esta operando com prejuizo de <strong>{fmt_pkr(abs(float(crash2[4]))) if crash2 else 'n/a'}</strong>
                    — recomenda-se avaliar desativacao ou limitacao de stakes.</li>
                <li><strong>{len(jogos_ggr_neg)} jogos</strong> apresentam GGR negativo (casa perdeu dinheiro). Avaliar quais manter no catalogo.</li>
                <li>A base ativa ainda e pequena (~150-200 jogadores unicos), o que amplifica todos os riscos de dependencia.</li>
            </ul>
            <p>O <strong>Hold% geral de {hold_pct:.2f}%</strong> esta dentro do esperado para casino online.
            Os jogos de melhor performance para a casa sao CRASH, WILD BOUNTY SHOWDOWN e GOLDEN OX.</p>
        </div>
    </div>

    <!-- ALERTAS -->
    <div class="section">
        <h2>Alertas Criticos</h2>
"""

    if top1_pct > 30:
        html += f"""
        <div class="alert-box alert-danger">
            <strong>Concentracao extrema:</strong> {top_players[0][0]} representa {top1_pct:.1f}% do turnover total
            ({fmt_pkr(top_players[0][3])} de {fmt_pkr(total_turn)}).
            Se este jogador parar, a operacao perde mais de 1/3 do volume.
        </div>"""

    if crash2 and float(crash2[4]) < -10000:
        html += f"""
        <div class="alert-box alert-danger">
            <strong>CRASH II sangrando:</strong> GGR de {fmt_pkr(crash2[4])} (prejuizo).
            Enquanto CRASH gera receita, o CRASH II esta destruindo margem.
            Avaliar desativar ou limitar stakes.
        </div>"""

    if len(jogos_ggr_neg) > 25:
        total_neg = float(sum(j[4] for j in jogos_ggr_neg))
        html += f"""
        <div class="alert-box alert-warning">
            <strong>{len(jogos_ggr_neg)} jogos com GGR negativo:</strong> prejuizo total de {fmt_pkr(abs(total_neg))}
            ({fmt_brl(abs(total_neg))}). Avaliar quais manter no catalogo.
        </div>"""

    html += """
    </div>

    <!-- REPORT 1 - GGR -->
    <div class="section">
        <h2>Report 1 — Ranking por GGR <span>(Gross Gaming Revenue — receita bruta da casa)</span></h2>
"""
    html += f"""        <p class="cambio-note">Taxas: 1 PKR = BRL {PKR_TO_BRL:.6f} | 1 PKR = USD {PKR_TO_USD:.6f} ({FX_DATE})</p>
        <table>
        <thead><tr>
            <th>#</th><th>Jogo</th>
            <th class="r">GGR (PKR)</th><th class="r">GGR (BRL)</th><th class="r">GGR (USD)</th>
            <th class="r">Turnover (PKR)</th><th class="r">Turnover (BRL)</th>
            <th class="r">Giros</th><th class="r">Hold%</th>
            <th class="r">RTP Cat</th><th class="r">Jogadores</th><th class="r">Dias</th>
        </tr></thead>
        <tbody>
"""
    for i, j in enumerate(jogos_all, 1):
        ggr_class = css_class_ggr(float(j[4]))
        rtp_cat = f"{float(j[1]):.1f}%" if j[1] else "—"
        html += f"""<tr>
            <td class="c">{i}</td><td>{j[0]}</td>
            <td class="r {ggr_class}">{fmt_pkr(j[4])}</td>
            <td class="r {ggr_class}">{fmt_brl(j[4])}</td>
            <td class="r {ggr_class}">{fmt_usd(j[4])}</td>
            <td class="r">{fmt_pkr(j[2])}</td>
            <td class="r">{fmt_brl(j[2])}</td>
            <td class="r">{fmt_int(j[5])}</td>
            <td class="r">{fmt_pct(j[7])}</td>
            <td class="r muted">{rtp_cat}</td>
            <td class="r">{j[8]}</td>
            <td class="r">{j[6]}</td>
        </tr>\n"""

    html += f"""
        <tr style="font-weight:700; background:#f8f9fa; border-top: 2px solid #dee2e6;">
            <td></td><td>TOTAL</td>
            <td class="r">{fmt_pkr(total_ggr)}</td>
            <td class="r">{fmt_brl(total_ggr)}</td>
            <td class="r">{fmt_usd(total_ggr)}</td>
            <td class="r">{fmt_pkr(total_turn)}</td>
            <td class="r">{fmt_brl(total_turn)}</td>
            <td class="r">{fmt_int(total_giros)}</td>
            <td class="r">{fmt_pct(hold_pct)}</td>
            <td></td><td></td><td></td>
        </tr>
        </tbody></table>

        <h3>Detalhamento por jogador — Top 10 jogos por GGR</h3>
        {_build_game_detail_html(data["detalhe_ggr"], "GGR", whitelist_set)}
    </div>
"""

    # REPORT 2 - TURNOVER
    html += f"""
    <div class="section">
        <h2>Report 2 — Ranking por Turnover <span>(volume total apostado)</span></h2>
        <p class="cambio-note">Taxas: 1 PKR = BRL {PKR_TO_BRL:.6f} | 1 PKR = USD {PKR_TO_USD:.6f} ({FX_DATE})</p>
        <table>
        <thead><tr>
            <th>#</th><th>Jogo</th>
            <th class="r">Turnover (PKR)</th><th class="r">Turnover (BRL)</th><th class="r">Turnover (USD)</th>
            <th class="r">GGR (PKR)</th><th class="r">GGR (BRL)</th>
            <th class="r">Giros</th><th class="r">Hold%</th>
            <th class="r">% Total</th><th class="r">Jogadores</th><th class="r">Dias</th>
        </tr></thead>
        <tbody>
"""
    for i, j in enumerate(jogos_by_turn, 1):
        ggr_class = css_class_ggr(float(j[4]))
        pct = float(j[2]) / total_turn * 100 if total_turn > 0 else 0
        html += f"""<tr>
            <td class="c">{i}</td><td>{j[0]}</td>
            <td class="r">{fmt_pkr(j[2])}</td>
            <td class="r">{fmt_brl(j[2])}</td>
            <td class="r">{fmt_usd(j[2])}</td>
            <td class="r {ggr_class}">{fmt_pkr(j[4])}</td>
            <td class="r {ggr_class}">{fmt_brl(j[4])}</td>
            <td class="r">{fmt_int(j[5])}</td>
            <td class="r">{fmt_pct(j[7])}</td>
            <td class="r">{pct:.1f}%</td>
            <td class="r">{j[8]}</td>
            <td class="r">{j[6]}</td>
        </tr>\n"""

    html += f"""
        <tr style="font-weight:700; background:#f8f9fa; border-top: 2px solid #dee2e6;">
            <td></td><td>TOTAL</td>
            <td class="r">{fmt_pkr(total_turn)}</td>
            <td class="r">{fmt_brl(total_turn)}</td>
            <td class="r">{fmt_usd(total_turn)}</td>
            <td class="r">{fmt_pkr(total_ggr)}</td>
            <td class="r">{fmt_brl(total_ggr)}</td>
            <td class="r">{fmt_int(total_giros)}</td>
            <td class="r">{fmt_pct(hold_pct)}</td>
            <td class="r">100%</td><td></td><td></td>
        </tr>
        </tbody></table>

        <div class="alert-box alert-info">
            <strong>Concentracao top 3 jogos:</strong> {top3_turn_jogos:.1f}% do turnover total
        </div>

        <h3>Detalhamento por jogador — Top 10 jogos por Turnover</h3>
        {_build_game_detail_html(data["detalhe_turn"], "Turnover", whitelist_set)}
    </div>
"""

    # REPORT 3 - GIROS
    html += """
    <div class="section">
        <h2>Report 3 — Ranking por Giros <span>(rodadas jogadas — engajamento)</span></h2>
        <table>
        <thead><tr>
            <th>#</th><th>Jogo</th>
            <th class="r">Giros</th><th class="r">% Total</th>
            <th class="r">Turnover (PKR)</th><th class="r">Turnover (BRL)</th>
            <th class="r">GGR (PKR)</th><th class="r">GGR (BRL)</th>
            <th class="r">Ticket Med (PKR)</th><th class="r">Hold%</th>
            <th class="r">Jogadores</th><th class="r">Dias</th>
        </tr></thead>
        <tbody>
"""
    for i, j in enumerate(jogos_by_giros, 1):
        ggr_class = css_class_ggr(float(j[4]))
        pct = float(j[5]) / total_giros * 100 if total_giros > 0 else 0
        ticket = float(j[2]) / float(j[5]) if j[5] and float(j[5]) > 0 else 0
        html += f"""<tr>
            <td class="c">{i}</td><td>{j[0]}</td>
            <td class="r">{fmt_int(j[5])}</td>
            <td class="r">{pct:.1f}%</td>
            <td class="r">{fmt_pkr(j[2])}</td>
            <td class="r">{fmt_brl(j[2])}</td>
            <td class="r {ggr_class}">{fmt_pkr(j[4])}</td>
            <td class="r {ggr_class}">{fmt_brl(j[4])}</td>
            <td class="r">{fmt_pkr(ticket)}</td>
            <td class="r">{fmt_pct(j[7])}</td>
            <td class="r">{j[8]}</td>
            <td class="r">{j[6]}</td>
        </tr>\n"""

    ticket_medio_total = total_turn / total_giros if total_giros else 0
    html += f"""
        <tr style="font-weight:700; background:#f8f9fa; border-top: 2px solid #dee2e6;">
            <td></td><td>TOTAL</td>
            <td class="r">{fmt_int(total_giros)}</td>
            <td class="r">100%</td>
            <td class="r">{fmt_pkr(total_turn)}</td>
            <td class="r">{fmt_brl(total_turn)}</td>
            <td class="r">{fmt_pkr(total_ggr)}</td>
            <td class="r">{fmt_brl(total_ggr)}</td>
            <td class="r">{fmt_pkr(ticket_medio_total)}</td>
            <td class="r">{fmt_pct(hold_pct)}</td>
            <td></td><td></td>
        </tr>
        </tbody></table>

        <div class="alert-box alert-info">
            <strong>Concentracao top 3 jogos:</strong> {top3_giros_jogos:.1f}% dos giros totais
        </div>

        <h3>Detalhamento por jogador — Top 10 jogos por Giros</h3>
        {_build_game_detail_html(data["detalhe_giros"], "Giros", whitelist_set)}
    </div>
"""

    # ANALISE EXECUTIVA — CONCENTRACAO + TOP 20
    html += """
    <div class="section">
        <h2>Analise Executiva — Concentracao e Riscos</h2>

        <h3>Top 20 Jogadores por Turnover</h3>
        <table>
        <thead><tr>
            <th>#</th><th>Username</th><th>PID</th>
            <th class="r">Turnover (PKR)</th><th class="r">Turnover (BRL)</th><th class="r">Turnover (USD)</th>
            <th class="r">GGR (PKR)</th><th class="r">GGR (BRL)</th>
            <th class="r">Giros</th><th class="r">Jogos</th><th class="r">Dias</th>
            <th class="r">Ticket Med</th>
        </tr></thead>
        <tbody>
"""
    for i, p in enumerate(top_players, 1):
        ggr_class = css_class_ggr(float(p[4]))
        flag = _flag_dpsq(p[0], whitelist_set)
        row_cls = ' class="dp-sq-row"' if p[0] in whitelist_set else ''
        html += f"""<tr{row_cls}>
            <td class="c">{i}</td><td>{p[0]}{flag}</td><td class="mono">{p[1]}</td>
            <td class="r">{fmt_pkr(p[3])}</td>
            <td class="r">{fmt_brl(p[3])}</td>
            <td class="r">{fmt_usd(p[3])}</td>
            <td class="r {ggr_class}">{fmt_pkr(p[4])}</td>
            <td class="r {ggr_class}">{fmt_brl(p[4])}</td>
            <td class="r">{fmt_int(p[5])}</td>
            <td class="r">{p[6]}</td>
            <td class="r">{p[7]}</td>
            <td class="r">{fmt_pkr(p[8]) if p[8] else '—'}</td>
        </tr>\n"""

    html += f"""
        </tbody></table>

        <h3>Concentracao de Turnover</h3>
        <div style="margin: 12px 0;">
            <div style="display:flex; align-items:center; gap:12px; margin: 6px 0;">
                <span style="width:120px; font-size:13px;">Top 1 jogador:</span>
                <div class="concentracao-bar" style="flex:1;">
                    <div class="concentracao-fill {'fill-danger' if top1_pct > 25 else 'fill-warning'}"
                         style="width:{min(top1_pct, 100):.0f}%;">{top1_pct:.1f}%</div>
                </div>
            </div>
            <div style="display:flex; align-items:center; gap:12px; margin: 6px 0;">
                <span style="width:120px; font-size:13px;">Top 3 jogadores:</span>
                <div class="concentracao-bar" style="flex:1;">
                    <div class="concentracao-fill {'fill-danger' if top3_pct > 50 else 'fill-warning'}"
                         style="width:{min(top3_pct, 100):.0f}%;">{top3_pct:.1f}%</div>
                </div>
            </div>
            <div style="display:flex; align-items:center; gap:12px; margin: 6px 0;">
                <span style="width:120px; font-size:13px;">Top 5 jogadores:</span>
                <div class="concentracao-bar" style="flex:1;">
                    <div class="concentracao-fill {'fill-danger' if top5_pct > 60 else 'fill-warning'}"
                         style="width:{min(top5_pct, 100):.0f}%;">{top5_pct:.1f}%</div>
                </div>
            </div>
        </div>

        <h3>Jogos com Poucos Jogadores (Risco de Dependencia)</h3>
        <table>
        <thead><tr>
            <th>Jogo</th><th class="r">Jogadores</th>
            <th class="r">Turnover (PKR)</th><th class="r">Turnover (BRL)</th>
            <th class="r">GGR (PKR)</th><th class="r">GGR (BRL)</th>
            <th class="r">Risco</th>
        </tr></thead>
        <tbody>
"""
    for jc in [j for j in jogos_concentrados if j[1] <= 3]:
        ggr_class = css_class_ggr(float(jc[3]))
        risco = "CRITICO" if jc[1] == 1 else ("ALTO" if jc[1] == 2 else "MEDIO")
        risco_class = "negative" if jc[1] <= 2 else "muted"
        html += f"""<tr>
            <td>{jc[0]}</td><td class="r">{jc[1]}</td>
            <td class="r">{fmt_pkr(jc[2])}</td><td class="r">{fmt_brl(jc[2])}</td>
            <td class="r {ggr_class}">{fmt_pkr(jc[3])}</td>
            <td class="r {ggr_class}">{fmt_brl(jc[3])}</td>
            <td class="r {risco_class}">{risco}</td>
        </tr>\n"""

    html += """
        </tbody></table>
    </div>
"""

    # TENDENCIA DIARIA
    max_turn = float(max(t[1] for t in tendencia)) if tendencia else 1
    today_date = date_cls.today()

    html += """
    <div class="section">
        <h2>Tendencia Diaria</h2>
        <table>
        <thead><tr>
            <th>Data</th>
            <th class="r">Turnover (PKR)</th><th class="r">Turnover (BRL)</th>
            <th class="r">GGR (PKR)</th><th class="r">GGR (BRL)</th>
            <th class="r">Giros</th><th class="r">Jogadores</th><th class="r">Jogos</th>
            <th style="width:200px;">Volume</th>
        </tr></thead>
        <tbody>
"""
    for t in tendencia:
        ggr_class = css_class_ggr(float(t[2]))
        bar_width = float(t[1]) / max_turn * 100 if max_turn > 0 else 0
        bar_color = "#27ae60" if float(t[2]) >= 0 else "#e74c3c"
        is_d0 = (t[0] == today_date)
        day_label = f"{t[0]} *" if is_d0 else str(t[0])
        row_style = ' style="background:#fff8e1; font-style:italic;"' if is_d0 else ''
        html += f"""<tr{row_style}>
            <td>{day_label}</td>
            <td class="r">{fmt_pkr(t[1])}</td><td class="r">{fmt_brl(t[1])}</td>
            <td class="r {ggr_class}">{fmt_pkr(t[2])}</td>
            <td class="r {ggr_class}">{fmt_brl(t[2])}</td>
            <td class="r">{fmt_int(t[3])}</td>
            <td class="r">{t[4]}</td><td class="r">{t[5]}</td>
            <td><div class="trend-bar" style="width:{bar_width:.0f}%; background:{bar_color};"></div></td>
        </tr>\n"""

    html += """
        </tbody></table>
        <div class="alert-box alert-warning" style="margin-top:12px;">
            <strong>* Dados parciais:</strong> o ultimo dia marcado com asterisco (*) ainda esta em andamento.
            Os numeros deste dia nao representam o dia completo e nao devem ser comparados
            diretamente com dias anteriores.
        </div>
    </div>
"""

    # RECOMENDACOES
    html += f"""
    <div class="section" style="border-left: 4px solid #27ae60;">
        <h2>Recomendacoes e Proximos Passos</h2>
        <table class="detail-table">
        <thead><tr>
            <th style="width:40px;">#</th>
            <th style="width:100px;">Prioridade</th>
            <th>Recomendacao</th>
            <th style="width:150px;">Responsavel sugerido</th>
        </tr></thead>
        <tbody>
            <tr>
                <td class="c">1</td>
                <td><span class="negative" style="font-weight:700;">URGENTE</span></td>
                <td><strong>Reduzir dependencia do jogador {top_players[0][0]}.</strong>
                    Representa {top1_pct:.1f}% do turnover total. Se parar de jogar, a operacao perde 1/3 do volume.
                    Estrategia: ativar jogadores mid-stake via CRM (campanhas para top 10-20), diversificar base.</td>
                <td>CRM / Marketing</td>
            </tr>
            <tr>
                <td class="c">2</td>
                <td><span class="negative" style="font-weight:700;">URGENTE</span></td>
                <td><strong>Avaliar CRASH II (prejuizo de {fmt_pkr(abs(float(crash2[4]))) if crash2 else 'n/a'}).</strong>
                    Opcoes: desativar o jogo, limitar stake maximo, ou monitorar por mais 7 dias com limite de perda.
                    CRASH (sem o II) e lucrativo — o problema e especifico do CRASH II.</td>
                <td>Operacoes / Provider</td>
            </tr>
            <tr>
                <td class="c">3</td>
                <td style="color:#f39c12; font-weight:700;">ALTA</td>
                <td><strong>Revisar catalogo de jogos com GGR negativo persistente.</strong>
                    {len(jogos_ggr_neg)} jogos estao dando prejuizo. Para jogos com poucos jogadores e GGR negativo,
                    avaliar remocao do catalogo. Manter apenas os que atraem volume (ex: Fortune Tiger, Lucky Neko).</td>
                <td>Operacoes / Produto</td>
            </tr>
            <tr>
                <td class="c">4</td>
                <td style="color:#f39c12; font-weight:700;">ALTA</td>
                <td><strong>Investigar jogos com 1-2 jogadores e alto turnover.</strong>
                    TEEN PATTI (Rs 98K, 1 jogador), ZOO ROULETTE (Rs 39K, 2 jogadores) — se esses jogadores saem,
                    o jogo fica sem receita. Avaliar se vale promover para mais jogadores ou descontinuar.</td>
                <td>Produto / CRM</td>
            </tr>
            <tr>
                <td class="c">5</td>
                <td style="color:#3498db; font-weight:700;">MEDIA</td>
                <td><strong>Ampliar base de jogadores ativos.</strong>
                    Apenas ~150-200 jogadores unicos no periodo. A operacao precisa de escala para diluir o risco
                    de concentracao. Focar em aquisicao + conversao FTD + retencao semanal.</td>
                <td>Marketing / CRM</td>
            </tr>
            <tr>
                <td class="c">6</td>
                <td style="color:#3498db; font-weight:700;">MEDIA</td>
                <td><strong>Monitorar o pico de 07-08/04.</strong>
                    O turnover saltou de Rs 66K para Rs 548K-951K nesses dias. Investigar se foi whale activity
                    (mehmood88), campanha promocional, ou evento especifico. Entender o que causou para replicar.</td>
                <td>Analytics / CRM</td>
            </tr>
        </tbody></table>
    </div>
"""

    # CONTAS DE TESTE
    html += f"""
    <div class="section">
        <h2>Contas de Teste Excluidas <span>({len(test_accounts)} contas removidas dos calculos)</span></h2>
        <p style="font-size:13px; color:#5a6c7d; margin-bottom:8px;">
            <strong>Criterios (UNION de 2 regras):</strong><br>
            <strong>(a) Heuristica:</strong> role diferente de USER, username contendo "test"/"teste"/"demo"/"admin",
            emails internos (@multi.bet.br, @grupo-pgs.com, @karinzitta.dev, @supernovagaming, @play4tune).<br>
            <strong>(b) Logica oficial do time de dev (16/04/2026):</strong> usuario com manipulacao manual de saldo
            (ADJUSTMENT_CREDIT / ADJUSTMENT_DEBIT) OU confirmacao manual de deposito (reviewed_by preenchido).
        </p>
""" + (f"""
        <p style="font-size:12px; color:#856404; background:#fff9e6; padding:8px 12px; border-left:3px solid #ffc107; border-radius:4px; margin-top:8px;">
            Alem das contas excluidas acima, <strong>{len(data["whitelisted"])} jogador(es) tiveram ajuste manual de saldo
            mas foram mantidos no report</strong> por serem reais (caso DP/SQ).
            Aparecem nas tabelas com a tag <span class="dp-sq-flag">DP/SQ</span>.
            Ver secao <em>Legenda de Flags</em> no final do report.
        </p>
""" if data["whitelisted"] else "") + """

        <h3>Contas com atividade (impacto nos dados):</h3>
        <table class="detail-table">
        <thead><tr>
            <th>Username</th><th>PID</th><th>Role</th><th>Email</th><th class="r">Bets</th><th class="r">Depositos</th>
        </tr></thead>
        <tbody>
"""
    active_test_usernames = {
        'testeste', 'teste977807', 'contato299776',
        'filipe.molon367635', 'marcelofresendeo330398',
    }
    for ta in test_accounts:
        if ta[1] in active_test_usernames:
            html += f"""<tr>
                <td>{ta[1]}</td><td class="mono">{ta[2]}</td>
                <td>{ta[3]}</td><td class="muted">{ta[4] or '—'}</td>
                <td class="r">sim</td><td class="r">sim</td>
            </tr>\n"""

    html += """
        </tbody></table>

        <h3>Lista completa:</h3>
        <ul class="test-list">
"""
    for ta in test_accounts:
        html += f"<li>{ta[1]} ({ta[3]}){' — ' + ta[4] if ta[4] else ''}</li>\n"

    html += "        </ul>\n    </div>\n"

    # LEGENDA DE FLAGS (so aparece se houver whitelisted no report)
    if whitelist_set:
        whitelisted_rows = "".join(f"""
                <tr>
                    <td>{w[1]}</td><td class="mono">{w[2]}</td><td>{w[3]}</td>
                    <td class="muted">{w[4] or '—'}</td>
                </tr>""" for w in data["whitelisted"])
        html += f"""
    <div class="section" style="border-left: 4px solid #ffc107;">
        <h2>Legenda de Flags</h2>
        <p style="font-size:13px; color:#2c3e50; line-height:1.7;">
            Alguns jogadores aparecem nas tabelas com uma tag laranja ao lado do nome.
            Abaixo a explicacao de cada flag e a lista completa de jogadores sinalizados.
        </p>

        <h3><span class="dp-sq-flag">DP/SQ</span>&nbsp; Ajuste operacional de deposito/saque</h3>
        <p style="font-size:13px; color:#2c3e50; line-height:1.7;">
            <strong>O que significa:</strong> o deposito do jogador <strong>nao entrou no caixa da casa</strong>
            (falha operacional de gateway/fluxo). Para nao deixar o jogador no prejuizo, o suporte
            <strong>ajustou manualmente o saldo dele</strong> via transacao do tipo <code>ADJUSTMENT_CREDIT</code>.
            O valor ficou com o jogador como "presente" — ele pode apostar, sacar e movimentar normalmente.
        </p>
        <p style="font-size:13px; color:#2c3e50; line-height:1.7;">
            <strong>Por que aparece no report:</strong> o jogador e <strong>real e legitimo</strong>,
            entao suas apostas, turnover e giros entram nos totais. A flag DP/SQ e apenas sinalizacao
            para o leitor saber que houve intervencao operacional no saldo daquele usuario.
        </p>
        <p style="font-size:13px; color:#2c3e50; line-height:1.7;">
            <strong>Implicacao financeira:</strong> a casa absorveu a perda do ajuste manual
            (nao houve entrada real de caixa), mas a movimentacao do jogador no cassino depois disso
            e contabilizada normalmente no GGR/turnover.
        </p>
        <p style="font-size:13px; color:#2c3e50;">
            <strong>Validacao:</strong> classificacao confirmada pelo time de dev em 16/04/2026. Revisado periodicamente.
        </p>

        <h3>Jogadores sinalizados com DP/SQ ({len(data["whitelisted"])}):</h3>
        <table class="detail-table">
        <thead><tr>
            <th>Username</th><th>PID</th><th>Role</th><th>Email</th>
        </tr></thead>
        <tbody>{whitelisted_rows}
        </tbody></table>
    </div>
"""

    # FONTE DOS DADOS
    html += f"""
    <div class="section">
        <h2>Fonte dos Dados e Conversao</h2>
        <table class="detail-table">
        <thead><tr><th>De</th><th>Para</th><th>Taxa</th><th>Data</th></tr></thead>
        <tbody>
            <tr><td>1 PKR</td><td>BRL</td><td>R$ {PKR_TO_BRL:.6f}</td><td>{FX_DATE}</td></tr>
            <tr><td>1 PKR</td><td>USD</td><td>$ {PKR_TO_USD:.6f}</td><td>{FX_DATE}</td></tr>
            <tr><td>1 USD</td><td>PKR</td><td>Rs {1/PKR_TO_USD:,.2f}</td><td>{FX_DATE}</td></tr>
            <tr><td>1 BRL</td><td>PKR</td><td>Rs {1/PKR_TO_BRL:,.2f}</td><td>{FX_DATE}</td></tr>
        </tbody></table>
        <p style="font-size:13px; color:#5a6c7d; margin-top:12px;">
            <strong>Banco:</strong> supernova_bet (PostgreSQL 15.14) |
            <strong>Tabelas:</strong> casino_game_metrics, casino_user_game_metrics, casino_games, users |
            <strong>Provider:</strong> 2J Games (unico ativo) |
            <strong>Operacao:</strong> 100% Casino |
            <strong>Timezone:</strong> UTC (timestamps sem timezone) |
            <strong>Valores:</strong> PKR direto (nao centavos) |
            <strong>Fonte FX:</strong> {FX_SOURCE} |
            <strong>Extracao:</strong> {ts}
        </p>
    </div>

    <div class="footer">
        Report gerado em {ts} | Play4Tune (P4T) — Super Nova Gaming |
        Periodo: {periodo[0]} a {periodo[1]} ({periodo[2]} dias) |
        {len(test_accounts)} contas de teste excluidas
    </div>

</div>
</body>
</html>
"""
    return _add_tooltips(html)


# =============================================
# SAVE REPORT (arquivo + versao datada)
# =============================================
def save_report(html, out_dir="reports", basename="report_jogos_play4tune"):
    """Salva HTML em 2 lugares: _FINAL.html (latest) + {basename}_DDMMYYYY_FINAL.html (historico)."""
    os.makedirs(out_dir, exist_ok=True)
    latest = os.path.join(out_dir, f"{basename}_FINAL.html")
    dated = os.path.join(out_dir, f"{basename}_{datetime.now(BRT).strftime('%d%m%Y')}_FINAL.html")
    for path in (latest, dated):
        with open(path, "w", encoding="utf-8") as f:
            f.write(html)
    return latest, dated


# =============================================
# ORQUESTRADOR
# =============================================
def run_report():
    """Pipeline completo: conecta -> fetch -> compute -> validate -> build -> save."""
    tunnel, conn, cur = connect()
    try:
        data = fetch_data(cur)
    finally:
        _safe_close(tunnel, conn, cur)

    log.info("Dados coletados. Computando metricas...")
    metrics = compute_metrics(data)

    log.info("Validando sanidade...")
    sanity_checks(data, metrics)

    periodo = data["periodo"]
    log.info(f"Periodo: {periodo[0]} a {periodo[1]} ({periodo[2]} dias) | "
             f"{len(data['jogos_all'])} jogos | {len(data['top_players'])} top players | "
             f"{len(data['test_accounts'])} contas teste excluidas")

    log.info("Gerando HTML...")
    ts = datetime.now(BRT).strftime("%d/%m/%Y %H:%M BRT")
    html = build_html(data, metrics, ts)

    latest, dated = save_report(html)

    log.info(f"Report HTML salvo: {latest} ({len(html):,} bytes)")
    log.info(f"Copia versionada: {dated}")
    log.info(f"Totais: GGR {fmt_pkr(metrics['total_ggr'])} | "
             f"Turnover {fmt_pkr(metrics['total_turn'])} | "
             f"Giros {fmt_int(metrics['total_giros'])}")


if __name__ == "__main__":
    try:
        run_report()
    except Exception as e:
        log.exception(f"FALHA pipeline Play4Tune report: {e}")
        sys.exit(1)
