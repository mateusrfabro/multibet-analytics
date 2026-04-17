"""
Gera 2 entregas para demo CTO/Castrin das views vw_front_*:
  1. reports/views_front_demo_<DATA>.html  → preview visual com amostras
  2. reports/views_front_demo_<DATA>.sql   → script SQL comentado para rodar no DBeaver

Cada view e explicada em 3 partes:
  - O que retorna
  - Para qual secao do front
  - SQL exemplo
"""
import sys, os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.supernova import execute_supernova

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DATA = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
DATAHORA = datetime.now(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M BRT")

REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)

HTML_PATH = os.path.join(REPORTS_DIR, f"views_front_demo_{DATA}.html")
SQL_PATH  = os.path.join(REPORTS_DIR, f"views_front_demo_{DATA}.sql")


# ─── Definicoes das views (ordem de apresentacao) ────────────────────────────

VIEWS = [
    {
        "nome": "vw_front_top_24h",
        "icone": "🔥",
        "titulo": "Mais jogados nas ultimas 24h",
        "para_que": "Carrossel principal 'Mais jogados' (rank 1 = jogo mais jogado nas ultimas 24h rolantes)",
        "exemplo_uso_front": "Mostra os top 50 com posicionamento ja calculado (rank 1, 2, 3...)",
        "sql_amostra": """
            SELECT rank, game_name, vendor, category, live_subtype,
                   rounds_24h, players_24h,
                   TO_CHAR(window_end_utc AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI') AS atualizado_em_brt
            FROM multibet.vw_front_top_24h
            ORDER BY rank
            LIMIT 15
        """,
    },
    {
        "nome": "vw_front_live_casino",
        "icone": "🎰",
        "titulo": "Cassino ao Vivo (com subtipo)",
        "para_que": "Filtros por Roleta / Blackjack / Baccarat / GameShow no Cassino ao Vivo",
        "exemplo_uso_front": "Front passa filtro: WHERE live_subtype = 'Roleta' → mostra so roletas",
        "sql_amostra": """
            SELECT live_subtype, COUNT(*) AS qtd_jogos
            FROM multibet.vw_front_live_casino
            GROUP BY live_subtype
            ORDER BY qtd_jogos DESC
        """,
        "sql_amostra2": """
            -- Top 10 roletas
            SELECT game_name, vendor, rounds_24h, rank
            FROM multibet.vw_front_live_casino
            WHERE live_subtype = 'Roleta'
            ORDER BY COALESCE(rank, 999999), game_name
            LIMIT 10
        """,
    },
    {
        "nome": "vw_front_by_vendor",
        "icone": "🏷️",
        "titulo": "Por Provedor (Pragmatic, PG Soft, etc)",
        "para_que": "Carrossel 'Jogos Pragmatic', 'Jogos PG Soft' — vendor_id agrupa por marca",
        "exemplo_uso_front": "Front passa: WHERE vendor = 'pragmaticplay' → carrossel Pragmatic",
        "sql_amostra": """
            SELECT vendor, COUNT(*) AS qtd_jogos
            FROM multibet.vw_front_by_vendor
            GROUP BY vendor
            ORDER BY qtd_jogos DESC
            LIMIT 8
        """,
        "sql_amostra2": """
            -- Top 10 jogos da Pragmatic
            SELECT game_name, category, live_subtype, rounds_24h, rank
            FROM multibet.vw_front_by_vendor
            WHERE vendor = 'pragmaticplay'
            ORDER BY COALESCE(rank, 999999)
            LIMIT 10
        """,
    },
    {
        "nome": "vw_front_by_category",
        "icone": "📂",
        "titulo": "Por Categoria (Slots / Live)",
        "para_que": "Filtros macro: Slots vs Live. Backup do front se nao quiser usar live_subtype",
        "exemplo_uso_front": "Front passa: WHERE category = 'live' → todos jogos ao vivo",
        "sql_amostra": """
            SELECT category, category_desc, COUNT(*) AS qtd_jogos
            FROM multibet.vw_front_by_category
            GROUP BY category, category_desc
            ORDER BY qtd_jogos DESC
        """,
    },
    {
        "nome": "vw_front_jackpot",
        "icone": "💎",
        "titulo": "Jogos com Jackpot",
        "para_que": "Carrossel 'Jackpots' — todos jogos onde has_jackpot = TRUE",
        "exemplo_uso_front": "Front consome direto, ja vem filtrado",
        "sql_amostra": """
            SELECT game_name, vendor, category, rank
            FROM multibet.vw_front_jackpot
            LIMIT 10
        """,
        "nota_atencao": "ATENCAO: atualmente retorna 0 linhas. Fonte vendor_ec2.tbl_vendor_games_mapping_mst esta vazia/sem acesso. Em investigacao com Mauro/Gusta.",
    },
]


# ─── Helpers ─────────────────────────────────────────────────────────────────

def fetch(sql):
    rows = execute_supernova(sql, fetch=True)
    return rows or []


def cols_from_select(sql):
    """Extrai nome das colunas da clausula SELECT (parser simples)."""
    s = sql.strip()
    # remove comentarios -- ate o fim da linha
    s = "\n".join(line.split("--")[0] for line in s.splitlines())
    s_upper = s.upper()
    sel_idx = s_upper.find("SELECT")
    from_idx = s_upper.find(" FROM ", sel_idx)
    select_clause = s[sel_idx + 6 : from_idx].strip()

    cols = []
    depth = 0
    cur = ""
    for ch in select_clause:
        if ch == "(":
            depth += 1
        elif ch == ")":
            depth -= 1
        if ch == "," and depth == 0:
            cols.append(cur.strip())
            cur = ""
        else:
            cur += ch
    if cur.strip():
        cols.append(cur.strip())

    aliases = []
    for c in cols:
        c_upper = c.upper()
        if " AS " in c_upper:
            alias = c[c_upper.rfind(" AS ") + 4 :].strip()
        else:
            parts = c.split(".")
            alias = parts[-1].strip()
        aliases.append(alias)
    return aliases


def render_table_html(cols, rows):
    if not rows:
        return '<p class="empty">⚠️ Nenhuma linha (view ainda sem dados).</p>'
    html = ['<table class="data">', "<thead><tr>"]
    for c in cols:
        html.append(f"<th>{c}</th>")
    html.append("</tr></thead><tbody>")
    for r in rows:
        html.append("<tr>")
        for v in r:
            txt = "" if v is None else str(v)
            html.append(f"<td>{txt}</td>")
        html.append("</tr>")
    html.append("</tbody></table>")
    return "".join(html)


# ─── Stats globais (header) ──────────────────────────────────────────────────

def get_stats():
    rows = fetch("""
        SELECT
            COUNT(*) AS total_jogos,
            SUM(CASE WHEN game_image_url IS NOT NULL THEN 1 ELSE 0 END) AS com_imagem,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS ativos,
            SUM(CASE WHEN popularity_rank_24h IS NOT NULL THEN 1 ELSE 0 END) AS com_rank_24h,
            MAX(popularity_window_end) AS ultima_janela
        FROM multibet.game_image_mapping
    """)
    return rows[0] if rows else (0, 0, 0, 0, None)


# ─── Gerar HTML ──────────────────────────────────────────────────────────────

def gerar_html():
    total, com_img, ativos, com_rank, ultima = get_stats()
    ultima_str = ultima.astimezone(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M") if ultima else "—"

    html = [f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="UTF-8">
<title>Demo Views vw_front_* — {DATAHORA}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, "Segoe UI", Roboto, Arial, sans-serif;
         margin: 0; padding: 24px; background: #f6f7fb; color: #1a1f2e; }}
  .container {{ max-width: 1180px; margin: 0 auto; }}
  h1 {{ color: #0a2540; margin: 0 0 8px; font-size: 28px; }}
  h2 {{ color: #0a2540; margin: 32px 0 12px; font-size: 22px;
       border-bottom: 2px solid #635bff; padding-bottom: 6px; }}
  .subtitle {{ color: #525f7f; margin-bottom: 28px; font-size: 14px; }}
  .stats {{ display: grid; grid-template-columns: repeat(5, 1fr); gap: 12px; margin: 20px 0 36px; }}
  .stat {{ background: #fff; padding: 14px; border-radius: 8px; box-shadow: 0 1px 3px rgba(0,0,0,0.06); }}
  .stat .label {{ color: #6b7280; font-size: 12px; text-transform: uppercase; letter-spacing: .5px; }}
  .stat .value {{ font-size: 24px; font-weight: 600; color: #0a2540; margin-top: 4px; }}
  .view-card {{ background: #fff; border-radius: 10px; padding: 24px; margin-bottom: 24px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.06); }}
  .view-card h2 {{ margin-top: 0; }}
  .view-name {{ background: #ebeefe; color: #635bff; padding: 4px 10px; border-radius: 4px;
                font-family: 'Courier New', monospace; font-size: 13px; }}
  .desc {{ color: #525f7f; margin: 8px 0 16px; line-height: 1.5; }}
  .desc strong {{ color: #0a2540; }}
  .nota-atencao {{ background: #fff7e6; border-left: 4px solid #f0a500; padding: 12px;
                   border-radius: 4px; margin: 12px 0; color: #5e3c00; font-size: 13px; }}
  table.data {{ width: 100%; border-collapse: collapse; margin: 12px 0; font-size: 13px; }}
  table.data thead th {{ background: #0a2540; color: white; padding: 8px 10px; text-align: left;
                         font-weight: 600; font-size: 12px; text-transform: uppercase; }}
  table.data tbody td {{ padding: 7px 10px; border-bottom: 1px solid #e6e9f0; }}
  table.data tbody tr:nth-child(even) {{ background: #fafbfd; }}
  .sql {{ background: #1a1f2e; color: #abb2bf; padding: 12px; border-radius: 6px;
          font-family: 'Courier New', monospace; font-size: 12px; overflow-x: auto;
          white-space: pre; line-height: 1.4; }}
  .legend-section {{ background: #fff; padding: 20px; border-radius: 10px; margin-top: 32px;
                     box-shadow: 0 1px 3px rgba(0,0,0,0.06); }}
  .legend-section h3 {{ color: #0a2540; margin-top: 0; }}
  .empty {{ color: #999; font-style: italic; }}
  @media print {{ body {{ background: white; padding: 0; }}
                  .view-card, .stat, .legend-section {{ box-shadow: none; border: 1px solid #ddd; }} }}
</style>
</head>
<body>
<div class="container">

<h1>🎯 Views <code>vw_front_*</code> — Demo para validacao</h1>
<p class="subtitle">
  <strong>Para:</strong> CTO Gabriel Barbosa (via Castrin) &nbsp;|&nbsp;
  <strong>Gerado:</strong> {DATAHORA} &nbsp;|&nbsp;
  <strong>Banco:</strong> Super Nova DB (PostgreSQL) — schema <code>multibet.*</code>
</p>

<div class="stats">
  <div class="stat"><div class="label">Total jogos catalogados</div><div class="value">{total:,}</div></div>
  <div class="stat"><div class="label">Com imagem</div><div class="value">{com_img:,}</div></div>
  <div class="stat"><div class="label">Ativos</div><div class="value">{ativos:,}</div></div>
  <div class="stat"><div class="label">Com atividade 24h</div><div class="value">{com_rank:,}</div></div>
  <div class="stat"><div class="label">Ultima atualizacao</div><div class="value" style="font-size:18px">{ultima_str}</div></div>
</div>
"""]

    # 1 secao por view
    for v in VIEWS:
        html.append(f"""
<div class="view-card">
  <h2>{v['icone']} {v['titulo']} <span class="view-name">multibet.{v['nome']}</span></h2>
  <p class="desc">
    <strong>Para que serve:</strong> {v['para_que']}<br>
    <strong>Como o front consome:</strong> {v['exemplo_uso_front']}
  </p>
""")

        if "nota_atencao" in v:
            html.append(f'<div class="nota-atencao">⚠️ {v["nota_atencao"]}</div>')

        # SQL 1 + tabela
        sql = v["sql_amostra"].strip()
        cols = cols_from_select(sql)
        rows = fetch(sql)
        html.append(f"<div class='sql'>{sql}</div>")
        html.append(render_table_html(cols, rows))

        # SQL 2 (se existir)
        if "sql_amostra2" in v:
            sql2 = v["sql_amostra2"].strip()
            cols2 = cols_from_select(sql2)
            rows2 = fetch(sql2)
            html.append(f"<div class='sql'>{sql2}</div>")
            html.append(render_table_html(cols2, rows2))

        html.append("</div>")

    # Legenda final
    html.append("""
<div class="legend-section">
  <h3>📖 Como ler este relatorio</h3>
  <ul>
    <li><strong>rank:</strong> 1 = jogo mais jogado nas ultimas 24h rolantes (1 = mais quente do momento)</li>
    <li><strong>rounds_24h:</strong> Total de rodadas/apostas registradas nas ultimas 24h</li>
    <li><strong>players_24h:</strong> Jogadores unicos nas ultimas 24h</li>
    <li><strong>category:</strong> <code>slots</code> | <code>live</code> | <code>(NULL para outros como DrawGames)</code></li>
    <li><strong>live_subtype:</strong> Roleta | Blackjack | Baccarat | GameShow | Outros (regex automatico — versao 1)</li>
    <li><strong>vendor:</strong> Identificador da marca/provider (pragmaticplay, alea_redtiger, etc.)</li>
    <li><strong>image_url:</strong> URL CDN da multi.bet pronta para o front consumir</li>
    <li><strong>slug:</strong> Path para abrir o jogo no front (ex: <code>/pb/gameplay/fortune_ox/real-game</code>)</li>
  </ul>

  <h3>🕐 Janela 24h rolante</h3>
  <p>
    O <code>rounds_24h</code>/<code>rank</code> reflete os ultimos 86.400 segundos a partir do momento do refresh.
    <br><strong>Refresh planejado:</strong> 00, 04, 08, 12, 16, 20 BRT (a cada 4h via cron na EC2 ETL).
  </p>
  <p>
    Exemplo: refresh as 16h00 BRT considera atividade de 15h59 do dia anterior ate 16h00 do dia atual.
    Refresh as 20h00 considera 20h00 do dia anterior ate 20h00 do dia atual.
    O timestamp da janela esta em <code>window_end_utc</code> em cada view.
  </p>

  <h3>🛡️ Validacao anti-erro (slot caindo no Live ao vivo)</h3>
  <p>
    A view <code>vw_front_live_casino</code> filtra <code>WHERE game_category = 'live'</code> no nosso lado.
    A categoria vem da <strong>fonte oficial Pragmatic</strong> (catalogo bireports). Se algum slot estiver
    classificado errado, e na origem (BackOffice Pragmatic) — corrigir la.
  </p>

  <h3>♻️ Atualizacao incremental ("preserva valor antigo se novo for NULL")</h3>
  <p>
    O upsert usa <code>COALESCE(EXCLUDED.coluna, atual)</code> — se a fonte nao retornar valor para um campo,
    o valor anterior eh mantido (cobre o caso "nao tem numero atualizado, mantem o antigo" pedido pelo CTO).
    <br>Excecao: <code>rounds_24h</code>/<code>rank</code> SEMPRE atualizam (refletem janela atual — se jogo
    nao teve atividade nas ultimas 24h, vira NULL/0 intencionalmente).
  </p>
</div>

</div>
</body>
</html>
""")

    with open(HTML_PATH, "w", encoding="utf-8") as f:
        f.write("".join(html))
    print(f"OK: {HTML_PATH}")


# ─── Gerar SQL comentado ─────────────────────────────────────────────────────

def gerar_sql():
    lines = [
        "-- ============================================================",
        "-- VIEWS vw_front_* — Demo para CTO/Castrin",
        f"-- Gerado em: {DATAHORA}",
        "-- Banco: Super Nova DB (PostgreSQL) — schema multibet.*",
        "-- ============================================================",
        "-- Cole/rode bloco a bloco no DBeaver para conferir cada view.",
        "-- ============================================================",
        "",
    ]

    # Stats globais
    lines += [
        "-- 0. STATS GLOBAIS (sanity check do refresh)",
        "SELECT",
        "    COUNT(*) AS total_jogos,",
        "    SUM(CASE WHEN game_image_url IS NOT NULL THEN 1 ELSE 0 END) AS com_imagem,",
        "    SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS ativos,",
        "    SUM(CASE WHEN popularity_rank_24h IS NOT NULL THEN 1 ELSE 0 END) AS com_atividade_24h,",
        "    MAX(popularity_window_end) AS ultima_janela_24h",
        "FROM multibet.game_image_mapping;",
        "",
    ]

    for i, v in enumerate(VIEWS, start=1):
        lines += [
            f"-- ============================================================",
            f"-- {i}. {v['titulo']}",
            f"--    View: multibet.{v['nome']}",
            f"--    Para que: {v['para_que']}",
            f"--    Como front consome: {v['exemplo_uso_front']}",
        ]
        if "nota_atencao" in v:
            lines.append(f"--    ⚠️ {v['nota_atencao']}")
        lines.append("-- ============================================================")
        lines.append(v["sql_amostra"].strip() + ";")
        lines.append("")

        if "sql_amostra2" in v:
            lines.append(v["sql_amostra2"].strip() + ";")
            lines.append("")

    lines += [
        "-- ============================================================",
        "-- LEGENDA",
        "-- ============================================================",
        "-- rank             1 = jogo mais jogado nas ultimas 24h rolantes",
        "-- rounds_24h       total de rodadas/apostas nas ultimas 24h",
        "-- players_24h      jogadores unicos nas ultimas 24h",
        "-- category         slots | live | (NULL=DrawGames/outros)",
        "-- live_subtype     Roleta | Blackjack | Baccarat | GameShow | Outros",
        "-- vendor           pragmaticplay, alea_redtiger, alea_pgsoft, etc",
        "-- image_url        URL CDN multi.bet pronta para o front",
        "-- slug             path para abrir o jogo (ex: /pb/gameplay/fortune_ox/real-game)",
        "-- window_end_utc   timestamp UTC do fim da janela 24h (=hora do refresh)",
        "",
        "-- Refresh planejado: 00, 04, 08, 12, 16, 20 BRT (cron 4h EC2 ETL)",
        "-- Apos deploy EC2, a tabela atualiza sozinha — front nao precisa fazer nada.",
    ]

    with open(SQL_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    print(f"OK: {SQL_PATH}")


if __name__ == "__main__":
    print("=== Gerando demo views_front ===")
    gerar_html()
    gerar_sql()
    print("=== Concluido ===")
