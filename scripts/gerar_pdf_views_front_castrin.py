"""
Gera PDF executivo para validacao do Head (Castrin) sobre as views vw_front_*

Demanda: CTO Gabriel Barbosa (via Castrin) — 17/04/2026
Entrega: views para consumo da categories-api (Yuki) + carrosseis do front
"""
import sys, os
from datetime import datetime, timezone, timedelta
from fpdf import FPDF

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.supernova import execute_supernova

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
REPORTS_DIR = os.path.join(PROJECT_ROOT, "reports")
os.makedirs(REPORTS_DIR, exist_ok=True)
DATA = datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d")
DATAHORA = datetime.now(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M BRT")
PDF_PATH = os.path.join(REPORTS_DIR, f"views_front_entrega_castrin_{DATA}.pdf")


def sanitize(txt):
    """Remove caracteres nao-latin1 para o FPDF."""
    if txt is None:
        return ""
    if not isinstance(txt, str):
        txt = str(txt)
    return (txt
            .replace("\u2014", "-").replace("\u2013", "-")
            .replace("\u2018", "'").replace("\u2019", "'")
            .replace("\u201c", '"').replace("\u201d", '"')
            .replace("\u2026", "...").replace("\u00a0", " ")
            .replace("\u25b6", ">").replace("\u2705", "[OK]")
            .replace("\u26a0", "[!]").replace("\u274c", "[X]")
            .encode("latin-1", "replace").decode("latin-1"))


class PDF(FPDF):
    def header(self):
        if self.page_no() > 1:
            self.set_font("Helvetica", "I", 8)
            self.set_text_color(120, 120, 120)
            self.cell(95, 6, "Views vw_front_* - Entrega para validacao | MultiBet", align="L")
            self.cell(95, 6, f"Pagina {self.page_no()}/{{nb}}", align="R", new_x="LMARGIN", new_y="NEXT")
            self.set_draw_color(200, 200, 200)
            self.line(10, 14, 200, 14)
            self.ln(4)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "I", 8)
        self.set_text_color(120, 120, 120)
        self.cell(0, 10, sanitize(f"Gerado em {DATAHORA} - Mateus Fabro (Analista de Dados)"), align="C")

    def titulo(self, num, texto):
        self.ln(4)
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(0, 70, 150)
        self.cell(0, 8, sanitize(f"{num}. {texto}"), new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(0, 102, 204)
        self.line(10, self.get_y(), 200, self.get_y())
        self.ln(4)

    def sub(self, texto):
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(60, 60, 60)
        self.cell(0, 7, sanitize(texto), new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def txt(self, texto):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.multi_cell(0, 5.5, sanitize(texto))
        self.ln(2)

    def bullet(self, texto):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 30, 30)
        self.set_x(12)
        self.cell(6, 5.5, sanitize("-"))
        self.multi_cell(180, 5.5, sanitize(texto))

    def code(self, sql):
        self.set_font("Courier", "", 8)
        self.set_fill_color(245, 245, 250)
        self.set_text_color(40, 40, 40)
        for line in sql.strip().splitlines():
            self.cell(0, 4.5, sanitize(line), fill=True, new_x="LMARGIN", new_y="NEXT")
        self.ln(2)

    def tabela(self, headers, rows, widths=None):
        if not widths:
            w = 190 / len(headers)
            widths = [w] * len(headers)
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(0, 70, 150)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(widths[i], 7, sanitize(h), border=1, fill=True, align="C")
        self.ln()
        self.set_font("Helvetica", "", 8)
        self.set_text_color(30, 30, 30)
        for row in rows:
            for i, val in enumerate(row):
                self.cell(widths[i], 6, sanitize(val), border=1, align="L" if i == 0 else "C")
            self.ln()
        self.ln(3)

    def box(self, cor, titulo_box, linhas):
        cores = {
            "green":  (230, 245, 230, 34, 139, 34),
            "orange": (255, 240, 230, 200, 100, 50),
            "blue":   (220, 235, 255, 0, 102, 204),
            "red":    (255, 230, 230, 200, 50, 50),
        }
        bg = cores.get(cor, cores["blue"])
        self.set_fill_color(bg[0], bg[1], bg[2])
        self.set_draw_color(bg[3], bg[4], bg[5])
        h = 9 + len(linhas) * 5.5
        y0 = self.get_y()
        if y0 + h > 280:
            self.add_page()
            y0 = self.get_y()
        self.rect(10, y0, 190, h, style="DF")
        self.set_xy(14, y0 + 2)
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(bg[3], bg[4], bg[5])
        self.cell(0, 6, sanitize(titulo_box), new_x="LMARGIN", new_y="NEXT")
        self.set_xy(14, self.get_y() - 1)
        self.set_font("Helvetica", "", 9)
        self.set_text_color(30, 30, 30)
        for linha in linhas:
            self.set_x(14)
            self.multi_cell(182, 5.5, sanitize(linha))
        self.set_y(y0 + h + 3)


def get_amostra_top(limit=10):
    rows = execute_supernova(f"""
        SELECT "rank", "name", "provider", "category", "totalBets",
               "totalBet", "totalWins"
        FROM multibet.vw_front_api_games
        WHERE "rank" IS NOT NULL
        ORDER BY "rank"
        LIMIT {limit}
    """, fetch=True)
    return [(str(r[0]), r[1][:22], (r[2] or "")[:18], (r[3] or ""),
             f"{r[4]:,}", f"R$ {float(r[5]):,.2f}", f"R$ {float(r[6]):,.2f}")
            for r in rows]


def get_stats():
    rows = execute_supernova("""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN game_image_url IS NOT NULL THEN 1 ELSE 0 END) AS com_img,
            SUM(CASE WHEN is_active THEN 1 ELSE 0 END) AS ativos,
            SUM(CASE WHEN popularity_rank_24h IS NOT NULL THEN 1 ELSE 0 END) AS com_ativ,
            COALESCE(SUM(total_bet_24h), 0) AS turnover,
            COALESCE(SUM(total_wins_24h), 0) AS wins
        FROM multibet.game_image_mapping
    """, fetch=True)
    return rows[0]


def get_live_subtype_dist():
    rows = execute_supernova("""
        SELECT live_subtype, COUNT(*) AS qtd
        FROM multibet.game_image_mapping
        WHERE game_category = 'live' AND is_active = TRUE
        GROUP BY live_subtype
        ORDER BY qtd DESC
    """, fetch=True)
    return [(str(r[0] or "(NULL)"), str(r[1])) for r in rows]


def get_vendor_top():
    rows = execute_supernova("""
        SELECT vendor_id, COUNT(*) AS qtd
        FROM multibet.game_image_mapping
        WHERE is_active = TRUE AND game_image_url IS NOT NULL
        GROUP BY vendor_id
        ORDER BY qtd DESC
        LIMIT 8
    """, fetch=True)
    return [(r[0] or "NULL", str(r[1])) for r in rows]


# ============================================================
# GERAR PDF
# ============================================================

pdf = PDF()
pdf.alias_nb_pages()
pdf.set_auto_page_break(auto=True, margin=15)

# ----------- CAPA -----------
pdf.add_page()
pdf.ln(40)
pdf.set_font("Helvetica", "B", 22)
pdf.set_text_color(0, 70, 150)
pdf.cell(0, 12, sanitize("Views vw_front_*"), align="C", new_x="LMARGIN", new_y="NEXT")
pdf.set_font("Helvetica", "", 14)
pdf.set_text_color(80, 80, 80)
pdf.cell(0, 10, sanitize("Catalogo de jogos para o front (categories-api)"), align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(10)
pdf.set_font("Helvetica", "", 11)
pdf.set_text_color(120, 120, 120)
pdf.cell(0, 6, sanitize("Entrega para validacao do Head antes do rollout"), align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(20)

pdf.set_font("Helvetica", "", 11)
pdf.set_text_color(60, 60, 60)
pdf.cell(0, 6, sanitize(f"Demanda: CTO Gabriel Barbosa (via Castrin)"), align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 6, sanitize(f"Autor: Mateus Fabro - Analista de Dados"), align="C", new_x="LMARGIN", new_y="NEXT")
pdf.cell(0, 6, sanitize(f"Data: {DATAHORA}"), align="C", new_x="LMARGIN", new_y="NEXT")
pdf.ln(30)

pdf.set_fill_color(255, 245, 225)
pdf.set_draw_color(230, 160, 50)
pdf.rect(20, pdf.get_y(), 170, 30, style="DF")
pdf.set_xy(24, pdf.get_y() + 3)
pdf.set_font("Helvetica", "B", 11)
pdf.set_text_color(150, 80, 0)
pdf.cell(0, 6, sanitize("Status: Aguardando validacao do Head"), new_x="LMARGIN", new_y="NEXT")
pdf.set_x(24)
pdf.set_font("Helvetica", "", 10)
pdf.set_text_color(60, 60, 60)
pdf.multi_cell(160, 5,
    sanitize("Infraestrutura pronta em producao local (Super Nova DB). Deploy EC2 e "
             "handoff formal para o Yuki (back-end categories-api) aguardando OK desta leitura."))

# ----------- 1. O QUE O CTO PEDIU -----------
pdf.add_page()
pdf.titulo(1, "O que o CTO pediu")
pdf.txt("Na conversa do CTO (print enviado por Castrin):")
pdf.bullet("Front precisa consumir views do banco para montar carrosseis "
           "(Mais jogados, Cassino ao vivo, Pragmatic, Jackpots, etc.)")
pdf.bullet("Views com nome padronizado (ex: vw_front_*)")
pdf.bullet("Refresh a cada 4h - se nao tem numero atualizado, mantem o antigo")
pdf.bullet("Validacao para evitar casos como 'slot aparecendo em Cassino ao vivo'")
pdf.bullet("Classificacao do Cassino ao vivo: Roletas, Blackjack, Baccarat, etc. "
           "(como ja temos para Casino, Live e Sportsbook)")
pdf.bullet("Parte de infra e consumo fica com Gusta/Yuki (back-end)")
pdf.bullet("Parte de dados (catalogo + metricas) e nossa responsabilidade")

pdf.ln(3)
pdf.sub("Trecho original da demanda:")
pdf.box("blue", "Resumo da conversa CTO -> Castrin -> Nos", [
    "'Mapear se ja temos todas informacoes necessarias para montar as views'",
    "'Montar as views com nome padronizado tipo view_front'",
    "'Atualizar a cada 4hrs (nao tem numero atualizado mantem o antigo)'",
    "'Ter uma tabela dim, ja temos' (evitar slot caindo em live casino)",
    "'Classificacao em cima dos jogos ao vivo, tipo so roletas, so blackbull'",
])

# ----------- 2. O QUE FOI ENTREGUE -----------
pdf.add_page()
pdf.titulo(2, "O que foi entregue")

pdf.sub("Arquitetura: 1 tabela enriquecida + 6 views")
pdf.txt("Centralizamos tudo em uma tabela base (multibet.game_image_mapping com 21 colunas) "
        "e criamos 6 views especializadas em cima dela. Isso garante consistencia: "
        "um jogo ranqueado como #1 em 'mais jogados' aparece com o mesmo numero em qualquer view.")

pdf.tabela(
    headers=["Objeto", "Tipo", "Funcao no front"],
    rows=[
        ["multibet.game_image_mapping", "Tabela", "Catalogo mestre (fonte de todas as views)"],
        ["vw_front_top_24h", "View", "Carrossel 'Mais jogados nas 24h'"],
        ["vw_front_live_casino", "View", "Cassino ao vivo categorizado (Roleta/BJ/Baccarat)"],
        ["vw_front_by_vendor", "View", "Filtros por provedor (Pragmatic, PG Soft, etc.)"],
        ["vw_front_by_category", "View", "Filtros macro (Slots vs Live)"],
        ["vw_front_jackpot", "View", "Carrossel 'Jackpots'"],
        ["vw_front_api_games", "View", "Shape completo da API (usada pelo Yuki)"],
    ],
    widths=[62, 20, 108]
)

pdf.sub("Fonte de dados (camada por camada)")
pdf.bullet("Catalogo de jogos: bireports_ec2.tbl_vendor_games_mapping_data (Athena) - 99%+ cobertura")
pdf.bullet("Categorias + sub-tipos: mesma fonte + vendor_ec2 para flags (jackpot, sub_vendor)")
pdf.bullet("Imagens: scraper Playwright do multi.bet.br (jogos.csv) + auto-fix CDN")
pdf.bullet("Metricas 24h (rank, rounds, R$ apostado, R$ ganho): silver_game_15min (Super Nova DB, granularidade 15min)")

# ----------- 3. CLASSIFICACAO DO CASSINO AO VIVO -----------
pdf.add_page()
pdf.titulo(3, "Classificacao do Cassino ao Vivo (ponto do CTO)")
pdf.txt("O CTO pediu explicitamente para classificar jogos ao vivo em: Roleta, Blackjack, "
        "Baccarat, etc. - igual ao que ja temos para Casino/Live/Sportsbook no macro.")

pdf.sub("Como resolvi")
pdf.txt("Criei uma coluna 'live_subtype' na tabela base, populada por regex sobre o campo "
        "'game_type_desc' do catalogo oficial (bireports). 5 grupos normalizados:")

live_dist = get_live_subtype_dist()
if live_dist:
    pdf.tabela(
        headers=["Subtipo", "Qtd de jogos ativos"],
        rows=live_dist,
        widths=[95, 95]
    )

pdf.sub("Regex aplicado (v1 - refinamos conforme uso)")
pdf.code("""# Ordem importa: primeiro match vence
Roleta:    roulette|roleta          (European Roulette, Speed Roulette, etc.)
Blackjack: blackjack                (Speed BJ, BlackjackX, VIP BJ, etc.)
Baccarat:  baccarat|punto banco     (Squeeze Baccarat, Punto Banco Soiree, etc.)
GameShow:  crazy time|monopoly|     (Crazy Time, Monopoly Live, Dragon Tiger,
           mega ball|sic bo|...      Sic Bo, Andar Bahar, Deal or No Deal, etc.)
Outros:    fallback (se nenhum     (Dead or Alive 2, BlackjackX 5, etc.)
           regex bateu)""")

pdf.sub("Validacao anti-erro (slot caindo no Live)")
pdf.txt("A view vw_front_live_casino filtra WHERE game_category = 'live'. A categoria vem da "
        "FONTE OFICIAL Pragmatic (bireports). Se um slot aparecer no live, o erro esta na ORIGEM "
        "(back-office Pragmatic) - a correcao deve ser feita la, nao em workaround na view.")

# ----------- 4. NUMEROS REAIS -----------
pdf.add_page()
pdf.titulo(4, "Numeros reais - Smoke test em producao")

total, com_img, ativos, com_ativ, turnover, wins = get_stats()
pdf.box("green", "Cobertura e atividade (ultimas 24h rolantes)", [
    f"Total de jogos catalogados: {total:,}",
    f"Com imagem (game_image_url): {com_img:,}",
    f"Ativos (c_status = 'active'): {ativos:,}",
    f"Com atividade nas ultimas 24h: {com_ativ:,}",
    f"Turnover total 24h: R$ {float(turnover):,.2f}",
    f"Wins total 24h: R$ {float(wins):,.2f}",
])

pdf.sub("Top 10 Mais Jogados (amostra de producao)")
top10 = get_amostra_top(10)
pdf.tabela(
    headers=["#", "Jogo", "Provedor", "Cat.", "Rounds", "Apostado", "Ganho"],
    rows=top10,
    widths=[10, 46, 32, 14, 24, 32, 32]
)

pdf.sub("Top 8 provedores (volume de jogos ativos)")
pdf.tabela(
    headers=["Provedor", "Qtd jogos ativos"],
    rows=get_vendor_top(),
    widths=[95, 95]
)

# ----------- 5. COMO O FRONT VAI CONSUMIR -----------
pdf.add_page()
pdf.titulo(5, "Como o front vai consumir (integracao com Yuki)")
pdf.txt("Descobri analisando o repo da categories-api do Yuki (branch feat/nrt) que a API dele "
        "esperava um shape especifico (GameResponseDto) com 12 campos em CamelCase.")

pdf.sub("Antes da nossa entrega")
pdf.bullet("API fazia query no Athena toda vez que o cache expirava (4h Redis TTL)")
pdf.bullet("Usava ps_bi.dim_game como catalogo - fonte com 0.2% de cobertura (PG Soft sumido)")
pdf.bullet("Metricas via fund_ec2.tbl_real_fund_txn com WITH/JOIN complexo")
pdf.bullet("Custo AWS Athena por cada scan")

pdf.sub("Depois da nossa entrega")
pdf.bullet("API faz 1 SELECT no PostgreSQL (Super Nova DB)")
pdf.bullet("Catalogo vem do bireports via nosso pipeline (99%+ cobertura)")
pdf.bullet("Metricas ja pre-agregadas pelo pipeline de 4h")
pdf.bullet("Custo Athena: zero no caminho critico")

pdf.sub("Exemplo de query que o Yuki vai rodar")
pdf.code("""-- Substitui toda a logica Athena no GameCachedRepository
SELECT "gameId", "name", "gameSlug", "gamePath", "image", "provider",
       "category", "totalBets", "uniquePlayers", "totalBet", "totalWins"
FROM multibet.vw_front_api_games
WHERE "rank" IS NOT NULL
ORDER BY "rank"
LIMIT $1 OFFSET $2;""")

# ----------- 6. REFRESH DE 4H -----------
pdf.add_page()
pdf.titulo(6, "Refresh de 4 em 4 horas")
pdf.txt("O CTO pediu atualizacao a cada 4h, e 'se nao tem numero novo, manter o antigo'. "
        "Implementei exatamente isso.")

pdf.sub("Cron planejado (EC2 ETL)")
pdf.code("""0 3,7,11,15,19,23 * * * /home/ec2-user/multibet/run_views_front.sh
# Em BRT: 00h, 04h, 08h, 12h, 16h, 20h""")

pdf.sub("Comportamento do upsert")
pdf.bullet("Campos de catalogo (nome, vendor, imagem, categoria): usam COALESCE - "
           "se a fonte vier NULL, mantem o valor antigo. Atende '100% se nao tem numero "
           "atualizado, mantem o antigo'.")
pdf.bullet("Campos de metrica (rank, rounds_24h, total_bet_24h, total_wins_24h): "
           "sempre atualizam para refletir a janela atual. Se um jogo nao teve atividade "
           "nas ultimas 24h, intencionalmente zera (ele sai do carrossel de mais jogados).")

pdf.sub("Janela 24h rolante (confirmado por voce hoje)")
pdf.txt("A janela e rolante - nao e o dia calendario. Se o pipeline rodar as 16h, "
        "considera atividade de 16h (de ontem) ate 16h (de hoje). Se rodar as 18h, "
        "considera de 18h ate 18h. Timestamp da janela e salvo em 'windowEndUtc' "
        "para o front poder mostrar 'atualizado ha X horas' se quiser.")

# ----------- 7. STATUS E PENDENCIAS -----------
pdf.add_page()
pdf.titulo(7, "Status atual e pendencias")

pdf.sub("O que ja esta pronto")
pdf.box("green", "Concluido", [
    "[OK] ALTER TABLE aplicado no Super Nova DB local (21 colunas)",
    "[OK] Pipeline game_image_mapper v3 populou 2.715 jogos (smoke test OK)",
    "[OK] 6 views criadas (vw_front_top_24h, live_casino, by_vendor, by_category, jackpot, api_games)",
    "[OK] Classificacao live_subtype aplicada em producao (2.311 jogos na view principal)",
    "[OK] Valores financeiros populados (R$ apostado/ganho por jogo nas 24h)",
    "[OK] Codigo versionado no GitHub (commit 05cf38b)",
    "[OK] Pacote ec2_deploy preparado (scripts de deploy + cron 4h)",
])

pdf.sub("O que aguarda sua validacao")
pdf.box("orange", "Pendente - acao do Head", [
    "[!] Aprovacao desta entrega antes de acionar o Yuki formalmente",
    "[!] Aprovacao para rodar deploy na EC2 ETL (ativa cron 4h automatico)",
    "[!] Validacao visual: abrir DBeaver e rodar os SELECTs da view",
])

pdf.sub("Problemas conhecidos (nao bloqueantes)")
pdf.box("red", "Atencao", [
    "[!] vw_front_jackpot retornando 0 linhas - tabela vendor_ec2.tbl_vendor_games_mapping_mst "
    "deu Empty no Athena com o user RO. Investigar acesso ou trocar fonte.",
    "[!] 8 jogos do top 50 sem imagem - rodar scraper Playwright ou aguardar auto-fix CDN "
    "do grandes_ganhos.py (proxima rodada resolve).",
])

pdf.sub("Correcao no processo")
pdf.box("blue", "Reconheco que pulei a validacao", [
    "Acabei mandando a view direto para o Yuki testar ANTES de passar por voce. "
    "Nao foi intencional - estava no fluxo de analise do repo dele. "
    "Ja avisei o Yuki que fica em stand-by ate sua aprovacao. "
    "Sigo o protocolo daqui: Castrin valida -> eu alinho com o Yuki -> deploy EC2.",
])

# ----------- 8. PROXIMOS PASSOS -----------
pdf.add_page()
pdf.titulo(8, "Proximos passos (se aprovado)")

pdf.sub("Sequencia")
pdf.tabela(
    headers=["#", "Acao", "Responsavel", "Tempo"],
    rows=[
        ["1", "Voce abre DBeaver e valida amostras das 6 views", "Castrin", "~15min"],
        ["2", "OK formal do Head para seguir", "Castrin", "-"],
        ["3", "Envio handoff formal pro Yuki (ja tenho msg + SQLs)", "Eu", "~10min"],
        ["4", "Deploy EC2 (ativa cron 4h)", "Eu", "~5min"],
        ["5", "Yuki refatora GameCachedRepository (7 metodos)", "Yuki", "~1h"],
        ["6", "Go-live do front consumindo as views", "Front/Yuki", "-"],
    ],
    widths=[10, 110, 40, 30]
)

pdf.sub("SQLs para voce validar no DBeaver")
pdf.code("""-- 1. Top 10 mais jogados
SELECT "rank", "name", "provider", "totalBets", "totalBet", "totalWins"
FROM multibet.vw_front_api_games
WHERE "rank" IS NOT NULL
ORDER BY "rank"
LIMIT 10;

-- 2. Distribuicao Live Casino (ponto que o CTO pediu)
SELECT live_subtype, COUNT(*) AS qtd
FROM multibet.vw_front_live_casino
GROUP BY live_subtype
ORDER BY qtd DESC;

-- 3. Jogos Pragmatic (top 10)
SELECT "name", "category", "rounds_24h"
FROM multibet.vw_front_by_vendor
WHERE vendor = 'pragmaticplay'
ORDER BY COALESCE("rank", 999999)
LIMIT 10;

-- 4. Sanity check - ultima atualizacao
SELECT MAX(popularity_window_end) AS ultima_janela_24h
FROM multibet.game_image_mapping;""")

pdf.sub("Pergunta direta para voce")
pdf.box("blue", "Decisao necessaria", [
    "1) Aprova a entrega? (sim/nao/ajustes)",
    "2) Aprova o handoff formal pro Yuki?",
    "3) Aprova o deploy EC2 hoje mesmo, ou prefere segurar ate reuniao com o CTO?",
])

# ----------- FOOTER FINAL -----------
pdf.ln(5)
pdf.set_font("Helvetica", "I", 9)
pdf.set_text_color(120, 120, 120)
pdf.multi_cell(0, 5, sanitize(
    "Qualquer ajuste que voce apontar, implemento antes de seguir para o Yuki. "
    "Obrigado pela paciencia - sigo o protocolo corretamente daqui em diante."))

pdf.output(PDF_PATH)
print(f"OK: {PDF_PATH}")
