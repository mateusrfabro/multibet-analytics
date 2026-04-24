"""
Gera PDF do feedback da segmentacao para o Castrin.
"""
from fpdf import FPDF
import os

OUTPUT = os.path.join(os.path.dirname(__file__), "..", "docs", "Feedback_Segmentacao_PVS.pdf")


class PDF(FPDF):
    def header(self):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(120, 120, 120)
        self.cell(0, 6, "Feedback - Segmentacao de Jogadores (PVS) | Mateus Fabro | 06/04/2026", align="R")
        self.ln(10)

    def footer(self):
        self.set_y(-15)
        self.set_font("Helvetica", "", 8)
        self.set_text_color(150, 150, 150)
        self.cell(0, 10, f"Pagina {self.page_no()}/{{nb}}", align="C")

    def section_title(self, num, title):
        self.ln(6)
        self.set_font("Helvetica", "B", 14)
        self.set_text_color(15, 52, 96)
        self.cell(0, 10, f"{num}. {title}", new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(15, 52, 96)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def sub_title(self, text):
        self.ln(3)
        self.set_font("Helvetica", "B", 11)
        self.set_text_color(52, 73, 94)
        self.cell(0, 7, text, new_x="LMARGIN", new_y="NEXT")
        self.ln(1)

    def body_text(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(44, 62, 80)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def bold_text(self, text):
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(44, 62, 80)
        self.multi_cell(0, 5.5, text)
        self.ln(1)

    def bullet(self, text):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(44, 62, 80)
        x = self.get_x()
        self.cell(6, 5.5, "-")
        self.multi_cell(0, 5.5, text)
        self.ln(0.5)

    def alert_box(self, text):
        self.ln(2)
        self.set_fill_color(253, 237, 236)
        self.set_draw_color(245, 183, 177)
        x, y = self.get_x(), self.get_y()
        self.set_font("Helvetica", "B", 10)
        self.set_text_color(180, 40, 40)
        self.multi_cell(0, 5.5, text, border=1, fill=True)
        self.ln(2)

    def info_box(self, text):
        self.ln(2)
        self.set_fill_color(234, 242, 248)
        self.set_draw_color(174, 214, 241)
        self.set_font("Helvetica", "", 10)
        self.set_text_color(30, 70, 110)
        self.multi_cell(0, 5.5, text, border=1, fill=True)
        self.ln(2)

    def add_table(self, headers, rows, col_widths=None):
        if col_widths is None:
            col_widths = [(self.w - self.l_margin - self.r_margin) / len(headers)] * len(headers)

        # Header
        self.set_font("Helvetica", "B", 9)
        self.set_fill_color(15, 52, 96)
        self.set_text_color(255, 255, 255)
        for i, h in enumerate(headers):
            self.cell(col_widths[i], 7, h, border=1, fill=True, align="C")
        self.ln()

        # Rows
        self.set_font("Helvetica", "", 9)
        self.set_text_color(44, 62, 80)
        fill = False
        for row in rows:
            if fill:
                self.set_fill_color(245, 247, 250)
            else:
                self.set_fill_color(255, 255, 255)
            for i, cell in enumerate(row):
                align = "L" if i == 0 else "C"
                self.cell(col_widths[i], 6.5, str(cell), border=1, fill=True, align=align)
            self.ln()
            fill = not fill
        self.ln(2)


def build_pdf():
    pdf = PDF()
    pdf.alias_nb_pages()
    pdf.set_auto_page_break(auto=True, margin=20)
    pdf.add_page()

    # Title
    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(15, 52, 96)
    pdf.cell(0, 12, "Feedback", new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 14)
    pdf.set_text_color(52, 73, 94)
    pdf.cell(0, 8, "Segmentacao de Jogadores (PVS)", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(100, 100, 100)
    pdf.cell(0, 6, "De: Mateus Fabro  |  Para: Castrin  |  Data: 06/04/2026", new_x="LMARGIN", new_y="NEXT")
    pdf.cell(0, 6, "Ref: Apresentacao_Segmentacao_Diretoria.html", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # ================================================================
    # SECAO 1
    # ================================================================
    pdf.section_title("1", "Perguntas tecnicas (clarificar antes da call)")

    pdf.sub_title("1.1 Fonte e formula do GGR")
    pdf.body_text(
        "O report nao menciona de onde vem o GGR nem a formula usada. "
        "Se veio de fund_ec2 (tipos 27-45): rollbacks (tipo 72) foram descontados? "
        "Sem rollbacks, GGR infla 5-7x. Tivemos incidente em 25/03 onde reportamos "
        "R$85.9M vs R$11.6M correto."
    )
    pdf.body_text(
        "Se veio de ps_bi.fct_player_activity_daily (campo casino_ggr): ja tem isolation, ok. "
        "O GGR de R$19.7M parece coerente com Q1 (R$17.8M), mas vale confirmar."
    )
    pdf.info_box("Sugestao: incluir nota de rodape com fonte + formula do GGR.")

    pdf.sub_title("1.2 Test users foram excluidos?")
    pdf.body_text(
        "167 contas de teste inflam ~3% do volume (R$26.6M). Se a base de 179.941 "
        "inclui test users, os numeros mudam. Filtro: is_test = false (ps_bi) ou "
        "c_test_user = false (bireports/ecr)."
    )

    pdf.sub_title("1.3 Definicao de 'jogador ativo'")
    pdf.body_text(
        "Base = 179.941, periodo Jan/2026 a presente (~95 dias). Qual o criterio? "
        "Pelo menos 1 deposito? 1 aposta? 1 login?"
    )
    pdf.body_text(
        "Se 'ativo' = fez 1 transacao em 95 dias, entao 50% da base (Casual) nao e "
        "ativo de verdade - sao jogadores que entraram, jogaram 1 dia e nao voltaram. "
        "A narrativa muda de '50% e casual' para '50% ja saiu'."
    )

    pdf.sub_title("1.4 O 'lifetime' cobre qual periodo?")
    pdf.body_text(
        "O report fala em 'Deposito Lifetime' e 'GGR Total', mas o periodo e Jan/2026 "
        "a presente. Jogadores registrados antes de janeiro tem historico anterior incluido? "
        "De qual fonte?"
    )

    pdf.sub_title("1.5 Normalizacao do PVS")
    pdf.body_text(
        "Os pesos positivos somam 85 e negativos 15, mas o score vai de 0 a 100. "
        "Como e o scaling? Min-max? Percentil? Log? Sem documentacao, nao da pra "
        "reproduzir nem auditar. A galera vai perguntar 'por que Fulano tem 71 e nao 72'."
    )

    pdf.sub_title("1.6 W/D Ratio do Regular = 0,00")
    pdf.body_text(
        "Regular tem 2 depositos, 2 dias ativos, mas saca zero. Duas hipoteses: "
        "(a) perderam tudo e nao tem saldo pra sacar (provavel) ou "
        "(b) bug no calculo (divisao por zero virou zero). "
        "Se for (a), vale explicar. Se for (b), corrigir."
    )

    pdf.sub_title("1.7 Hold Rate do Premium parece alto")
    pdf.body_text(
        "Premium com 21.2% de hold rate esta acima da media da operacao "
        "(Jan 13.2%, Fev 19.2%, Mar meta 16.97%). Pode ser efeito de mix de produto "
        "(slots mais volateis), mas vale investigar se nao tem distorcao."
    )

    # ================================================================
    # SECAO 2
    # ================================================================
    pdf.section_title("2", "Lacunas analiticas")

    pdf.sub_title("2.1 GGR negativo de 75% da base - falta o 'por que'")
    pdf.body_text(
        "Este e o achado mais importante do report. Regular e Casual tem GGR negativo "
        "- a casa perde dinheiro com 75% da base. Mas por que?"
    )
    pdf.bullet("Estao ganhando nas apostas? (improvavel em escala)")
    pdf.bullet("Custo de bonus esta comendo a margem? (provavel)")
    pdf.bullet("BTR (bonus convertido em saque) alto?")
    pdf.alert_box(
        "Sem separar GGR Real vs GGR Bonus, nao da pra saber se o problema e o jogador "
        "ou a politica de bonus. Temos o pipeline de sub-fund isolation que faz essa separacao."
    )

    pdf.sub_title("2.2 NGR ausente")
    pdf.body_text(
        "Reconhecido nas limitacoes, mas e uma lacuna critica. "
        "GGR - BTR - RCA = NGR. Um Whale com R$5.166 de GGR e R$4.000 em bonus "
        "e muito diferente de um com R$5.166 e R$500 em bonus. "
        "Sem NGR, a rentabilidade real por segmento e desconhecida."
    )

    pdf.sub_title("2.3 Sem fonte de aquisicao")
    pdf.body_text(
        "Temos os IDs dos affiliates mapeados (Google: 297657, 445431, 468114 | "
        "Meta: 532570, 532571, 464673). Cruzar segmento com fonte de aquisicao "
        "responde: 'Whales vem mais de Google ou Meta?' - muda investimento em midia."
    )

    pdf.sub_title("2.4 Padrao temporal pode ser sazonal")
    pdf.body_text(
        "O pico de apostas sport na terca para Casual (19.1%) pode ser efeito de "
        "Champions League / Libertadores, nao comportamento intrinseco. "
        "Se o periodo amostral e curto (30 dias), o calendario esportivo distorce."
    )

    # ================================================================
    # SECAO 3
    # ================================================================
    pdf.section_title("3", "Sugestoes para fortalecer antes da call")

    headers = ["#", "Sugestao", "Impacto", "Esforco"]
    widths = [8, 100, 40, 30]
    rows = [
        ["1", "Incluir fonte + formula do GGR (rodape)", "Credibilidade", "Baixo"],
        ["2", "Confirmar exclusao de test users", "Acuracia", "Baixo"],
        ["3", "Documentar normalizacao do PVS", "Reprodutibilidade", "Baixo"],
        ["4", "Explicar W/D ratio = 0 do Regular", "Clareza", "Baixo"],
        ["5", "Separar GGR Real vs Bonus (segm. negativos)", "Profundidade", "Medio"],
        ["6", "Cruzar segmentos com affiliate/aquisicao", "Estrategia midia", "Medio"],
        ["7", "Definir caminho ativacao CRM (BQ suspenso)", "Operacionalizacao", "Medio"],
        ["8", "Adicionar lamina 'como operacionalizar'", "Acao pos-call", "Baixo"],
    ]
    pdf.add_table(headers, rows, widths)

    # ================================================================
    # SECAO 4
    # ================================================================
    pdf.section_title("4", "O que posso contribuir")

    pdf.body_text("Tenho pipelines prontos que podem enriquecer essa segmentacao:")
    pdf.ln(2)

    pdf.bullet(
        "Validacao cruzada do GGR - scripts fct_player_activity_daily e "
        "fct_casino_activity_daily ja rodando"
    )
    pdf.bullet(
        "Sub-fund isolation - separacao Real vs Bonus por jogador via "
        "tbl_realcash_sub_fund_txn + tbl_bonus_sub_fund_txn"
    )
    pdf.bullet(
        "Enriquecimento com affiliate - cruzar dim_user.affiliate_id com a base segmentada"
    )
    pdf.bullet(
        "Dashboard CRM - ja temos estrutura Flask + HTML que pode receber "
        "os segmentos como filtro"
    )

    pdf.ln(4)
    pdf.info_box("Posso rodar essas analises complementares antes da call de quarta se for util.")

    pdf.output(OUTPUT)
    print(f"PDF gerado: {OUTPUT}")
    print(f"Tamanho: {os.path.getsize(OUTPUT) / 1024:.0f} KB")


if __name__ == "__main__":
    build_pdf()
