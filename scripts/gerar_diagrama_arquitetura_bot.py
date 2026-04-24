"""
Gera diagrama visual da arquitetura do Bot de Analytics via WhatsApp.
Output:
  - reports/arquitetura_bot_analytics.png  (pra WhatsApp)
  - reports/arquitetura_bot_analytics.pdf  (pra apresentacao formal)
"""
import os
import sys
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

OUT_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "reports")
os.makedirs(OUT_DIR, exist_ok=True)

PNG_PATH = os.path.join(OUT_DIR, "arquitetura_bot_analytics.png")
PDF_PATH = os.path.join(OUT_DIR, "arquitetura_bot_analytics.pdf")


def box(ax, x, y, w, h, text, facecolor="#E5E7EB", edgecolor="#374151",
        fontsize=9, fontweight="normal", textcolor="#111827", boxstyle="round,pad=0.04"):
    """Desenha uma caixa com texto centralizado."""
    b = FancyBboxPatch(
        (x, y), w, h,
        boxstyle=boxstyle,
        linewidth=1.2, facecolor=facecolor, edgecolor=edgecolor,
        mutation_scale=1,
    )
    ax.add_patch(b)
    ax.text(x + w / 2, y + h / 2, text,
            ha="center", va="center",
            fontsize=fontsize, fontweight=fontweight, color=textcolor,
            wrap=True)


def arrow(ax, x1, y1, x2, y2, text="", color="#374151", style="->", linewidth=1.4,
          text_offset=(0, 0), text_fontsize=7.5):
    """Desenha uma seta com texto opcional."""
    a = FancyArrowPatch(
        (x1, y1), (x2, y2),
        arrowstyle=style, mutation_scale=14,
        linewidth=linewidth, color=color,
    )
    ax.add_patch(a)
    if text:
        ax.text((x1 + x2) / 2 + text_offset[0], (y1 + y2) / 2 + text_offset[1],
                text, ha="center", va="center", fontsize=text_fontsize,
                color=color, style="italic",
                bbox=dict(boxstyle="round,pad=0.2", facecolor="white",
                          edgecolor="none", alpha=0.85))


def build():
    fig, ax = plt.subplots(figsize=(16, 11))
    ax.set_xlim(0, 16)
    ax.set_ylim(0, 11)
    ax.axis("off")

    # Titulo
    ax.text(8, 10.55, "Bot de Analytics via WhatsApp — Arquitetura MVP",
            ha="center", va="center", fontsize=18, fontweight="bold", color="#111827")
    ax.text(8, 10.15,
            "Super Nova Gaming  |  MultiBet + Play4Tune  |  v1 — 20/04/2026",
            ha="center", va="center", fontsize=10, color="#6B7280", style="italic")

    # Cores por camada
    COLOR_USER = "#DCFCE7"; COLOR_USER_EDGE = "#15803D"   # verde — stakeholder
    COLOR_CHAN = "#FEF3C7"; COLOR_CHAN_EDGE = "#A16207"   # amarelo — canal
    COLOR_BOT  = "#DBEAFE"; COLOR_BOT_EDGE  = "#1D4ED8"   # azul — app
    COLOR_DB   = "#FED7AA"; COLOR_DB_EDGE   = "#C2410C"   # laranja — DB central
    COLOR_SRC  = "#E5E7EB"; COLOR_SRC_EDGE  = "#6B7280"   # cinza — fontes brutas
    COLOR_CRIT = "#FEE2E2"; COLOR_CRIT_EDGE = "#B91C1C"   # vermelho — bloqueio

    # 1. Stakeholder (topo)
    box(ax, 6.5, 9.0, 3.0, 0.85,
        "STAKEHOLDER (Head, CTO, CGO, gestores)\nmanda pergunta via WhatsApp",
        facecolor=COLOR_USER, edgecolor=COLOR_USER_EDGE, fontsize=9, fontweight="bold")

    # 2. Z-API (canal)
    box(ax, 6.5, 7.7, 3.0, 0.85,
        "Z-API  (provedor WhatsApp)\nwebhook recebe/envia  |  ~R$ 150/mes",
        facecolor=COLOR_CHAN, edgecolor=COLOR_CHAN_EDGE, fontsize=9, fontweight="bold")

    # 3. Bot App (grande caixa central com subcaixas)
    box(ax, 1.0, 4.5, 14.0, 2.7,
        "", facecolor="#F9FAFB", edgecolor=COLOR_BOT_EDGE, fontsize=9, boxstyle="round,pad=0.05")
    ax.text(1.2, 7.05, "BOT APP  —  Flask + Python  (EC2 existente, zero custo incremental)",
            fontsize=10, fontweight="bold", color=COLOR_BOT_EDGE)

    box(ax, 1.3, 5.0, 2.9, 1.7,
        "1. GUARDRAILS\n\n• whitelist numeros\n• rate limit\n• LGPD check\n• log auditoria",
        facecolor=COLOR_BOT, edgecolor=COLOR_BOT_EDGE, fontsize=8)

    box(ax, 4.5, 5.0, 3.3, 1.7,
        "2. LLM ROUTER\n\n• Claude Haiku / GPT-mini\n• le catalogo + memory/\n• retorna intent+params\n• NAO escreve SQL",
        facecolor=COLOR_BOT, edgecolor=COLOR_BOT_EDGE, fontsize=8)

    box(ax, 8.1, 5.0, 3.3, 1.7,
        "3. INTENT EXECUTOR\n\n• SELECT em view do SN DB\n• aplica filtros padrao\n  (D-1, test users, etc.)\n• query parametrizada",
        facecolor=COLOR_BOT, edgecolor=COLOR_BOT_EDGE, fontsize=8)

    box(ax, 11.7, 5.0, 3.0, 1.7,
        "4. FORMATTER\n\n• texto (tabela pequena)\n• PNG matplotlib (tabela)\n• Excel c/ legenda\n• devolve via Z-API",
        facecolor=COLOR_BOT, edgecolor=COLOR_BOT_EDGE, fontsize=8)

    # 4. Super Nova DB (bloco central grande)
    box(ax, 1.0, 1.3, 10.5, 2.8,
        "", facecolor="#FFF7ED", edgecolor=COLOR_DB_EDGE, fontsize=9, boxstyle="round,pad=0.05")
    ax.text(1.2, 3.95,
            "SUPER NOVA DB  (PostgreSQL)  —  UNICA FONTE QUE O BOT CONSULTA",
            fontsize=10, fontweight="bold", color=COLOR_DB_EDGE)

    box(ax, 1.3, 1.7, 3.0, 2.0,
        "SCHEMA  bot_ana\n(infra do bot)\n\n• intents\n• logs\n• whitelist\n• cache_resultado\n• feedback",
        facecolor=COLOR_DB, edgecolor=COLOR_DB_EDGE, fontsize=8)

    box(ax, 4.6, 1.7, 3.3, 2.0,
        "VIEWS MULTIBET BR\n(ja em producao)\n\n• matriz_financeiro\n• matriz_aquisicao\n• mv_cohort_aquisicao\n• heatmap_hour\n• tab_cassino / tab_sports\n• vw_active_player_retention",
        facecolor=COLOR_DB, edgecolor=COLOR_DB_EDGE, fontsize=8)

    box(ax, 8.2, 1.7, 3.2, 2.0,
        "VIEWS PLAY4TUNE\n(schema play4 — ja em prod)\n\n• vw_ggr_player_game_daily\n• vw_top_jogos_ggr\n• vw_cadastros_ftd / vw_ativos\n• vw_casino_resumo\n• vw_creditacoes\n• vw_movimentacao_financeira",
        facecolor=COLOR_DB, edgecolor=COLOR_DB_EDGE, fontsize=8)

    # 5. Fontes brutas (laterais, marcadas como NAO acessadas)
    box(ax, 12.0, 2.5, 3.5, 1.5,
        "ATHENA  (Iceberg DL)\nMultiBet BR — raw\n\n[BLOQUEADO]\nbot NAO acessa direto\npipelines ETL alimentam\nas views no SN DB",
        facecolor=COLOR_CRIT, edgecolor=COLOR_CRIT_EDGE, fontsize=8, fontweight="bold")

    box(ax, 12.0, 0.6, 3.5, 1.5,
        "SUPER NOVA BET DB\nPlay4Tune — raw\n\n[BLOQUEADO]\nbot NAO acessa direto\npipelines ETL alimentam\nas views no SN DB",
        facecolor=COLOR_CRIT, edgecolor=COLOR_CRIT_EDGE, fontsize=8, fontweight="bold")

    # Setas principais de fluxo
    arrow(ax, 8.0, 8.98, 8.0, 8.60, text="pergunta", color=COLOR_USER_EDGE)
    arrow(ax, 8.0, 7.68, 8.0, 7.20, text="HTTP webhook", color=COLOR_CHAN_EDGE)

    # Setas internas do Bot (1->2->3->4)
    arrow(ax, 4.2, 5.85, 4.5, 5.85, style="->", color=COLOR_BOT_EDGE)
    arrow(ax, 7.8, 5.85, 8.1, 5.85, style="->", color=COLOR_BOT_EDGE)
    arrow(ax, 11.4, 5.85, 11.7, 5.85, style="->", color=COLOR_BOT_EDGE)

    # Bot -> SN DB (SELECT)
    arrow(ax, 9.8, 4.98, 8.5, 4.10, text="SELECT (ms)", color=COLOR_DB_EDGE, linewidth=1.8)
    # SN DB -> Bot (dados de volta)
    arrow(ax, 8.0, 4.10, 9.3, 4.98, text="", color=COLOR_DB_EDGE, linewidth=0.8, style="-|>")

    # Pipelines alimentam as views (setas finas das fontes -> SN DB)
    arrow(ax, 12.0, 3.2, 11.5, 2.8, text="ETL cron",
          color=COLOR_SRC_EDGE, linewidth=0.9, text_offset=(0, 0.15))
    arrow(ax, 12.0, 1.3, 11.5, 2.1, text="ETL cron",
          color=COLOR_SRC_EDGE, linewidth=0.9, text_offset=(0, -0.15))

    # Retorno ao usuário (formatter -> Z-API -> WhatsApp)
    arrow(ax, 13.2, 6.7, 13.2, 8.15, text="PNG/Excel/texto",
          color=COLOR_CHAN_EDGE, text_offset=(0.1, 0.2))
    arrow(ax, 13.2, 8.55, 9.5, 9.4, text="resposta no WhatsApp",
          color=COLOR_USER_EDGE)

    # Legenda/rodape
    ax.text(0.2, 0.3,
            "Premissas: bot consulta APENAS views/matviews ja existentes no Super Nova DB.  "
            "NAO ha criacao de views pro bot — se surgirem views novas no futuro (criadas pra outros fins), o bot passa a consumir.",
            fontsize=8, color="#6B7280", style="italic")
    ax.text(0.2, 0.05,
            "Vantagens: (1) protege Athena de queries pesadas  "
            "(2) reusa camada semantica dos produtos SN existentes  "
            "(3) bot vira produto plug-and-play pra outras operacoes SN que replicarem o mesmo contrato de views.",
            fontsize=8, color="#6B7280", style="italic")

    plt.tight_layout()
    fig.savefig(PNG_PATH, dpi=200, bbox_inches="tight", facecolor="white")
    fig.savefig(PDF_PATH, bbox_inches="tight", facecolor="white")
    plt.close(fig)

    print(f"[PNG] {PNG_PATH}")
    print(f"[PDF] {PDF_PATH}")


if __name__ == "__main__":
    build()
