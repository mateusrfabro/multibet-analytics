"""
Gera PDF da analise de contribuicao ao relatorio do Mauro.
Output: docs/_migration/contribuicao_ao_relatorio_mauro.pdf

Padrao visual alinhado com o PDF original do Mauro:
- Titulo h1 azul escuro #1a3f6c
- Secoes numeradas com linha horizontal azul
- Tabelas: header azul + linhas alternadas cinza claro
- Bullets com marcadores
- Linguagem de squad (sem divisao "nos vs time")
"""
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, ListFlowable, ListItem
)
from reportlab.lib.enums import TA_LEFT
from pathlib import Path

OUTPUT = Path("docs/_migration/contribuicao_ao_relatorio_mauro.pdf")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# --- Cores ---
AZUL_ESCURO = colors.HexColor("#1a3f6c")
AZUL_MEDIO = colors.HexColor("#2d5a8e")
VERDE_OK = colors.HexColor("#2d8659")
LARANJA_PEND = colors.HexColor("#c47a1f")
CINZA_LINHA_ALT = colors.HexColor("#f5f5f7")
CINZA_BORDA = colors.HexColor("#d0d0d0")
CINZA_TEXTO = colors.HexColor("#333333")
CINZA_META = colors.HexColor("#666666")
BRANCO = colors.white

styles = getSampleStyleSheet()

H1 = ParagraphStyle("H1", parent=styles["Heading1"],
    fontName="Helvetica-Bold", fontSize=18, textColor=AZUL_ESCURO,
    spaceAfter=6, leading=22, alignment=TA_LEFT)
META = ParagraphStyle("META", parent=styles["Normal"],
    fontName="Helvetica", fontSize=9, textColor=CINZA_META,
    spaceAfter=2, leading=13)
RESUMO = ParagraphStyle("RESUMO", parent=styles["Normal"],
    fontName="Helvetica-Bold", fontSize=10, textColor=CINZA_TEXTO,
    spaceBefore=4, spaceAfter=4, leading=14)
H2 = ParagraphStyle("H2", parent=styles["Heading2"],
    fontName="Helvetica-Bold", fontSize=13, textColor=AZUL_ESCURO,
    spaceBefore=16, spaceAfter=6, leading=16)
H3 = ParagraphStyle("H3", parent=styles["Heading3"],
    fontName="Helvetica-Bold", fontSize=11, textColor=AZUL_MEDIO,
    spaceBefore=10, spaceAfter=4, leading=14)
P = ParagraphStyle("P", parent=styles["Normal"],
    fontName="Helvetica", fontSize=9.5, textColor=CINZA_TEXTO,
    spaceAfter=4, leading=13, alignment=TA_LEFT)
CELL = ParagraphStyle("CELL", parent=styles["Normal"],
    fontName="Helvetica", fontSize=8.5, textColor=CINZA_TEXTO, leading=11)
CELL_HEADER = ParagraphStyle("CELL_HEADER", parent=styles["Normal"],
    fontName="Helvetica-Bold", fontSize=9, textColor=BRANCO, leading=11)


def hr():
    return HRFlowable(width="100%", thickness=0.75, color=CINZA_BORDA, spaceBefore=2, spaceAfter=10)


def section_header(num, title):
    return [Paragraph(f"{num}. {title}", H2),
            HRFlowable(width="100%", thickness=0.5, color=AZUL_ESCURO, spaceBefore=0, spaceAfter=8)]


def make_table(header, rows, col_widths=None, header_color=AZUL_ESCURO):
    data = [[Paragraph(str(c), CELL_HEADER) for c in header]]
    for r in rows:
        data.append([Paragraph(str(c), CELL) for c in r])
    t = Table(data, colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), header_color),
        ("TEXTCOLOR", (0, 0), (-1, 0), BRANCO),
        ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, -1), "LEFT"),
        ("VALIGN", (0, 0), (-1, -1), "TOP"),
        ("GRID", (0, 0), (-1, -1), 0.4, CINZA_BORDA),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("TOPPADDING", (0, 0), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
    ]
    for i in range(1, len(data)):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), CINZA_LINHA_ALT))
    t.setStyle(TableStyle(style))
    return t


def bullet_list(items):
    return ListFlowable(
        [ListItem(Paragraph(it, P), leftIndent=12, value="-") for it in items],
        bulletType="bullet", start="-", leftIndent=14, bulletFontSize=9,
        spaceBefore=2, spaceAfter=6)


# --- CONTEUDO ---
story = []

# TITULO
story.append(Paragraph(
    "Contribui&ccedil;&atilde;o ao Relat&oacute;rio do Mauro &mdash; An&aacute;lise Completa",
    H1))
story.append(hr())
story.append(Paragraph(
    "<b>Relat&oacute;rio base do Mauro:</b> 16/04/2026 17:11 &mdash; 114 objetos | 33 em uso (28%) | 81 sem refer&ecirc;ncia",
    META))
story.append(Paragraph(
    "<b>An&aacute;lise por:</b> Mateus Fabro (Squad 3 Intelligence Engine) &nbsp;|&nbsp; "
    "<b>Base t&eacute;cnica:</b> SSH EC2 + clone 19 repos GL-Analytics-M-L + invent&aacute;rio do ambiente local",
    META))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "<b>Proposta:</b> complementar o mapeamento feito pelo Mauro, cobrindo os objetos cuja autoria est&aacute; no meu "
    "escopo (Intelligence Engine) via 3 PRs em sequ&ecirc;ncia. Cobertura do meu escopo salta de <b>28% para ~53%</b>.",
    RESUMO))

# --- SECAO 1: RESUMO EXECUTIVO ---
story.extend(section_header("1", "Evolu&ccedil;&atilde;o da cobertura com as contribui&ccedil;&otilde;es propostas"))
story.append(make_table(
    ["Est&aacute;gio", "Cobertura", "Objetos", "Delta"],
    [
        ["Baseline do relat&oacute;rio (16/04 manh&atilde;)", "28%", "33 / 114", "&mdash;"],
        ["Ap&oacute;s PR #1 (mergeado)", "<b>31%</b>", "36 / 114", "+3 tabelas"],
        ["Ap&oacute;s PR #2 (pipelines Intelligence Engine)", "<b>~45%</b>", "~51 / 114", "+15 tabelas"],
        ["Ap&oacute;s PR #3 (DDLs de views)", "<b>~53%</b>", "~61 / 114", "+10 views"],
        ["<b>Contribui&ccedil;&atilde;o total do meu escopo</b>", "<b>~53%</b>", "<b>~61 / 114</b>", "<b>+28 objetos</b>"],
    ],
    col_widths=[6.5*cm, 2.5*cm, 3.5*cm, 4.5*cm]
))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "<b>Interpreta&ccedil;&atilde;o:</b> a contribui&ccedil;&atilde;o do meu escopo (Intelligence Engine) quase dobra a cobertura "
    "documentada. Os demais objetos pertencem a outras &aacute;reas da squad (sync/ingest, APIs, views de dashboard) &mdash; "
    "s&atilde;o complementares e fazem parte do mesmo ecossistema.", P))

# --- SECAO 2: PR #1 ---
story.extend(section_header("2", "PR #1 &mdash; j&aacute; contribui (em revis&atilde;o)"))
story.append(Paragraph(
    "<b>PR:</b> sync-ec2-prod-20260416 &nbsp;|&nbsp; 8 commits at&ocirc;micos | 44 arquivos | +7.336 / -1.412 linhas",
    P))

story.append(Paragraph(
    "<b>Tabelas que passam de &lsquo;Sem ref&rsquo; &rarr; &lsquo;Em uso&rsquo;</b>", H3))
story.append(make_table(
    ["Tabela", "Tamanho", "Pipeline que passa a referenciar"],
    [
        ["fact_sports_odds_performance", "280 kB, 855 regs",
         "pipelines/fact_sports_odds_performance.py"],
        ["pcr_ratings", "37 MB, 156K regs",
         "pipelines/pcr_pipeline.py"],
        ["fact_ad_spend", "944 kB, 2.464 regs",
         "pipelines/sync_google_ads_spend.py + sync_meta_spend.py"],
    ],
    col_widths=[5.5*cm, 3.2*cm, 8.3*cm],
    header_color=VERDE_OK
))
story.append(Spacer(1, 4))
story.append(Paragraph(
    "<b>B&ocirc;nus no PR #1:</b> 15 pipelines + 10 views Casino+Sportsbook (handoff Gusta 08/04) + 3 conectores db + "
    "DEPLOY.md expandido de 5 para 10 pipelines.", P))

# --- SECAO 3: PR #2 ---
story.extend(section_header("3", "PR #2 &mdash; pipelines do meu escopo que podem ir pro repo"))
story.append(Paragraph(
    "Tabelas do relat&oacute;rio &lsquo;Sem ref&rsquo; cujos pipelines Python est&atilde;o no meu ambiente local:", P))

story.append(Paragraph("<b>3.1 Dimens&otilde;es (2)</b>", H3))
story.append(make_table(
    ["Tabela", "Tamanho", "Pipeline local"],
    [
        ["dim_marketing_mapping", "1.968 kB, 3.241 regs",
         "pipelines/dim_marketing_mapping.py + _canonical.py"],
        ["dim_games_catalog", "152 kB, 381 regs",
         "pipelines/dim_games_catalog.py"],
    ],
    col_widths=[5.5*cm, 3.2*cm, 8.3*cm],
    header_color=LARANJA_PEND
))

story.append(Paragraph("<b>3.2 Fatos granulares (9)</b>", H3))
story.append(make_table(
    ["Tabela", "Tamanho", "Pipeline local"],
    [
        ["fact_attribution", "67 MB, 154K regs", "pipelines/fact_attribution.py"],
        ["fact_ftd_deposits", "12 MB, 29K regs", "pipelines/fact_ftd_deposits.py"],
        ["fact_redeposits", "22 MB, 154K regs", "pipelines/fact_redeposits.py"],
        ["fact_registrations", "56 kB, 171 regs", "pipelines/fact_registrations.py"],
        ["fact_player_activity", "80 kB, 142 regs", "pipelines/fact_player_activity.py"],
        ["fact_player_engagement_daily", "24 MB, 154K regs", "pipelines/fact_player_engagement_daily.py"],
        ["fact_gaming_activity_daily", "54 MB, 116K regs", "pipelines/fact_gaming_activity_daily.py"],
        ["fact_jackpots", "56 kB, 1 reg", "pipelines/fact_jackpots.py"],
        ["fact_live_casino", "2.936 kB, 11K regs", "pipelines/fact_live_casino.py"],
    ],
    col_widths=[5.5*cm, 3.2*cm, 8.3*cm],
    header_color=LARANJA_PEND
))

story.append(Paragraph("<b>3.3 Agrega&ccedil;&otilde;es (3)</b>", H3))
story.append(make_table(
    ["Tabela", "Tamanho", "Pipeline local"],
    [
        ["agg_cohort_acquisition", "32 MB, 189K regs", "pipelines/agg_cohort_acquisition.py"],
        ["agg_game_performance", "5.736 kB, 27K regs", "pipelines/agg_game_performance.py"],
        ["agg_btr_by_utm_campaign", "32 kB, 51 regs",
         "relacionado ao clustering-btr-utm-campaign (confirmar com o autor)"],
    ],
    col_widths=[5.5*cm, 3.2*cm, 8.3*cm],
    header_color=LARANJA_PEND
))

story.append(Paragraph("<b>3.4 CRM &mdash; Campaign tables (8)</b>", H3))
story.append(make_table(
    ["Tabela", "Tamanho", "Pipeline local"],
    [
        ["crm_campaign_comparison", "24 kB", "pipelines/crm_report_daily_v3_agent.py"],
        ["crm_campaign_daily", "2.376 kB, 3K regs", "pipelines/crm_report_daily.py + ddl_crm_report.py"],
        ["crm_campaign_game_daily", "104 kB, 20 regs", "idem"],
        ["crm_campaign_segment_daily", "24 kB", "idem"],
        ["crm_dispatch_budget", "56 kB, 6 regs", "pipelines/report_crm_promocoes.py (confirmar)"],
        ["crm_player_vip_tier", "32 kB", "idem"],
        ["crm_recovery_daily", "56 kB, 1 reg", "pipelines/crm_report_daily_v3_agent.py (confirmar)"],
        ["crm_vip_group_daily", "56 kB, 3 regs", "idem"],
    ],
    col_widths=[5.5*cm, 3.2*cm, 8.3*cm],
    header_color=LARANJA_PEND
))
story.append(Paragraph(
    "<b>Total PR #2:</b> ~22 tabelas document&aacute;veis. "
    "<b>Pr&eacute;-requisito:</b> validar empiricamente quais rodam em produ&ccedil;&atilde;o antes de commitar + "
    "alinhar com Mauro/Gusta pra evitar duplica&ccedil;&atilde;o com trabalhos em curso.", P))

# --- SECAO 4: PR #3 ---
story.extend(section_header("4", "PR #3 &mdash; DDLs de views criadas via DBeaver"))

story.append(Paragraph("<b>4.1 Derivadas diretas do meu escopo (contexto claro)</b>", H3))
story.append(make_table(
    ["View", "Tabela fonte", "Projeto origem"],
    [
        ["pcr_atual", "pcr_ratings", "PCR &mdash; Player Credit Rating"],
        ["pcr_resumo", "pcr_ratings", "PCR &mdash; Player Credit Rating"],
        ["vw_ad_spend_daily", "fact_ad_spend", "Ad Spend multicanal"],
        ["vw_ad_spend_by_source", "fact_ad_spend", "Ad Spend multicanal"],
        ["vw_odds_performance_by_range", "fact_sports_odds_performance", "Sports Odds Performance"],
        ["vw_odds_performance_summary", "fact_sports_odds_performance", "Sports Odds Performance"],
    ],
    col_widths=[6.5*cm, 5.2*cm, 5.3*cm],
    header_color=LARANJA_PEND
))

story.append(Paragraph("<b>4.2 Derivadas de dims/fatos do meu escopo (confirmar autoria)</b>", H3))
story.append(make_table(
    ["View", "Tabela fonte", "Projeto origem prov&aacute;vel"],
    [
        ["vw_acquisition_channel", "dim_marketing_mapping", "dim_marketing_mapping"],
        ["vw_attribution_metrics", "fact_attribution", "fact_attribution"],
        ["vw_ltv_cac_ratio", "fact_attribution", "fact_attribution"],
        ["vw_player_performance_period", "fct_player_performance_by_period", "views_casino_sportsbook"],
    ],
    col_widths=[6.5*cm, 5.2*cm, 5.3*cm],
    header_color=LARANJA_PEND
))
story.append(Paragraph(
    "<b>Total PR #3:</b> ~10 views. <b>M&eacute;todo:</b> extrair DDL via "
    "<i>psql \\d+ nome_da_view</i> ou <i>pg_dump --schema-only --view</i> e "
    "commitar em <b>multibet_pipelines/sql/views/</b>.", P))

# --- SECAO 5: Escopos complementares da squad ---
story.extend(section_header("5", "Outros objetos do relat&oacute;rio &mdash; escopos complementares da squad"))
story.append(Paragraph(
    "Os ~47 objetos restantes no &lsquo;Sem ref&rsquo; pertencem a outras &aacute;reas da squad e j&aacute; t&ecirc;m "
    "repos pr&oacute;prios. O scan do Mauro identifica corretamente que n&atilde;o est&atilde;o em multibet_pipelines "
    "&mdash; fazem parte do ecossistema mais amplo:", P))

story.append(Paragraph("<b>5.1 Camada de sync / ingest (repos do Gusta)</b>", H3))
story.append(bullet_list([
    "<b>tab_*</b> (19 tabelas) &rarr; alimentadas por <b>sync_all</b>, <b>sync_all_aquisicao</b>, <b>sync_user_daily</b>",
    "<b>silver_tab_user_ftd</b>, <b>migrations</b>, <b>etl_control</b> &rarr; infraestrutura de sync",
    "<b>silver_*</b> (game_activity, game_15min, jogadores_ganhos, jogos_jogadores_ativos) &rarr; "
    "repos <b>top_wins</b> e <b>game_activity_30d</b>",
]))

story.append(Paragraph("<b>5.2 Views de apoio a dashboards (escopo compartilhado)</b>", H3))
story.append(bullet_list([
    "<b>matriz_financeiro</b> + variantes (mensal/semanal/hora)",
    "<b>matriz_aquisicao</b>, <b>matriz_risco</b> (view)",
    "<b>cohort_aquisicao</b>, <b>cohort_retencao_ftd</b>, <b>heatmap_hour</b>",
    "<b>active_users</b>, <b>game_paid_15min</b>, <b>jogo_total_pago_hoje</b>, "
    "<b>top_jogadores_ganhos</b>, <b>user_game_activity_30d</b>",
]))

story.append(Paragraph("<b>5.3 Sem origem identificada (investigar com a squad)</b>", H3))
story.append(bullet_list([
    "dim_affiliate_source, dim_campaign_affiliate, dim_crm_friendly_names",
    "fact_affiliate_revenue, segment_tags",
    "vw_roi_by_source, vw_segmentacao_hibrida, vw_casino_by_category/provider/top_games",
]))

story.append(Spacer(1, 4))
story.append(Paragraph(
    "<i>Essa separa&ccedil;&atilde;o &eacute; puramente organizacional (escopo de trabalho individual dentro da squad). "
    "Todos os objetos fazem parte do mesmo ecossistema da Squad 3 Intelligence Engine + &aacute;reas adjacentes.</i>", META))

# --- SECAO 6: RESUMO FINAL ---
story.extend(section_header("6", "Resumo da contribui&ccedil;&atilde;o poss&iacute;vel"))
story.append(make_table(
    ["Categoria", "Total no relat&oacute;rio", "Contribui&ccedil;&atilde;o do meu escopo", "Outros escopos da squad"],
    [
        ["Tabelas sem ref", "52", "<b>~24</b> (3 no PR #1 + ~21 no PR #2)", "~28 (sync/ingest)"],
        ["Views sem ref", "29", "<b>~10</b> (PR #3)", "~19 (views de dashboard)"],
        ["<b>TOTAL</b>", "<b>81</b>", "<b>~34 (42% dos &oacute;rf&atilde;os)</b>", "~47"],
    ],
    col_widths=[4.5*cm, 3.0*cm, 5.5*cm, 4.0*cm],
    header_color=AZUL_ESCURO
))

# --- SECAO 7: PROXIMOS PASSOS ---
story.extend(section_header("7", "Pr&oacute;xima a&ccedil;&atilde;o sugerida"))
story.append(bullet_list([
    "<b>Fase B.2 (PR #2)</b> &mdash; varredura sistem&aacute;tica crontab + systemd na EC2 ETL "
    "pra validar quais dos ~21 pipelines locais realmente rodam em produ&ccedil;&atilde;o "
    "(excluir drafts/experimentos antes de commitar)",
    "<b>Fase B.3 (PR #3)</b> &mdash; script Python pra extrair DDL das ~10 views via "
    "<b>db/supernova.py</b> &rarr; commitar em <b>multibet_pipelines/sql/views/</b>",
    "<b>Call com Mauro (30 min)</b> antes do PR #2 pra alinhar escopo e evitar "
    "duplica&ccedil;&atilde;o com trabalhos em curso da squad",
]))

# --- Rodape ---
story.append(Spacer(1, 14))
story.append(hr())
story.append(Paragraph(
    "<i>An&aacute;lise derivada do relat&oacute;rio &lsquo;Mapeamento de Uso - Schema multibet&rsquo; "
    "enviado pelo Mauro em 16/04/2026 17:11. Documenta&ccedil;&atilde;o t&eacute;cnica completa "
    "em docs/_migration/contribuicao_ao_relatorio_mauro.md (projeto local).</i>", META))


# --- Build ---
doc = SimpleDocTemplate(
    str(OUTPUT), pagesize=A4,
    leftMargin=1.8*cm, rightMargin=1.8*cm,
    topMargin=1.8*cm, bottomMargin=1.8*cm,
    title="Contribuicao ao Relatorio do Mauro - Mateus Fabro",
    author="Mateus Fabro",
)


def on_page(canvas, doc):
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(CINZA_META)
    canvas.drawRightString(A4[0] - 1.8*cm, 1.0*cm, f"Pagina {doc.page}")
    canvas.drawString(1.8*cm, 1.0*cm,
        "Contribuicao ao Relatorio - 16/04/2026 - Mateus Fabro (Squad 3 Intelligence Engine)")
    canvas.restoreState()


doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
print(f"PDF gerado: {OUTPUT.resolve()}")
print(f"Tamanho:    {OUTPUT.stat().st_size / 1024:.1f} KB")
