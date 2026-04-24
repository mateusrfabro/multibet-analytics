"""
Gera PDF do mini relatorio PR #1 no padrao do Mauro.
Input:  docs/_migration/mini_relatorio_pr1_mauro.md (referencia de conteudo)
Output: docs/_migration/mini_relatorio_pr1_mauro.pdf

Padrao visual (baseado no PDF do Mauro 16/04):
- Titulo h1 azul escuro #1a3f6c
- Subtitulo cinza com data/contexto
- Secoes numeradas com linha horizontal
- Tabelas: header azul escuro + linhas alternadas cinza claro
- Bullets com marcadores redondos
"""
from reportlab.lib import colors
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import cm, mm
from reportlab.platypus import (
    SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle,
    HRFlowable, KeepTogether, PageBreak, ListFlowable, ListItem
)
from reportlab.lib.enums import TA_LEFT, TA_CENTER
from pathlib import Path

OUTPUT = Path("docs/_migration/mini_relatorio_pr1_mauro.pdf")
OUTPUT.parent.mkdir(parents=True, exist_ok=True)

# --- Cores (padrao Mauro) ---
AZUL_ESCURO = colors.HexColor("#1a3f6c")
AZUL_MEDIO = colors.HexColor("#2d5a8e")
CINZA_HEADER_TABELA = colors.HexColor("#1a3f6c")
CINZA_LINHA_ALT = colors.HexColor("#f5f5f7")
CINZA_BORDA = colors.HexColor("#d0d0d0")
CINZA_TEXTO = colors.HexColor("#333333")
CINZA_META = colors.HexColor("#666666")
BRANCO = colors.white

# --- Estilos ---
styles = getSampleStyleSheet()

H1 = ParagraphStyle(
    "H1", parent=styles["Heading1"],
    fontName="Helvetica-Bold", fontSize=18, textColor=AZUL_ESCURO,
    spaceAfter=6, leading=22, alignment=TA_LEFT,
)
META = ParagraphStyle(
    "META", parent=styles["Normal"],
    fontName="Helvetica", fontSize=9, textColor=CINZA_META,
    spaceAfter=2, leading=13,
)
RESUMO = ParagraphStyle(
    "RESUMO", parent=styles["Normal"],
    fontName="Helvetica-Bold", fontSize=10, textColor=CINZA_TEXTO,
    spaceBefore=4, spaceAfter=4, leading=14,
)
H2 = ParagraphStyle(
    "H2", parent=styles["Heading2"],
    fontName="Helvetica-Bold", fontSize=13, textColor=AZUL_ESCURO,
    spaceBefore=16, spaceAfter=6, leading=16,
)
H3 = ParagraphStyle(
    "H3", parent=styles["Heading3"],
    fontName="Helvetica-Bold", fontSize=11, textColor=AZUL_MEDIO,
    spaceBefore=10, spaceAfter=4, leading=14,
)
P = ParagraphStyle(
    "P", parent=styles["Normal"],
    fontName="Helvetica", fontSize=9.5, textColor=CINZA_TEXTO,
    spaceAfter=4, leading=13, alignment=TA_LEFT,
)
P_CODE = ParagraphStyle(
    "P_CODE", parent=styles["Normal"],
    fontName="Courier", fontSize=8.5, textColor=CINZA_TEXTO, leading=12,
)
CELL = ParagraphStyle(
    "CELL", parent=styles["Normal"],
    fontName="Helvetica", fontSize=8.5, textColor=CINZA_TEXTO, leading=11,
)
CELL_BOLD = ParagraphStyle(
    "CELL_BOLD", parent=styles["Normal"],
    fontName="Helvetica-Bold", fontSize=8.5, textColor=CINZA_TEXTO, leading=11,
)
CELL_HEADER = ParagraphStyle(
    "CELL_HEADER", parent=styles["Normal"],
    fontName="Helvetica-Bold", fontSize=9, textColor=BRANCO, leading=11,
)


def hr():
    return HRFlowable(width="100%", thickness=0.75, color=CINZA_BORDA,
                      spaceBefore=2, spaceAfter=10)


def section_header(num, title):
    return [
        Paragraph(f"{num}. {title}", H2),
        HRFlowable(width="100%", thickness=0.5, color=AZUL_ESCURO,
                   spaceBefore=0, spaceAfter=8),
    ]


def make_table(header, rows, col_widths=None):
    data = [[Paragraph(str(c), CELL_HEADER) for c in header]]
    for r in rows:
        data.append([Paragraph(str(c), CELL) for c in r])

    t = Table(data, colWidths=col_widths, repeatRows=1)
    style = [
        ("BACKGROUND", (0, 0), (-1, 0), CINZA_HEADER_TABELA),
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
    # zebra striping
    for i in range(1, len(data)):
        if i % 2 == 0:
            style.append(("BACKGROUND", (0, i), (-1, i), CINZA_LINHA_ALT))
    t.setStyle(TableStyle(style))
    return t


def bullet_list(items):
    return ListFlowable(
        [ListItem(Paragraph(it, P), leftIndent=12, value="-") for it in items],
        bulletType="bullet", start="-", leftIndent=14, bulletFontSize=9,
        spaceBefore=2, spaceAfter=6,
    )


# --- Conteudo ---
story = []

# TITULO
story.append(Paragraph(
    "PR #1 multibet_pipelines &mdash; Delta vs Relat&oacute;rio de 16/04",
    H1))
story.append(hr())

story.append(Paragraph(
    "<b>Gerado em:</b> 2026-04-16 17:30 &nbsp;|&nbsp; "
    "<b>Repo:</b> GL-Analytics-M-L/multibet_pipelines &nbsp;|&nbsp; "
    "<b>Branch:</b> sync-ec2-prod-20260416",
    META))
story.append(Paragraph(
    '<b>PR URL:</b> <link href="https://github.com/GL-Analytics-M-L/multibet_pipelines/pull/new/sync-ec2-prod-20260416" color="blue">'
    'https://github.com/GL-Analytics-M-L/multibet_pipelines/pull/new/sync-ec2-prod-20260416</link>',
    META))
story.append(Spacer(1, 6))
story.append(Paragraph(
    "<b>Resumo:</b> 8 commits at&ocirc;micos &nbsp;|&nbsp; 44 arquivos &nbsp;|&nbsp; "
    "+7.336 linhas &nbsp;|&nbsp; -1.412 linhas",
    RESUMO))
story.append(Paragraph(
    "<b>Impacto no relat&oacute;rio de 16/04:</b> 3 objetos saem de "
    "&lsquo;sem refer&ecirc;ncia&rsquo; &rarr; &lsquo;em uso&rsquo; &nbsp;|&nbsp; "
    "15 novos artefatos no repo",
    RESUMO))

# --- SECAO 1 ---
story.extend(section_header(
    "1",
    "Objetos que passam de [ ] Sem ref para [x] Em uso ap&oacute;s merge"
))

story.append(make_table(
    ["Status antes", "Status depois", "Objeto", "Pipeline/Script que passa a referenciar"],
    [
        ["[ ] Sem ref", "[x] Em uso", "fact_sports_odds_performance (280 kB)",
         "pipelines/fact_sports_odds_performance.py"],
        ["[ ] Sem ref", "[x] Em uso", "pcr_ratings (37 MB, 156K regs)",
         "pipelines/pcr_pipeline.py"],
        ["[ ] Sem ref", "[x] Em uso", "fact_ad_spend (944 kB, 2.464 regs)",
         "pipelines/sync_google_ads_spend.py + pipelines/sync_meta_spend.py"],
    ],
    col_widths=[2.3*cm, 2.3*cm, 5.2*cm, 7.2*cm]
))
story.append(Spacer(1, 8))
story.append(Paragraph(
    "<b>Delta no c&aacute;lculo do relat&oacute;rio:</b> Antes 33/114 (28%) &rarr; "
    "Depois <b>36/114 (31%)</b> &mdash; ganho de +3 tabelas documentadas.",
    P))

# --- SECAO 2 ---
story.extend(section_header(
    "2",
    "Novos arquivos no repo multibet_pipelines (44 total)"
))

story.append(Paragraph("<b>2.1 Conectores (db/) &mdash; 3 arquivos</b>", H3))
story.append(make_table(
    ["Arquivo", "Tamanho", "Fun&ccedil;&atilde;o"],
    [
        ["db/google_ads.py", "7.252 B", "Conector Google Ads API (alimenta fact_ad_spend)"],
        ["db/meta_ads.py", "6.760 B", "Conector Meta Graph API (alimenta fact_ad_spend)"],
        ["db/smartico_api.py", "13.588 B", "Conector Smartico S2S API (push de tags)"],
    ],
    col_widths=[5.0*cm, 2.2*cm, 9.8*cm]
))

story.append(Paragraph(
    "<b>2.2 Pipelines produtivos (pipelines/) &mdash; 6 novos + 1 atualizado</b>", H3))
story.append(make_table(
    ["Arquivo", "Cron (BRT &rarr; UTC)", "Tabela destino", "Obs"],
    [
        ["grandes_ganhos.py", "00:30 &rarr; 03:30", "grandes_ganhos",
         "Atualizado +197 linhas (Athena + file lock + CDN)"],
        ["sync_google_ads_spend.py", "01:00 &rarr; 04:00", "fact_ad_spend (Google)", "Novo"],
        ["sync_meta_spend.py", "01:15 &rarr; 04:15", "fact_ad_spend (Meta)", "Novo"],
        ["push_risk_to_smartico.py", "02:30 &rarr; 05:30", "Push externo (Smartico)", "Novo"],
        ["export_smartico_sent_today.py", "under-demand", "Support/audit", "Novo"],
        ["pcr_pipeline.py", "03:30 &rarr; 06:30", "pcr_ratings",
         "Novo &mdash; Player Credit Rating D-AAA"],
        ["fact_sports_odds_performance.py", "05:00 &rarr; 08:00",
         "fact_sports_odds_performance", "Novo &mdash; Win/Loss por odds"],
    ],
    col_widths=[5.3*cm, 3.4*cm, 4.2*cm, 4.1*cm]
))

story.append(Paragraph(
    "<b>2.3 Views Casino+Sportsbook (views_casino_sportsbook/) &mdash; pasta nova completa</b>", H3))
story.append(make_table(
    ["Arquivo", "Cron", "Alimenta"],
    [
        ["create_views_casino_sportsbook.py (DDL)", "manual", "10 tabelas fact/fct"],
        ["fact_casino_rounds.py", "04:30 BRT", "fact_casino_rounds"],
        ["fact_sports_bets.py", "04:30 BRT", "fact_sports_bets"],
        ["fact_sports_bets_by_sport.py", "04:30 BRT", "fact_sports_bets_by_sport"],
        ["fct_casino_activity.py", "04:30 BRT", "fct_casino_activity"],
        ["fct_sports_activity.py", "04:30 BRT", "fct_sports_activity"],
        ["fct_active_players_by_period.py", "intraday 12:07 + 18:07",
         "fct_active_players_by_period"],
        ["fct_player_performance_by_period.py", "intraday 12:07 + 18:07",
         "fct_player_performance_by_period"],
        ["vw_active_player_retention_weekly.py", "04:30 BRT", "view (retention)"],
        ["agg_cohort_acquisition.py", "04:30 BRT", "mv_cohort_aquisicao"],
        ["deploy/run/rollback scripts (.sh)", "-", "Deploy scripts EC2"],
    ],
    col_widths=[7.0*cm, 3.5*cm, 6.5*cm]
))

story.append(Paragraph(
    "<b>2.4 Scripts deploy/run &mdash; 7 novos</b>", H3))
story.append(bullet_list([
    "deploy_fact_sports_odds_performance.sh",
    "deploy_push_smartico.sh",
    "run_fact_sports_odds_performance.sh",
    "run_pcr_pipeline.sh",
    "run_push_smartico.sh",
    "run_sync_google_ads.sh",
    "run_sync_meta_ads.sh",
]))

story.append(Paragraph("<b>2.5 Documenta&ccedil;&atilde;o</b>", H3))
story.append(Paragraph(
    "<b>DEPLOY.md</b> expandido de <b>5 &rarr; 10 pipelines</b> documentados "
    "(cron, paths EC2, deploy steps por pipeline).", P))

# --- SECAO 3 ---
story.extend(section_header("3", "Bug estrutural corrigido (commit 1)"))
story.append(bullet_list([
    "Removido <b>db/db/</b> e <b>pipelines/pipelines/</b> (pastas duplicadas de commit antigo acidental).",
    "Zero imports usavam <b>db.db.*</b> &mdash; validado via grep em todo o projeto + clone. Remo&ccedil;&atilde;o safe.",
    "<b>1.412 linhas de c&oacute;digo morto</b> removidas.",
]))

# --- SECAO 4 ---
story.extend(section_header(
    "4", "Ainda [ ] Sem ref ap&oacute;s este PR (escopo de PRs seguintes)"))

story.append(Paragraph(
    "<b>PR #2 &mdash; Pipelines existentes no projeto local, pendentes de valida&ccedil;&atilde;o em prod</b>", H3))
story.append(bullet_list([
    "dim_marketing_mapping (3.241 regs) &mdash; tem pipelines/dim_marketing_mapping.py + _canonical.py",
    "dim_games_catalog (381 regs) &mdash; tem pipelines/dim_games_catalog.py",
    "fact_attribution (67 MB, 154K regs) &mdash; tem pipelines/fact_attribution.py",
    "fact_ftd_deposits, fact_redeposits, fact_registrations, fact_player_activity, "
    "fact_player_engagement_daily, fact_gaming_activity_daily, fact_jackpots, fact_live_casino",
    "fact_crm_daily_performance &mdash; tem pipelines/crm_daily_performance.py",
    "agg_cohort_acquisition, agg_game_performance",
    "crm_campaign_* (7 tables) &mdash; tem pipelines/crm_report_daily*.py + report_crm_promocoes.py",
]))

story.append(Paragraph(
    "<b>PR #3 &mdash; DDLs de views criadas manualmente via DBeaver</b>", H3))
story.append(bullet_list([
    "pcr_atual, pcr_resumo (fontes: pcr_ratings)",
    "vw_ad_spend_daily, vw_ad_spend_by_source (fontes: fact_ad_spend)",
    "vw_odds_performance_by_range, vw_odds_performance_summary (fontes: fact_sports_odds_performance)",
    "vw_acquisition_channel (fontes: dim_marketing_mapping)",
]))

story.append(Paragraph(
    "<b>Fora do escopo deste PR &mdash; outras &aacute;reas da squad (j&aacute; t&ecirc;m repos pr&oacute;prios)</b>", H3))
story.append(bullet_list([
    "matriz_financeiro (+ mensal/semanal/hora), matriz_aquisicao &mdash; views de dashboard",
    "cohort_aquisicao, cohort_retencao_ftd, heatmap_hour &mdash; usadas em MVs de dashboard",
    "tab_* &mdash; alimentadas por sync_all / sync_all_aquisicao / sync_user_daily",
    "silver_* &mdash; alimentadas por top_wins / game_activity_30d",
]))

# --- SECAO 5 ---
story.extend(section_header("5", "Valida&ccedil;&otilde;es de seguran&ccedil;a executadas"))
story.append(make_table(
    ["Item", "Status"],
    [
        ["SSH EC2 ETL (54.197.63.138) para confirmar vers&otilde;es em produ&ccedil;&atilde;o", "[x] OK"],
        ["Snapshot EC2 antes (multibet_pipelines_snapshot_20260416.tar.gz, 141 MB)",
         "[x] OK &mdash; rollback dispon&iacute;vel"],
        ["Scan de credenciais hard-coded (AKIA, ghp_, Bearer, PASSWORD=, etc.)",
         "[x] OK &mdash; zero ocorr&ecirc;ncias"],
        ["Scan de imports quebrados (db.db.*) antes de remover duplica&ccedil;&atilde;o",
         "[x] OK &mdash; zero ocorr&ecirc;ncias"],
        ["Ordem de commits (db/ antes dos pipelines que importam)",
         "[x] OK &mdash; previne broken checkout"],
        ["Diff de arquivos j&aacute; existentes (repo vs EC2, ignorando CRLF/LF)",
         "[x] 12 iguais, 1 atualizado (grandes_ganhos.py)"],
        ["origin/main sem commit novo desde clone",
         "[x] OK &mdash; zero conflito de rebase"],
        ["Processo: EC2 &rarr; git (nunca git &rarr; EC2)",
         "[x] OK &mdash; nenhum bit da EC2 tocado"],
    ],
    col_widths=[12.5*cm, 4.5*cm]
))

# --- SECAO 6 ---
story.extend(section_header("6", "Impacto em produ&ccedil;&atilde;o"))
story.append(Paragraph(
    "<b>Zero.</b> Este PR apenas espelha no git o que j&aacute; est&aacute; rodando h&aacute; "
    "semanas na EC2. N&atilde;o altera EC2.", P))
story.append(Paragraph(
    "Ap&oacute;s o merge, o git passa a ser a fonte de verdade &mdash; base necess&aacute;ria para "
    "qualquer deploy futuro seguir o fluxo <b>&lsquo;git primeiro, depois EC2&rsquo;</b> "
    "(regra formalizada em CLAUDE.md do projeto).", P))

# --- SECAO 7 ---
story.extend(section_header("7", "Reviewers sugeridos"))
story.append(bullet_list([
    "<b>Mauro</b> &mdash; pipelines/ + conectores db/",
    "<b>Gusta</b> &mdash; deploy/run scripts + paths EC2",
    "<b>Castrin</b> &mdash; vis&atilde;o executiva / aprova&ccedil;&atilde;o final merge",
]))

# --- Rodape ---
story.append(Spacer(1, 16))
story.append(hr())
story.append(Paragraph(
    "<i>Gerado ap&oacute;s auditoria completa: SSH EC2 ETL + SSM EC2 Apps + revis&atilde;o "
    "por 2 agentes (auditor + best-practices). Documento t&eacute;cnico completo dispon&iacute;vel "
    "em docs/_migration/mapping_arquivo_repo.md.</i>",
    META))


# --- Build ---
doc = SimpleDocTemplate(
    str(OUTPUT), pagesize=A4,
    leftMargin=1.8*cm, rightMargin=1.8*cm,
    topMargin=1.8*cm, bottomMargin=1.8*cm,
    title="PR #1 multibet_pipelines - Delta vs Relatorio 16/04",
    author="Mateus Fabro",
)


def on_page(canvas, doc):
    """Rodape com numero de pagina."""
    canvas.saveState()
    canvas.setFont("Helvetica", 8)
    canvas.setFillColor(CINZA_META)
    canvas.drawRightString(
        A4[0] - 1.8*cm, 1.0*cm,
        f"P&aacute;gina {doc.page}".replace("&aacute;", "a")
    )
    canvas.drawString(
        1.8*cm, 1.0*cm,
        "PR #1 multibet_pipelines - Delta 16/04/2026 - Mateus Fabro (Squad Intelligence Engine)"
    )
    canvas.restoreState()


doc.build(story, onFirstPage=on_page, onLaterPages=on_page)
print(f"PDF gerado: {OUTPUT.resolve()}")
print(f"Tamanho:    {OUTPUT.stat().st_size / 1024:.1f} KB")
