"""
Gera PDF do Relatorio de Distribuicao de Bonus, Freespins e Cashbacks — Marco/2026
Mesmo formato do PDF de Dez/2025.
"""
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import cm
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors

output_path = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/reports/Relatorio_Distribuicao_Bonus_03_2026.pdf"

doc = SimpleDocTemplate(
    output_path,
    pagesize=A4,
    leftMargin=2.5 * cm,
    rightMargin=2.5 * cm,
    topMargin=2.5 * cm,
    bottomMargin=2.5 * cm,
)

styles = getSampleStyleSheet()

# Estilos customizados
title_style = ParagraphStyle(
    "CustomTitle",
    parent=styles["Normal"],
    fontSize=12,
    fontName="Helvetica-Bold",
    spaceAfter=4,
)
subtitle_style = ParagraphStyle(
    "CustomSubtitle",
    parent=styles["Normal"],
    fontSize=12,
    fontName="Helvetica-Bold",
    spaceAfter=12,
)
section_style = ParagraphStyle(
    "SectionTitle",
    parent=styles["Normal"],
    fontSize=12,
    fontName="Helvetica-Bold",
    spaceBefore=18,
    spaceAfter=8,
)
body_style = ParagraphStyle(
    "CustomBody",
    parent=styles["Normal"],
    fontSize=11,
    fontName="Helvetica",
    spaceAfter=6,
    leading=16,
)

elements = []

# Titulo
elements.append(Paragraph("Relatório de Distribuição de bonus, Freespins e Cashbacks", title_style))
elements.append(Spacer(1, 4))
elements.append(Paragraph("Período: Março de 2026", subtitle_style))

# Secao 1 - Resumo
elements.append(Paragraph("1. Resumo", section_style))
elements.append(Paragraph(
    "Em Março de 2026, foram distribuídos R$ 1.901.789,86 em Bônus e "
    "Freespins aos nossos jogadores com base nas informações disponibilizadas na "
    "plataforma.",
    body_style,
))

# Secao 2 - Tabela
elements.append(Paragraph("2. Tabela de Distribuição", section_style))
elements.append(Paragraph(
    "Abaixo, segue a tabela com a distribuição do mês:",
    body_style,
))
elements.append(Spacer(1, 8))

table_data = [
    ["Tipo", "Valor"],
    ["Bônus", "R$      741.855,53"],
    ["Freespins", "R$   1.159.934,33"],
    ["Total", "R$   1.901.789,86"],
]

table = Table(table_data, colWidths=[5 * cm, 6 * cm])
table.setStyle(TableStyle([
    ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
    ("FONTNAME", (0, 1), (-1, -1), "Helvetica"),
    ("FONTSIZE", (0, 0), (-1, -1), 11),
    ("ALIGN", (1, 0), (1, -1), "LEFT"),
    ("LINEBELOW", (0, 0), (-1, 0), 0.5, colors.black),
    ("LINEBELOW", (0, -1), (-1, -1), 0.5, colors.black),
    ("TOPPADDING", (0, 0), (-1, -1), 4),
    ("BOTTOMPADDING", (0, 0), (-1, -1), 4),
]))
elements.append(table)

# Secao 3 - Metodo e Fonte
elements.append(Paragraph("3. Método e Fonte", section_style))
elements.append(Paragraph(
    "O relatório foi feito utilizando dados extraídos da base de dados da plataforma "
    "(Pragmatic Solutions) via AWS Athena (Iceberg Data Lake), onde levantamos os "
    "valores distribuídos no período.",
    body_style,
))

doc.build(elements)
print(f"PDF gerado: {output_path}")