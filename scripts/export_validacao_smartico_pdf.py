"""
export_validacao_smartico_pdf.py
================================================================
Converte docs/validacao_smartico_push_abr2026.md em PDF publicavel,
formatado para compartilhamento com CRM e gestao.

Uso:
    python scripts/export_validacao_smartico_pdf.py

Saida:
    docs/validacao_smartico_push_abr2026.pdf
================================================================
"""
from __future__ import annotations

from pathlib import Path

import markdown
from xhtml2pdf import pisa

ROOT = Path(__file__).resolve().parent.parent
MD_PATH = ROOT / "docs" / "validacao_smartico_push_abr2026.md"
PDF_PATH = ROOT / "docs" / "validacao_smartico_push_abr2026.pdf"

CSS = """
@page {
  size: A4;
  margin: 2cm 1.8cm 2.5cm 1.8cm;
  @frame footer_frame {
    -pdf-frame-content: footer_content;
    left: 1.8cm; width: 17.4cm; top: 27.5cm; height: 1cm;
  }
}
body { font-family: Helvetica, Arial, sans-serif; font-size: 10pt; color: #222; line-height: 1.45; }
h1 { font-size: 20pt; color: #0b3b6f; border-bottom: 2px solid #0b3b6f; padding-bottom: 6px; margin-top: 4px; }
/* page-break-after: avoid so no h2/h4; h3 sem avoid para nao gerar pagina em branco quando tabela grande segue */
h2 { font-size: 15pt; color: #0b3b6f; margin-top: 14px; margin-bottom: 6px; border-bottom: 1px solid #ccc; padding-bottom: 3px; page-break-after: avoid; }
h3 { font-size: 12pt; color: #333; margin-top: 10px; margin-bottom: 4px; }
h4 { font-size: 11pt; color: #444; margin-top: 8px; margin-bottom: 3px; page-break-after: avoid; }
p  { margin: 6px 0; orphans: 2; widows: 2; }
code { background: #f4f4f4; padding: 1px 4px; font-family: Consolas, monospace; font-size: 9pt; border: 1px solid #e0e0e0; word-break: break-all; }
pre  { background: #f7f7f7; padding: 8px; border: 1px solid #ddd; font-family: Consolas, monospace; font-size: 8.5pt; white-space: pre-wrap; word-wrap: break-word; }
pre code { border: 0; background: transparent; padding: 0; word-break: normal; }
/* table-layout: fixed + word-break forcam celulas a respeitar largura. Tabelas podem quebrar entre linhas para evitar pagina em branco. */
table { border-collapse: collapse; width: 100%; margin: 8px 0; table-layout: fixed; }
th, td { border: 1px solid #bbb; padding: 5px 6px; text-align: left; font-size: 8.5pt; vertical-align: top; word-wrap: break-word; word-break: break-word; overflow-wrap: break-word; white-space: normal; }
th { background: #e8eef7; color: #0b3b6f; }
ul, ol { margin: 6px 0 6px 18px; }
li { margin: 2px 0; }
strong { color: #000; }
a { color: #0b3b6f; text-decoration: none; }
hr { border: 0; border-top: 1px solid #ccc; margin: 12px 0; }
.footer { font-size: 8pt; color: #888; text-align: center; }
"""

FOOTER_HTML = """
<div id="footer_content" class="footer">
  Matriz de Risco v2 - Validacao Smartico - PGX/MultiBet - pagina <pdf:pagenumber/> de <pdf:pagecount/>
</div>
"""


def main() -> None:
    if not MD_PATH.exists():
        raise SystemExit(f"Arquivo nao encontrado: {MD_PATH}")

    md_text = MD_PATH.read_text(encoding="utf-8")
    html_body = markdown.markdown(
        md_text,
        extensions=["tables", "fenced_code", "sane_lists"],
    )
    html = f"""<!DOCTYPE html>
<html lang="pt-BR"><head><meta charset="utf-8"><style>{CSS}</style></head>
<body>
{html_body}
{FOOTER_HTML}
</body></html>"""

    with PDF_PATH.open("wb") as f:
        result = pisa.CreatePDF(src=html, dest=f, encoding="utf-8")
    if result.err:
        raise SystemExit(f"Falha na conversao: {result.err} erros")

    size_kb = PDF_PATH.stat().st_size / 1024
    print(f"OK: {PDF_PATH.relative_to(ROOT)} ({size_kb:.1f} KB)")


if __name__ == "__main__":
    main()
