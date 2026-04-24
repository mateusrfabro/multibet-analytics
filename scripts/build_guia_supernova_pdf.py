"""
Converte docs/guia_supernova_dashboard_usuario_FINAL.md em PDF estilizado.
Pipeline: markdown -> HTML com CSS print-friendly -> Edge headless -> PDF.
"""
import os
import subprocess
import sys
from pathlib import Path

import markdown

ROOT = Path(__file__).resolve().parent.parent
MD = ROOT / "docs" / "guia_supernova_dashboard_usuario_FINAL.md"
HTML = ROOT / "docs" / "guia_supernova_dashboard_usuario_FINAL.html"
PDF = ROOT / "docs" / "guia_supernova_dashboard_usuario_FINAL.pdf"

EDGE = r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe"

CSS = """
@page {
  size: A4;
  margin: 18mm 16mm 20mm 16mm;
}
* { box-sizing: border-box; }
html, body {
  font-family: 'Segoe UI', 'Helvetica Neue', Arial, sans-serif;
  color: #1f2937;
  font-size: 10.5pt;
  line-height: 1.55;
  -webkit-print-color-adjust: exact;
  print-color-adjust: exact;
}
body { max-width: 780px; margin: 0 auto; padding: 0 4px; }
h1 {
  font-size: 22pt; color: #6238C1; border-bottom: 3px solid #6238C1;
  padding-bottom: 8px; margin-top: 28px; margin-bottom: 14px;
  page-break-after: avoid;
}
h1:first-of-type { margin-top: 0; }
h2 {
  font-size: 15pt; color: #4c1d95; margin-top: 24px; margin-bottom: 10px;
  border-left: 4px solid #A78BFA; padding-left: 10px;
  page-break-after: avoid;
}
h3 {
  font-size: 12.5pt; color: #5b21b6; margin-top: 18px; margin-bottom: 8px;
  page-break-after: avoid;
}
h4 { font-size: 11pt; color: #6b21a8; margin-top: 14px; margin-bottom: 6px; }
p { margin: 6px 0 10px 0; text-align: justify; }
ul, ol { margin: 6px 0 12px 22px; padding: 0; }
li { margin-bottom: 4px; }
strong { color: #111827; }
code {
  background: #f3f4f6; border-radius: 3px; padding: 1px 5px;
  font-family: 'Consolas', 'Courier New', monospace; font-size: 9.5pt;
  color: #be185d;
}
blockquote {
  border-left: 4px solid #A78BFA; background: #faf5ff;
  margin: 10px 0; padding: 8px 14px; color: #4c1d95;
  border-radius: 0 6px 6px 0; font-size: 10pt;
}
blockquote p { margin: 4px 0; }
hr { border: none; border-top: 1px solid #e5e7eb; margin: 18px 0; }
table {
  width: 100%; border-collapse: collapse; margin: 10px 0 14px 0;
  font-size: 9.5pt; page-break-inside: avoid;
}
th {
  background: #6238C1; color: #fff; text-align: left;
  padding: 7px 9px; font-weight: 600;
}
td { padding: 6px 9px; border-bottom: 1px solid #e5e7eb; vertical-align: top; }
tr:nth-child(even) td { background: #fafafa; }
tr { page-break-inside: avoid; }
/* Capa */
.cover {
  text-align: center; padding: 60px 20px 80px 20px;
  page-break-after: always;
}
.cover .tag {
  font-size: 11pt; letter-spacing: 0.25em; color: #6238C1;
  text-transform: uppercase; font-weight: 600;
}
.cover .title {
  font-size: 34pt; font-weight: 800; color: #1f2937;
  margin: 20px 0 8px 0; letter-spacing: -0.5px;
}
.cover .sub {
  font-size: 14pt; color: #6b7280; font-weight: 400;
  margin-bottom: 40px;
}
.cover .badge {
  display: inline-block; background: #6238C1; color: #fff;
  padding: 8px 22px; border-radius: 999px; font-size: 10pt;
  letter-spacing: 0.1em; text-transform: uppercase;
}
.cover .meta {
  margin-top: 50px; color: #9ca3af; font-size: 10pt;
}
.cover .logo {
  font-size: 56pt; color: #6238C1; margin-bottom: 10px;
}
/* Section break before each H1 (exceto o primeiro) */
h1 { page-break-before: always; }
h1:first-of-type { page-break-before: auto; }
"""

COVER_HTML = """
<div class="cover">
  <div class="logo">&#x25C8;</div>
  <div class="tag">Plataforma de Analytics</div>
  <div class="title">Super Nova DB</div>
  <div class="sub">Guia do Usuário Final</div>
  <div class="badge">Abril / 2026</div>
  <div class="meta">
    Operações, CRM, Tráfego, Produto, Diretoria<br>
    Super Nova Gaming &middot; Apostas Reguladas
  </div>
</div>
"""


def build_html():
    md_text = MD.read_text(encoding="utf-8")
    body = markdown.markdown(
        md_text,
        extensions=["extra", "tables", "sane_lists", "toc"],
    )
    html = f"""<!DOCTYPE html>
<html lang="pt-BR">
<head>
<meta charset="utf-8">
<title>Super Nova DB - Guia do Usuario</title>
<style>{CSS}</style>
</head>
<body>
{COVER_HTML}
{body}
</body>
</html>"""
    HTML.write_text(html, encoding="utf-8")
    print(f"[OK] HTML gerado: {HTML}")


def build_pdf():
    html_uri = HTML.as_uri()
    cmd = [
        EDGE,
        "--headless=new",
        "--disable-gpu",
        "--no-pdf-header-footer",
        f"--print-to-pdf={PDF}",
        html_uri,
    ]
    print("[CMD]", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    if result.returncode != 0:
        print("[STDERR]", result.stderr)
        print("[STDOUT]", result.stdout)
        sys.exit(1)
    print(f"[OK] PDF gerado: {PDF}")
    print(f"[SIZE] {PDF.stat().st_size / 1024:.1f} KB")


if __name__ == "__main__":
    build_html()
    build_pdf()
