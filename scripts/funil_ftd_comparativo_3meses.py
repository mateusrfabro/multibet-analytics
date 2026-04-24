"""PNG comparativo lado a lado - Fev, Mar, Abr/2026 - Visao B."""
import os, matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch

OUT = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/output/funil_ftd_COMPARATIVO_fev_mar_abr.png"

# Dados Visao B (organic + google + meta + tiktok, sem afiliado parceiro)
meses = [
    {"nome": "Fevereiro/2026", "periodo": "01 a 28/02",
     "ftd": 13186, "std": 5789, "ttd": 3496, "q4": 2524, "cor": "#2a7a3b"},
    {"nome": "Marco/2026",     "periodo": "01 a 31/03",
     "ftd": 26013, "std": 9503, "ttd": 5419, "q4": 3825, "cor": "#c88116"},
    {"nome": "Abril/2026",     "periodo": "01 a 23/04 (D-1)*",
     "ftd": 34785, "std": 9522, "ttd": 4459, "q4": 2872, "cor": "#1f4e79"},
]

fmt = lambda n: f"{n:,}".replace(",", ".")

def pct(a, b, d=1):
    return f"{round(a/b*100, d)}%".replace(".", ",") if b else "-"

fig = plt.figure(figsize=(18, 10))

fig.text(0.5, 0.965, "Funil FTD > STD > TTD > QTD+  -  Comparativo 2026",
         ha="center", va="top", fontsize=22, weight="bold")
fig.text(0.5, 0.930, "Casa toda - sem afiliado",
         ha="center", va="top", fontsize=13, color="#333")
fig.text(0.5, 0.905, "Inclui: Organico + Google + Meta + TikTok",
         ha="center", va="top", fontsize=10, style="italic", color="#666")

# 3 tabelas lado a lado
for idx, m in enumerate(meses):
    x0 = 0.03 + idx * 0.325
    w  = 0.30

    # Cabecalho do mes
    fig.text(x0 + w/2, 0.87, m["nome"], ha="center", va="top",
             fontsize=17, weight="bold", color=m["cor"])
    fig.text(x0 + w/2, 0.84, m["periodo"], ha="center", va="top",
             fontsize=10, style="italic", color="#555")

    ax = fig.add_axes([x0, 0.50, w, 0.32]); ax.axis("off")

    ps  = pct(m["std"], m["ftd"])
    pt  = pct(m["ttd"], m["ftd"])
    pq  = pct(m["q4"],  m["ftd"])
    stt = pct(m["ttd"], m["std"])
    stq = pct(m["q4"],  m["ttd"])

    rows = [
        ["FTD",    fmt(m["ftd"]), "100,0%", "—"],
        ["STD",    fmt(m["std"]), ps, ps],
        ["TTD",    fmt(m["ttd"]), pt, stt],
        ["QTD+",   fmt(m["q4"]),  pq, stq],
    ]
    cols = ["Etapa", "Jogadores", "% vs FTD", "% vs etapa anterior"]
    tbl = ax.table(cellText=rows, colLabels=cols, cellLoc="center",
                   loc="center", colWidths=[0.18, 0.26, 0.22, 0.34])
    tbl.auto_set_font_size(False); tbl.set_fontsize(11); tbl.scale(1, 2.2)
    for j in range(len(cols)):
        c = tbl[0, j]; c.set_facecolor(m["cor"])
        c.set_text_props(color="white", weight="bold")
    for i in range(1, len(rows)+1):
        tbl[i, 0].set_facecolor("#f0f4f9"); tbl[i, 0].set_text_props(weight="bold")
        tbl[i, 1].set_text_props(weight="bold")

    # Leitura curta
    gargalo = round(100 - m["std"]/m["ftd"]*100)
    leitura = (
        f"Dos {fmt(m['ftd'])} FTDs,\n"
        f"{ps} voltaram para STD.\n"
        f"{pq} dos FTDs se fidelizaram\n"
        f"(4+ depositos no mes).\n\n"
        f"GARGALO FTD>STD: {gargalo}% saem."
    )
    fig.text(x0 + w/2, 0.42, leitura, ha="center", va="top", fontsize=10,
             color="#222",
             bbox=dict(boxstyle="round,pad=0.5", facecolor="#f7f9fc",
                       edgecolor=m["cor"], linewidth=1.2))

# Box de insights consolidados
insight = (
    "TENDENCIA TRIMESTRAL (Fev > Mar > Abr):\n\n"
    "Volume de FTDs cresceu 2,6x de fev para abr, MAS a taxa de fidelizacao (QTD+) caiu de 19% para 8%.\n"
    "Conversao FTD>STD caiu de 44% (fev) para 27% (abr) - sinal de dilui"
    "cao da qualidade da coorte conforme o volume escala.\n"
    "Acao sugerida: investigar se o mix de canais/campanhas em abril esta trazendo jogadores com menor intencao de deposito recorrente."
)
fig.text(0.5, 0.21, insight, ha="center", va="top", fontsize=11,
         color="#1f4e79", weight="normal",
         bbox=dict(boxstyle="round,pad=0.8", facecolor="#fff8e7",
                   edgecolor="#c88116", linewidth=1.5))

fig.text(0.5, 0.03,
         "*Abril parcial (D-1 fechado em 23/04). Coorte: jogadores cujo 1o deposito ocorreu no mes. Funil = 2o/3o/4o+ deposito DENTRO do mesmo mes.  |  "
         "Fonte: Athena ps_bi.dim_user + cashier_ec2  |  Squad Intelligence Engine  |  24/04/2026",
         ha="center", va="bottom", fontsize=8, style="italic", color="#888")

plt.savefig(OUT, dpi=170, bbox_inches="tight", facecolor="white")
print(f"OK: {OUT}")
