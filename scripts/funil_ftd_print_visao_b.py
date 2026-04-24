"""PNG final - Visao B - Funil FTD->STD->TTD->QTD+ Abril/2026."""
import os, matplotlib.pyplot as plt

OUT = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/output/funil_ftd_abril_FINAL.png"

ftd, std, ttd, q4 = 34785, 9522, 4459, 2872
fmt = lambda n: f"{n:,}".replace(",", ".")

fig = plt.figure(figsize=(10, 11))

# ---- Titulo ----
fig.text(0.5, 0.965, "Funil FTD > STD > TTD > QTD+",
         ha="center", va="top", fontsize=20, weight="bold")
fig.text(0.5, 0.925, "Abril/2026 (01-23, D-1 fechado)  |  Casa toda - sem afiliado",
         ha="center", va="top", fontsize=12, color="#333")
fig.text(0.5, 0.898, "Inclui: Organico + Google + Meta + TikTok",
         ha="center", va="top", fontsize=10, style="italic", color="#666")

# ---- Tabela ----
ax_tab = fig.add_axes([0.04, 0.52, 0.92, 0.33])
ax_tab.axis("off")

pct_std = round(std/ftd*100, 1)
pct_ttd = round(ttd/ftd*100, 1)
pct_q4  = round(q4/ftd*100, 1)
step_ttd = round(ttd/std*100, 1)
step_q4  = round(q4/ttd*100, 1)

rows = [
    ["FTD  (1o deposito)",   fmt(ftd), "100,0%", "—"],
    ["STD  (2o deposito)",   fmt(std), f"{pct_std}%".replace(".", ","),  f"{pct_std}%".replace(".", ",")],
    ["TTD  (3o deposito)",   fmt(ttd), f"{pct_ttd}%".replace(".", ","),  f"{step_ttd}%".replace(".", ",")],
    ["QTD+ (4o+ depositos)", fmt(q4),  f"{pct_q4}%".replace(".", ","),   f"{step_q4}%".replace(".", ",")],
]
cols = ["Etapa", "Jogadores", "% vs FTD", "% vs etapa anterior"]
colw = [0.36, 0.20, 0.18, 0.26]

tbl = ax_tab.table(cellText=rows, colLabels=cols, cellLoc="center",
                   loc="center", colWidths=colw)
tbl.auto_set_font_size(False)
tbl.set_fontsize(12)
tbl.scale(1, 2.4)

for j in range(len(cols)):
    c = tbl[0, j]
    c.set_facecolor("#1f4e79")
    c.set_text_props(color="white", weight="bold")

for i in range(1, len(rows)+1):
    tbl[i, 0].set_facecolor("#f0f4f9")
    tbl[i, 0].set_text_props(weight="bold", ha="left")
    tbl[i, 0].PAD = 0.05
    tbl[i, 1].set_text_props(weight="bold")

# ---- Leitura ----
leitura = (
    "LEITURA DO FUNIL\n\n"
    f"Dos {fmt(ftd)} jogadores que fizeram o 1o deposito (FTD) em abril,\n"
    f"apenas 27% voltaram para um 2o deposito (STD = {fmt(std)}).\n\n"
    f"Desses que voltaram, 47% chegaram ao 3o deposito (TTD = {fmt(ttd)})\n"
    "- ou seja, 13% do total de FTDs do mes.\n\n"
    f"E apenas 8% dos FTDs ({fmt(q4)} jogadores) se fidelizaram com\n"
    "4+ depositos no mes (QTD+) - a base recorrente de verdade.\n\n"
    "GARGALO: a maior perda acontece no FTD > STD (73% nao retornam)."
)
fig.text(0.5, 0.42, leitura, ha="center", va="top", fontsize=11.5,
         color="#222",
         bbox=dict(boxstyle="round,pad=0.8", facecolor="#f7f9fc",
                   edgecolor="#1f4e79", linewidth=1.4))

# ---- Rodape ----
fig.text(0.5, 0.025,
         "Coorte: 1o deposito entre 01 e 23/04. Funil = 2o/3o/4o+ deposito DENTRO do mes. Casa toda sem afiliado parceiro.\n"
         "Fonte: Athena ps_bi.dim_user + cashier_ec2  |  Gerado em 24/04/2026  |  Squad Intelligence Engine",
         ha="center", va="bottom", fontsize=8, style="italic", color="#888")

os.makedirs(os.path.dirname(OUT), exist_ok=True)
plt.savefig(OUT, dpi=180, bbox_inches="tight", facecolor="white")
print(f"OK: {OUT}")
