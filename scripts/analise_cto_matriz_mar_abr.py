"""
Analise matriz_financeiro: Marco completo vs Abril-ate-D-1 (15/04/2026).
Identifica dias outliers de fraude (FTD/deposito anormal) e recalcula metricas.

Fonte: Super Nova DB, schema multibet, views matriz_financeiro*
Demanda: CTO Gabriel Barbosa (pedido via WhatsApp, 16/04/2026)
Autor: Mateus F + Claude

Saidas:
    output/cto_matriz_diaria_mar_abr.csv        -- matriz diaria completa
    output/cto_outliers_fraude.csv              -- dias flagados como outlier
    output/cto_comparativo_mar_vs_abr.csv       -- KPIs com e sem outliers
"""

import sys
import os
from datetime import date
sys.path.insert(0, ".")

from db.supernova import execute_supernova
import csv

OUT_DIR = "output"
os.makedirs(OUT_DIR, exist_ok=True)


# =============================================================================
# 1. MATRIZ DIARIA (marco + abril ate D-1)
# =============================================================================
# D-1 porque regra: feedback_sempre_usar_d_menos_1.md
SQL_DIARIA = """
SELECT
    data,
    deposit, adpu, avg_dep, withdrawal, net_deposit,
    users, ftd, conversion, ftd_amount, avg_ftd_amount,
    turnover_cassino, win_cassino, ggr_cassino,
    turnover_sports, win_sports, ggr_sport,
    ggr_total, ngr, retencao, arpu, ativos
FROM multibet.matriz_financeiro
WHERE data >= DATE '2026-03-01'
  AND data <  DATE '2026-04-16'   -- ate D-1 (hoje = 16/04)
ORDER BY data ASC;
"""

# =============================================================================
# 2. MATRIZ MENSAL (marco + abril parcial)
# =============================================================================
SQL_MENSAL = """
SELECT
    data AS mes,
    deposit, adpu, avg_dep, withdrawal, net_deposit,
    users, ftd, conversion, ftd_amount, avg_ftd_amount,
    turnover_cassino, win_cassino, ggr_cassino,
    turnover_sports, win_sports, ggr_sport,
    ggr_total, ngr, retencao, arpu, ativos
FROM multibet.matriz_financeiro_mensal
WHERE data IN (DATE '2026-03-01', DATE '2026-04-01')
ORDER BY data ASC;
"""


def detectar_outliers(rows, col_idx_deposit, col_idx_ftd_amount, col_idx_avg_ftd):
    """
    Heuristica de deteccao de dias outliers:
      - deposit > media + 2*stddev        (dias de pico anormal)
      - avg_ftd_amount > 3x mediana       (FTD medio muito acima)
    Retorna set de datas outlier.
    """
    deposits = [float(r[col_idx_deposit] or 0) for r in rows]
    avg_ftds = [float(r[col_idx_avg_ftd] or 0) for r in rows if r[col_idx_avg_ftd]]

    if not deposits:
        return set()

    n = len(deposits)
    mean_dep = sum(deposits) / n
    var = sum((x - mean_dep) ** 2 for x in deposits) / n
    std_dep = var ** 0.5

    mediana_ftd = sorted(avg_ftds)[len(avg_ftds) // 2] if avg_ftds else 0

    outliers = set()
    for r in rows:
        d = r[0]  # data
        dep = float(r[col_idx_deposit] or 0)
        aftd = float(r[col_idx_avg_ftd] or 0)

        # Outlier tipo 1: deposito anormalmente alto (>2 sigma)
        if dep > mean_dep + 2 * std_dep:
            outliers.add(d)

        # Outlier tipo 2: ticket medio de FTD 3x acima da mediana
        if mediana_ftd and aftd > 3 * mediana_ftd:
            outliers.add(d)

    return outliers


def kpis_agregados(rows, cols, outliers=None):
    """Calcula KPIs agregados (soma / medias) de um conjunto de linhas."""
    rows_f = [r for r in rows if (outliers is None or r[0] not in outliers)]
    if not rows_f:
        return {}

    def s(idx):
        return sum(float(r[idx] or 0) for r in rows_f)

    def m(idx):
        vals = [float(r[idx] or 0) for r in rows_f if r[idx] is not None]
        return sum(vals) / len(vals) if vals else 0

    idx = {c: i for i, c in enumerate(cols)}

    return {
        "dias": len(rows_f),
        "deposit_total": s(idx["deposit"]),
        "deposit_medio_dia": m(idx["deposit"]),
        "withdrawal_total": s(idx["withdrawal"]),
        "net_deposit_total": s(idx["net_deposit"]),
        "ftd_total": int(s(idx["ftd"])),
        "ftd_amount_total": s(idx["ftd_amount"]),
        "avg_ftd_amount_medio": m(idx["avg_ftd_amount"]),
        "users_total": int(s(idx["users"])),
        "conversion_media": m(idx["conversion"]),
        "ggr_cassino_total": s(idx["ggr_cassino"]),
        "ggr_sport_total": s(idx["ggr_sport"]),
        "ggr_total": s(idx["ggr_total"]),
        "ngr_total": s(idx["ngr"]),
        "retencao_total": s(idx["retencao"]),
        "arpu_medio": m(idx["arpu"]),
        "ativos_medio_dia": m(idx["ativos"]),
    }


def salvar_csv(path, header, rows):
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(header)
        for r in rows:
            w.writerow(r)


def main():
    print("=" * 78)
    print("ANALISE CTO - Matriz Financeiro: Marco vs Abril (16/04/2026)")
    print("=" * 78)

    COLS = [
        "data", "deposit", "adpu", "avg_dep", "withdrawal", "net_deposit",
        "users", "ftd", "conversion", "ftd_amount", "avg_ftd_amount",
        "turnover_cassino", "win_cassino", "ggr_cassino",
        "turnover_sports", "win_sports", "ggr_sport",
        "ggr_total", "ngr", "retencao", "arpu", "ativos",
    ]

    # --- 1. Diaria
    print("\n[1/3] Extraindo matriz_financeiro DIARIA (2026-03-01 -> 2026-04-15)...")
    rows = execute_supernova(SQL_DIARIA, fetch=True)
    print(f"      {len(rows)} linhas obtidas.")

    salvar_csv(
        os.path.join(OUT_DIR, "cto_matriz_diaria_mar_abr.csv"),
        COLS, rows
    )
    print(f"      -> output/cto_matriz_diaria_mar_abr.csv")

    # --- 2. Outliers
    print("\n[2/3] Detectando dias outliers (fraude)...")
    idx_deposit = COLS.index("deposit")
    idx_ftd_amount = COLS.index("ftd_amount")
    idx_avg_ftd = COLS.index("avg_ftd_amount")

    outliers = detectar_outliers(rows, idx_deposit, idx_ftd_amount, idx_avg_ftd)
    print(f"      {len(outliers)} dias flagados como outlier:")
    for d in sorted(outliers):
        # Encontra a linha correspondente
        lin = next((r for r in rows if r[0] == d), None)
        if lin:
            print(f"        {d}: deposit=R${float(lin[idx_deposit]):>12,.2f}  "
                  f"ftd={lin[COLS.index('ftd')]}  "
                  f"avg_ftd=R${float(lin[idx_avg_ftd] or 0):>10,.2f}")

    outliers_rows = [r for r in rows if r[0] in outliers]
    salvar_csv(
        os.path.join(OUT_DIR, "cto_outliers_fraude.csv"),
        COLS, outliers_rows
    )
    print(f"      -> output/cto_outliers_fraude.csv")

    # --- 3. Comparativo Marco vs Abril (com e sem outliers)
    print("\n[3/3] Comparativo KPIs Marco vs Abril (com e sem outliers)...")

    marco = [r for r in rows if r[0].month == 3]
    abril = [r for r in rows if r[0].month == 4]

    kpis_mar_all = kpis_agregados(marco, COLS)
    kpis_mar_clean = kpis_agregados(marco, COLS, outliers)
    kpis_abr_all = kpis_agregados(abril, COLS)
    kpis_abr_clean = kpis_agregados(abril, COLS, outliers)

    print(f"\n  Marco:  {kpis_mar_all['dias']} dias brutos / {kpis_mar_clean['dias']} dias limpos")
    print(f"  Abril:  {kpis_abr_all['dias']} dias brutos / {kpis_abr_clean['dias']} dias limpos (parcial)")

    # Normaliza abril proporcional a 31 dias (para comparacao justa)
    def normaliza_proporcional(kpis_abril_clean, dias_marco):
        dias_abril = kpis_abril_clean["dias"]
        if dias_abril == 0:
            return {}
        fator = dias_marco / dias_abril
        # Normaliza metricas de soma (nao medias)
        somas = ["deposit_total", "withdrawal_total", "net_deposit_total",
                 "ftd_total", "ftd_amount_total", "users_total",
                 "ggr_cassino_total", "ggr_sport_total", "ggr_total",
                 "ngr_total", "retencao_total"]
        norm = {}
        for k, v in kpis_abril_clean.items():
            if k in somas:
                norm[k] = v * fator
            else:
                norm[k] = v
        norm["dias"] = dias_marco
        norm["_fator_normalizacao"] = fator
        return norm

    dias_marco_clean = kpis_mar_clean["dias"]
    kpis_abr_clean_norm = normaliza_proporcional(kpis_abr_clean, dias_marco_clean)

    # Salva comparativo consolidado
    comparativo_rows = []
    for metrica in kpis_mar_all.keys():
        if metrica.startswith("_"):
            continue
        comparativo_rows.append([
            metrica,
            kpis_mar_all.get(metrica, 0),
            kpis_mar_clean.get(metrica, 0),
            kpis_abr_all.get(metrica, 0),
            kpis_abr_clean.get(metrica, 0),
            kpis_abr_clean_norm.get(metrica, 0),
        ])

    salvar_csv(
        os.path.join(OUT_DIR, "cto_comparativo_mar_vs_abr.csv"),
        ["metrica", "marco_bruto", "marco_limpo",
         "abril_bruto_parcial", "abril_limpo_parcial", "abril_limpo_normalizado_28d"],
        comparativo_rows,
    )
    print(f"      -> output/cto_comparativo_mar_vs_abr.csv")

    # --- Print consolidado
    print("\n" + "=" * 78)
    print("RESUMO (marco completo vs abril parcial normalizado para 31 dias)")
    print("=" * 78)
    print(f"{'Metrica':<28} {'Marco (limpo)':>18} {'Abr norm. 31d':>18} {'Delta %':>10}")
    print("-" * 78)

    def fmt(v):
        if abs(v) >= 1_000_000:
            return f"R${v/1_000_000:,.2f}M"
        if abs(v) >= 1_000:
            return f"R${v/1_000:,.1f}k"
        return f"{v:,.2f}"

    def delta(a, b):
        if a == 0:
            return 0
        return (b - a) / a * 100

    linhas = [
        ("Deposit total", "deposit_total"),
        ("Net Deposit total", "net_deposit_total"),
        ("FTD (quantidade)", "ftd_total"),
        ("FTD Amount", "ftd_amount_total"),
        ("Avg FTD (ticket)", "avg_ftd_amount_medio"),
        ("Users cadastros", "users_total"),
        ("Conversion %", "conversion_media"),
        ("GGR Cassino", "ggr_cassino_total"),
        ("GGR Sports", "ggr_sport_total"),
        ("GGR Total", "ggr_total"),
        ("NGR", "ngr_total"),
        ("Retencao (bonus)", "retencao_total"),
        ("ARPU", "arpu_medio"),
        ("Ativos (media/dia)", "ativos_medio_dia"),
    ]

    for label, key in linhas:
        mar = kpis_mar_clean.get(key, 0)
        abr = kpis_abr_clean_norm.get(key, 0)
        d = delta(mar, abr)
        print(f"{label:<28} {fmt(mar):>18} {fmt(abr):>18} {d:>9.1f}%")

    print("\n" + "=" * 78)
    print("CONCLUIDO. Arquivos gerados em output/:")
    print("  - cto_matriz_diaria_mar_abr.csv")
    print("  - cto_outliers_fraude.csv")
    print("  - cto_comparativo_mar_vs_abr.csv")
    print("=" * 78)


if __name__ == "__main__":
    main()
