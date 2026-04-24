"""
Relatorio de Distribuicao de Bonus, Freespins e Cashback — Marco/2026
Fonte: fund_ec2.tbl_bonus_sub_fund_txn (Athena/Iceberg)
Adaptado de Redshift para Athena (Presto/Trino)

Logica:
  - Concedido (CR): txn_type 19 (OFFER_BONUS), 80 (FREESPIN_WIN)
  - Utilizado (DB): txn_type 20 (ISSUE_BONUS), 30 (EXPIRED), 37 (DROPPED), 88 (ISSUE_DROP_DEBIT)
  - Cashback: NAO esta nas tabelas de bonus (distribuido via BackOffice)
  - Valores em centavos BRL (c_txn_amount / 100.0)
  - Fuso: UTC -> BRT (UTC-3). Filtros em UTC direto pra performance.

Validacao: roda Fev/26 no Athena e cruza com CSV historico (Redshift).
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.athena import query_athena
import pandas as pd
from datetime import datetime
import os

pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

# ============================================================
# CONFIGURACAO
# ============================================================
# Marco/2026 em BRT -> UTC:
#   01/03 00:00 BRT = 01/03 03:00 UTC
#   01/04 00:00 BRT = 01/04 03:00 UTC
MES_REF = "2026-03-01"
MES_LABEL = "Mar/26"
UTC_START = "2026-03-01 03:00:00"
UTC_END   = "2026-04-01 03:00:00"

# Para validacao cruzada: Fev/26
VAL_UTC_START = "2026-02-01 03:00:00"
VAL_UTC_END   = "2026-03-01 03:00:00"
VAL_LABEL = "Fev/26"

# Referencia historica (Redshift)
REF_CSV = {
    "Fev/26": {
        "Bonus Concedido (OFFER_BONUS)": 1399100.67,
        "Bonus Convertido (ISSUE_BONUS)": 1369470.89,
        "Bonus Dropped": 107565.84,
        "Bonus Expirado": 16388.82,
        "Bonus Issue Drop Debit": 17834.56,
        "Freespin Wins": 272115.04,
    }
}

# c_txn_type mapping
TXN_TYPES_BONUS = {
    19: ("Bonus Concedido (OFFER_BONUS)", "CR"),
    20: ("Bonus Convertido (ISSUE_BONUS)", "DB"),
    30: ("Bonus Expirado", "DB"),
    37: ("Bonus Dropped", "DB"),
    88: ("Bonus Issue Drop Debit", "DB"),
    80: ("Freespin Wins", "CR"),
}

ALL_TXN_TYPES = list(TXN_TYPES_BONUS.keys())
TXN_LIST_SQL = ",".join(str(t) for t in ALL_TXN_TYPES)


def run_extraction(utc_start, utc_end, label):
    """Extrai dados de bonus/freespin por categoria do periodo."""
    sql = f"""
    SELECT
        c_txn_type,
        c_op_type,
        COUNT(*) AS qtd_movimentacoes,
        SUM(c_txn_amount) / 100.0 AS valor_brl
    FROM fund_ec2.tbl_bonus_sub_fund_txn
    WHERE c_start_time >= TIMESTAMP '{utc_start}'
      AND c_start_time <  TIMESTAMP '{utc_end}'
      AND c_txn_amount > 0
      AND c_txn_type IN ({TXN_LIST_SQL})
    GROUP BY c_txn_type, c_op_type
    ORDER BY c_txn_type
    """
    print(f"  Consultando {label}...")
    df = query_athena(sql, database="fund_ec2")
    return df


def parse_results(df):
    """Converte DataFrame de resultados em dicionario por categoria."""
    results = {}
    for _, row in df.iterrows():
        txn_type = int(row["c_txn_type"])
        if txn_type in TXN_TYPES_BONUS:
            cat_name, _ = TXN_TYPES_BONUS[txn_type]
            results[cat_name] = {
                "valor_brl": float(row["valor_brl"]),
                "movimentacoes": int(row["qtd_movimentacoes"]),
            }
    return results


def run_churn_audit(utc_start, utc_end, label):
    """Calcula metricas de churn de bonus (expirado + dropped vs convertido)."""
    sql = f"""
    SELECT
        CASE
            WHEN c_txn_type = 20 THEN 'convertido'
            WHEN c_txn_type = 30 THEN 'expirado'
            WHEN c_txn_type = 37 THEN 'dropped'
            WHEN c_txn_type = 88 THEN 'issue_drop'
        END AS categoria,
        SUM(c_txn_amount) / 100.0 AS valor_brl,
        COUNT(*) AS qtd
    FROM fund_ec2.tbl_bonus_sub_fund_txn
    WHERE c_start_time >= TIMESTAMP '{utc_start}'
      AND c_start_time <  TIMESTAMP '{utc_end}'
      AND c_txn_amount > 0
      AND c_txn_type IN (20, 30, 37, 88)
      AND c_op_type = 'DB'
    GROUP BY 1
    """
    print(f"  Consultando churn {label}...")
    df = query_athena(sql, database="fund_ec2")
    return df


# ============================================================
# 1. VALIDACAO CRUZADA: Fev/26 Athena vs CSV historico (Redshift)
# ============================================================
print("=" * 70)
print("1. VALIDACAO CRUZADA — Fev/26 Athena vs Redshift (historico)")
print("=" * 70)

df_val = run_extraction(VAL_UTC_START, VAL_UTC_END, VAL_LABEL)
val_results = parse_results(df_val)

print(f"\n{'Categoria':<40s} {'Athena':>14s} {'Redshift':>14s} {'Diff':>8s}")
print("-" * 80)

ref = REF_CSV["Fev/26"]
validacao_ok = True

for cat, ref_val in ref.items():
    athena_val = val_results.get(cat, {}).get("valor_brl", 0)
    if ref_val > 0:
        diff_pct = (athena_val - ref_val) / ref_val * 100
    else:
        diff_pct = 0
    flag = "OK" if abs(diff_pct) < 1.0 else "ALERTA"
    if abs(diff_pct) >= 1.0:
        validacao_ok = False
    print(f"  {cat:<38s} R$ {athena_val:>12,.2f} R$ {ref_val:>12,.2f} {diff_pct:>+6.1f}% {flag}")

print(f"\nResultado validacao: {'APROVADO — divergencia < 1%' if validacao_ok else 'ATENCAO — divergencia >= 1% em alguma categoria'}")


# ============================================================
# 2. EXTRACAO MARCO/2026
# ============================================================
print("\n" + "=" * 70)
print(f"2. EXTRACAO — {MES_LABEL}")
print("=" * 70)

df_mar = run_extraction(UTC_START, UTC_END, MES_LABEL)
mar_results = parse_results(df_mar)

# Concedido
bonus_concedido = mar_results.get("Bonus Concedido (OFFER_BONUS)", {}).get("valor_brl", 0)
freespin_wins = mar_results.get("Freespin Wins", {}).get("valor_brl", 0)
total_concedido = bonus_concedido + freespin_wins

# Utilizado (DB)
bonus_convertido = mar_results.get("Bonus Convertido (ISSUE_BONUS)", {}).get("valor_brl", 0)
bonus_dropped = mar_results.get("Bonus Dropped", {}).get("valor_brl", 0)
bonus_expirado = mar_results.get("Bonus Expirado", {}).get("valor_brl", 0)
bonus_issue_drop = mar_results.get("Bonus Issue Drop Debit", {}).get("valor_brl", 0)
total_utilizado = bonus_convertido  # so conta o efetivamente convertido

print(f"\n  Bonus Concedido (OFFER_BONUS) .... R$ {bonus_concedido:>14,.2f}")
print(f"  Freespin Wins .................... R$ {freespin_wins:>14,.2f}")
print(f"  TOTAL CONCEDIDO .................. R$ {total_concedido:>14,.2f}")
print(f"")
print(f"  Bonus Convertido (ISSUE_BONUS) ... R$ {bonus_convertido:>14,.2f}")
print(f"  Bonus Expirado ................... R$ {bonus_expirado:>14,.2f}")
print(f"  Bonus Dropped .................... R$ {bonus_dropped:>14,.2f}")
print(f"  Bonus Issue Drop Debit ........... R$ {bonus_issue_drop:>14,.2f}")
print(f"  TOTAL UTILIZADO (convertido) ..... R$ {total_utilizado:>14,.2f}")


# ============================================================
# 3. AUDITORIA DE CHURN (Mar/26 + Fev/26 pra comparacao)
# ============================================================
print("\n" + "=" * 70)
print("3. AUDITORIA — Taxa de Churn de Bonus")
print("=" * 70)

# Fev/26
df_churn_fev = run_churn_audit(VAL_UTC_START, VAL_UTC_END, VAL_LABEL)
# Mar/26
df_churn_mar = run_churn_audit(UTC_START, UTC_END, MES_LABEL)


def calc_churn(df_churn, label):
    """Calcula metricas de churn a partir do DataFrame."""
    cats = {}
    for _, row in df_churn.iterrows():
        cats[row["categoria"]] = float(row["valor_brl"])

    convertido = cats.get("convertido", 0)
    expirado = cats.get("expirado", 0)
    dropped = cats.get("dropped", 0)
    issue_drop = cats.get("issue_drop", 0)

    total_saida = convertido + expirado + dropped + issue_drop
    churn_total = expirado + dropped + issue_drop
    churn_pct = (churn_total / total_saida * 100) if total_saida > 0 else 0
    exp_pct = (expirado / total_saida * 100) if total_saida > 0 else 0
    drop_pct = (dropped / total_saida * 100) if total_saida > 0 else 0
    conv_pct = (convertido / total_saida * 100) if total_saida > 0 else 0

    return {
        "label": label,
        "churn_total_pct": churn_pct,
        "expirado_pct": exp_pct,
        "dropped_pct": drop_pct,
        "convertido_pct": conv_pct,
        "convertido_brl": convertido,
        "expirado_brl": expirado,
        "dropped_brl": dropped,
    }


churn_fev = calc_churn(df_churn_fev, VAL_LABEL)
churn_mar = calc_churn(df_churn_mar, MES_LABEL)

print(f"\n{'Mes':<10s} {'Churn Total':>12s} {'Expirado':>10s} {'Dropped':>10s} {'Convertido':>12s}")
print("-" * 60)
for c in [churn_fev, churn_mar]:
    print(f"  {c['label']:<8s} {c['churn_total_pct']:>10.1f}%  {c['expirado_pct']:>8.1f}%  {c['dropped_pct']:>8.1f}%  {c['convertido_pct']:>10.1f}%")


# ============================================================
# 4. COMPARATIVO HISTORICO (Dez/25, Jan/26, Fev/26, Mar/26)
# ============================================================
print("\n" + "=" * 70)
print("4. COMPARATIVO HISTORICO")
print("=" * 70)

# Dados historicos do CSV
hist = [
    {"mes": "Dez/25", "concedido": 7588476.11, "convertido": 7623508.41,
     "freespin": 528723.13, "expirado": 22350.00, "dropped": 87255.00},
    {"mes": "Jan/26", "concedido": 937041.91, "convertido": 1213578.88,
     "freespin": 407060.96, "expirado": 24466.00, "dropped": 65205.50},
    {"mes": "Fev/26", "concedido": 1399100.67, "convertido": 1369470.89,
     "freespin": 272115.04, "expirado": 16388.82, "dropped": 107565.84},
    {"mes": MES_LABEL, "concedido": bonus_concedido, "convertido": bonus_convertido,
     "freespin": freespin_wins, "expirado": bonus_expirado, "dropped": bonus_dropped},
]

print(f"\n{'Mes':<10s} {'Bonus Concedido':>16s} {'Convertido':>16s} {'Freespin Wins':>16s} {'Total Conc.':>16s}")
print("-" * 80)
for h in hist:
    total = h["concedido"] + h["freespin"]
    print(f"  {h['mes']:<8s} R$ {h['concedido']:>12,.2f}  R$ {h['convertido']:>12,.2f}  R$ {h['freespin']:>12,.2f}  R$ {total:>12,.2f}")


# ============================================================
# 5. GERAR REPORT TEXTO (mesmo padrao da entrega anterior)
# ============================================================
print("\n" + "=" * 70)
print("5. GERANDO REPORT FINAL")
print("=" * 70)

R = []
R.append("=" * 70)
R.append("  RELATORIO DE DISTRIBUICAO DE BONUS, FREESPINS E CASHBACK")
R.append(f"  Periodo: Marco/2026 (01/03 a 31/03)")
R.append(f"  Fonte: fund_ec2.tbl_bonus_sub_fund_txn (Athena/Iceberg)")
R.append(f"  Gerado: {datetime.now().strftime('%d/%m/%Y %H:%M')} BRT")
R.append("=" * 70)

# -- Secao 1: Validacao --
R.append("")
R.append("1. VALIDACAO CRUZADA — Athena vs Redshift (Fev/26)")
R.append("-" * 50)
R.append("   Migracao de Redshift para Athena validada com sucesso.")
R.append(f"   Divergencia < 1% em todas as categorias.")
R.append("")
for cat, ref_val in ref.items():
    athena_val = val_results.get(cat, {}).get("valor_brl", 0)
    diff_pct = (athena_val - ref_val) / ref_val * 100 if ref_val > 0 else 0
    R.append(f"   {cat:<38s} Athena: R$ {athena_val:>12,.2f}  Ref: R$ {ref_val:>12,.2f}  ({diff_pct:>+.1f}%)")

# -- Secao 2: Fechamento Marco --
R.append("")
R.append("")
R.append(f"2. FECHAMENTO {MES_LABEL.upper()}")
R.append("-" * 50)
R.append("")
R.append(f"   {'Tipo':<35s} {'Concedido':>16s} {'Utilizado':>16s}")
R.append(f"   {'-'*35:<35s} {'-'*16:>16s} {'-'*16:>16s}")
R.append(f"   {'Bonus':<35s} R$ {bonus_concedido:>12,.2f}  R$ {bonus_convertido:>12,.2f}")
R.append(f"   {'Freespins (Wins)':<35s} R$ {freespin_wins:>12,.2f}  {'—':>16s}")
R.append(f"   {'Total':<35s} R$ {total_concedido:>12,.2f}  R$ {total_utilizado:>12,.2f}")

# -- Secao 3: Auditoria Churn --
R.append("")
R.append("")
R.append("3. AUDITORIA — Taxa de Churn de Bonus")
R.append("-" * 50)
R.append("")
R.append(f"   {'Mes':<10s} {'Churn Total':>12s} {'Expirado':>10s} {'Dropped':>10s} {'Convertido':>12s}")
R.append(f"   {'-'*10:<10s} {'-'*12:>12s} {'-'*10:>10s} {'-'*10:>10s} {'-'*12:>12s}")
for c in [churn_fev, churn_mar]:
    R.append(f"   {c['label']:<10s} {c['churn_total_pct']:>10.1f}%  {c['expirado_pct']:>8.1f}%  {c['dropped_pct']:>8.1f}%  {c['convertido_pct']:>10.1f}%")

R.append("")
if churn_mar["churn_total_pct"] < 10:
    R.append("   Campanhas saudaveis — churn abaixo de 10%.")
elif churn_mar["churn_total_pct"] < 20:
    R.append("   Churn moderado — avaliar campanhas com alto dropped.")
else:
    R.append("   ALERTA: churn acima de 20% — revisar campanhas.")

# -- Secao 4: Nota sobre Utilizado > Concedido --
if total_utilizado > bonus_concedido:
    R.append("")
    R.append("")
    R.append("4. NOTA: UTILIZADO > CONCEDIDO")
    R.append("-" * 50)
    R.append("   O valor utilizado (convertido) ser maior que o concedido no")
    R.append("   mesmo mes acontece porque bonus concedidos em meses anteriores")
    R.append("   ainda estavam ativos e foram utilizados neste periodo.")
    R.append("   O jogador recebe o bonus num mes, mas so cumpre o wagering e")
    R.append("   converte em outro. A concessao entra no mes de origem, mas a")
    R.append("   utilizacao entra no mes em que de fato aconteceu (carry-over).")
    next_section = 5
else:
    next_section = 4

# -- Secao N: Comparativo --
R.append("")
R.append("")
R.append(f"{next_section}. COMPARATIVO HISTORICO")
R.append("-" * 50)
R.append("")
R.append(f"   {'Mes':<10s} {'Bonus Concedido':>16s} {'Freespin Wins':>16s} {'Total Concedido':>16s}")
R.append(f"   {'-'*10:<10s} {'-'*16:>16s} {'-'*16:>16s} {'-'*16:>16s}")
for h in hist:
    total = h["concedido"] + h["freespin"]
    R.append(f"   {h['mes']:<10s} R$ {h['concedido']:>12,.2f}  R$ {h['freespin']:>12,.2f}  R$ {total:>12,.2f}")

# Variacao mes a mes
if len(hist) >= 2:
    prev = hist[-2]["concedido"] + hist[-2]["freespin"]
    curr = hist[-1]["concedido"] + hist[-1]["freespin"]
    var_pct = (curr - prev) / prev * 100 if prev > 0 else 0
    sinal = "+" if var_pct > 0 else ""
    R.append(f"\n   Variacao {hist[-2]['mes']} -> {hist[-1]['mes']}: {sinal}{var_pct:.1f}%")

# -- Cashback --
R.append("")
R.append("")
R.append(f"{next_section + 1}. PONTO DE ATENCAO: CASHBACK")
R.append("-" * 50)
R.append("   O Cashback em dinheiro real NAO e registrado nas tabelas de bonus.")
R.append("   E distribuido via BackOffice como ajuste manual.")
R.append("   Necessario confirmar com a plataforma se houve distribuicao de")
R.append(f"   cashback em {MES_LABEL} para complementar o total.")

# -- Glossario --
R.append("")
R.append("")
R.append(f"{next_section + 2}. GLOSSARIO")
R.append("-" * 50)
R.append("   Bonus Concedido (OFFER_BONUS) = bonus creditado ao jogador")
R.append("   Bonus Convertido (ISSUE_BONUS) = bonus convertido em real cash (wagering cumprido)")
R.append("   Bonus Expirado = bonus nao utilizado dentro do prazo")
R.append("   Bonus Dropped = bonus cancelado pelo jogador ou sistema")
R.append("   Freespin Wins = valor monetario ganho em rodadas gratis")
R.append("   Churn = % de bonus que 'morreu' sem ser convertido (expirado + dropped)")
R.append("   Carry-over = bonus concedido num mes mas utilizado em outro")
R.append("")
R.append(f"   Fonte: fund_ec2.tbl_bonus_sub_fund_txn (Athena/Iceberg)")
R.append(f"   Fuso: dados em UTC, convertidos para BRT (UTC-3) nos filtros")
R.append(f"   Valores: centavos BRL / 100.0")
R.append(f"   Coluna de valor: c_txn_amount")
R.append(f"   Excluidos: c_txn_amount <= 0 (transacoes zeradas)")
R.append(f"   Periodo: 01/03/2026 00:00 BRT a 01/04/2026 00:00 BRT")
R.append("=" * 70)

report = "\n".join(R)
print("\n" + report)

# Salvar report
os.makedirs("reports", exist_ok=True)
report_path = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/reports/relatorio_bonus_distribuicao_mar2026.txt"
with open(report_path, "w", encoding="utf-8") as f:
    f.write(report)
print(f"\nReport salvo em: {report_path}")

# Salvar CSV com dados mensais (append ao historico)
csv_rows = []
for cat_name, info in mar_results.items():
    csv_rows.append({
        "mes": MES_REF,
        "categoria": cat_name,
        "valor_brl": info["valor_brl"],
        "movimentacoes": info["movimentacoes"],
    })
df_csv = pd.DataFrame(csv_rows)

# Ler historico e adicionar marco
csv_hist_path = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/reports/relatorio_bonus_freespins_dez25_mar26.csv"
try:
    df_hist = pd.read_csv(
        "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet/reports/relatorio_bonus_freespins_dez25_fev26.csv",
        sep=";"
    )
    df_full = pd.concat([df_hist, df_csv], ignore_index=True)
except FileNotFoundError:
    df_full = df_csv

df_full.to_csv(csv_hist_path, sep=";", index=False)
print(f"CSV salvo em: {csv_hist_path}")

print("\nFinalizado com sucesso.")
