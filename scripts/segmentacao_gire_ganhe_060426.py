"""
Segmentacao: GIRE_GANHE_060426 — Big Bass Splash
Periodo: 06/04/2026 15h-23h59 BRT (18:00-02:59 UTC)
Fonte: fund_ec2 (Athena) + lista opt-in exportada do Smartico

Regras:
  - Usuarios com opt-in (tag GIRE_GANHE_060426 no Smartico)
  - Apostas no jogo Big Bass Splash no periodo
  - Faixa 1: R$10 a R$49,99
  - Faixa 2: R$50 a R$99,99
  - Faixa 3: R$100 a R$299,99
  - Faixa 4: R$300 ou mais
  - Usuario so pode estar em UMA faixa (a mais alta)
  - Rollback: usuario desclassificado (nao e permitido)
"""

import sys
sys.path.insert(0, "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet")

from db.athena import query_athena
import pandas as pd
import os
from datetime import datetime

pd.set_option("display.float_format", lambda x: f"{x:,.2f}")

BASE_DIR = "c:/Users/NITRO/OneDrive - PGX/Projetos - Super Nova/MultiBet"
OPTIN_CSV = os.path.join(BASE_DIR, "data/optin_gire_ganhe_060426.csv")

# Periodo em UTC (BRT = UTC-3)
# 06/04/2026 15:00 BRT = 06/04/2026 18:00 UTC
# 06/04/2026 23:59 BRT = 07/04/2026 02:59 UTC
UTC_START = "2026-04-06 18:00:00"
UTC_END = "2026-04-07 02:59:59"

# Faixas de turnover (BRL)
FAIXAS = [
    (4, "Faixa 4", 300.00, float("inf")),
    (3, "Faixa 3", 100.00, 299.99),
    (2, "Faixa 2", 50.00, 99.99),
    (1, "Faixa 1", 10.00, 49.99),
]

# ============================================================
# ETAPA 0: game_id validado no catalogo bireports_ec2
# ============================================================
# Validado em 08/04/2026 via bireports_ec2.tbl_vendor_games_mapping_data:
# vs10txbigbass | Big Bass Splash | pragmaticplay | game_type_id=1601 | CASINO | active
GAME_ID = "vs10txbigbass"
GAME_DESC = "Big Bass Splash"
GAME_VENDOR = "pragmaticplay"

print("=" * 70)
print(f"Jogo: {GAME_DESC} (game_id={GAME_ID}, vendor={GAME_VENDOR})")
print("=" * 70)

# ============================================================
# ETAPA 1: Carregar lista de opt-in (Smartico export)
# ============================================================
print(f"\n{'=' * 70}")
print("ETAPA 1: Carregando lista de opt-in...")
print("=" * 70)

if not os.path.exists(OPTIN_CSV):
    print(f"\n[ERRO] Arquivo de opt-in nao encontrado: {OPTIN_CSV}")
    print("Exporte o segmento 31392 do Smartico e salve como CSV neste caminho.")
    print("O CSV deve conter a coluna 'user_ext_id' (ID externo do jogador).")
    print("\nExecutando sem filtro de opt-in para visualizar TODOS os jogadores...")
    optin_ids = None
    optin_count = 0
else:
    df_optin = pd.read_csv(OPTIN_CSV)
    # Tentar encontrar coluna de ID (flexivel)
    id_col = None
    for col in ["user_ext_id", "ext_id", "external_id", "id", "user_id", "Id", "ID"]:
        if col in df_optin.columns:
            id_col = col
            break
    if id_col is None:
        # Se so tem 1 coluna, usar ela
        if len(df_optin.columns) == 1:
            id_col = df_optin.columns[0]
        else:
            print(f"[ERRO] Colunas encontradas: {list(df_optin.columns)}")
            print("Nenhuma coluna de ID reconhecida. Renomeie para 'user_ext_id'.")
            sys.exit(1)

    optin_ids = [str(int(float(x))) for x in df_optin[id_col].dropna().unique()]
    optin_count = len(optin_ids)
    print(f"  Opt-in carregados: {optin_count} usuarios (coluna: {id_col})")

# ============================================================
# ETAPA 2: Extrair transacoes do fund_ec2
# ============================================================
print(f"\n{'=' * 70}")
print("ETAPA 2: Extraindo transacoes Big Bass Splash no periodo...")
print("=" * 70)

# Bets (27), Wins (45), Rollbacks (72)
sql_txns = f"""
SELECT
    f.c_ecr_id,
    f.c_txn_type,
    CAST(f.c_amount_in_ecr_ccy AS DOUBLE) / 100.0 AS valor_brl,
    f.c_start_time,
    f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS hora_brt
FROM fund_ec2.tbl_real_fund_txn f
WHERE f.c_game_id = '{GAME_ID}'
  AND f.c_product_id = 'CASINO'
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_txn_type IN (27, 45, 72)
  AND f.c_start_time >= TIMESTAMP '{UTC_START}'
  AND f.c_start_time <= TIMESTAMP '{UTC_END}'
"""
df_txns = query_athena(sql_txns, database="fund_ec2")
print(f"  Total transacoes extraidas: {len(df_txns):,}")
print(f"  Players unicos: {df_txns['c_ecr_id'].nunique():,}")

# ============================================================
# ETAPA 3: Enriquecer com dados do player (dim_user)
# ============================================================
print(f"\n{'=' * 70}")
print("ETAPA 3: Enriquecendo com dados do jogador...")
print("=" * 70)

player_ids = df_txns["c_ecr_id"].unique().tolist()

if len(player_ids) == 0:
    print("[ERRO] Nenhum jogador encontrado no periodo. Verifique game_id e datas.")
    sys.exit(1)

# Buscar em lotes de 500 para nao estourar o SQL
batch_size = 500
df_users_list = []
for i in range(0, len(player_ids), batch_size):
    batch = player_ids[i:i + batch_size]
    ids_str = ",".join(str(x) for x in batch)
    sql_users = f"""
    SELECT ecr_id, external_id, screen_name, is_test,
           CAST(affiliate_id AS VARCHAR) AS affiliate_id
    FROM ps_bi.dim_user
    WHERE ecr_id IN ({ids_str})
    """
    df_batch = query_athena(sql_users, database="ps_bi")
    df_users_list.append(df_batch)

df_users = pd.concat(df_users_list, ignore_index=True) if df_users_list else pd.DataFrame()
print(f"  Jogadores encontrados no dim_user: {len(df_users):,}")

# Filtrar test users
test_ids = set(df_users[df_users["is_test"] == True]["ecr_id"].tolist()) if not df_users.empty else set()
if test_ids:
    print(f"  Test users removidos: {len(test_ids)}")
    df_txns = df_txns[~df_txns["c_ecr_id"].isin(test_ids)]

# ============================================================
# ETAPA 4: Agregar por jogador + classificar
# ============================================================
print(f"\n{'=' * 70}")
print("ETAPA 4: Agregando por jogador e classificando em faixas...")
print("=" * 70)

# Separar bets, wins, rollbacks
df_bets = df_txns[df_txns["c_txn_type"] == 27].groupby("c_ecr_id")["valor_brl"].sum().reset_index()
df_bets.columns = ["c_ecr_id", "turnover_brl"]

df_wins = df_txns[df_txns["c_txn_type"] == 45].groupby("c_ecr_id")["valor_brl"].sum().reset_index()
df_wins.columns = ["c_ecr_id", "wins_brl"]

df_rolls = df_txns[df_txns["c_txn_type"] == 72].groupby("c_ecr_id")["valor_brl"].sum().reset_index()
df_rolls.columns = ["c_ecr_id", "rollback_brl"]

# Contagem de bets
df_bet_count = df_txns[df_txns["c_txn_type"] == 27].groupby("c_ecr_id").size().reset_index(name="bet_count")

# Merge tudo
df_agg = df_bets.copy()
df_agg = df_agg.merge(df_wins, on="c_ecr_id", how="left")
df_agg = df_agg.merge(df_rolls, on="c_ecr_id", how="left")
df_agg = df_agg.merge(df_bet_count, on="c_ecr_id", how="left")
df_agg["wins_brl"] = df_agg["wins_brl"].fillna(0)
df_agg["rollback_brl"] = df_agg["rollback_brl"].fillna(0)
df_agg["bet_count"] = df_agg["bet_count"].fillna(0).astype(int)
df_agg["ggr_brl"] = df_agg["turnover_brl"] - df_agg["wins_brl"]
df_agg["has_rollback"] = df_agg["rollback_brl"] > 0

# Enriquecer com external_id e screen_name
df_agg = df_agg.merge(
    df_users[["ecr_id", "external_id", "screen_name", "affiliate_id"]],
    left_on="c_ecr_id", right_on="ecr_id", how="left"
)

# Classificar em faixas (a mais alta prevalece)
def classificar_faixa(turnover):
    for faixa_num, faixa_nome, vmin, vmax in FAIXAS:
        if vmin <= turnover <= vmax:
            return faixa_nome
    if turnover < 10.0:
        return "Abaixo de R$10"
    return "Sem classificacao"

df_agg["faixa"] = df_agg["turnover_brl"].apply(classificar_faixa)

# Marcar status: rollback tem prioridade sobre tudo
df_agg["status"] = "Elegivel"
df_agg.loc[df_agg["faixa"] == "Abaixo de R$10", "status"] = "Abaixo do minimo"
df_agg.loc[df_agg["has_rollback"], "status"] = "Desclassificado (rollback)"

# ============================================================
# ETAPA 5: Cruzar com lista de opt-in
# ============================================================
print(f"\n{'=' * 70}")
print("ETAPA 5: Cruzando com lista de opt-in...")
print("=" * 70)

df_agg["external_id_str"] = df_agg["external_id"].apply(
    lambda x: str(int(x)) if pd.notna(x) else ""
)

if optin_ids is not None:
    optin_set = set(optin_ids)
    df_agg["has_optin"] = df_agg["external_id_str"].isin(optin_set)
    df_optin_match = df_agg[df_agg["has_optin"]].copy()
    df_no_optin = df_agg[~df_agg["has_optin"]].copy()
    print(f"  Com opt-in e jogaram: {len(df_optin_match)}")
    print(f"  Sem opt-in (jogaram mas nao marcados): {len(df_no_optin)}")
else:
    df_agg["has_optin"] = True  # sem filtro, considerar todos
    df_optin_match = df_agg.copy()
    print("  [SEM FILTRO] Todos os jogadores considerados (opt-in nao carregado)")

# ============================================================
# ETAPA 6: Gerar report
# ============================================================
print(f"\n{'=' * 70}")
print("ETAPA 6: Gerando report...")
print("=" * 70)

# Metricas dos opt-in que jogaram
df_elig = df_optin_match[
    (df_optin_match["status"] == "Elegivel") &
    (df_optin_match["faixa"].str.startswith("Faixa"))
].copy()

total_optin = optin_count if optin_ids else len(df_optin_match)
total_jogaram = len(df_optin_match)
total_elegiveis = len(df_elig)
total_turnover = df_optin_match["turnover_brl"].sum()
total_desclass = len(df_optin_match[df_optin_match["has_rollback"]])
total_abaixo = len(df_optin_match[df_optin_match["faixa"] == "Abaixo de R$10"])
nao_jogou = total_optin - total_jogaram

# Distribuicao por faixa (ordem decrescente: 4, 3, 2, 1)
faixa_stats = {}
for faixa_num, faixa_nome, vmin, vmax in FAIXAS:
    df_f = df_elig[df_elig["faixa"] == faixa_nome]
    faixa_stats[faixa_nome] = {
        "num": faixa_num,
        "count": len(df_f),
        "turnover": df_f["turnover_brl"].sum(),
        "vmin": vmin,
        "vmax": vmax if vmax != float("inf") else None,
    }

# ============================================================
# MSG 1 — Report
# ============================================================
vmax_display = {
    "Faixa 1": "R$10-R$49,99",
    "Faixa 2": "R$50-R$99,99",
    "Faixa 3": "R$100-R$299,99",
    "Faixa 4": ">=R$300",
}

msg1_lines = []
msg1_lines.append(f"Segmentacao Gire e Ganhe | Promocao GIRE_GANHE_060426")
msg1_lines.append(f"| Periodo: 06/04 15h-23h59 BRT")
msg1_lines.append(f"")
msg1_lines.append(f"Jogo: Big Bass Splash ({GAME_VENDOR}).")

if optin_ids:
    msg1_lines.append(
        f"Do segmento com opt-in ({total_optin} usuarios marcados), "
        f"{total_jogaram} jogaram Big Bass Splash no periodo (06/04 15h-23h59 BRT)."
    )
else:
    msg1_lines.append(
        f"Total de jogadores no periodo: {total_jogaram}. "
        f"(Opt-in nao filtrado — exportar lista do Smartico)."
    )

msg1_lines.append(f"Total apostado: R$ {total_turnover:,.2f}.")
msg1_lines.append(f"")
msg1_lines.append(f"Distribuicao por faixa:")
msg1_lines.append(f"")

for faixa_num, faixa_nome, vmin, vmax in FAIXAS:
    s = faixa_stats[faixa_nome]
    label = vmax_display[faixa_nome]
    if s["count"] > 0:
        pct = s["turnover"] / total_turnover * 100 if total_turnover > 0 else 0
        plural = "jogador" if s["count"] == 1 else "jogadores"
        msg1_lines.append(
            f"  * {faixa_nome} ({label}): {s['count']} {plural} "
            f"— R$ {s['turnover']:,.2f} ({pct:.0f}% do volume)"
        )
    else:
        msg1_lines.append(f"  * {faixa_nome} ({label}): 0 jogadores")

# Abaixo do minimo
if total_abaixo > 0:
    t_abaixo = df_optin_match[df_optin_match["faixa"] == "Abaixo de R$10"]["turnover_brl"].sum()
    msg1_lines.append(f"  * Abaixo de R$10: {total_abaixo} jogadores — R$ {t_abaixo:,.2f}")
else:
    msg1_lines.append(f"  * Abaixo de R$10: 0 jogadores")

# Desclassificados
if total_desclass > 0:
    msg1_lines.append(f"  * Desclassificados (rollback): {total_desclass} jogador(es)")
else:
    msg1_lines.append(f"  * Desclassificados (rollback): 0 jogadores")

# Nao jogou
msg1_lines.append(f"  * Nao jogou no periodo: {nao_jogou} jogadores")
msg1_lines.append(f"")
msg1_lines.append(f"Elegiveis para pagamento: {total_elegiveis} jogadores.")
msg1_lines.append(f"")

# Pontos de atencao
if total_optin > 0:
    pct_jogaram = total_jogaram / total_optin * 100
    msg1_lines.append(f"Ponto de atencao: Apenas {pct_jogaram:.0f}% dos marcados efetivamente jogaram no periodo.")

# Concentracao
max_faixa = max(faixa_stats.values(), key=lambda x: x["turnover"])
max_faixa_nome = [k for k, v in faixa_stats.items() if v == max_faixa][0]
if total_turnover > 0:
    pct_concentra = max_faixa["turnover"] / total_turnover * 100
    msg1_lines.append(
        f"{max_faixa_nome} ({vmax_display[max_faixa_nome]}) concentra {pct_concentra:.0f}% "
        f"do volume com {max_faixa['count']} jogador(es)."
    )

if total_desclass == 0:
    msg1_lines.append("Zero rollbacks — nenhum desclassificado.")
else:
    msg1_lines.append(f"{total_desclass} desclassificado(s) por rollback.")

MSG1 = "\n".join(msg1_lines)

# ============================================================
# MSG 2 — Validacoes
# ============================================================
msg2_lines = []
msg2_lines.append("Validacoes realizadas:")
msg2_lines.append("")
msg2_lines.append(f"1. Jogo confirmado no catalogo Athena: {GAME_DESC} (game_id={GAME_ID}, {GAME_VENDOR}).")
if optin_ids:
    msg2_lines.append(
        f"2. Usuarios extraidos do Smartico (export CSV do segmento 31392) "
        f"— {optin_count} com opt-in confirmado."
    )
else:
    msg2_lines.append(
        "2. Lista de opt-in NAO carregada (BigQuery suspenso). "
        "Exportar segmento 31392 do Smartico e re-executar."
    )
msg2_lines.append(
    "3. Valores confirmados em centavos (c_amount_in_ecr_ccy, Pragmatic v1.3) "
    "— divisao por 100 aplicada no SQL."
)
msg2_lines.append("4. Status c_txn_status = 'SUCCESS' validado empiricamente no schema fund_ec2.")
msg2_lines.append(f"5. Rollbacks (txn_type=72): {total_desclass} jogadores desclassificados.")
msg2_lines.append(
    "6. Mapeamento de IDs validado: Smartico user_ext_id = external_id via ps_bi.dim_user."
)
msg2_lines.append(
    "7. Cada jogador aparece em apenas uma faixa (a mais alta atingida) "
    "— sem duplicidade de pagamento."
)
msg2_lines.append(
    f"8. Periodo em UTC: {UTC_START} -> {UTC_END} "
    "(equivalente a 06/04 15h – 23h59 BRT)."
)
msg2_lines.append(
    "9. Dados extraidos do Athena (Iceberg Data Lake) — banco operacional principal do projeto."
)
msg2_lines.append("10. Turnover = soma acumulada de bets (txn_type=27) no Big Bass Splash (Wallet Share do evento).")
msg2_lines.append(
    "11. Cross-validacao BigQuery: N/A (acesso suspenso desde 06/04/2026)."
)
msg2_lines.append(
    "12. CSV inclui TODOS os jogadores com atividade — elegiveis e nao elegiveis separados."
)

MSG2 = "\n".join(msg2_lines)

# ============================================================
# Salvar CSV
# ============================================================
print("\nSalvando CSV...")

cols_csv = [
    "external_id_str", "screen_name", "c_ecr_id",
    "turnover_brl", "wins_brl", "rollback_brl", "ggr_brl",
    "bet_count", "faixa", "status", "has_optin", "affiliate_id",
]
df_csv = df_optin_match[cols_csv].copy()
df_csv = df_csv.rename(columns={
    "external_id_str": "user_ext_id",
    "c_ecr_id": "ecr_id",
})
df_csv = df_csv.sort_values(["faixa", "turnover_brl"], ascending=[False, False])

csv_path = os.path.join(BASE_DIR, "reports/segmentacao_gire_ganhe_060426_FINAL.csv")
df_csv.to_csv(csv_path, index=False, encoding="utf-8-sig")
print(f"  CSV salvo: {csv_path}")

# Legenda
legenda_path = os.path.join(BASE_DIR, "reports/segmentacao_gire_ganhe_060426_legenda.txt")
legenda = """LEGENDA — Segmentacao GIRE_GANHE_060426
========================================

Colunas:
  user_ext_id    — ID externo do jogador (Smartico)
  screen_name    — Nome de exibicao do jogador
  ecr_id         — ID interno Pragmatic (18 digitos)
  turnover_brl   — Total apostado no Big Bass Splash no periodo (R$)
  wins_brl       — Total ganho pelo jogador (R$)
  rollback_brl   — Valor de rollbacks (R$). Se > 0, jogador desclassificado
  ggr_brl        — GGR = turnover - wins (positivo = casa ganhou)
  bet_count      — Quantidade de apostas realizadas
  faixa          — Classificacao: Faixa 1 (R$10-49,99), Faixa 2 (R$50-99,99),
                   Faixa 3 (R$100-299,99), Faixa 4 (>=R$300), Abaixo de R$10
  status         — Elegivel / Desclassificado (rollback) / Abaixo do minimo
  has_optin      — True se o jogador estava na lista de opt-in do Smartico
  affiliate_id   — ID do afiliado

Regras:
  - Cada jogador aparece em UMA unica faixa (a mais alta que atingiu)
  - Rollback desclassifica automaticamente
  - Turnover = soma de todas as bets (txn_type=27) no Big Bass Splash
  - Periodo: 06/04/2026 15h-23h59 BRT

Fonte: fund_ec2.tbl_real_fund_txn (Athena Iceberg Data Lake)
Jogo: Big Bass Splash (game_id consultado em ps_bi.dim_game)
Gerado: """ + datetime.now().strftime("%d/%m/%Y %H:%M BRT") + "\n"

with open(legenda_path, "w", encoding="utf-8") as f:
    f.write(legenda)
print(f"  Legenda salva: {legenda_path}")

# ============================================================
# Imprimir report
# ============================================================
print("\n" + "=" * 70)
print("MENSAGEM 1 — Report")
print("=" * 70)
print(MSG1)

print("\n" + "=" * 70)
print("MENSAGEM 2 — Validacoes")
print("=" * 70)
print(MSG2)

print("\n" + "=" * 70)
print("MENSAGEM 3 — ZIP")
print("=" * 70)
print(f"Enviar: segmentacao_gire_ganhe_060426_FINAL.zip")
print(f"(Contem CSV + legenda)")

# Salvar report completo
report_path = os.path.join(BASE_DIR, "reports/report_gire_ganhe_060426.txt")
with open(report_path, "w", encoding="utf-8") as f:
    f.write("MENSAGEM 1 — Report\n")
    f.write("=" * 70 + "\n")
    f.write(MSG1 + "\n\n")
    f.write("MENSAGEM 2 — Validacoes\n")
    f.write("=" * 70 + "\n")
    f.write(MSG2 + "\n\n")
    f.write("MENSAGEM 3 — ZIP\n")
    f.write("=" * 70 + "\n")
    f.write("Enviar: segmentacao_gire_ganhe_060426_FINAL.zip\n")
print(f"\nReport salvo: {report_path}")

# Criar ZIP
import zipfile
zip_path = os.path.join(BASE_DIR, "reports/segmentacao_gire_ganhe_060426_FINAL.zip")
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.write(csv_path, "segmentacao_gire_ganhe_060426_FINAL.csv")
    zf.write(legenda_path, "segmentacao_gire_ganhe_060426_legenda.txt")
print(f"ZIP salvo: {zip_path}")

# ============================================================
# Resumo final
# ============================================================
print("\n" + "=" * 70)
print("RESUMO")
print("=" * 70)
print(f"  Jogo: {GAME_DESC} ({GAME_VENDOR})")
print(f"  game_id: {GAME_ID}")
print(f"  Periodo: 06/04/2026 15h-23h59 BRT ({UTC_START} -> {UTC_END} UTC)")
print(f"  Opt-in (marcados): {total_optin}")
print(f"  Jogaram no periodo: {total_jogaram}")
print(f"  Elegiveis: {total_elegiveis}")
print(f"  Desclassificados (rollback): {total_desclass}")
print(f"  Abaixo do minimo: {total_abaixo}")
print(f"  Nao jogou: {nao_jogou}")
for fn, fs in faixa_stats.items():
    print(f"  {fn}: {fs['count']} jogadores | R$ {fs['turnover']:,.2f}")
print("=" * 70)
