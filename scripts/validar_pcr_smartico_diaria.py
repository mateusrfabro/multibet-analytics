"""
Validacao diaria do pipeline PCR + push Smartico (orquestrador)
================================================================
Confere se a rodada de hoje (cron 03:30 PCR + 04:00 SegSA na EC2 do
orquestrador) ficou consistente comparando o snapshot D vs D-1 da
tabela `multibet.pcr_ratings` no Super Nova DB.

O que valida (camada 2 — banco):
  1. Distribuicao por rating hoje vs ontem (sanity de variacao)
  2. Matriz de transicao (D-1 -> D): quem subiu, desceu, manteve
  3. Novos jogadores (entraram hoje, nao estavam ontem -> tag ADD)
  4. Sumidos (estavam ontem, nao estao hoje -> tag REMOVE)
  5. Amostra de 10 user_ext_ids cobrindo todos os casos para
     spot-check VISUAL no painel Smartico (camada 3)

Saida:
  - reports/validacao_pcr_smartico_<DATA>.txt   (resumo legivel)
  - reports/validacao_pcr_smartico_<DATA>_spotcheck.csv (10 IDs)

Uso:
    python scripts/validar_pcr_smartico_diaria.py
    python scripts/validar_pcr_smartico_diaria.py --data-hoje 2026-04-30
    python scripts/validar_pcr_smartico_diaria.py --data-hoje 2026-04-30 --data-ontem 2026-04-29
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

# Permite rodar de qualquer diretorio
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db.supernova import execute_supernova  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("validar_pcr")

RATINGS_ORDER = ["S", "A", "B", "C", "D", "E", "NEW"]
# Hierarquia para detectar promocao/rebaixamento (S = topo, NEW separado)
RATING_RANK = {"S": 6, "A": 5, "B": 4, "C": 3, "D": 2, "E": 1, "NEW": 0}


# ============================================================
# 1. Detectar snapshots disponiveis
# ============================================================
def detectar_snapshots(data_hoje: str | None, data_ontem: str | None
                       ) -> tuple[str, str]:
    """Detecta os 2 ultimos snapshots da pcr_ratings se nao passados."""
    rows = execute_supernova(
        """
        SELECT snapshot_date, COUNT(*) AS n
        FROM multibet.pcr_ratings
        GROUP BY snapshot_date
        ORDER BY snapshot_date DESC
        LIMIT 10;
        """,
        fetch=True,
    )
    if not rows:
        raise RuntimeError("multibet.pcr_ratings vazia.")

    log.info("Snapshots disponiveis (ultimos 10):")
    for d, n in rows:
        log.info(f"  {d}  -> {n:,} jogadores")

    snapshots = [str(r[0]) for r in rows]
    if data_hoje is None:
        data_hoje = snapshots[0]
    if data_ontem is None:
        # primeiro snapshot anterior ao 'hoje'
        for d in snapshots:
            if d < data_hoje:
                data_ontem = d
                break
    if data_ontem is None:
        raise RuntimeError(f"Sem snapshot anterior a {data_hoje} disponivel.")

    log.info(f"Comparando: HOJE={data_hoje}  vs  ONTEM={data_ontem}")
    return data_hoje, data_ontem


# ============================================================
# 2. Carregar snapshots (apenas colunas necessarias)
# ============================================================
def carregar_snapshot(snapshot_date: str) -> pd.DataFrame:
    rows = execute_supernova(
        """
        SELECT
            player_id::bigint   AS player_id,
            external_id::text   AS external_id,
            UPPER(TRIM(rating)) AS rating,
            pvs,
            c_category
        FROM multibet.pcr_ratings
        WHERE snapshot_date = %s;
        """,
        params=(snapshot_date,),
        fetch=True,
    )
    df = pd.DataFrame(rows, columns=[
        "player_id", "external_id", "rating", "pvs", "c_category"
    ])
    log.info(f"  snapshot {snapshot_date}: {len(df):,} jogadores")
    return df


# ============================================================
# 3. Diff: distribuicao, transicao, novos, sumidos
# ============================================================
def calcular_diff(df_hoje: pd.DataFrame, df_ontem: pd.DataFrame) -> dict:
    # 3.1 Distribuicao por rating
    dist_hoje = df_hoje["rating"].value_counts().reindex(RATINGS_ORDER, fill_value=0)
    dist_ontem = df_ontem["rating"].value_counts().reindex(RATINGS_ORDER, fill_value=0)
    delta = (dist_hoje - dist_ontem)
    pct = ((dist_hoje - dist_ontem) / dist_ontem.replace(0, 1) * 100).round(2)

    distribuicao = pd.DataFrame({
        "ontem": dist_ontem,
        "hoje": dist_hoje,
        "delta": delta,
        "pct": pct,
    })

    # 3.2 Merge para identificar mudancas (inner = comuns aos 2 dias)
    merged = df_hoje[["player_id", "external_id", "rating", "pvs"]].merge(
        df_ontem[["player_id", "rating"]].rename(columns={"rating": "rating_ontem"}),
        on="player_id",
        how="left",  # left = mantem todos de hoje, marca NaN pra novos
        indicator=True,
    )

    novos = merged[merged["_merge"] == "left_only"].drop(columns=["_merge", "rating_ontem"])

    em_ambos = merged[merged["_merge"] == "both"].drop(columns=["_merge"])
    em_ambos["rank_hoje"] = em_ambos["rating"].map(RATING_RANK)
    em_ambos["rank_ontem"] = em_ambos["rating_ontem"].map(RATING_RANK)
    em_ambos["mudanca"] = em_ambos.apply(
        lambda r: (
            "PROMOVIDO" if r["rank_hoje"] > r["rank_ontem"]
            else "REBAIXADO" if r["rank_hoje"] < r["rank_ontem"]
            else "ESTAVEL"
        ),
        axis=1,
    )

    # Sumidos: estavam ontem mas nao estao hoje
    sumidos_ids = set(df_ontem["player_id"]) - set(df_hoje["player_id"])
    sumidos = df_ontem[df_ontem["player_id"].isin(sumidos_ids)].copy()

    # Matriz de transicao (D-1 -> D)
    transicao = pd.crosstab(
        em_ambos["rating_ontem"],
        em_ambos["rating"],
        margins=True,
        margins_name="TOTAL",
    ).reindex(index=RATINGS_ORDER + ["TOTAL"], columns=RATINGS_ORDER + ["TOTAL"], fill_value=0)

    return {
        "distribuicao": distribuicao,
        "transicao": transicao,
        "novos": novos,
        "sumidos": sumidos,
        "promovidos": em_ambos[em_ambos["mudanca"] == "PROMOVIDO"],
        "rebaixados": em_ambos[em_ambos["mudanca"] == "REBAIXADO"],
        "estaveis": em_ambos[em_ambos["mudanca"] == "ESTAVEL"],
        "total_hoje": len(df_hoje),
        "total_ontem": len(df_ontem),
    }


# ============================================================
# 4. Spot-check: 10 user_ext_ids cobrindo todos os casos
# ============================================================
def montar_spotcheck(diff: dict) -> pd.DataFrame:
    """
    Pega ate 2 IDs de cada caso (NOVO, SUMIDO, PROMOVIDO, REBAIXADO) +
    2 ESTAVEIS para controle. Total alvo: 10.
    Filtra external_id nao nulo.
    """
    blocos = []

    def amostra(df: pd.DataFrame, n: int, caso: str, rating_ontem_col: str | None):
        if df.empty:
            return None
        valid = df[df["external_id"].notna() & (df["external_id"].astype(str) != "")]
        if valid.empty:
            return None
        s = valid.sample(n=min(n, len(valid)), random_state=42).copy()
        s["caso"] = caso
        s["rating_ontem"] = s[rating_ontem_col] if rating_ontem_col else "(novo)"
        if "rating" not in s.columns:
            s["rating"] = "(saiu)"
        return s[["caso", "player_id", "external_id", "rating_ontem",
                  "rating", "pvs"]].rename(columns={"rating": "rating_hoje"})

    blocos.append(amostra(diff["novos"], 2, "NOVO_HOJE", None))
    blocos.append(amostra(diff["sumidos"], 2, "SUMIU_HOJE", "rating"))
    blocos.append(amostra(diff["promovidos"], 2, "PROMOVIDO", "rating_ontem"))
    blocos.append(amostra(diff["rebaixados"], 2, "REBAIXADO", "rating_ontem"))
    blocos.append(amostra(diff["estaveis"], 2, "ESTAVEL", "rating_ontem"))

    blocos = [b for b in blocos if b is not None]
    if not blocos:
        return pd.DataFrame()
    out = pd.concat(blocos, ignore_index=True)
    return out


# ============================================================
# 5. Render do relatorio
# ============================================================
def render_relatorio(data_hoje: str, data_ontem: str, diff: dict,
                      spotcheck: pd.DataFrame) -> str:
    L = []
    L.append("=" * 78)
    L.append(f"  VALIDACAO PCR + SMARTICO — {data_hoje}")
    L.append(f"  Comparativo D vs D-1: {data_hoje}  vs  {data_ontem}")
    L.append(f"  Gerado em: {datetime.now():%Y-%m-%d %H:%M:%S}")
    L.append("=" * 78)
    L.append("")

    L.append(f"TOTAL ONTEM  ({data_ontem}): {diff['total_ontem']:>8,} jogadores")
    L.append(f"TOTAL HOJE   ({data_hoje}): {diff['total_hoje']:>8,} jogadores")
    L.append(f"DELTA TOTAL                : {diff['total_hoje'] - diff['total_ontem']:+,} "
             f"({(diff['total_hoje'] - diff['total_ontem']) / max(diff['total_ontem'], 1) * 100:+.2f}%)")
    L.append("")

    # 1. Distribuicao por rating
    L.append("-" * 78)
    L.append("1. DISTRIBUICAO POR RATING")
    L.append("-" * 78)
    L.append(diff["distribuicao"].to_string())
    L.append("")
    L.append("  (delta = hoje - ontem | pct = variacao % vs ontem)")
    L.append("  Sanity: variacao tipica diaria <5%. Acima disso = investigar.")
    L.append("")

    # 2. Mudancas
    L.append("-" * 78)
    L.append("2. RESUMO DE MUDANCAS")
    L.append("-" * 78)
    L.append(f"  NOVOS HOJE        : {len(diff['novos']):>8,}  (entraram na base PCR — receberam tag PCR_RATING_X nova)")
    L.append(f"  SUMIDOS HOJE      : {len(diff['sumidos']):>8,}  (sairam da base PCR — receberam REMOVE PCR_RATING_*)")
    L.append(f"  PROMOVIDOS        : {len(diff['promovidos']):>8,}  (subiram tier — tag mudou)")
    L.append(f"  REBAIXADOS        : {len(diff['rebaixados']):>8,}  (caiu tier — tag mudou)")
    L.append(f"  ESTAVEIS (mesmo)  : {len(diff['estaveis']):>8,}  (rating preservado — tag re-aplicada)")
    L.append("")

    # 3. Matriz de transicao
    L.append("-" * 78)
    L.append("3. MATRIZ DE TRANSICAO (linhas = ONTEM | colunas = HOJE)")
    L.append("-" * 78)
    L.append(diff["transicao"].to_string())
    L.append("")
    L.append("  Diagonal = jogadores que mantiveram rating.")
    L.append("  Acima da diagonal = subiram (S esta no topo).")
    L.append("  Abaixo da diagonal = cairam.")
    L.append("")

    # 4. Spot-check
    L.append("-" * 78)
    L.append("4. SPOT-CHECK SMARTICO — abrir cada user_ext_id no painel")
    L.append("-" * 78)
    L.append("")
    L.append("Para cada ID abaixo, validar no painel Smartico (External Markers):")
    L.append("  - Tag PCR_RATING_<rating_hoje> esta presente")
    L.append("  - NAO ha tag PCR_RATING_<rating_ontem> sobrando (operacao atomica)")
    L.append("  - Tags RISK_* de outros pipelines estao preservadas")
    L.append("")
    if not spotcheck.empty:
        L.append(spotcheck.to_string(index=False))
    else:
        L.append("  (sem amostra disponivel)")
    L.append("")

    # 5. Conclusao automatica
    L.append("-" * 78)
    L.append("5. SEMAFORO")
    L.append("-" * 78)
    flags = []
    var_total = abs(diff['total_hoje'] - diff['total_ontem']) / max(diff['total_ontem'], 1) * 100
    if var_total > 5:
        flags.append(f"VARIACAO TOTAL DA BASE >5% ({var_total:.2f}%) — investigar")
    for r, row in diff["distribuicao"].iterrows():
        if abs(row["pct"]) > 10 and row["ontem"] > 100:
            flags.append(f"Rating {r}: variacao {row['pct']:+.2f}% — investigar")
    if not flags:
        L.append("  VERDE — variacoes dentro do esperado (<5% total, <10% por tier).")
        L.append("  Se spot-check Smartico tambem ok, rodada esta saudavel.")
    else:
        L.append("  AMARELO — flags abaixo merecem investigacao:")
        for f in flags:
            L.append(f"    * {f}")
    L.append("")
    L.append("=" * 78)
    return "\n".join(L)


# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-hoje", default=None,
                    help="Snapshot a validar (default: ultimo)")
    ap.add_argument("--data-ontem", default=None,
                    help="Snapshot anterior (default: penultimo)")
    args = ap.parse_args()

    log.info("Conectando ao Super Nova DB para detectar snapshots...")
    data_hoje, data_ontem = detectar_snapshots(args.data_hoje, args.data_ontem)

    log.info("Carregando snapshot HOJE...")
    df_hoje = carregar_snapshot(data_hoje)
    log.info("Carregando snapshot ONTEM...")
    df_ontem = carregar_snapshot(data_ontem)

    log.info("Calculando diff...")
    diff = calcular_diff(df_hoje, df_ontem)

    log.info("Montando spot-check...")
    spotcheck = montar_spotcheck(diff)

    relatorio = render_relatorio(data_hoje, data_ontem, diff, spotcheck)
    print("\n" + relatorio)

    # Persistencia
    reports = ROOT / "reports"
    reports.mkdir(exist_ok=True)
    txt_path = reports / f"validacao_pcr_smartico_{data_hoje}.txt"
    csv_path = reports / f"validacao_pcr_smartico_{data_hoje}_spotcheck.csv"
    txt_path.write_text(relatorio, encoding="utf-8")
    if not spotcheck.empty:
        spotcheck.to_csv(csv_path, sep=";", index=False, encoding="utf-8-sig")

    log.info(f"Relatorio salvo: {txt_path}")
    if not spotcheck.empty:
        log.info(f"Spot-check CSV: {csv_path}")


if __name__ == "__main__":
    main()
