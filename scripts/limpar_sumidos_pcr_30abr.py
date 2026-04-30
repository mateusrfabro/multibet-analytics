"""
One-shot — Limpar tags PCR_RATING_* fantasmas dos 986 jogadores que sumiram
da base PCR no dia 30/04/2026
============================================================================

CONTEXTO:
  Bug descoberto em 30/04 — `_carregar_snapshot_anterior` em
  `pipelines/segmentacao_sa_smartico.py` buscava o snapshot anterior em
  `multibet.segmentacao_sa_diaria` (so A+S, ~11k) ao inves de
  `multibet.pcr_ratings` (base completa). Resultado: jogadores que sairam
  da base PCR (banidos, churned, fora do filtro D-3y) NUNCA recebiam o
  REMOVE puro das tags PCR_RATING_* — ficavam com tag fantasma no Smartico.

  Fix do pipeline ja foi aplicado e commitado (handoff Gusta no commit
  c8f6777). Este script limpa o "passivo" — os 986 jogadores que sumiram
  no dia 30/04 e ficaram com tag PCR_RATING_<antiga>.

ESTRATEGIA DE REMOVE:
  Usar `-core_external_markers` com LISTA EXPLICITA de todas as tags
  PCR_RATING_* possiveis (S/A/B/C/D/E/NEW). NAO usar o operador `^pattern`
  porque ele pode engolir o evento (pd:0) quando o pattern nao matcha
  nada — ver smartico_api.py:247-251.

  Operacao por jogador:
      {
        "-core_external_markers": ["PCR_RATING_S", "PCR_RATING_A",
                                   "PCR_RATING_B", "PCR_RATING_C",
                                   "PCR_RATING_D", "PCR_RATING_E",
                                   "PCR_RATING_NEW"],
        "skip_cjm": true
      }

  Idempotente: se a tag nao existe, nao faz nada. Se existe, remove.
  Preserva tags RISK_*, WHATSAPP_*, AQUISICAO_* etc. (nao sao tocadas).

USO:
    # Dry-run (default — salva JSON, nao envia):
    python scripts/limpar_sumidos_pcr_30abr.py

    # Canary (1 jogador real — envia para Smartico):
    python scripts/limpar_sumidos_pcr_30abr.py --canary --confirm

    # Limit (envio real para N jogadores):
    python scripts/limpar_sumidos_pcr_30abr.py --limit 10 --confirm

    # Full (envio real para todos os 986):
    python scripts/limpar_sumidos_pcr_30abr.py --full --confirm
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from db.supernova import execute_supernova  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("limpar_sumidos")

# Snapshots a comparar (HARDCODED — script one-shot pra dia 30/04)
SNAPSHOT_HOJE = "2026-04-30"
SNAPSHOT_ONTEM = "2026-04-29"

# Lista explicita de tags PCR_RATING_* a remover (todos os 7 tiers)
TAGS_PCR_RATING = [
    "PCR_RATING_S",
    "PCR_RATING_A",
    "PCR_RATING_B",
    "PCR_RATING_C",
    "PCR_RATING_D",
    "PCR_RATING_E",
    "PCR_RATING_NEW",
]


# ============================================================
# 1. Carregar lista de sumidos
# ============================================================
def carregar_sumidos() -> pd.DataFrame:
    """
    Sumidos = player_ids que estavam em pcr_ratings (29/04) mas nao
    estao em pcr_ratings (30/04). Retorna DF com player_id + external_id +
    rating_anterior (so pra log/auditoria).
    """
    log.info(f"Carregando sumidos: {SNAPSHOT_ONTEM} - {SNAPSHOT_HOJE}")
    rows = execute_supernova(
        """
        SELECT
            o.player_id::bigint   AS player_id,
            o.external_id::text   AS external_id,
            UPPER(TRIM(o.rating)) AS rating_anterior
        FROM multibet.pcr_ratings o
        WHERE o.snapshot_date = %s
          AND NOT EXISTS (
            SELECT 1 FROM multibet.pcr_ratings n
             WHERE n.snapshot_date = %s
               AND n.player_id = o.player_id
          );
        """,
        params=(SNAPSHOT_ONTEM, SNAPSHOT_HOJE),
        fetch=True,
    )
    df = pd.DataFrame(rows, columns=["player_id", "external_id", "rating_anterior"])
    log.info(f"  Sumidos carregados: {len(df):,}")

    # Filtra external_id valido
    df["external_id"] = df["external_id"].astype(str).str.strip()
    invalidos = df[df["external_id"].isin(["", "nan", "None"])]
    if not invalidos.empty:
        log.warning(f"  Excluindo {len(invalidos)} sumidos com external_id invalido.")
    df_valid = df[~df["external_id"].isin(["", "nan", "None"])].copy()
    log.info(f"  Sumidos validos para envio: {len(df_valid):,}")
    log.info(f"  Distribuicao por rating anterior:")
    for r, q in df_valid["rating_anterior"].value_counts().items():
        log.info(f"    {r}: {q:,}")
    return df_valid


# ============================================================
# 2. Construir e enviar eventos
# ============================================================
def construir_eventos(df: pd.DataFrame, skip_cjm: bool = True) -> list[dict]:
    """Constroi payload por jogador (sem instanciar SmarticoEvent ainda)."""
    eventos = []
    for _, row in df.iterrows():
        eventos.append({
            "user_ext_id": row["external_id"],
            "player_id": str(row["player_id"]),
            "rating_anterior": row["rating_anterior"],
            "payload": {
                "-core_external_markers": TAGS_PCR_RATING,
                "skip_cjm": skip_cjm,
            },
        })
    return eventos


def salvar_dry_run(eventos: list[dict], modo: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = ROOT / "reports" / f"limpar_sumidos_pcr_dryrun_{modo}_{ts}.json"
    out.parent.mkdir(exist_ok=True)
    data = {
        "generated_at": datetime.now().isoformat(),
        "snapshot_hoje": SNAPSHOT_HOJE,
        "snapshot_ontem": SNAPSHOT_ONTEM,
        "modo": modo,
        "total_eventos": len(eventos),
        "tags_remove": TAGS_PCR_RATING,
        "sample": eventos[:5],
    }
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"  Dry-run JSON salvo: {out}")
    return out


def enviar_real(eventos: list[dict], skip_cjm: bool = True) -> dict:
    """Envia via SmarticoClient. Retorna dict com sent/failed/errors."""
    from db.smartico_api import SmarticoClient

    client = SmarticoClient()
    smartico_events = []
    for ed in eventos:
        ev = client.build_external_markers_event(
            user_ext_id=ed["user_ext_id"],
            remove_tags=TAGS_PCR_RATING,
            skip_cjm=skip_cjm,
        )
        smartico_events.append(ev)

    log.info(f"Enviando {len(smartico_events)} eventos para Smartico...")
    result = client.send_events(smartico_events)
    sent = result.get("sent", 0)
    failed = result.get("failed", 0)
    total = len(smartico_events)
    diff = total - (sent + failed)
    log.info(f"  Resultado: sent={sent} | failed={failed} | total={total} | diff={diff}")
    if diff > 0:
        log.warning(f"  ATENCAO: {diff} eventos sumiram sem reportar erro (drop silencioso?)")

    # Exporta falhas se houver
    errors = result.get("errors", [])
    if errors:
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        falhas = ROOT / "reports" / f"limpar_sumidos_pcr_falhas_{ts}.csv"
        falhas.parent.mkdir(exist_ok=True)
        ext_to_pid = {ed["user_ext_id"]: ed["player_id"] for ed in eventos}
        with open(falhas, "w", encoding="utf-8") as f:
            f.write("eid;error_code;error_message;user_ext_id;player_id\n")
            for err in errors:
                eid = err.get("eid", "")
                ext = eid.split("_")[0] if "_" in eid else eid
                pid = ext_to_pid.get(ext, "")
                f.write(f"{eid};{err.get('error_code','')};"
                        f"{err.get('error_message','')};{ext};{pid}\n")
        log.warning(f"  Falhas exportadas: {falhas}")
    return result


# ============================================================
# Main
# ============================================================
def main():
    ap = argparse.ArgumentParser()
    grp = ap.add_mutually_exclusive_group()
    grp.add_argument("--canary", action="store_true",
                    help="Envia apenas 1 jogador (canario).")
    grp.add_argument("--limit", type=int, default=0,
                    help="Limita aos N primeiros jogadores.")
    grp.add_argument("--full", action="store_true",
                    help="Envia para TODOS os sumidos (986).")
    ap.add_argument("--confirm", action="store_true",
                    help="Obrigatorio para envio real (sem isso, dry-run).")
    args = ap.parse_args()

    df = carregar_sumidos()

    if df.empty:
        log.error("Sem sumidos. Abortando.")
        return

    # Selecao
    if args.canary:
        # Pega o primeiro com rating B ou C (meio-termo) ou primeiro disponivel
        meio = df[df["rating_anterior"].isin(["B", "C"])]
        df_send = meio.head(1) if not meio.empty else df.head(1)
        modo = "canary"
    elif args.limit and args.limit > 0:
        df_send = df.head(args.limit)
        modo = f"limit{args.limit}"
    elif args.full:
        df_send = df
        modo = "full"
    else:
        df_send = df.head(5)  # default = mini-amostra pra dry-run
        modo = "dryrun5"
        log.info("Sem flag de envio — modo dry-run com amostra de 5.")

    log.info(f"Modo: {modo} | Total a enviar: {len(df_send):,}")
    log.info(f"\nIDs selecionados:")
    for _, r in df_send.head(10).iterrows():
        log.info(f"  player_id={r['player_id']} | external_id={r['external_id']} "
                 f"| rating_anterior={r['rating_anterior']}")
    if len(df_send) > 10:
        log.info(f"  ... e mais {len(df_send) - 10}")

    eventos = construir_eventos(df_send, skip_cjm=True)

    # Decisao envio real vs dry-run
    if not args.confirm:
        log.info("Sem --confirm — DRY-RUN apenas (salvando JSON).")
        path = salvar_dry_run(eventos, modo)
        print(f"\nDry-run completo. JSON: {path}")
        return

    log.info(f"--confirm presente — ENVIANDO {len(eventos)} eventos REAIS para Smartico...")
    result = enviar_real(eventos, skip_cjm=True)

    # Persistencia do log do envio real
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = ROOT / "reports" / f"limpar_sumidos_pcr_envio_{modo}_{ts}.json"
    log_path.parent.mkdir(exist_ok=True)
    log_data = {
        "generated_at": datetime.now().isoformat(),
        "snapshot_hoje": SNAPSHOT_HOJE,
        "snapshot_ontem": SNAPSHOT_ONTEM,
        "modo": modo,
        "total_enviados": len(eventos),
        "sent": result.get("sent", 0),
        "failed": result.get("failed", 0),
        "reconciliacao_diff": len(eventos) - result.get("sent", 0) - result.get("failed", 0),
        "user_ext_ids_enviados": [e["user_ext_id"] for e in eventos],
    }
    log_path.write_text(json.dumps(log_data, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info(f"Log do envio salvo: {log_path}")
    print(f"\nResultado: sent={result.get('sent',0)} failed={result.get('failed',0)} "
          f"total={len(eventos)}")


if __name__ == "__main__":
    main()
