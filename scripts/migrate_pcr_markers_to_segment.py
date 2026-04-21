"""
migrate_pcr_markers_to_segment.py
==================================
Migracao one-shot: move tags PCR_RATING_* do bucket `core_external_markers`
para o bucket `core_external_segment` no Smartico.

Contexto (21/04/2026):
    - PCR v1.2 (20/04) pushou tags PCR_RATING_* em `core_external_markers`
      (bucket incorreto — mesmo das tags operacionais da Matriz de Risco).
    - PCR v1.4 (21/04) usa `core_external_segment` (bucket correto pra
      segmentos comportamentais, alinhado com Raphael do CRM).
    - Este script corrige o estado dos jogadores que receberam push anterior.

Operacao por jogador (payload atomico):
    - Remove do bucket antigo: -core_external_markers com as 6 tags possiveis
      (PCR_RATING_S, _A, _B, _C, _D, _E) — defensivo contra qualquer estado.
    - Adiciona no bucket novo:  +core_external_segment com a tag atual do snapshot.

Modos de uso (mesmo padrao do push_risk_matrix_to_smartico.py):

    # Dry-run com 10 jogadores, salva JSON pra review
    python scripts/migrate_pcr_markers_to_segment.py --dry-run --limit 10

    # Canary (Fase 1): seleciona 1 jogador seguro (rating B ou C)
    python scripts/migrate_pcr_markers_to_segment.py --pick-canary

    # Fase 1: subir 1 jogador especifico
    python scripts/migrate_pcr_markers_to_segment.py --user 12345 --skip-cjm --confirm

    # Fase 2: arquivo CSV com lista de user_ext_id (amostra 10)
    python scripts/migrate_pcr_markers_to_segment.py --file amostra.csv --skip-cjm --confirm

    # Fase 3: producao full (todos jogadores do snapshot atual)
    python scripts/migrate_pcr_markers_to_segment.py --skip-cjm --confirm

    # Filtrar rating NEW out (se nao quiser migrar novatos)
    python scripts/migrate_pcr_markers_to_segment.py --skip-cjm --confirm --exclude-new

Idempotente: rodar multiplas vezes nao causa dano (remove markers e reinsere
em segment com o rating atual do snapshot).

Apos rodar com sucesso, o push diario regular (push_pcr_to_smartico.py v1.4)
ja publica direto em core_external_segment.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import pandas as pd

SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
REPORTS_DIR = PROJECT_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(PROJECT_DIR))

from db.smartico_api import SmarticoClient, SmarticoEvent  # noqa: E402
from db.supernova import get_supernova_connection  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("migrate_pcr")


# Todas as tags PCR possiveis que podem estar em markers (vindas do push v1.2).
# Usado no payload de migracao pra garantir remocao completa independente do
# rating anterior do jogador.
PCR_ALL_MARKER_TAGS = [
    "PCR_RATING_S",
    "PCR_RATING_A",
    "PCR_RATING_B",
    "PCR_RATING_C",
    "PCR_RATING_D",
    "PCR_RATING_E",
    "PCR_RATING_NEW",  # incluido por defesa, mesmo que NEW nunca tenha sido pushado
]

RATING_TO_SMARTICO: Dict[str, str] = {
    "S":   "PCR_RATING_S",
    "A":   "PCR_RATING_A",
    "B":   "PCR_RATING_B",
    "C":   "PCR_RATING_C",
    "D":   "PCR_RATING_D",
    "E":   "PCR_RATING_E",
    "NEW": "PCR_RATING_NEW",
}


@dataclass
class PlayerPcr:
    player_id: str
    user_ext_id: str
    rating: str
    pvs: Optional[float] = None
    snapshot_date: Optional[str] = None

    def smartico_tag(self) -> Optional[str]:
        return RATING_TO_SMARTICO.get(self.rating)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------


def _clean_ext_id(raw) -> Optional[str]:
    """Normaliza user_ext_id (trata None/NaN/'12345.0'/espacos)."""
    if raw is None:
        return None
    try:
        if pd.isna(raw):
            return None
    except (TypeError, ValueError):
        pass
    s = str(raw).strip()
    if not s or s.lower() in ("none", "nan"):
        return None
    if s.endswith(".0") and s[:-2].isdigit():
        s = s[:-2]
    return s


def load_current_snapshot(exclude_new: bool = False) -> List[PlayerPcr]:
    """Le o snapshot mais recente de multibet.pcr_ratings."""
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT MAX(snapshot_date) FROM multibet.pcr_ratings"
            )
            (snapshot_date,) = cur.fetchone()
            if snapshot_date is None:
                raise RuntimeError("Nenhum snapshot em multibet.pcr_ratings.")
            log.info("Snapshot atual: %s", snapshot_date)

            where_extra = ""
            if exclude_new:
                where_extra = "AND rating != 'NEW'"

            cur.execute(
                f"""
                SELECT player_id, external_id, rating, pvs
                FROM multibet.pcr_ratings
                WHERE snapshot_date = %s
                  AND external_id IS NOT NULL
                  AND rating IN ('S', 'A', 'B', 'C', 'D', 'E', 'NEW')
                  {where_extra}
                """,
                (snapshot_date,),
            )
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
    finally:
        conn.close()
        tunnel.stop()

    players: List[PlayerPcr] = []
    for row in rows:
        d = dict(zip(cols, row))
        ext_id = _clean_ext_id(d.get("external_id"))
        if ext_id is None:
            continue
        rating = str(d.get("rating", "")).upper()
        if rating not in RATING_TO_SMARTICO:
            continue
        pvs_val = d.get("pvs")
        players.append(PlayerPcr(
            player_id=str(d["player_id"]),
            user_ext_id=ext_id,
            rating=rating,
            pvs=float(pvs_val) if pvs_val is not None else None,
            snapshot_date=str(snapshot_date),
        ))
    log.info("Snapshot carregado: %d jogadores elegiveis", len(players))
    return players


def pick_canary(players: List[PlayerPcr]) -> Optional[PlayerPcr]:
    """Seleciona 1 jogador seguro para Fase 1 (rating B ou C)."""
    candidates = [p for p in players if p.rating in ("B", "C")]
    if not candidates:
        log.error("Nenhum candidato canary (rating B ou C) no snapshot.")
        return None
    # Determinismo: pega o de menor ext_id pra ter sempre o mesmo canary
    # (permite Raphael validar no painel Smartico pelo ID).
    candidates.sort(key=lambda p: p.user_ext_id)
    return candidates[0]


def filter_by_user_ext_ids(
    players: List[PlayerPcr], ext_ids: List[str]
) -> List[PlayerPcr]:
    ext_set = {str(e).strip() for e in ext_ids if e}
    return [p for p in players if p.user_ext_id in ext_set]


# ---------------------------------------------------------------------------
# Event building
# ---------------------------------------------------------------------------


def build_migration_events(
    client: SmarticoClient,
    players: List[PlayerPcr],
    skip_cjm: bool,
) -> List[SmarticoEvent]:
    """
    Monta 1 evento de migracao por jogador:

        {
            "-core_external_markers": [6 tags PCR possiveis],
            "+core_external_segment": [tag_atual],
            "skip_cjm": true (opcional)
        }
    """
    events: List[SmarticoEvent] = []
    for p in players:
        tag = p.smartico_tag()
        if not tag:
            log.debug("Pulando %s: rating %s sem mapeamento", p.user_ext_id, p.rating)
            continue

        ev = client.build_external_segment_event(
            user_ext_id=p.user_ext_id,
            add_tags=[tag],
            remove_from_markers=PCR_ALL_MARKER_TAGS,
            skip_cjm=skip_cjm,
        )
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def save_payload_json(events: List[SmarticoEvent], prefix: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"{prefix}_{ts}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump([e.to_dict() for e in events], f, indent=2, ensure_ascii=False)
    return path


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Migra tags PCR_RATING_* de core_external_markers para core_external_segment"
    )
    parser.add_argument("--dry-run", action="store_true", help="Nao envia; salva payload JSON pra review")
    parser.add_argument("--pick-canary", action="store_true", help="Seleciona 1 jogador seguro (rating B/C)")
    parser.add_argument("--user", type=str, help="Migra 1 user_ext_id especifico")
    parser.add_argument("--file", type=str, help="CSV com coluna user_ext_id")
    parser.add_argument("--limit", type=int, default=None, help="Limita N jogadores (para testes)")
    parser.add_argument("--skip-cjm", action="store_true", help="Nao dispara Automations/Journeys")
    parser.add_argument("--confirm", action="store_true", help="Obrigatorio para envio real")
    parser.add_argument("--exclude-new", action="store_true", help="Exclui jogadores com rating NEW da migracao")
    parser.add_argument("--batch-size", type=int, default=500, help="Eventos por batch")
    return parser.parse_args()


def main():
    args = parse_args()

    dry_run = args.dry_run or not args.confirm
    if not args.confirm and not args.dry_run:
        log.warning("Sem --confirm -> forcando --dry-run pra seguranca.")

    client = SmarticoClient(dry_run=dry_run)

    # Carregar base
    if args.pick_canary:
        all_players = load_current_snapshot(exclude_new=True)
        canary = pick_canary(all_players)
        if not canary:
            sys.exit(1)
        players = [canary]
        log.info("CANARY selecionado: user_ext_id=%s rating=%s",
                 canary.user_ext_id, canary.rating)
    elif args.user:
        all_players = load_current_snapshot(exclude_new=args.exclude_new)
        players = [p for p in all_players if p.user_ext_id == str(args.user).strip()]
        if not players:
            log.error("user_ext_id %s nao encontrado no snapshot atual.", args.user)
            sys.exit(1)
    elif args.file:
        all_players = load_current_snapshot(exclude_new=args.exclude_new)
        df = pd.read_csv(args.file)
        if "user_ext_id" not in df.columns:
            log.error("Arquivo %s nao contem coluna user_ext_id.", args.file)
            sys.exit(1)
        ext_ids = df["user_ext_id"].astype(str).tolist()
        players = filter_by_user_ext_ids(all_players, ext_ids)
        log.info("%d jogadores do CSV bateram com o snapshot atual.", len(players))
    else:
        players = load_current_snapshot(exclude_new=args.exclude_new)

    if args.limit:
        players = players[: args.limit]
        log.info("Aplicado --limit %d: %d jogadores", args.limit, len(players))

    if not players:
        log.error("Nenhum jogador para migrar.")
        sys.exit(1)

    # Distribuicao por rating
    from collections import Counter
    dist = Counter(p.rating for p in players)
    log.info("Distribuicao por rating: %s", dict(dist))

    # Build events
    events = build_migration_events(client, players, skip_cjm=args.skip_cjm)
    log.info("%d eventos montados (skip_cjm=%s)", len(events), args.skip_cjm)

    # Sempre salva o payload pra auditoria
    prefix = "smartico_pcr_migration_dryrun" if dry_run else "smartico_pcr_migration_push"
    json_path = save_payload_json(events, prefix)
    log.info("Payload salvo em %s", json_path)

    if dry_run:
        log.info("DRY-RUN completo. Nenhum evento enviado.")
        return

    # Envio real
    log.info("=== ENVIANDO %d eventos ao Smartico ===", len(events))
    result = client.send_events(events, batch_size=args.batch_size)
    log.info("Resultado: sent=%d failed=%d total=%d", result["sent"], result["failed"], result["total"])
    if result["errors"]:
        log.warning("Primeiros erros: %s", result["errors"][:5])


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log.error("Migracao falhou: %s", e, exc_info=True)
        sys.exit(1)
