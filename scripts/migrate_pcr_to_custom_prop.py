"""
migrate_pcr_to_custom_prop.py
==============================
Migracao one-shot: move o rating PCR de qualquer bucket antigo
(core_external_markers ou core_external_segment) para a Custom Property
dedicada core_custom_prop1 no Smartico.

Contexto (22/04/2026):
    - PCR v1.2 (20/04) pushou PCR_RATING_* em core_external_markers (bucket errado).
    - PCR v1.4 (21/04) migrou pra core_external_segment (testado, funciona, mas
      nao e o mais correto semanticamente pra 1 valor por jogador).
    - PCR v1.5 (22/04) usa core_custom_prop1 — slot reservado pelo Raphael no CRM.
      Vantagens:
        * Single string (1 rating por jogador — reflete a realidade)
        * Zero colisao com outros sistemas (cada sistema ganha sua prop)
        * Query Builder mais limpo: `core_custom_prop1 = 'PCR_RATING_S'`
        * Automation trigger direto na mudanca de valor

Operacao por jogador (payload atomico):
    {
      "core_custom_prop1": "PCR_RATING_B",           # REPLACE (valor novo)
      "-core_external_markers": [6 tags possiveis],  # limpa bucket antigo v1.2
      "-core_external_segment": [6 tags possiveis],  # limpa bucket antigo v1.4
      "skip_cjm": true                                # nao dispara jornadas
    }

Modos de uso (mesmo padrao dos outros scripts de push):

    # Dry-run
    python scripts/migrate_pcr_to_custom_prop.py --dry-run --limit 10

    # Canary (Fase 1): 1 jogador seguro
    python scripts/migrate_pcr_to_custom_prop.py --pick-canary

    # Fase 1: 1 jogador especifico
    python scripts/migrate_pcr_to_custom_prop.py --user 30352025 --skip-cjm --confirm

    # Fase 2: amostra via CSV
    python scripts/migrate_pcr_to_custom_prop.py --file amostra.csv --skip-cjm --confirm

    # Fase 3: producao full
    python scripts/migrate_pcr_to_custom_prop.py --skip-cjm --confirm

Idempotente: rodar multiplas vezes nao causa dano. Os pushes ja feitos para
core_custom_prop1 sao apenas sobrescritos com o mesmo valor, e os removes dos
buckets antigos sao no-op se ja estiverem vazios.

Apos a Fase 3 com sucesso, o push diario regular (push_pcr_to_smartico.py v1.5)
ja publica direto em core_custom_prop1.
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
log = logging.getLogger("migrate_pcr_custom")


# Slot reservado pelo Raphael no tenant Multibet (22/04/2026)
PCR_CUSTOM_PROP = "core_custom_prop1"

# Todas as tags PCR possiveis a limpar dos buckets antigos
PCR_ALL_TAGS = [
    "PCR_RATING_S",
    "PCR_RATING_A",
    "PCR_RATING_B",
    "PCR_RATING_C",
    "PCR_RATING_D",
    "PCR_RATING_E",
    "PCR_RATING_NEW",
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


def _clean_ext_id(raw) -> Optional[str]:
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
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(snapshot_date) FROM multibet.pcr_ratings")
            (snapshot_date,) = cur.fetchone()
            if snapshot_date is None:
                raise RuntimeError("Nenhum snapshot em multibet.pcr_ratings.")
            log.info("Snapshot atual: %s", snapshot_date)

            where_extra = "AND rating != 'NEW'" if exclude_new else ""

            cur.execute(
                f"""
                SELECT player_id, external_id, rating, pvs
                FROM multibet.pcr_ratings
                WHERE snapshot_date = %s
                  AND external_id IS NOT NULL
                  AND rating IN ('S','A','B','C','D','E','NEW')
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
    candidates = [p for p in players if p.rating in ("B", "C")]
    if not candidates:
        log.error("Nenhum candidato canary (rating B/C).")
        return None
    candidates.sort(key=lambda p: p.user_ext_id)
    return candidates[0]


def filter_by_user_ext_ids(
    players: List[PlayerPcr], ext_ids: List[str]
) -> List[PlayerPcr]:
    ext_set = {str(e).strip() for e in ext_ids if e}
    return [p for p in players if p.user_ext_id in ext_set]


def build_migration_events(
    client: SmarticoClient,
    players: List[PlayerPcr],
    skip_cjm: bool,
) -> List[SmarticoEvent]:
    """
    Monta evento atomico por jogador:
      - SET core_custom_prop1 com o rating atual
      - REMOVE qualquer PCR_RATING_* de core_external_markers
      - REMOVE qualquer PCR_RATING_* de core_external_segment
    """
    events: List[SmarticoEvent] = []
    for p in players:
        tag = p.smartico_tag()
        if not tag:
            log.debug("Pulando %s: rating %s sem mapeamento", p.user_ext_id, p.rating)
            continue

        ev = client.build_custom_property_event(
            user_ext_id=p.user_ext_id,
            prop_name=PCR_CUSTOM_PROP,
            value=tag,
            remove_from_markers=PCR_ALL_TAGS,
            remove_from_segment=PCR_ALL_TAGS,
            skip_cjm=skip_cjm,
        )
        events.append(ev)
    return events


def save_payload_json(events: List[SmarticoEvent], prefix: str) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    path = REPORTS_DIR / f"{prefix}_{ts}.json"
    with path.open("w", encoding="utf-8") as f:
        json.dump([e.to_dict() for e in events], f, indent=2, ensure_ascii=False)
    return path


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Migra PCR para core_custom_prop1 (limpa markers + segment)"
    )
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--pick-canary", action="store_true")
    p.add_argument("--user", type=str)
    p.add_argument("--file", type=str)
    p.add_argument("--limit", type=int, default=None)
    p.add_argument("--skip-cjm", action="store_true")
    p.add_argument("--confirm", action="store_true")
    p.add_argument("--exclude-new", action="store_true")
    p.add_argument("--batch-size", type=int, default=500)
    return p.parse_args()


def main():
    args = parse_args()
    dry_run = args.dry_run or not args.confirm
    if not args.confirm and not args.dry_run:
        log.warning("Sem --confirm -> forcando --dry-run.")

    client = SmarticoClient(dry_run=dry_run)

    if args.pick_canary:
        all_players = load_current_snapshot(exclude_new=True)
        canary = pick_canary(all_players)
        if not canary:
            sys.exit(1)
        players = [canary]
        log.info("CANARY: %s rating=%s", canary.user_ext_id, canary.rating)
    elif args.user:
        all_players = load_current_snapshot(exclude_new=args.exclude_new)
        players = [p for p in all_players if p.user_ext_id == str(args.user).strip()]
        if not players:
            log.error("user_ext_id %s nao encontrado.", args.user)
            sys.exit(1)
    elif args.file:
        all_players = load_current_snapshot(exclude_new=args.exclude_new)
        df = pd.read_csv(args.file)
        if "user_ext_id" not in df.columns:
            log.error("CSV %s sem coluna user_ext_id.", args.file)
            sys.exit(1)
        players = filter_by_user_ext_ids(all_players, df["user_ext_id"].astype(str).tolist())
        log.info("%d jogadores do CSV bateram.", len(players))
    else:
        players = load_current_snapshot(exclude_new=args.exclude_new)

    if args.limit:
        players = players[: args.limit]
        log.info("Aplicado --limit %d: %d jogadores", args.limit, len(players))

    if not players:
        log.error("Nenhum jogador para migrar.")
        sys.exit(1)

    from collections import Counter
    dist = Counter(p.rating for p in players)
    log.info("Distribuicao por rating: %s", dict(dist))

    events = build_migration_events(client, players, skip_cjm=args.skip_cjm)
    log.info("%d eventos montados (skip_cjm=%s)", len(events), args.skip_cjm)

    prefix = "smartico_pcr_custom_dryrun" if dry_run else "smartico_pcr_custom_push"
    json_path = save_payload_json(events, prefix)
    log.info("Payload salvo em %s", json_path)

    if dry_run:
        log.info("DRY-RUN completo. Nenhum evento enviado.")
        return

    log.info("=== ENVIANDO %d eventos ===", len(events))
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
