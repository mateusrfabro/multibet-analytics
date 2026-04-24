"""
push_risk_matrix_to_smartico.py
================================================================
Publica as tags da Matriz de Risco v2 no Smartico via S2S API.

Estrategia de tags (naming padronizado):
    - 1 tag de tier por jogador:
        RISK_TIER_MUITO_BOM | RISK_TIER_BOM | RISK_TIER_MEDIANO
        RISK_TIER_RUIM      | RISK_TIER_MUITO_RUIM
    - N tags comportamentais (so as ATIVAS do jogador), prefixo RISK_:
        RISK_FAST_CASHOUT, RISK_VIP_WHALE_PLAYER, etc.
    - Operacao no Smartico eh ATOMICA (doc oficial):
        * "^core_external_markers": ["RISK_*"]   -> remove todas as nossas antigas
        * "+core_external_markers": [novas tags] -> adiciona as novas
      Essa combinacao PRESERVA tags de outras integracoes (nao-RISK_*).

Fonte dos dados:
    multibet.risk_tags (tabela base, Super Nova DB PostgreSQL)
    Snapshot mais recente = MAX(snapshot_date)

Deduplicacao (regra da doc Smartico):
    "update_profile" soh deve ser enviado se houve mudanca real na propriedade.
    Por padrao, o script compara snapshot atual vs anterior e envia apenas diffs.
    Use --force pra ignorar o diff e enviar todos (CUIDADO: rate limit).

Modos de uso:
    # Fase 0: dry-run com 10 jogadores, salva JSON pra review
    python scripts/push_risk_matrix_to_smartico.py --dry-run --limit 10

    # Selecionar jogador canary pra Fase 1 (tier Mediano, sem tags extremas)
    python scripts/push_risk_matrix_to_smartico.py --pick-canary

    # Fase 1: subir 1 jogador especifico (skip_cjm=True = nao dispara automation)
    python scripts/push_risk_matrix_to_smartico.py --user 12345 --skip-cjm --confirm

    # Fase 2: arquivo CSV com lista de user_ext_id
    python scripts/push_risk_matrix_to_smartico.py --file amostra.csv --skip-cjm --confirm

    # Fase 4: producao full (so diffs vs snapshot anterior)
    python scripts/push_risk_matrix_to_smartico.py --confirm

    # Fase 4 com force (ignora diff, reenviando tudo - cuidar do rate limit)
    python scripts/push_risk_matrix_to_smartico.py --confirm --force

Dependencias:
    pip install pandas psycopg2-binary sshtunnel python-dotenv requests
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import pandas as pd

# Setup paths
SCRIPT_DIR = Path(__file__).resolve().parent
PROJECT_DIR = SCRIPT_DIR.parent
REPORTS_DIR = PROJECT_DIR / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

sys.path.insert(0, str(PROJECT_DIR))

from db.smartico_api import SmarticoClient, SmarticoEvent  # noqa: E402
from db.supernova import get_supernova_connection  # noqa: E402

# Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("push_smartico")

# ---------------------------------------------------------------------------
# Mapeamento de tags (do risk_matrix_pipeline.py)
# ---------------------------------------------------------------------------

TAG_COLUMNS_TO_SMARTICO: Dict[str, str] = {
    "regular_depositor": "RISK_REGULAR_DEPOSITOR",
    "promo_only": "RISK_PROMO_ONLY",
    "zero_risk_player": "RISK_ZERO_RISK_PLAYER",
    "fast_cashout": "RISK_FAST_CASHOUT",
    "sustained_player": "RISK_SUSTAINED_PLAYER",
    "non_bonus_depositor": "RISK_NON_BONUS_DEPOSITOR",
    "promo_chainer": "RISK_PROMO_CHAINER",
    "cashout_and_run": "RISK_CASHOUT_AND_RUN",
    "reinvest_player": "RISK_REINVEST_PLAYER",
    "non_promo_player": "RISK_NON_PROMO_PLAYER",
    "engaged_player": "RISK_ENGAGED_PLAYER",
    "rg_alert_player": "RISK_RG_ALERT_PLAYER",
    "behav_risk_player": "RISK_BEHAV_RISK_PLAYER",
    "potencial_abuser": "RISK_POTENCIAL_ABUSER",
    "player_reengaged": "RISK_PLAYER_REENGAGED",
    "sleeper_low_player": "RISK_SLEEPER_LOW_PLAYER",
    "vip_whale_player": "RISK_VIP_WHALE_PLAYER",
    "winback_hi_val_player": "RISK_WINBACK_HI_VAL_PLAYER",
    "behav_slotgamer": "RISK_BEHAV_SLOTGAMER",
    "multi_game_player": "RISK_MULTI_GAME_PLAYER",
    "rollback_player": "RISK_ROLLBACK_PLAYER",
}

TIER_TO_SMARTICO: Dict[str, str] = {
    "Muito Bom": "RISK_TIER_MUITO_BOM",
    "Bom": "RISK_TIER_BOM",
    "Mediano": "RISK_TIER_MEDIANO",
    "Ruim": "RISK_TIER_RUIM",
    "Muito Ruim": "RISK_TIER_MUITO_RUIM",
}

TAG_COLUMNS = list(TAG_COLUMNS_TO_SMARTICO.keys())
TAG_SELECT_LIST = ", ".join(TAG_COLUMNS)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------


@dataclass
class PlayerSnapshot:
    user_id: str
    user_ext_id: str
    tier: str
    score_norm: float
    active_tag_columns: List[str]
    snapshot_date: Optional[str] = None  # data do snapshot de origem

    def smartico_tags(self) -> List[str]:
        """Retorna a lista completa de tags RISK_* que o jogador deve ter."""
        tags: List[str] = []
        tier_tag = TIER_TO_SMARTICO.get(self.tier)
        if tier_tag:
            tags.append(tier_tag)
        for col in self.active_tag_columns:
            mapped = TAG_COLUMNS_TO_SMARTICO.get(col)
            if mapped:
                tags.append(mapped)
        return sorted(set(tags))


def _query_snapshot(cursor, snapshot_date) -> pd.DataFrame:
    """Le todas as colunas de tag de um snapshot especifico."""
    sql = f"""
        SELECT
            user_id,
            user_ext_id,
            tier,
            score_norm,
            {TAG_SELECT_LIST}
        FROM multibet.risk_tags
        WHERE snapshot_date = %s
          AND user_ext_id IS NOT NULL
          AND tier IS NOT NULL
          AND tier != 'SEM SCORE'
    """
    cursor.execute(sql, (snapshot_date,))
    cols = [d[0] for d in cursor.description]
    rows = cursor.fetchall()
    return pd.DataFrame(rows, columns=cols)


def _get_last_two_snapshot_dates(cursor) -> List:
    cursor.execute(
        "SELECT DISTINCT snapshot_date FROM multibet.risk_tags "
        "ORDER BY snapshot_date DESC LIMIT 2"
    )
    return [r[0] for r in cursor.fetchall()]


def load_current_and_previous_snapshots() -> Tuple[
    Tuple[pd.DataFrame, str], Optional[Tuple[pd.DataFrame, str]]
]:
    """
    Retorna ((df_atual, date_atual), (df_anterior, date_anterior) ou None).
    df_anterior eh None no primeiro run (so existe 1 snapshot).
    """
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            dates = _get_last_two_snapshot_dates(cur)
            if not dates:
                raise RuntimeError("Nenhum snapshot em multibet.risk_tags.")
            log.info("Snapshot dates encontrados: %s", dates)

            df_current = _query_snapshot(cur, dates[0])
            log.info("Snapshot atual (%s): %d jogadores", dates[0], len(df_current))

            result_previous = None
            if len(dates) > 1:
                df_previous = _query_snapshot(cur, dates[1])
                log.info(
                    "Snapshot anterior (%s): %d jogadores", dates[1], len(df_previous)
                )
                result_previous = (df_previous, str(dates[1]))
            else:
                log.warning("So ha 1 snapshot na base - primeiro run (nao da pra diffar).")
    finally:
        conn.close()
        tunnel.stop()

    return (df_current, str(dates[0])), result_previous


def _clean_ext_id(raw) -> Optional[str]:
    """
    Normaliza user_ext_id vindo do Postgres.
    Trata 3 casos chatos:
      1. None / NaN / string "None"
      2. Valor persistido como "12345.0" (pipeline legado armazenou float->str)
      3. Espaco em branco
    """
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


def _tag_int(val) -> int:
    """
    Casta valor de coluna de tag pra 0/1 robustamente.

    ATENCAO: em multibet.risk_tags as colunas de tag NAO sao binarias - elas
    guardam o SCORE da tag (ex: fast_cashout=-25, behav_slotgamer=5). Uma tag
    esta ATIVA quando o valor eh DIFERENTE DE ZERO (nao quando == 1).
    """
    if val is None:
        return 0
    try:
        if pd.isna(val):
            return 0
    except (TypeError, ValueError):
        pass
    try:
        return 1 if int(val) != 0 else 0
    except (TypeError, ValueError):
        return 0


def dataframe_to_player_snapshots(
    df: pd.DataFrame, snapshot_date: Optional[str] = None
) -> Dict[str, PlayerSnapshot]:
    """Converte df -> dict[user_ext_id -> PlayerSnapshot]."""
    players: Dict[str, PlayerSnapshot] = {}
    for _, row in df.iterrows():
        ext_id = _clean_ext_id(row.get("user_ext_id"))
        if ext_id is None:
            continue
        active = [c for c in TAG_COLUMNS if _tag_int(row.get(c)) == 1]
        players[ext_id] = PlayerSnapshot(
            user_id=str(row["user_id"]),
            user_ext_id=ext_id,
            tier=str(row["tier"]),
            score_norm=float(row.get("score_norm") or 0),
            active_tag_columns=active,
            snapshot_date=snapshot_date,
        )
    return players


# ---------------------------------------------------------------------------
# Diff logic (deduplicacao exigida pela doc Smartico)
# ---------------------------------------------------------------------------


def diff_players(
    current: Dict[str, PlayerSnapshot],
    previous: Optional[Dict[str, PlayerSnapshot]],
) -> List[PlayerSnapshot]:
    """
    Retorna apenas os jogadores cujo conjunto de tags RISK_* mudou.
    Se nao ha snapshot anterior, retorna todos (baseline inicial).
    """
    if previous is None:
        log.info("Sem snapshot anterior - enviando todos os %d", len(current))
        return list(current.values())

    changed: List[PlayerSnapshot] = []
    for ext_id, player in current.items():
        prev_player = previous.get(ext_id)
        if prev_player is None:
            changed.append(player)  # jogador novo
            continue
        if set(player.smartico_tags()) != set(prev_player.smartico_tags()):
            changed.append(player)

    log.info(
        "Diff: %d de %d jogadores mudaram de tags (vs snapshot anterior)",
        len(changed),
        len(current),
    )
    return changed


# ---------------------------------------------------------------------------
# Canary picker
# ---------------------------------------------------------------------------


def pick_canary_user() -> Optional[PlayerSnapshot]:
    """
    Seleciona 1 jogador SEGURO pra teste Fase 1:
        - tier = Mediano
        - SEM tags extremas (vip_whale, fast_cashout, cashout_and_run, rg_alert)
        - user_ext_id nao-nulo e nao-vazio
        - pelo menos 1 tag comportamental ativa (pra visualizar no BO)
    """
    (df_current, current_date), _ = load_current_and_previous_snapshots()

    # Normaliza colunas de tag pra int (robusto contra object/None)
    for c in TAG_COLUMNS:
        df_current[c] = df_current[c].apply(_tag_int)

    mask = (
        (df_current["tier"] == "Mediano")
        & (df_current["vip_whale_player"] == 0)
        & (df_current["fast_cashout"] == 0)
        & (df_current["cashout_and_run"] == 0)
        & (df_current["rg_alert_player"] == 0)
        & (df_current["user_ext_id"].notna())
    )
    candidates = df_current[mask].copy()
    if candidates.empty:
        log.error("Nenhum candidato canary encontrado.")
        return None

    # Pelo menos 2 tags ativas (melhor pra visualizar no BackOffice)
    candidates["num_tags"] = candidates[TAG_COLUMNS].sum(axis=1)
    candidates = candidates[candidates["num_tags"] >= 2]
    if candidates.empty:
        log.error("Nenhum candidato com >= 2 tags ativas.")
        return None

    # Prefere jogadores com 2-4 tags (nao poluido, mas com coisa pra ver)
    sweet = candidates[(candidates["num_tags"] >= 2) & (candidates["num_tags"] <= 4)]
    if not sweet.empty:
        candidates = sweet

    # Escolhe 1 random com seed fixo (reproduzivel)
    row = candidates.sample(n=1, random_state=42).iloc[0]
    ext_id = _clean_ext_id(row.get("user_ext_id"))
    if ext_id is None:
        log.error("Candidato sorteado tem user_ext_id invalido.")
        return None
    active = [c for c in TAG_COLUMNS if _tag_int(row.get(c)) == 1]
    return PlayerSnapshot(
        user_id=str(row["user_id"]),
        user_ext_id=ext_id,
        tier=str(row["tier"]),
        score_norm=float(row.get("score_norm") or 0),
        active_tag_columns=active,
    )


# ---------------------------------------------------------------------------
# Event building
# ---------------------------------------------------------------------------


def build_events(
    client: SmarticoClient,
    players: List[PlayerSnapshot],
    skip_cjm: bool,
) -> List[SmarticoEvent]:
    """
    Monta 1 evento update_profile por jogador usando a combinacao atomica:
        ^core_external_markers: ["RISK_*"]  -> remove todas as nossas antigas
        +core_external_markers: [tags atuais] -> adiciona as novas

    Isso preserva tags de outras integracoes (nao-RISK_*).
    """
    events: List[SmarticoEvent] = []
    for p in players:
        tags = p.smartico_tags()
        if not tags:
            log.debug("Pulando %s: sem tags RISK_* pra aplicar", p.user_ext_id)
            continue
        ev = client.build_external_markers_event(
            user_ext_id=p.user_ext_id,
            remove_pattern=["RISK_*"],
            add_tags=tags,
            skip_cjm=skip_cjm,
        )
        events.append(ev)
    return events


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------


def save_dry_run_report(events: List[SmarticoEvent], players: List[PlayerSnapshot]) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = REPORTS_DIR / f"smartico_dryrun_{ts}.json"
    data = {
        "generated_at": datetime.now().isoformat(),
        "total_events": len(events),
        "sample": [
            {
                "player": {
                    "user_id": p.user_id,
                    "user_ext_id": p.user_ext_id,
                    "tier": p.tier,
                    "score_norm": p.score_norm,
                    "snapshot_date": p.snapshot_date,
                    "active_tag_count": len(p.active_tag_columns),
                    "tags_aplicadas": p.smartico_tags(),
                },
                "payload": e.to_dict(),
            }
            for p, e in zip(players[:20], events[:20])
        ],
    }
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")
    log.info("Dry-run JSON salvo em: %s", out)
    return out


def save_run_log(result: Dict, events_count: int) -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = REPORTS_DIR / f"smartico_push_{ts}.log"
    lines = [
        f"Smartico push run @ {datetime.now().isoformat()}",
        f"Total eventos preparados: {events_count}",
        f"Enviados com sucesso:     {result.get('sent', 0)}",
        f"Falhas:                   {result.get('failed', 0)}",
        "",
        "Erros (amostra):",
        json.dumps(result.get("errors", []), indent=2, ensure_ascii=False),
    ]
    out.write_text("\n".join(lines), encoding="utf-8")
    log.info("Log do run salvo em: %s", out)
    return out


# ---------------------------------------------------------------------------
# Filters
# ---------------------------------------------------------------------------


def _lookup_user_any_snapshot(user_ext_id: str) -> Optional[PlayerSnapshot]:
    """
    Fallback: busca o jogador no snapshot mais recente onde ele aparece.
    Usado quando --user nao encontra no snapshot atual (jogador saiu da
    janela de 90 dias mas a classificacao antiga ainda eh valida).
    """
    ext_id = str(user_ext_id).strip()
    # Tenta tambem a variante com ".0" (legado float->str)
    ext_variants = [ext_id]
    if not ext_id.endswith(".0"):
        ext_variants.append(ext_id + ".0")

    placeholders = ", ".join(["%s"] * len(ext_variants))
    sql = f"""
        SELECT
            snapshot_date,
            user_id,
            user_ext_id,
            tier,
            score_norm,
            {TAG_SELECT_LIST}
        FROM multibet.risk_tags
        WHERE user_ext_id IN ({placeholders})
          AND tier IS NOT NULL
          AND tier != 'SEM SCORE'
        ORDER BY snapshot_date DESC
        LIMIT 1
    """
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(sql, ext_variants)
            row = cur.fetchone()
            if row is None:
                return None
            cols = [d[0] for d in cur.description]
            data = dict(zip(cols, row))

            active = [c for c in TAG_COLUMNS if _tag_int(data.get(c)) == 1]
            snap_date = str(data["snapshot_date"])
            clean_id = _clean_ext_id(data["user_ext_id"])

            log.info(
                "Fallback: jogador %s encontrado no snapshot %s (tier=%s)",
                clean_id,
                snap_date,
                data["tier"],
            )
            return PlayerSnapshot(
                user_id=str(data["user_id"]),
                user_ext_id=clean_id or ext_id,
                tier=str(data["tier"]),
                score_norm=float(data.get("score_norm") or 0),
                active_tag_columns=active,
                snapshot_date=snap_date,
            )
    finally:
        conn.close()
        tunnel.stop()


def filter_by_user(
    players: Dict[str, PlayerSnapshot], user_ext_id: str
) -> List[PlayerSnapshot]:
    p = players.get(str(user_ext_id).strip())
    if p is not None:
        return [p]

    # Fallback: busca no ultimo snapshot onde o jogador aparece
    log.warning(
        "user_ext_id %s nao encontrado no snapshot atual. "
        "Buscando no historico de snapshots...",
        user_ext_id,
    )
    fallback = _lookup_user_any_snapshot(user_ext_id)
    if fallback is None:
        log.error(
            "user_ext_id %s nao encontrado em NENHUM snapshot.", user_ext_id
        )
        return []
    return [fallback]


def filter_by_file(
    players: Dict[str, PlayerSnapshot], csv_path: Path
) -> List[PlayerSnapshot]:
    df = pd.read_csv(csv_path)
    col = None
    for candidate in ("user_ext_id", "ext_id", "user_id"):
        if candidate in df.columns:
            col = candidate
            break
    if col is None:
        raise ValueError(f"CSV precisa ter coluna user_ext_id: {csv_path}")
    ids = {str(x).strip() for x in df[col].dropna()}
    result = [players[i] for i in ids if i in players]
    log.info(
        "Filtro por arquivo: %d ids no CSV, %d encontrados no snapshot",
        len(ids),
        len(result),
    )
    return result


# ---------------------------------------------------------------------------
# Main CLI
# ---------------------------------------------------------------------------


def main():
    ap = argparse.ArgumentParser(
        description="Sobe tags da matriz de risco pro Smartico"
    )
    ap.add_argument("--dry-run", action="store_true", help="Nao chama a API (gera JSON)")
    ap.add_argument("--confirm", action="store_true", help="Envia de fato (sem isso, implicit dry-run)")
    ap.add_argument("--user", type=str, help="user_ext_id especifico (Fase 1)")
    ap.add_argument("--file", type=str, help="CSV com lista de user_ext_id (Fase 2)")
    ap.add_argument("--limit", type=int, default=0, help="Processar no maximo N jogadores")
    ap.add_argument(
        "--pick-canary",
        action="store_true",
        help="Apenas seleciona um jogador canary seguro e imprime no stdout",
    )
    ap.add_argument(
        "--skip-cjm",
        action="store_true",
        help="payload.skip_cjm=true (nao dispara Automation - usar nas Fases 1-3)",
    )
    ap.add_argument(
        "--force",
        action="store_true",
        help="Ignora diff vs snapshot anterior (envia todos)",
    )
    ap.add_argument("--batch-size", type=int, default=1000, help="Tamanho do batch")
    args = ap.parse_args()

    # Modo --pick-canary: so seleciona o jogador e sai
    if args.pick_canary:
        log.info("Selecionando canary user...")
        player = pick_canary_user()
        if player is None:
            print("\n[ERRO] Nenhum candidato canary encontrado.")
            sys.exit(1)
        print("\n" + "=" * 60)
        print("CANARY USER SELECIONADO (Fase 1)")
        print("=" * 60)
        print(f"user_id:      {player.user_id}")
        print(f"user_ext_id:  {player.user_ext_id}")
        print(f"tier:         {player.tier}")
        print(f"score_norm:   {player.score_norm}")
        print(f"tags ativas ({len(player.active_tag_columns)}):")
        for t in player.smartico_tags():
            print(f"  - {t}")
        print("=" * 60)
        print("\nProximo passo (apos OK do Raphael):")
        print(f"  python scripts/push_risk_matrix_to_smartico.py \\")
        print(f"      --user {player.user_ext_id} --skip-cjm --confirm")
        return

    # Carrega snapshots
    (df_current, current_date), prev_result = load_current_and_previous_snapshots()
    current_players = dataframe_to_player_snapshots(df_current, current_date)
    previous_players = (
        dataframe_to_player_snapshots(prev_result[0], prev_result[1])
        if prev_result is not None
        else None
    )

    # Seleciona jogadores a processar
    if args.user:
        selected = filter_by_user(current_players, args.user)
    elif args.file:
        selected = filter_by_file(current_players, Path(args.file))
    elif args.force:
        log.info("--force: ignorando diff, enviando TODOS do snapshot atual")
        selected = list(current_players.values())
    else:
        selected = diff_players(current_players, previous_players)

    if args.limit and args.limit > 0:
        selected = selected[: args.limit]
        log.info("Limitado a %d jogadores (--limit)", len(selected))

    if not selected:
        log.info("Nenhum jogador a processar. Nada a fazer.")
        return

    # Determina modo: confirm=False => dry-run forcado
    is_dry = args.dry_run or not args.confirm
    if not is_dry and args.dry_run:
        is_dry = True  # --dry-run sempre vence
    if args.confirm and args.dry_run:
        log.warning("--dry-run + --confirm: dry-run prevalece.")
        is_dry = True

    client = SmarticoClient(dry_run=is_dry)
    log.info(
        "Modo: %s | skip_cjm=%s | brand=%s",
        "DRY-RUN" if is_dry else "LIVE",
        args.skip_cjm,
        client.brand,
    )

    # Monta eventos
    events = build_events(client, selected, skip_cjm=args.skip_cjm)
    log.info("Eventos montados: %d", len(events))

    if is_dry:
        save_dry_run_report(events, selected)
        log.info("DRY-RUN finalizado. Nenhuma chamada a API foi feita.")
        return

    # Envio real
    result = client.send_events(events, batch_size=args.batch_size)
    log.info(
        "Envio concluido: sent=%d failed=%d total=%d",
        result["sent"],
        result["failed"],
        result["total"],
    )
    save_run_log(result, len(events))

    if result["failed"] > 0:
        log.warning("Houve %d falhas - verifique o log", result["failed"])
        sys.exit(2)


if __name__ == "__main__":
    main()
