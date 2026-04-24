"""
Auditoria de Cobertura de Imagens de Jogos
============================================
Cruza 3 fontes:
  1. Athena (bireports_ec2.tbl_vendor_games_mapping_data) - catalogo ativo
  2. CSV pipelines/jogos.csv - scraper do site multi.bet.br
  3. Super Nova DB (multibet.game_image_mapping) - mapping consolidado

Tambem cruza com grandes_ganhos para priorizar jogos sem imagem que
aparecem em CASINO_WIN.

Saida: reports/auditoria_cobertura_imagens_20260414.txt
"""

import sys
import os
import csv
import logging
import requests
from datetime import datetime, timezone, timedelta
from collections import Counter

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
CSV_PATH = os.path.join(PROJECT_ROOT, "pipelines", "jogos.csv")
REPORT_PATH = os.path.join(PROJECT_ROOT, "reports", "auditoria_cobertura_imagens_20260414.txt")

# ============================================================================
# A) Query Athena - todos os jogos ativos (dedup por game_name)
# ============================================================================
QUERY_ATHENA_ACTIVE_GAMES = """
SELECT
    UPPER(TRIM(c_game_desc))  AS game_name_upper,
    c_vendor_id               AS vendor_id,
    c_game_id                 AS provider_game_id,
    c_game_desc               AS game_name_original
FROM (
    SELECT
        c_game_desc,
        c_vendor_id,
        c_game_id,
        ROW_NUMBER() OVER (
            PARTITION BY UPPER(TRIM(c_game_desc))
            ORDER BY
                CASE WHEN c_client_platform = 'WEB' THEN 0 ELSE 1 END,
                c_game_id
        ) AS rn
    FROM bireports_ec2.tbl_vendor_games_mapping_data
    WHERE c_status = 'active'
      AND c_game_id IS NOT NULL
      AND c_game_desc IS NOT NULL
)
WHERE rn = 1
"""

# ============================================================================
# Query para top jogos no grandes_ganhos (ultimos 30 dias de CASINO_WIN)
# ============================================================================
QUERY_TOP_CASINO_WINS = """
WITH game_catalog AS (
    SELECT c_game_id, c_game_desc, c_vendor_id
    FROM (
        SELECT
            c_game_id, c_game_desc, c_vendor_id,
            ROW_NUMBER() OVER (
                PARTITION BY c_game_id
                ORDER BY CASE WHEN c_client_platform = 'WEB' THEN 0 ELSE 1 END
            ) AS rn
        FROM bireports_ec2.tbl_vendor_games_mapping_data
        WHERE c_status = 'active'
          AND c_game_id IS NOT NULL
          AND c_game_desc IS NOT NULL
    )
    WHERE rn = 1
)
SELECT
    UPPER(TRIM(COALESCE(g.c_game_desc, g2.c_game_desc))) AS game_name_upper,
    COALESCE(g.c_vendor_id, g2.c_vendor_id, f.c_sub_vendor_id) AS vendor_id,
    COUNT(*) AS win_count,
    COUNT(DISTINCT f.c_ecr_id) AS unique_players,
    SUM(f.c_amount_in_ecr_ccy / 100.0) AS total_win_brl
FROM fund_ec2.tbl_real_fund_txn f
LEFT JOIN game_catalog g
    ON f.c_game_id = g.c_game_id
LEFT JOIN game_catalog g2
    ON CASE
        WHEN STRPOS(f.c_game_id, '_') > 0
        THEN SPLIT_PART(f.c_game_id, '_', 1)
        ELSE f.c_game_id
       END = g2.c_game_id
    AND g.c_game_id IS NULL
WHERE f.c_txn_type = 45
  AND f.c_txn_status = 'SUCCESS'
  AND f.c_product_id = 'CASINO'
  AND f.c_start_time >= TIMESTAMP '{date_start}'
  AND f.c_start_time <  TIMESTAMP '{date_end}'
  AND f.c_amount_in_ecr_ccy > 0
  AND COALESCE(g.c_game_desc, g2.c_game_desc) IS NOT NULL
GROUP BY 1, 2
ORDER BY win_count DESC
"""


def load_csv(path):
    """Le o CSV do scraper e retorna dict {NOME_UPPER: url}."""
    jogos = {}
    if not os.path.exists(path):
        log.warning(f"CSV nao encontrado: {path}")
        return jogos
    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            nome = row.get("nome", "").strip()
            url = row.get("url", "").strip()
            if nome and url and "placeholder" not in url.lower():
                key = nome.upper().strip()
                if key not in jogos:
                    jogos[key] = {"nome": nome, "url": url}
    return jogos


def check_cdn_url(game_id, vendor_id):
    """Tenta HEAD request em variantes de URL CDN para encontrar imagem."""
    # Prefixos conhecidos por vendor
    vendor_prefix_map = {
        "pragmaticplay": "pp",
        "pgsoft": "alea_pg",
        "evolution": "alea_evo",
        "spribe": "alea_spr",
        "playngo": "alea_pla",
        "hacksaw": "alea_hac",
        "relaxgaming": "alea_rlx",
        "netent": "alea_ne",
        "nolimitcity": "alea_nlc",
        "pushgaming": "alea_psg",
        "thunderkick": "alea_thk",
        "bigtimegaming": "alea_btg",
        "redtiger": "alea_rt",
        "blueprint": "alea_blu",
        "elk": "alea_elk",
        "evoplay": "alea_evp",
        "1x2gaming": "alea_1x2",
        "amusnet": "alea_amn",
        "betsoft": "alea_bts",
        "booming": "alea_boo",
        "booongo": "alea_bog",
        "endorphina": "alea_end",
        "gameart": "alea_ga",
        "gamebeat": "alea_gb",
        "habanero": "alea_hab",
        "kagaming": "alea_ka",
        "mascot": "alea_msc",
        "platipus": "alea_plt",
        "playson": "alea_pls",
        "wazdan": "alea_waz",
        "yggdrasil": "alea_ygg",
        "kalamba": "alea_klb",
        "bgaming": "alea_bg",
        "3oaks": "alea_3ok",
    }

    if not game_id:
        return None

    # Tentativas de URL
    urls_to_try = []

    # Variante 1: com prefixo do vendor
    if vendor_id and vendor_id.lower() in vendor_prefix_map:
        prefix = vendor_prefix_map[vendor_id.lower()]
        slug = f"{prefix}{game_id}"
        urls_to_try.append(f"https://multi.bet.br//uploads/games/MUL//{slug}/{slug}.webp")

    # Variante 2: Pragmatic direto (game_id ja pode ter o prefixo pp/vs)
    if game_id.startswith("pp") or game_id.startswith("vs"):
        slug = f"pp{game_id}" if not game_id.startswith("pp") else game_id
        urls_to_try.append(f"https://multi.bet.br//uploads/games/MUL//{game_id}/{game_id}.webp")

    # Variante 3: game_id puro
    urls_to_try.append(f"https://multi.bet.br//uploads/games/MUL//{game_id}/{game_id}.webp")

    for url in urls_to_try:
        try:
            resp = requests.head(url, timeout=5, allow_redirects=True)
            if resp.status_code == 200:
                content_type = resp.headers.get("content-type", "")
                if "image" in content_type or "webp" in content_type or resp.headers.get("content-length", "0") != "0":
                    return url
        except Exception:
            pass

    return None


def main():
    lines = []

    def out(text=""):
        lines.append(text)
        log.info(text)

    out("=" * 80)
    out("  AUDITORIA DE COBERTURA DE IMAGENS DE JOGOS")
    out(f"  Data: {datetime.now(timezone(timedelta(hours=-3))).strftime('%Y-%m-%d %H:%M BRT')}")
    out("=" * 80)
    out()

    # ── A) Athena: catalogo de jogos ativos ──────────────────────────────────
    out(">>> FONTE 1: Athena (bireports_ec2.tbl_vendor_games_mapping_data)")
    out("-" * 70)

    try:
        df_athena = query_athena(QUERY_ATHENA_ACTIVE_GAMES, database="bireports_ec2")
        athena_games = {}
        for _, row in df_athena.iterrows():
            key = str(row["game_name_upper"]).strip()
            if key and key != "NONE":
                athena_games[key] = {
                    "vendor_id": str(row["vendor_id"]) if row["vendor_id"] else None,
                    "provider_game_id": str(row["provider_game_id"]) if row["provider_game_id"] else None,
                    "game_name_original": str(row["game_name_original"]) if row["game_name_original"] else key.title(),
                }
        out(f"  Jogos ativos unicos (dedup por nome): {len(athena_games)}")
    except Exception as e:
        out(f"  ERRO ao consultar Athena: {e}")
        athena_games = {}
        return

    # Contagem por vendor
    vendor_counts = Counter()
    for g in athena_games.values():
        v = g["vendor_id"] or "DESCONHECIDO"
        vendor_counts[v] += 1
    out(f"  Vendors encontrados: {len(vendor_counts)}")
    out()
    out("  Top 15 vendors por qtd de jogos ativos:")
    for vendor, count in vendor_counts.most_common(15):
        out(f"    {vendor:30s} {count:5d} jogos")
    out()

    # ── B) CSV do scraper ────────────────────────────────────────────────────
    out(">>> FONTE 2: CSV scraper (pipelines/jogos.csv)")
    out("-" * 70)

    csv_games = load_csv(CSV_PATH)
    out(f"  Jogos unicos no CSV: {len(csv_games)}")
    out()

    # ── C) Super Nova DB: mapping atual ──────────────────────────────────────
    out(">>> FONTE 3: Super Nova DB (multibet.game_image_mapping)")
    out("-" * 70)

    try:
        mapping_rows = execute_supernova(
            """SELECT game_name_upper, game_image_url, source, vendor_id, provider_game_id
               FROM multibet.game_image_mapping""",
            fetch=True,
        ) or []

        mapping_all = {}
        mapping_with_img = {}
        for r in mapping_rows:
            key = r[0]
            mapping_all[key] = {
                "game_image_url": r[1],
                "source": r[2],
                "vendor_id": r[3],
                "provider_game_id": r[4],
            }
            if r[1]:  # tem imagem
                mapping_with_img[key] = r[1]

        out(f"  Total registros no mapping: {len(mapping_all)}")
        out(f"  Com imagem (game_image_url preenchida): {len(mapping_with_img)}")
        out(f"  Sem imagem (game_image_url NULL):       {len(mapping_all) - len(mapping_with_img)}")

        # Breakdown por source
        source_counts = Counter()
        for m in mapping_all.values():
            source_counts[m["source"] or "NULL"] += 1
        out()
        out("  Distribuicao por source:")
        for src, cnt in source_counts.most_common():
            out(f"    {src:25s} {cnt:5d}")
    except Exception as e:
        out(f"  ERRO ao consultar Super Nova DB: {e}")
        mapping_all = {}
        mapping_with_img = {}
    out()

    # ── D) Cruzamento ────────────────────────────────────────────────────────
    out("=" * 80)
    out("  ANALISE DE COBERTURA")
    out("=" * 80)
    out()

    # D1: Jogos no Athena SEM imagem (nem CSV nem mapping)
    athena_no_img = {}
    for name, data in athena_games.items():
        has_csv = name in csv_games
        has_mapping = name in mapping_with_img
        if not has_csv and not has_mapping:
            athena_no_img[name] = data

    out(f">>> Jogos no Athena SEM imagem (nem no CSV, nem no mapping): {len(athena_no_img)} de {len(athena_games)}")
    out(f"    Cobertura geral: {((len(athena_games) - len(athena_no_img)) / len(athena_games) * 100):.1f}%")
    out()

    # D2: Cobertura por vendor
    out(">>> Cobertura por vendor/provider:")
    out("-" * 70)
    out(f"  {'VENDOR':30s} {'TOTAL':>6s} {'C/ IMG':>7s} {'S/ IMG':>7s} {'COBERT':>8s}")
    out(f"  {'-'*30} {'-'*6} {'-'*7} {'-'*7} {'-'*8}")

    vendor_stats = {}
    for name, data in athena_games.items():
        v = data["vendor_id"] or "DESCONHECIDO"
        if v not in vendor_stats:
            vendor_stats[v] = {"total": 0, "with_img": 0, "without_img": 0}
        vendor_stats[v]["total"] += 1
        if name in csv_games or name in mapping_with_img:
            vendor_stats[v]["with_img"] += 1
        else:
            vendor_stats[v]["without_img"] += 1

    for v in sorted(vendor_stats, key=lambda x: vendor_stats[x]["total"], reverse=True):
        s = vendor_stats[v]
        cob = (s["with_img"] / s["total"] * 100) if s["total"] > 0 else 0
        out(f"  {v:30s} {s['total']:6d} {s['with_img']:7d} {s['without_img']:7d} {cob:7.1f}%")
    out()

    # ── Query grandes_ganhos - ultimos 30 dias ───────────────────────────────
    out(">>> Cruzamento com CASINO_WIN (grandes_ganhos) - ultimos 30 dias")
    out("-" * 70)

    today_brt = datetime.now(timezone(timedelta(hours=-3))).date()
    date_end = (today_brt + timedelta(days=1)).strftime("%Y-%m-%d")
    date_start = (today_brt - timedelta(days=30)).strftime("%Y-%m-%d")

    try:
        query_gg = QUERY_TOP_CASINO_WINS.format(date_start=date_start, date_end=date_end)
        df_gg = query_athena(query_gg, database="fund_ec2")
        out(f"  Jogos distintos com CASINO_WIN nos ultimos 30d: {len(df_gg)}")
        total_wins = df_gg["win_count"].sum() if not df_gg.empty else 0
        out(f"  Total de wins no periodo: {int(total_wins):,}")
        out()

        # Jogos no grandes_ganhos SEM imagem
        gg_no_img = []
        for _, row in df_gg.iterrows():
            name = str(row["game_name_upper"]).strip()
            if name and name != "NONE":
                has_img = name in csv_games or name in mapping_with_img
                if not has_img:
                    gg_no_img.append({
                        "game_name": name,
                        "vendor_id": str(row["vendor_id"]) if row["vendor_id"] else "?",
                        "win_count": int(row["win_count"]),
                        "unique_players": int(row["unique_players"]),
                        "total_win_brl": float(row["total_win_brl"]),
                    })

        out(f"  Jogos com CASINO_WIN SEM imagem: {len(gg_no_img)}")

        # Quanto % dos wins ficam sem imagem
        wins_sem_img = sum(g["win_count"] for g in gg_no_img)
        pct_wins_sem = (wins_sem_img / total_wins * 100) if total_wins > 0 else 0
        out(f"  Wins afetados (sem imagem): {wins_sem_img:,} ({pct_wins_sem:.2f}% do total)")
        out()

        # Top 20 jogos mais frequentes no grandes_ganhos SEM imagem
        gg_no_img_sorted = sorted(gg_no_img, key=lambda x: x["win_count"], reverse=True)

        out("  TOP 20 jogos mais frequentes no grandes_ganhos SEM IMAGEM:")
        out(f"  {'#':>3s} {'JOGO':40s} {'VENDOR':20s} {'WINS':>8s} {'PLAYERS':>8s} {'TOTAL R$':>12s}")
        out(f"  {'---':>3s} {'-'*40} {'-'*20} {'-'*8} {'-'*8} {'-'*12}")

        for i, g in enumerate(gg_no_img_sorted[:20], 1):
            out(f"  {i:3d} {g['game_name'][:40]:40s} {g['vendor_id'][:20]:20s} {g['win_count']:8,d} {g['unique_players']:8,d} {g['total_win_brl']:12,.2f}")
        out()

        # D3: Jogos "CASINO_WIN popular" sem imagem (top 100 jogos mais jogados)
        # Esses sao os que realmente importam pro grandes_ganhos
        top100_names = set()
        for _, row in df_gg.head(100).iterrows():
            top100_names.add(str(row["game_name_upper"]).strip())

        top100_no_img = [g for g in gg_no_img_sorted if g["game_name"] in top100_names]
        out(f"  Jogos no TOP 100 casino mais jogados SEM imagem: {len(top100_no_img)}")
        if top100_no_img:
            out("  Detalhamento:")
            for g in top100_no_img[:30]:
                out(f"    - {g['game_name']} ({g['vendor_id']}) — {g['win_count']:,} wins, {g['unique_players']} players")
        out()

    except Exception as e:
        out(f"  ERRO ao consultar grandes_ganhos no Athena: {e}")
        gg_no_img_sorted = []
        df_gg = None
    out()

    # ── E) Verificacao CDN para top jogos sem imagem ─────────────────────────
    out("=" * 80)
    out("  VERIFICACAO CDN - TOP 30 JOGOS SEM IMAGEM")
    out("=" * 80)
    out()
    out("  Tentando HEAD request em URLs padrao do CDN multi.bet.br...")
    out()

    # Priorizar jogos que aparecem no grandes_ganhos
    games_to_check = []
    already_checked = set()

    # Primeiro: jogos do grandes_ganhos sem imagem (mais importantes)
    if gg_no_img_sorted:
        for g in gg_no_img_sorted[:20]:
            name = g["game_name"]
            if name not in already_checked:
                athena_data = athena_games.get(name, {})
                games_to_check.append({
                    "game_name": name,
                    "vendor_id": g.get("vendor_id") or athena_data.get("vendor_id"),
                    "provider_game_id": athena_data.get("provider_game_id"),
                    "source": "grandes_ganhos",
                })
                already_checked.add(name)

    # Depois: outros jogos do Athena sem imagem (completar ate 30)
    remaining = 30 - len(games_to_check)
    if remaining > 0:
        for name, data in sorted(athena_no_img.items())[:remaining]:
            if name not in already_checked:
                games_to_check.append({
                    "game_name": name,
                    "vendor_id": data.get("vendor_id"),
                    "provider_game_id": data.get("provider_game_id"),
                    "source": "athena_catalog",
                })
                already_checked.add(name)

    found_cdn = []
    not_found_cdn = []

    for g in games_to_check[:30]:
        url = check_cdn_url(g["provider_game_id"], g["vendor_id"])
        if url:
            found_cdn.append({"game_name": g["game_name"], "url": url, "vendor_id": g["vendor_id"]})
            out(f"  [ENCONTRADA] {g['game_name'][:40]:40s} -> {url}")
        else:
            not_found_cdn.append(g)
            out(f"  [NAO ENCONTRADA] {g['game_name'][:40]:40s} (vendor={g['vendor_id']}, game_id={g['provider_game_id']})")

    out()
    out(f"  Resultado CDN check: {len(found_cdn)} encontradas, {len(not_found_cdn)} nao encontradas de {len(games_to_check)} verificadas")
    out()

    # ── F) Jogos no CSV que NAO estao no Athena ──────────────────────────────
    out("=" * 80)
    out("  JOGOS NO CSV QUE NAO ESTAO NO CATALOGO ATHENA")
    out("=" * 80)
    out()

    csv_not_in_athena = set(csv_games.keys()) - set(athena_games.keys())
    out(f"  Total: {len(csv_not_in_athena)} jogos no CSV sem correspondencia no Athena")
    if csv_not_in_athena:
        out("  Primeiros 30:")
        for name in sorted(csv_not_in_athena)[:30]:
            out(f"    - {csv_games[name]['nome']}")
    out()

    # ── G) Jogos no Athena que NAO estao no CSV ─────────────────────────────
    athena_not_in_csv = set(athena_games.keys()) - set(csv_games.keys())
    out(f"  Jogos no Athena sem correspondencia no CSV: {len(athena_not_in_csv)} de {len(athena_games)}")
    out()

    # ── H) Resumo executivo ──────────────────────────────────────────────────
    out("=" * 80)
    out("  RESUMO EXECUTIVO")
    out("=" * 80)
    out()
    out(f"  Jogos ativos no Athena (catalogo):     {len(athena_games):,}")
    out(f"  Jogos no CSV (scraper site):           {len(csv_games):,}")
    out(f"  Registros no mapping (Super Nova DB):  {len(mapping_all):,}")
    out(f"    - Com imagem:                        {len(mapping_with_img):,}")
    out(f"    - Sem imagem:                        {len(mapping_all) - len(mapping_with_img):,}")
    out()

    # Cobertura real: jogos do Athena que tem imagem em QUALQUER fonte
    athena_with_any_img = sum(
        1 for name in athena_games
        if name in csv_games or name in mapping_with_img
    )
    cob_total = (athena_with_any_img / len(athena_games) * 100) if athena_games else 0
    out(f"  COBERTURA TOTAL (Athena com imagem):   {athena_with_any_img:,} / {len(athena_games):,} = {cob_total:.1f}%")
    out(f"  JOGOS SEM IMAGEM:                      {len(athena_no_img):,} ({100 - cob_total:.1f}%)")
    out()

    if gg_no_img_sorted:
        out(f"  Jogos no grandes_ganhos (30d) sem imagem: {len(gg_no_img_sorted)}")
        out(f"  Wins afetados: {wins_sem_img:,} de {int(total_wins):,} ({pct_wins_sem:.2f}%)")
        out()

    if found_cdn:
        out(f"  URLs encontraveis via CDN (HEAD OK):   {len(found_cdn)}")
        out("  ** Esses jogos podem ser corrigidos automaticamente **")
        out()
        out("  Jogos com URL CDN encontrada (prontos para fix):")
        for g in found_cdn:
            out(f"    - {g['game_name']} -> {g['url']}")
        out()

    out("  ACOES RECOMENDADAS:")
    out("  1. Rodar fix_missing_game_images.py com os jogos CDN encontrados")
    out("  2. Re-executar scraper (capturar_jogos_pc.py) para jogos novos")
    out("  3. Para jogos Live Casino (Evolution), verificar se CDN tem imagem alternativa")
    out("  4. Jogos sem imagem no top grandes_ganhos impactam a experiencia do usuario")
    out()
    out("=" * 80)
    out("  FIM DA AUDITORIA")
    out("=" * 80)

    # ── Salvar relatorio ─────────────────────────────────────────────────────
    os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    log.info(f"Relatorio salvo em: {REPORT_PATH}")
    return "\n".join(lines)


if __name__ == "__main__":
    log.info("=== Iniciando Auditoria de Cobertura de Imagens ===")
    result = main()
    log.info("=== Auditoria concluida ===")
