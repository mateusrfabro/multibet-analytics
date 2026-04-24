#!/bin/bash
# =================================================================
# DEPLOY: Grandes Ganhos v2 (Athena) na EC2
# Cola TUDO no terminal SSH da EC2.
#
# O QUE FAZ:
#   - Substitui pipelines/grandes_ganhos.py (BigQuery -> Athena)
#   - Substitui run_grandes_ganhos.sh (adiciona venv activation)
#   - Reativa o cron diario
#
# O QUE NAO FAZ:
#   - NAO altera db/ (athena.py, supernova.py ja existem)
#   - NAO altera outros pipelines
#   - NAO altera outras entradas do cron
#   - NAO instala pacotes (pyathena ja esta instalado)
# =================================================================
set -e

echo "========================================="
echo "DEPLOY GRANDES GANHOS v2 (Athena)"
echo "========================================="

cd /home/ec2-user/multibet

# 1. Verificar pre-requisitos
echo "[1/5] Verificando pre-requisitos..."
ERRORS=0

if [ ! -d "venv" ]; then
    echo "  ERRO: venv/ nao existe"
    ERRORS=1
fi
if [ ! -f "db/athena.py" ]; then
    echo "  ERRO: db/athena.py nao existe (deploy ETL aquisicao primeiro)"
    ERRORS=1
fi
if [ ! -f "db/supernova.py" ]; then
    echo "  ERRO: db/supernova.py nao existe"
    ERRORS=1
fi
if [ ! -f ".env" ]; then
    echo "  ERRO: .env nao existe"
    ERRORS=1
fi

# Verifica se pyathena esta instalado
source venv/bin/activate
if ! python3 -c "import pyathena" 2>/dev/null; then
    echo "  ERRO: pyathena nao instalado no venv"
    ERRORS=1
fi

# Verifica se variaveis Athena estao no .env
if ! grep -q "ATHENA_AWS_ACCESS_KEY_ID" .env; then
    echo "  ERRO: ATHENA_AWS_ACCESS_KEY_ID nao encontrado no .env"
    ERRORS=1
fi

if [ $ERRORS -eq 1 ]; then
    echo "  ABORTANDO: corrija os erros acima antes de continuar"
    exit 1
fi

echo "  OK: todos os pre-requisitos atendidos"

# 2. Backup do arquivo antigo
echo "[2/5] Backup do pipeline antigo..."
if [ -f "pipelines/grandes_ganhos.py" ]; then
    cp pipelines/grandes_ganhos.py "pipelines/grandes_ganhos.py.bkp_$(date +%Y%m%d_%H%M%S)"
    echo "  OK: backup criado"
else
    echo "  SKIP: arquivo antigo nao existe"
fi

# 3. Criar novo pipeline (Athena)
echo "[3/5] Atualizando pipelines/grandes_ganhos.py..."
cat > pipelines/grandes_ganhos.py << 'PYEOF'
"""
Pipeline: Grandes Ganhos do Dia (v2 — Athena)
===============================================
Origem 1: Athena (fund_ec2)       — ganhos casino (c_txn_type=45 CASINO_WIN)
Origem 2: Athena (bireports_ec2)  — nomes de jogos e providers
Origem 3: Athena (ecr_ec2)        — nomes de jogadores (hash LGPD)
Origem 4: Super Nova DB           — mapeamento de imagens (multibet.game_image_mapping)
Destino : Super Nova DB (PostgreSQL) — tabela multibet.grandes_ganhos

Execucao:
    python3 pipelines/grandes_ganhos.py

Frequencia: 1x/dia as 00:30 BRT via cron na EC2.
Pre-requisito: game_image_mapper.py deve ter rodado ao menos 1x para popular mapeamento.

Historico:
    - v1: BigQuery + Redshift (ate 06/04/2026)
    - v2: Migrado para Athena (fund_ec2 + bireports_ec2 + ecr_ec2) — 06/04/2026
"""

import sys
import os
import re
import unicodedata
import logging
from datetime import datetime, timezone, timedelta
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection

import pandas as pd
import psycopg2.extras

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

DDL_SCHEMA = "CREATE SCHEMA IF NOT EXISTS multibet;"

DDL_TABLE = """
CREATE TABLE IF NOT EXISTS multibet.grandes_ganhos (
    id                  SERIAL PRIMARY KEY,
    game_name           VARCHAR(255),
    provider_name       VARCHAR(100),
    game_slug           VARCHAR(200),
    game_image_url      VARCHAR(500),
    player_name_hashed  VARCHAR(50),
    ecr_id              BIGINT,
    win_amount          NUMERIC(15, 2),
    event_time          TIMESTAMPTZ,
    refreshed_at        TIMESTAMPTZ
);
"""

DDL_INDEX = """
CREATE INDEX IF NOT EXISTS idx_gg_event_time
    ON multibet.grandes_ganhos (event_time DESC);
"""

QUERY_ATHENA_TEMPLATE = """
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
),
wins AS (
    SELECT
        f.c_ecr_id,
        f.c_game_id AS game_id_raw,
        CASE
            WHEN STRPOS(f.c_game_id, '_') > 0
            THEN SPLIT_PART(f.c_game_id, '_', 1)
            ELSE f.c_game_id
        END AS game_id_base,
        f.c_sub_vendor_id AS fund_vendor,
        f.c_amount_in_ecr_ccy / 100.0 AS win_amount,
        f.c_start_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS event_time_brt,
        f.c_start_time AS event_time_utc
    FROM fund_ec2.tbl_real_fund_txn f
    WHERE f.c_txn_type = 45
      AND f.c_txn_status = 'SUCCESS'
      AND f.c_product_id = 'CASINO'
      AND f.c_start_time >= TIMESTAMP '{date_start}'
      AND f.c_start_time <  TIMESTAMP '{date_end}'
      AND f.c_amount_in_ecr_ccy > 0
)
SELECT
    COALESCE(g.c_game_desc, g2.c_game_desc)                 AS game_name,
    COALESCE(g.c_vendor_id, g2.c_vendor_id, w.fund_vendor)  AS provider_name,
    CONCAT(SUBSTR(p.c_fname, 1, 2), '***')                  AS player_name_hashed,
    w.c_ecr_id                                               AS ecr_id,
    w.win_amount,
    w.event_time_brt,
    w.event_time_utc
FROM wins w
LEFT JOIN game_catalog g
    ON w.game_id_raw = g.c_game_id
LEFT JOIN game_catalog g2
    ON w.game_id_base = g2.c_game_id
    AND g.c_game_id IS NULL
LEFT JOIN ecr_ec2.tbl_ecr_profile p
    ON w.c_ecr_id = p.c_ecr_id
WHERE COALESCE(g.c_game_desc, g2.c_game_desc) IS NOT NULL
  AND p.c_fname IS NOT NULL
ORDER BY w.win_amount DESC
LIMIT 50
"""

QUERY_MAPPING = """
SELECT game_name_upper, game_image_url, game_slug
FROM multibet.game_image_mapping
WHERE game_image_url IS NOT NULL
"""


def slugify(name):
    name = unicodedata.normalize("NFKD", name)
    name = name.encode("ascii", "ignore").decode("ascii")
    name = name.lower().strip()
    name = re.sub(r"[^\w\s-]", "", name)
    name = re.sub(r"[\s-]+", "_", name)
    name = re.sub(r"_+", "_", name)
    return name


def build_game_url(game_name):
    if not game_name:
        return None
    return f"/pb/gameplay/{slugify(game_name)}/real-game"


# --- CDN auto-discovery (validador pos-refresh) --------------------------
CDN_BASE_URL = "https://multi.bet.br/uploads/games/MUL"

# Vendor -> prefixo CDN (validados empiricamente 14/04/2026)
VENDOR_CDN_PREFIX = {
    "pragmaticplay": "pp",
    "pragmaticexternal": "pp",
    "evolution": "alea_evo",
    "tadagaming": "alea_tad",
}


def check_url_exists(url, timeout=5):
    """HEAD request para verificar se URL do CDN existe (HTTP 200)."""
    try:
        req = urllib.request.Request(url, method="HEAD")
        req.add_header("User-Agent", "Mozilla/5.0")
        resp = urllib.request.urlopen(req, timeout=timeout)
        return resp.status == 200
    except Exception:
        return False


def try_resolve_cdn_url(game_id, vendor_id):
    """Tenta construir URL CDN baseado em vendor + game_id e verifica com HEAD.

    Padrao CDN: https://multi.bet.br/uploads/games/MUL/{key}/{key}.webp
    Retorna a URL se encontrada, None caso contrario.
    """
    if not game_id or not vendor_id:
        return None

    vendor_lower = str(vendor_id).lower().strip()
    game_id_str = str(game_id).strip()
    candidates = []

    # 1. Prefixo conhecido do vendor
    prefix = VENDOR_CDN_PREFIX.get(vendor_lower)
    if prefix:
        key = f"{prefix}{game_id_str}"
        candidates.append(f"{CDN_BASE_URL}/{key}/{key}.webp")

    # 2. Padrao generico alea_{vendor_3chars}{game_id}
    vendor_short = vendor_lower[:3]
    key_generic = f"alea_{vendor_short}{game_id_str}"
    if not any(key_generic in c for c in candidates):
        candidates.append(f"{CDN_BASE_URL}/{key_generic}/{key_generic}.webp")

    for url in candidates:
        if check_url_exists(url):
            return url

    return None


def setup_table():
    log.info("Verificando/criando tabela multibet.grandes_ganhos...")
    execute_supernova(DDL_SCHEMA)
    execute_supernova(DDL_TABLE)
    execute_supernova(DDL_INDEX)
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS game_slug VARCHAR(200);")
    execute_supernova("ALTER TABLE multibet.grandes_ganhos DROP COLUMN IF EXISTS game_url;")
    execute_supernova("ALTER TABLE multibet.grandes_ganhos ADD COLUMN IF NOT EXISTS ecr_id BIGINT;")
    log.info("Tabela pronta.")


def refresh():
    today_brt = datetime.now(timezone(timedelta(hours=-3))).date()
    date_start = today_brt.strftime("%Y-%m-%d")
    date_end = (today_brt + timedelta(days=1)).strftime("%Y-%m-%d")

    query = QUERY_ATHENA_TEMPLATE.format(date_start=date_start, date_end=date_end)
    log.info(f"Buscando maiores ganhos no Athena (fund_ec2) para {date_start}...")
    df = query_athena(query, database="fund_ec2")
    log.info(f"{len(df)} registros obtidos do Athena.")

    if df.empty:
        log.warning("Nenhum ganho encontrado hoje. Abortando refresh.")
        return

    log.info("Buscando mapeamento de imagens no Super Nova DB...")
    try:
        rows = execute_supernova(QUERY_MAPPING, fetch=True) or []
        df_mapping = pd.DataFrame(rows, columns=["game_name_upper", "game_image_url", "game_slug"])
        log.info(f"{len(df_mapping)} jogos com imagem no mapeamento.")
    except Exception as e:
        log.warning(f"Falha ao buscar mapeamento (continuando sem imagens): {e}")
        df_mapping = pd.DataFrame(columns=["game_name_upper", "game_image_url", "game_slug"])

    df["game_name_upper"] = df["game_name"].str.upper().str.strip()
    df_mapping_dedup = df_mapping.drop_duplicates(subset="game_name_upper", keep="first")

    df = df.merge(
        df_mapping_dedup[["game_name_upper", "game_image_url", "game_slug"]],
        on="game_name_upper",
        how="left",
    )

    mask_no_img = df["game_image_url"].isna()
    if mask_no_img.any():
        mapping_lookup = df_mapping_dedup.set_index("game_name_upper")
        for idx in df[mask_no_img].index:
            name = df.at[idx, "game_name_upper"]
            base_name = re.sub(r"\s+\d+$", "", name).strip()
            if base_name != name and base_name in mapping_lookup.index:
                row = mapping_lookup.loc[base_name]
                df.at[idx, "game_image_url"] = row["game_image_url"]
                df.at[idx, "game_slug"] = row["game_slug"]
                log.info(f"  Fallback variante: '{name}' -> imagem de '{base_name}'")

    mask_no_slug = df["game_slug"].isna()
    if mask_no_slug.any():
        df.loc[mask_no_slug, "game_slug"] = df.loc[mask_no_slug, "game_name"].apply(build_game_url)

    with_img = df["game_image_url"].notna().sum()
    with_slug = df["game_slug"].notna().sum()
    log.info(f"Join concluido: {with_img}/{len(df)} com image_url | {with_slug}/{len(df)} com game_slug.")

    now_utc = datetime.now(timezone.utc)

    insert_sql = """
        INSERT INTO multibet.grandes_ganhos
            (game_name, provider_name, game_slug, game_image_url,
             player_name_hashed, ecr_id, win_amount, event_time, refreshed_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    records = [
        (
            row["game_name"],
            row["provider_name"],
            row.get("game_slug"),
            row.get("game_image_url") if not isinstance(row.get("game_image_url"), float) else None,
            row["player_name_hashed"],
            int(row["ecr_id"]),
            float(row["win_amount"]),
            row["event_time_utc"],
            now_utc,
        )
        for _, row in df.iterrows()
    ]

    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE multibet.grandes_ganhos RESTART IDENTITY;")
            psycopg2.extras.execute_batch(cur, insert_sql, records)
        conn.commit()
    finally:
        conn.close()
        tunnel.stop()

    log.info(f"{len(records)} registros inseridos em multibet.grandes_ganhos.")


def validate_and_fix_images():
    """Validador pos-refresh: verifica se ha jogos sem game_image_url na grandes_ganhos.

    Se houver, consulta o catalogo Athena para obter game_id/vendor, tenta descobrir
    a URL CDN automaticamente (HEAD request), e atualiza game_image_mapping + grandes_ganhos.

    So roda quando ha jogos com game_image_url NULL — se tudo estiver OK, pula.
    """
    log.info("--- Validando game_image_url nos registros inseridos ---")

    # 1. Busca jogos sem imagem na grandes_ganhos
    rows_missing = execute_supernova(
        """
        SELECT DISTINCT game_name
        FROM multibet.grandes_ganhos
        WHERE game_image_url IS NULL
           OR TRIM(game_image_url) = ''
        """,
        fetch=True,
    ) or []

    if not rows_missing:
        log.info("Validacao OK: todos os jogos possuem game_image_url.")
        return

    # Mapeia UPPER -> nome original (preserva casing)
    missing_map = {r[0].upper().strip(): r[0] for r in rows_missing}
    missing_names = list(missing_map.keys())
    log.warning(f"{len(missing_names)} jogos sem game_image_url: {list(missing_map.values())}")

    # 2. Busca game_id + vendor no catalogo Athena (bireports)
    names_escaped = [n.replace("'", "''") for n in missing_names]
    names_sql = ", ".join([f"'{n}'" for n in names_escaped])

    query_catalog = f"""
    SELECT
        UPPER(TRIM(c_game_desc))  AS game_name_upper,
        c_game_id,
        c_vendor_id
    FROM (
        SELECT
            c_game_desc, c_game_id, c_vendor_id,
            ROW_NUMBER() OVER (
                PARTITION BY UPPER(TRIM(c_game_desc))
                ORDER BY CASE WHEN c_client_platform = 'WEB' THEN 0 ELSE 1 END
            ) AS rn
        FROM bireports_ec2.tbl_vendor_games_mapping_data
        WHERE c_status = 'active'
          AND UPPER(TRIM(c_game_desc)) IN ({names_sql})
    )
    WHERE rn = 1
    """

    try:
        df_catalog = query_athena(query_catalog, database="bireports_ec2")
        log.info(f"Catalogo Athena: {len(df_catalog)} jogos encontrados para resolucao.")
    except Exception as e:
        log.error(f"Falha ao buscar catalogo Athena: {e}")
        return

    if df_catalog.empty:
        log.warning("Nenhum jogo faltante no catalogo Athena. Correcao manual necessaria (fix_missing_game_images.py).")
        return

    # 3. Tenta descobrir URL CDN para cada jogo (HEAD request)
    fixed = []
    for _, row in df_catalog.iterrows():
        game_name_upper = row["game_name_upper"]
        game_id = row["c_game_id"]
        vendor_id = row["c_vendor_id"]

        url = try_resolve_cdn_url(game_id, vendor_id)
        if url:
            log.info(f"  CDN encontrado: {game_name_upper} -> {url}")
            original_name = missing_map.get(game_name_upper, game_name_upper.title())
            fixed.append({
                "game_name": original_name,
                "game_name_upper": game_name_upper,
                "game_id": str(game_id),
                "vendor_id": vendor_id,
                "url": url,
            })
        else:
            log.warning(f"  CDN NAO encontrado: {game_name_upper} (vendor={vendor_id}, game_id={game_id})")

    if not fixed:
        log.warning("Nenhuma URL CDN descoberta automaticamente. Correcao manual necessaria (fix_missing_game_images.py).")
        return

    # 4. Atualiza game_image_mapping + grandes_ganhos
    now_utc = datetime.now(timezone.utc)
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for f in fixed:
                slug = build_game_url(f["game_name"])

                # Upsert no game_image_mapping (persiste para proximas execucoes)
                cur.execute("""
                    INSERT INTO multibet.game_image_mapping
                        (game_name, game_name_upper, provider_game_id, vendor_id,
                         game_image_url, game_slug, source, updated_at)
                    VALUES (%s, %s, %s, %s, %s, %s, 'auto_fix', %s)
                    ON CONFLICT (game_name_upper) DO UPDATE SET
                        game_image_url = EXCLUDED.game_image_url,
                        game_slug      = EXCLUDED.game_slug,
                        source         = EXCLUDED.source,
                        updated_at     = EXCLUDED.updated_at
                """, (f["game_name"], f["game_name_upper"], f["game_id"],
                      f["vendor_id"], f["url"], slug, now_utc))

                # Update direto na grandes_ganhos (corrige o dado que o front le)
                cur.execute("""
                    UPDATE multibet.grandes_ganhos
                    SET game_image_url = %s,
                        game_slug     = COALESCE(game_slug, %s)
                    WHERE UPPER(TRIM(game_name)) = %s
                      AND (game_image_url IS NULL OR TRIM(game_image_url) = '')
                """, (f["url"], slug, f["game_name_upper"]))

        conn.commit()
        log.info(f"Auto-fix concluido: {len(fixed)} jogos corrigidos em game_image_mapping + grandes_ganhos.")
    finally:
        conn.close()
        tunnel.stop()


if __name__ == "__main__":
    log.info("=== Iniciando pipeline Grandes Ganhos (v2 Athena) ===")
    setup_table()
    refresh()
    validate_and_fix_images()
    log.info("=== Pipeline concluido ===")
PYEOF
echo "  OK: pipeline atualizado"

# 4. Atualizar wrapper do cron (corrige falta de venv activation)
echo "[4/5] Atualizando run_grandes_ganhos.sh..."
cat > run_grandes_ganhos.sh << 'SHEOF'
#!/bin/bash
# Grandes Ganhos v2 — cron diario (00:30 BRT = 03:30 UTC)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/grandes_ganhos_$(date +%Y-%m-%d).log"
echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
cd "$SCRIPT_DIR"
source venv/bin/activate
python3 pipelines/grandes_ganhos.py >> "$LOGFILE" 2>&1
EXIT_CODE=$?
echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"
exit $EXIT_CODE
SHEOF
chmod +x run_grandes_ganhos.sh
echo "  OK: wrapper atualizado (com venv activation)"

# 5. Reativar cron (ADICIONA sem alterar existentes)
echo "[5/5] Reativando cron diario..."
CRON_LINE="30 3 * * * /home/ec2-user/multibet/run_grandes_ganhos.sh"
if crontab -l 2>/dev/null | grep -q "run_grandes_ganhos"; then
    echo "  Cron existente encontrado. Substituindo..."
    # Remove a entrada antiga e adiciona a nova
    (crontab -l 2>/dev/null | grep -v "grandes_ganhos"; echo "$CRON_LINE") | crontab -
    echo "  OK: cron atualizado"
else
    (crontab -l 2>/dev/null; echo "$CRON_LINE") | crontab -
    echo "  OK: cron adicionado"
fi

echo ""
echo "========================================="
echo "DEPLOY COMPLETO!"
echo "========================================="
echo ""
echo "Crontab atual:"
crontab -l
echo ""
echo "Proximo passo: testar manualmente"
echo "  cd /home/ec2-user/multibet"
echo "  source venv/bin/activate"
echo "  python3 pipelines/grandes_ganhos.py"
echo ""
echo "Verificar logs:"
echo "  tail -f pipelines/logs/grandes_ganhos_$(date +%Y-%m-%d).log"
echo "========================================="
