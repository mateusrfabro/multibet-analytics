#!/bin/bash
# =================================================================
# DEPLOY: Game Enrichment v4 (catalogo enriquecido)
# Data: 2026-04-22
#
# O QUE FAZ (escopo MINIMO — somente 2 pipelines + 1 DDL):
#   1. Backup dos 2 arquivos vigentes (.bak_YYYYMMDD_HHMMSS)
#   2. Substitui pipelines/game_image_mapper.py (v4 — provider_display_name + categorize_front)
#   3. Substitui pipelines/grandes_ganhos.py (v4 — JOIN traz 3 cols enriquecidas)
#   4. Aplica DDL v4 (multibet.game_image_mapping + recreate vw_front_api_games)
#   5. Smoke test: roda ambos, valida cobertura
#   6. Ajusta crontab APENAS da linha do grandes_ganhos (1x/dia -> 4h)
#      (NAO TOCA em outras linhas do cron — append-only safe)
#
# O QUE NAO FAZ (REGRA do squad — feedback_ec2_deploy_nao_mexer_existente):
#   - NAO mexe em outros pipelines (risk_matrix, ETL aquisicao, sports_odds, etc)
#   - NAO mexe em cron de outras aplicacoes
#   - NAO altera db/, .env, venv/, requirements
#   - NAO instala pacotes (todos ja existem)
#   - NAO renomeia/dropa coluna existente (so ALTER ADD COLUMN IF NOT EXISTS)
#
# ROLLBACK (se algo quebrar):
#   cp pipelines/game_image_mapper.py.bak_YYYYMMDD_HHMMSS pipelines/game_image_mapper.py
#   cp pipelines/grandes_ganhos.py.bak_YYYYMMDD_HHMMSS  pipelines/grandes_ganhos.py
#   psql ... -c "ALTER TABLE multibet.game_image_mapping DROP COLUMN provider_display_name, DROP COLUMN game_category_front;"
#   crontab -l > /tmp/cron.bak && (edit manual)
# =================================================================
set -e

PROJETO_DIR="/home/ec2-user/multibet"
TS=$(date +%Y%m%d_%H%M%S)

echo "========================================="
echo "DEPLOY GAME ENRICH v4 — $(date)"
echo "========================================="

cd "$PROJETO_DIR"

# 1. Pre-requisitos (so checa, nao instala/altera)
echo "[1/6] Verificando pre-requisitos..."
ERRORS=0
[ ! -d "venv" ]                            && { echo "  ERRO: venv/ nao existe"; ERRORS=1; }
[ ! -f "db/athena.py" ]                    && { echo "  ERRO: db/athena.py nao existe"; ERRORS=1; }
[ ! -f "db/supernova.py" ]                 && { echo "  ERRO: db/supernova.py nao existe"; ERRORS=1; }
[ ! -f ".env" ]                            && { echo "  ERRO: .env nao existe"; ERRORS=1; }
[ ! -f "pipelines/game_image_mapper.py" ]  && { echo "  ERRO: game_image_mapper.py nao existe — primeiro deploy?"; ERRORS=1; }
[ ! -f "pipelines/grandes_ganhos.py" ]     && { echo "  ERRO: grandes_ganhos.py nao existe"; ERRORS=1; }
source venv/bin/activate
python3 -c "import pyathena, psycopg2" 2>/dev/null || { echo "  ERRO: pyathena/psycopg2 nao instalados"; ERRORS=1; }
[ $ERRORS -eq 1 ] && { echo "  ABORTANDO."; exit 1; }
echo "  OK"

# 2. Git pull (busca v4 do origin/main — commit 714cc75)
echo "[2/6] git pull origin main..."
git fetch origin main 2>&1
git log --oneline HEAD..origin/main | head -5
echo "  Aplicando..."
git pull origin main

# 3. Backup dos 2 arquivos vigentes (preserva versao em prod)
echo "[3/6] Backup vigente -> .bak_$TS..."
cp pipelines/game_image_mapper.py "pipelines/game_image_mapper.py.bak_$TS"
cp pipelines/grandes_ganhos.py    "pipelines/grandes_ganhos.py.bak_$TS"
echo "  OK: 2 backups criados"

# 4. Aplica DDL v4 (ALTER ADD COLUMN IF NOT EXISTS + CREATE OR REPLACE VIEW)
echo "[4/6] Aplicando DDL v4 no Postgres..."
python3 <<'PYEOF'
import sys
sys.path.insert(0, '.')
from db.supernova import execute_supernova
ddl = open('pipelines/ddl/ddl_game_image_mapping_v4.sql').read()
execute_supernova(ddl)
print("  OK: DDL v4 aplicada")
ddl_gg = open('pipelines/ddl/ddl_grandes_ganhos.sql').read()
execute_supernova(ddl_gg)
print("  OK: ALTERs grandes_ganhos aplicados")
PYEOF

# 5. Smoke test (roda 1x e valida cobertura)
echo "[5/6] Smoke test..."
echo "  -- game_image_mapper..."
python3 pipelines/game_image_mapper.py 2>&1 | tail -5
echo "  -- grandes_ganhos..."
python3 pipelines/grandes_ganhos.py 2>&1 | tail -5
echo "  -- Validacao cobertura..."
python3 <<'PYEOF'
import sys; sys.path.insert(0, '.')
from db.supernova import execute_supernova
gg = execute_supernova("""
    SELECT COUNT(*) total, COUNT(provider_display_name) pdn,
           COUNT(game_category) gc, COUNT(game_category_front) gcf
    FROM multibet.grandes_ganhos
""", fetch=True)
print(f"  grandes_ganhos cobertura: {gg}")
gim = execute_supernova("""
    SELECT COUNT(*) total, COUNT(provider_display_name) pdn,
           COUNT(game_category_front) gcf
    FROM multibet.game_image_mapping
""", fetch=True)
print(f"  game_image_mapping cobertura: {gim}")
PYEOF

# 6. Crontab — ajuste APENAS da linha do grandes_ganhos (idempotente, nao mexe em outras)
echo "[6/6] Ajustando crontab — APENAS linha grandes_ganhos..."
CRONTAB_TMP="/tmp/crontab_$TS.bak"
crontab -l > "$CRONTAB_TMP" 2>/dev/null || echo "" > "$CRONTAB_TMP"
echo "  Backup do crontab atual: $CRONTAB_TMP"

# Conta quantas linhas referenciam grandes_ganhos (sanity check)
N_GG=$(grep -c "grandes_ganhos" "$CRONTAB_TMP" || true)
echo "  Linhas com 'grandes_ganhos' no crontab: $N_GG"

if [ "$N_GG" -eq 0 ]; then
    echo "  AVISO: nenhuma linha de grandes_ganhos no crontab — NADA SERA ALTERADO."
    echo "         Adicione manualmente quando quiser ativar:"
    echo "         30 0,4,8,12,16,20 * * * cd $PROJETO_DIR && ./run_grandes_ganhos.sh >> /var/log/grandes_ganhos.log 2>&1"
elif [ "$N_GG" -eq 1 ]; then
    # Substitui APENAS a linha do grandes_ganhos, mantendo todas as outras intactas
    sed -i.bak_$TS \
        "s|^\([^#]*\)[0-9*,/-]\{1,\} [0-9*,/-]\{1,\} \([^|]*grandes_ganhos[^|]*\)\$|30 0,4,8,12,16,20 * * * \2|" \
        "$CRONTAB_TMP"
    echo "  Diff aplicado:"
    diff /var/spool/cron/$(whoami) "$CRONTAB_TMP" 2>/dev/null || diff <(crontab -l) "$CRONTAB_TMP" || true
    echo ""
    read -p "  Aplicar este novo crontab? (s/N) " ans
    if [ "$ans" = "s" ] || [ "$ans" = "S" ]; then
        crontab "$CRONTAB_TMP"
        echo "  OK: crontab atualizado (grandes_ganhos -> 4h)"
    else
        echo "  CANCELADO: crontab nao foi alterado. Ajuste manual com: crontab -e"
    fi
else
    echo "  AVISO: $N_GG linhas referenciam grandes_ganhos — NAO vou tentar adivinhar qual ajustar."
    echo "         Edite manualmente: crontab -e"
    echo "         Linha sugerida: 30 0,4,8,12,16,20 * * * cd $PROJETO_DIR && ./run_grandes_ganhos.sh >> /var/log/grandes_ganhos.log 2>&1"
fi

echo ""
echo "========================================="
echo "DEPLOY v4 CONCLUIDO"
echo "========================================="
echo "Backups: pipelines/*.py.bak_$TS  +  $CRONTAB_TMP"
echo "Proxima execucao do cron: rode 'crontab -l | grep grandes_ganhos' pra confirmar"
