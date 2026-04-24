#!/bin/bash
# =================================================================
# DEPLOY: Sync Meta Ads Spend na EC2
# Cola TUDO no terminal SSH da EC2.
#
# O QUE FAZ:
#   - Cria/atualiza pipelines/sync_meta_spend.py
#   - Cria/atualiza db/meta_ads.py
#   - Cria run_sync_meta_ads.sh (wrapper cron)
#   - Adiciona cron diario 01:15 BRT (04:15 UTC)
#
# O QUE NAO FAZ:
#   - NAO altera db/athena.py, db/supernova.py (ja existem)
#   - NAO altera outros pipelines ou crons
#
# PRE-REQUISITO:
#   - Credenciais Meta Ads no .env da EC2:
#     META_ADS_ACCESS_TOKEN=EAA...
#     META_ADS_ACCOUNT_IDS=act_1418521646228655,act_846913941192022,...
# =================================================================
set -e

echo "========================================="
echo "DEPLOY SYNC META ADS SPEND"
echo "========================================="

cd /home/ec2-user/multibet

# 1. Verificar pre-requisitos
echo "[1/6] Verificando pre-requisitos..."
ERRORS=0

if [ ! -d "venv" ]; then
    echo "  ERRO: venv/ nao existe"
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

source venv/bin/activate

# Verifica credenciais Meta Ads no .env
for VAR in META_ADS_ACCESS_TOKEN META_ADS_ACCOUNT_IDS; do
    if ! grep -q "$VAR" .env; then
        echo "  ERRO: $VAR nao encontrado no .env"
        ERRORS=1
    fi
done

if [ $ERRORS -eq 1 ]; then
    echo "  ABORTANDO: corrija os erros acima antes de continuar"
    exit 1
fi

echo "  OK: todos os pre-requisitos atendidos"

# 2. Verificar dependencias Python (psycopg2, sshtunnel, dotenv)
echo "[2/6] Verificando dependencias Python..."
python3 -c "import psycopg2, sshtunnel, dotenv" 2>/dev/null || {
    echo "  Instalando dependencias faltantes..."
    pip install psycopg2-binary sshtunnel python-dotenv --quiet
}
echo "  OK: dependencias OK"

# 3. Backup de arquivos existentes
echo "[3/6] Backup de arquivos existentes..."
for FILE in pipelines/sync_meta_spend.py db/meta_ads.py; do
    if [ -f "$FILE" ]; then
        cp "$FILE" "${FILE}.bkp_$(date +%Y%m%d_%H%M%S)"
        echo "  OK: backup de $FILE"
    fi
done

# 4. Atualizar db/meta_ads.py
echo "[4/6] Atualizando db/meta_ads.py..."
echo "  NOTA: copie o arquivo do repo local para a EC2 via scp:"
echo "    scp -i etl-key.pem db/meta_ads.py ec2-user@<IP>:/home/ec2-user/multibet/db/"
echo "  OU cole o conteudo via cat > db/meta_ads.py << 'EOF' ... EOF"
echo ""
echo "  Se ja copiou, prossiga."

# 5. Atualizar pipelines/sync_meta_spend.py
echo "[5/6] Atualizando pipelines/sync_meta_spend.py..."
echo "  NOTA: copie o arquivo do repo local para a EC2 via scp:"
echo "    scp -i etl-key.pem pipelines/sync_meta_spend.py ec2-user@<IP>:/home/ec2-user/multibet/pipelines/"
echo ""
echo "  Se ja copiou, prossiga."

# 6. Criar wrapper cron e adicionar na crontab
echo "[6/6] Criando wrapper cron..."
cat > run_sync_meta_ads.sh << 'SHEOF'
#!/bin/bash
# Sync Meta Ads Spend — cron diario (01:15 BRT = 04:15 UTC)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/sync_meta_ads_$(date +%Y-%m-%d).log"
echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
cd "$SCRIPT_DIR"
source venv/bin/activate
python3 pipelines/sync_meta_spend.py --days 3 >> "$LOGFILE" 2>&1
EXIT_CODE=$?
echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"
exit $EXIT_CODE
SHEOF
chmod +x run_sync_meta_ads.sh
echo "  OK: wrapper criado"

# Adicionar cron (append-only, sem mexer em crons existentes)
CRON_LINE="15 4 * * * /home/ec2-user/multibet/run_sync_meta_ads.sh"
if crontab -l 2>/dev/null | grep -q "sync_meta_ads"; then
    echo "  Cron existente encontrado. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "sync_meta_ads"; echo "$CRON_LINE") | crontab -
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
echo "  python3 pipelines/sync_meta_spend.py --days 3"
echo ""
echo "Verificar logs:"
echo "  tail -f pipelines/logs/sync_meta_ads_$(date +%Y-%m-%d).log"
echo "========================================="
