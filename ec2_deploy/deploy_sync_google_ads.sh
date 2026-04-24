#!/bin/bash
# =================================================================
# DEPLOY: Sync Google Ads Spend na EC2
# Cola TUDO no terminal SSH da EC2.
#
# O QUE FAZ:
#   - Cria/atualiza pipelines/sync_google_ads_spend.py
#   - Cria/atualiza db/google_ads.py
#   - Cria run_sync_google_ads.sh (wrapper cron)
#   - Adiciona cron diario 03:00 BRT (06:00 UTC)
#   - Instala google-ads no venv se necessario
#
# O QUE NAO FAZ:
#   - NAO altera db/athena.py, db/supernova.py (ja existem)
#   - NAO altera outros pipelines ou crons
#
# PRE-REQUISITO:
#   - Credenciais Google Ads no .env da EC2:
#     GOOGLE_ADS_DEVELOPER_TOKEN=...
#     GOOGLE_ADS_CLIENT_ID=...
#     GOOGLE_ADS_CLIENT_SECRET=...
#     GOOGLE_ADS_REFRESH_TOKEN=...
#     GOOGLE_ADS_CUSTOMER_ID=4985069191
#     GOOGLE_ADS_LOGIN_CUSTOMER_ID=1004058739
# =================================================================
set -e

echo "========================================="
echo "DEPLOY SYNC GOOGLE ADS SPEND"
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

# Verifica credenciais Google Ads no .env
for VAR in GOOGLE_ADS_DEVELOPER_TOKEN GOOGLE_ADS_CLIENT_ID GOOGLE_ADS_CLIENT_SECRET GOOGLE_ADS_REFRESH_TOKEN; do
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

# 2. Instalar google-ads no venv se necessario
echo "[2/6] Verificando biblioteca google-ads..."
if python3 -c "from google.ads.googleads.client import GoogleAdsClient" 2>/dev/null; then
    echo "  OK: google-ads ja instalado"
else
    echo "  Instalando google-ads..."
    pip install google-ads --quiet
    echo "  OK: google-ads instalado"
fi

# 3. Backup de arquivos existentes
echo "[3/6] Backup de arquivos existentes..."
for FILE in pipelines/sync_google_ads_spend.py db/google_ads.py; do
    if [ -f "$FILE" ]; then
        cp "$FILE" "${FILE}.bkp_$(date +%Y%m%d_%H%M%S)"
        echo "  OK: backup de $FILE"
    fi
done

# 4. Atualizar db/google_ads.py
echo "[4/6] Atualizando db/google_ads.py..."
echo "  NOTA: copie o arquivo do repo local para a EC2 via scp:"
echo "    scp -i etl-key.pem db/google_ads.py ec2-user@<IP>:/home/ec2-user/multibet/db/"
echo "  OU cole o conteudo via cat > db/google_ads.py << 'EOF' ... EOF"
echo ""
echo "  Se ja copiou, prossiga."

# 5. Atualizar pipelines/sync_google_ads_spend.py
echo "[5/6] Atualizando pipelines/sync_google_ads_spend.py..."
echo "  NOTA: copie o arquivo do repo local para a EC2 via scp:"
echo "    scp -i etl-key.pem pipelines/sync_google_ads_spend.py ec2-user@<IP>:/home/ec2-user/multibet/pipelines/"
echo ""
echo "  Se ja copiou, prossiga."

# 6. Criar wrapper cron e adicionar na crontab
echo "[6/6] Criando wrapper cron..."
cat > run_sync_google_ads.sh << 'SHEOF'
#!/bin/bash
# Sync Google Ads Spend — cron diario (03:00 BRT = 06:00 UTC)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/sync_google_ads_$(date +%Y-%m-%d).log"
echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
cd "$SCRIPT_DIR"
source venv/bin/activate
python3 pipelines/sync_google_ads_spend.py --days 3 >> "$LOGFILE" 2>&1
EXIT_CODE=$?
echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"
exit $EXIT_CODE
SHEOF
chmod +x run_sync_google_ads.sh
echo "  OK: wrapper criado"

# Adicionar cron (append-only, sem mexer em crons existentes)
CRON_LINE="0 6 * * * /home/ec2-user/multibet/run_sync_google_ads.sh"
if crontab -l 2>/dev/null | grep -q "sync_google_ads"; then
    echo "  Cron existente encontrado. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "sync_google_ads"; echo "$CRON_LINE") | crontab -
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
echo "  python3 pipelines/sync_google_ads_spend.py --days 3"
echo ""
echo "Verificar logs:"
echo "  tail -f pipelines/logs/sync_google_ads_$(date +%Y-%m-%d).log"
echo "========================================="
