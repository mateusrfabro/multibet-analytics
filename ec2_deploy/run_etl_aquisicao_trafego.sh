#!/bin/bash
# =================================================================
# ETL Aquisicao Trafego — cron horario (a cada 60 min)
# Atualiza multibet.aquisicao_trafego_diario no Super Nova DB
# com dados de D-1 (fechado) + hoje (parcial).
#
# Crontab:
#   0 * * * * /home/ec2-user/multibet/run_etl_aquisicao_trafego.sh
#
# Log: pipelines/logs/etl_aquisicao_trafego_YYYY-MM-DD.log
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/etl_aquisicao_trafego_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S BRT')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

# --days 2: reprocessa D-1 (consolidado) + D-2 (seguranca)
# Hoje (parcial) e incluido por default
python3 pipelines/etl_aquisicao_trafego_diario.py --days 2 >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S BRT') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"

exit $EXIT_CODE
