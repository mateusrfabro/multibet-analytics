#!/bin/bash
# =================================================================
# Matriz de Risco — cron diario (02:00 BRT = 05:00 UTC)
# Executa 21 tags no Athena, calcula scores, persiste no PostgreSQL.
#
# Crontab:
#   0 5 * * * /home/ec2-user/multibet/run_risk_matrix.sh
#
# Log: pipelines/logs/risk_matrix_YYYY-MM-DD.log
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/risk_matrix_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

python3 pipelines/risk_matrix_pipeline.py >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"

exit $EXIT_CODE
