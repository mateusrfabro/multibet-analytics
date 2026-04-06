#!/bin/bash
# =================================================================
# Grandes Ganhos — cron diario (00:30 BRT = 03:30 UTC)
# Atualiza multibet.grandes_ganhos no Super Nova DB
# com top 50 maiores ganhos casino do dia (Athena).
#
# Crontab:
#   30 3 * * * /home/ec2-user/multibet/run_grandes_ganhos.sh
#
# Log: pipelines/logs/grandes_ganhos_YYYY-MM-DD.log
# =================================================================

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/grandes_ganhos_$(date +%Y-%m-%d).log"

echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
echo "=========================================" >> "$LOGFILE"

cd "$SCRIPT_DIR"
source venv/bin/activate

python3 pipelines/grandes_ganhos.py >> "$LOGFILE" 2>&1

EXIT_CODE=$?

echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit code: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"

exit $EXIT_CODE
