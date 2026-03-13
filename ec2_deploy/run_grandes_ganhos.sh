#!/bin/bash
# Wrapper do pipeline Grandes Ganhos — chamado pelo cron
# Log salvo em pipelines/logs/grandes_ganhos_YYYY-MM-DD.log

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"

mkdir -p "$LOG_DIR"

LOGFILE="$LOG_DIR/grandes_ganhos_$(date +%Y-%m-%d).log"

cd "$SCRIPT_DIR"
python3 pipelines/grandes_ganhos.py >> "$LOGFILE" 2>&1
