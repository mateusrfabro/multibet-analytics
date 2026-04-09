#!/bin/bash
# =================================================================
# DEPLOY: Matriz de Risco na EC2 ETL
#
# PRE-REQUISITO: Copiar arquivos para a EC2 ANTES de rodar este script.
#   Veja as instrucoes de SCP no DEPLOY.md (secao "Risk Matrix").
#
# O QUE FAZ:
#   - Verifica pre-requisitos (venv, db/, .env, pyathena, SQLs)
#   - Da permissao ao run_risk_matrix.sh
#   - Testa o pipeline com --dry-run (apenas CSV, sem PostgreSQL)
#   - Adiciona cron diario (05:00 UTC = 02:00 BRT)
#
# O QUE NAO FAZ:
#   - NAO altera db/ (athena.py, supernova.py ja existem)
#   - NAO altera outros pipelines
#   - NAO altera outras entradas do cron
#   - NAO instala pacotes (pyathena/psycopg2 ja devem estar no venv)
# =================================================================
set -e

echo "========================================="
echo "DEPLOY MATRIZ DE RISCO (EC2 ETL)"
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
    echo "  ERRO: db/athena.py nao existe"
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
if [ ! -f "pipelines/risk_matrix_pipeline.py" ]; then
    echo "  ERRO: pipelines/risk_matrix_pipeline.py nao existe (SCP primeiro)"
    ERRORS=1
fi
if [ ! -d "sql/risk_matrix" ]; then
    echo "  ERRO: sql/risk_matrix/ nao existe (SCP primeiro)"
    ERRORS=1
fi
if [ ! -f "run_risk_matrix.sh" ]; then
    echo "  ERRO: run_risk_matrix.sh nao existe (SCP primeiro)"
    ERRORS=1
fi

# Conta SQLs
SQL_COUNT=$(ls sql/risk_matrix/*.sql 2>/dev/null | wc -l)
if [ "$SQL_COUNT" -lt 21 ]; then
    echo "  ERRO: Apenas $SQL_COUNT SQLs encontrados (esperado: 21)"
    ERRORS=1
else
    echo "  OK: $SQL_COUNT SQLs encontrados"
fi

# Verifica venv e deps
source venv/bin/activate
if ! python3 -c "import pyathena" 2>/dev/null; then
    echo "  ERRO: pyathena nao instalado no venv"
    ERRORS=1
fi
if ! python3 -c "import psycopg2" 2>/dev/null; then
    echo "  ERRO: psycopg2 nao instalado no venv"
    ERRORS=1
fi
if ! python3 -c "import sshtunnel" 2>/dev/null; then
    echo "  ERRO: sshtunnel nao instalado no venv"
    ERRORS=1
fi

# Verifica variaveis Athena no .env
if ! grep -q "ATHENA_AWS_ACCESS_KEY_ID" .env; then
    echo "  ERRO: ATHENA_AWS_ACCESS_KEY_ID nao encontrado no .env"
    ERRORS=1
fi

if [ $ERRORS -eq 1 ]; then
    echo ""
    echo "  ABORTANDO: corrija os erros acima antes de continuar"
    exit 1
fi

echo "  OK: todos os pre-requisitos atendidos"

# 2. Permissoes
echo "[2/5] Configurando permissoes..."
chmod +x run_risk_matrix.sh
echo "  OK: run_risk_matrix.sh executavel"

# 3. Criar diretorio de output e logs
echo "[3/5] Criando diretorios..."
mkdir -p output
mkdir -p pipelines/logs
echo "  OK: output/ e pipelines/logs/ criados"

# 4. Teste dry-run
echo "[4/5] Testando pipeline (--dry-run)..."
echo "  Isso vai executar as 21 queries no Athena (pode levar 10-20 min)."
echo "  O resultado sera salvo em output/ sem gravar no PostgreSQL."
read -p "  Deseja rodar o teste agora? (s/N): " CONFIRM
if [ "$CONFIRM" = "s" ] || [ "$CONFIRM" = "S" ]; then
    python3 pipelines/risk_matrix_pipeline.py --dry-run
    if [ $? -eq 0 ]; then
        echo "  OK: teste dry-run passou!"
        echo "  Verifique o CSV em output/"
        ls -la output/risk_matrix_*_FINAL.csv 2>/dev/null
    else
        echo "  AVISO: teste falhou. Verifique os logs acima."
        echo "  O cron sera configurado mesmo assim (pode corrigir e re-testar)."
    fi
else
    echo "  SKIP: teste adiado. Pode rodar manualmente depois:"
    echo "    cd /home/ec2-user/multibet"
    echo "    source venv/bin/activate"
    echo "    python3 pipelines/risk_matrix_pipeline.py --dry-run"
fi

# 5. Configurar cron (05:00 UTC = 02:00 BRT)
echo "[5/5] Configurando cron diario..."
CRON_LINE="0 5 * * * /home/ec2-user/multibet/run_risk_matrix.sh"
if crontab -l 2>/dev/null | grep -q "run_risk_matrix"; then
    echo "  Cron existente encontrado. Substituindo..."
    (crontab -l 2>/dev/null | grep -v "risk_matrix"; echo "$CRON_LINE") | crontab -
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
echo "Horario: 05:00 UTC (02:00 BRT) — diario"
echo ""
echo "Comandos uteis:"
echo "  # Rodar manualmente (com gravacao no PostgreSQL):"
echo "  cd /home/ec2-user/multibet && source venv/bin/activate"
echo "  python3 pipelines/risk_matrix_pipeline.py"
echo ""
echo "  # Rodar apenas dry-run (sem PostgreSQL):"
echo "  python3 pipelines/risk_matrix_pipeline.py --dry-run"
echo ""
echo "  # Rodar apenas tags especificas:"
echo "  python3 pipelines/risk_matrix_pipeline.py --only VIP_WHALE_PLAYER FAST_CASHOUT"
echo ""
echo "  # Ver logs:"
echo "  tail -f pipelines/logs/risk_matrix_\$(date +%Y-%m-%d).log"
echo ""
echo "  # Verificar output:"
echo "  ls -la output/risk_matrix_*"
echo "========================================="
