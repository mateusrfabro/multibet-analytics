#!/bin/bash
# =================================================================
# DEPLOY: ETL Aquisicao Trafego na EC2
# Cola TUDO no terminal SSH. NAO altera arquivos existentes.
# Apenas cria 3 arquivos novos + instala pyathena + agenda cron.
# =================================================================
set -e

echo "========================================="
echo "DEPLOY ETL AQUISICAO TRAFEGO"
echo "========================================="

cd /home/ec2-user/multibet

# 1. Verificar que estamos no lugar certo
echo "[1/6] Verificando estrutura..."
if [ ! -d "venv" ] || [ ! -d "db" ] || [ ! -d "pipelines" ]; then
    echo "ERRO: pasta /home/ec2-user/multibet/ nao tem a estrutura esperada"
    exit 1
fi
echo "  OK: venv/, db/, pipelines/ existem"

# 2. Instalar pyathena (se nao tiver)
echo "[2/6] Instalando pyathena..."
source venv/bin/activate
pip install pyathena>=3.0 -q
echo "  OK: pyathena instalado"

# 3. Criar db/athena.py (SE NAO EXISTIR — nao sobrescreve)
echo "[3/6] Criando db/athena.py..."
if [ -f "db/athena.py" ]; then
    echo "  SKIP: db/athena.py ja existe, nao sobrescrevendo"
else
cat > db/athena.py << 'PYEOF'
"""
Conexao com AWS Athena (Iceberg Data Lake) — somente leitura.
Regiao: sa-east-1
"""
import os, time, logging, pandas as pd
from pyathena import connect
from dotenv import load_dotenv

log = logging.getLogger(__name__)
load_dotenv()

def get_connection(database="default"):
    return connect(
        aws_access_key_id=os.getenv("ATHENA_AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("ATHENA_AWS_SECRET_ACCESS_KEY"),
        s3_staging_dir=os.getenv("ATHENA_S3_STAGING"),
        region_name=os.getenv("ATHENA_REGION", "sa-east-1"),
        schema_name=database,
    )

def query_athena(sql, database="default", retries=3, retry_delay=5.0):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            conn = get_connection(database)
            return pd.read_sql(sql, conn)
        except Exception as e:
            last_err = e
            if attempt < retries:
                log.warning(f"Athena falhou ({attempt}/{retries}): {e}. Retry em {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                log.error(f"Athena falhou apos {retries} tentativas.")
    raise last_err
PYEOF
    echo "  OK: db/athena.py criado"
fi

# 4. Criar pipeline
echo "[4/6] Criando pipelines/etl_aquisicao_trafego_diario.py..."
cat > pipelines/etl_aquisicao_trafego_diario.py << 'PYEOF'
"""
Pipeline: etl_aquisicao_trafego_diario (Athena -> Super Nova DB)
================================================================
Extrai metricas diarias de aquisicao por canal de trafego (Google, Meta, etc.)
e persiste no Super Nova DB em multibet.aquisicao_trafego_diario.

Fontes:
  - REG: bireports_ec2.tbl_ecr (c_sign_up_time BRT, c_test_user=false)
  - FTD: bireports_ec2.tbl_ecr (REGs base) + ps_bi.dim_user (ftd_datetime same-day)
  - Financeiro: bireports_ec2.tbl_ecr_wise_daily_bi_summary (centavos/100)
  - Canal: affiliate_id mapeado via CHANNEL_MAP

Execucao:
    python pipelines/etl_aquisicao_trafego_diario.py              # D-1
    python pipelines/etl_aquisicao_trafego_diario.py --days 7     # ultimos 7 dias
"""
import sys, os, logging, argparse
from datetime import date, timedelta

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from db.athena import query_athena
from db.supernova import execute_supernova, get_supernova_connection

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
log = logging.getLogger("etl_aquisicao_trafego")

CHANNEL_MAP = {
    "Google": ["297657", "445431", "468114"],
    "Meta": ["532570", "532571", "464673"],
}
ALL_MAPPED_IDS = []
for _ids in CHANNEL_MAP.values():
    ALL_MAPPED_IDS.extend(_ids)

DDL = """
CREATE TABLE IF NOT EXISTS multibet.aquisicao_trafego_diario (
    dt                  DATE         NOT NULL,
    channel             VARCHAR(30)  NOT NULL,
    registros           INTEGER      DEFAULT 0,
    ftd_count           INTEGER      DEFAULT 0,
    ftd_amount          NUMERIC(14,2) DEFAULT 0,
    conv_reg_ftd_pct    NUMERIC(5,1) DEFAULT 0,
    ftd_ticket_medio    NUMERIC(14,2) DEFAULT 0,
    depositos_amount    NUMERIC(14,2) DEFAULT 0,
    saques_amount       NUMERIC(14,2) DEFAULT 0,
    net_deposit         NUMERIC(14,2) DEFAULT 0,
    ggr_casino          NUMERIC(14,2) DEFAULT 0,
    ggr_sport           NUMERIC(14,2) DEFAULT 0,
    bonus_cost          NUMERIC(14,2) DEFAULT 0,
    ngr                 NUMERIC(14,2) DEFAULT 0,
    players_ativos      INTEGER      DEFAULT 0,
    refreshed_at        TIMESTAMPTZ  DEFAULT NOW(),
    CONSTRAINT pk_aquisicao_trafego PRIMARY KEY (dt, channel)
);
CREATE INDEX IF NOT EXISTS idx_aqt_dt ON multibet.aquisicao_trafego_diario (dt);
CREATE INDEX IF NOT EXISTS idx_aqt_channel ON multibet.aquisicao_trafego_diario (channel);
"""

DDL_VIEW = """
CREATE OR REPLACE VIEW multibet.vw_aquisicao_trafego AS
SELECT
    dt AS data, channel AS canal, registros AS cadastros,
    ftd_count AS ftd, ftd_amount, conv_reg_ftd_pct AS conversao_ftd_pct,
    ftd_ticket_medio AS ticket_medio_ftd, depositos_amount AS deposito,
    saques_amount AS saque, net_deposit, ggr_casino, ggr_sport,
    ggr_casino + ggr_sport AS ggr_total, bonus_cost, ngr,
    players_ativos, refreshed_at
FROM multibet.aquisicao_trafego_diario
ORDER BY dt DESC, channel;
"""

def ensure_table():
    log.info("Verificando/criando tabela...")
    execute_supernova(DDL)
    log.info("Tabela OK")
    execute_supernova(DDL_VIEW)
    log.info("View OK")

def _aff_sql(ids):
    return "(" + ", ".join(f"'{i}'" for i in ids) + ")"

def query_reg(dt, aff):
    return f"""
    SELECT COUNT(*) AS reg
    FROM bireports_ec2.tbl_ecr
    WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
      AND CAST(c_affiliate_id AS VARCHAR) IN {aff}
      AND c_test_user = false
    """

def query_ftd(dt, aff):
    return f"""
    WITH regs_do_dia AS (
        SELECT c_ecr_id
        FROM bireports_ec2.tbl_ecr
        WHERE CAST(c_sign_up_time AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
          AND CAST(c_affiliate_id AS VARCHAR) IN {aff}
          AND c_test_user = false
    )
    SELECT COUNT(*) AS ftd, COALESCE(SUM(u.ftd_amount_inhouse), 0) AS ftd_amount
    FROM regs_do_dia r
    JOIN ps_bi.dim_user u ON r.c_ecr_id = u.ecr_id
    WHERE CAST(u.ftd_datetime AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo' AS DATE) = DATE '{dt}'
    """

def query_financeiro(dt, aff):
    return f"""
    WITH base_players AS (
        SELECT DISTINCT ecr_id FROM ps_bi.dim_user
        WHERE CAST(affiliate_id AS VARCHAR) IN {aff} AND is_test = false
    )
    SELECT
        COALESCE(SUM(s.c_deposit_success_amount), 0) / 100.0 AS dep_amount,
        COALESCE(SUM(s.c_co_success_amount), 0) / 100.0 AS saques,
        COALESCE(SUM(s.c_casino_realcash_bet_amount - s.c_casino_realcash_win_amount), 0) / 100.0 AS ggr_casino,
        COALESCE(SUM(s.c_sb_realcash_bet_amount - s.c_sb_realcash_win_amount), 0) / 100.0 AS ggr_sport,
        COALESCE(SUM(s.c_bonus_issued_amount), 0) / 100.0 AS bonus_cost,
        COUNT(DISTINCT s.c_ecr_id) AS players_ativos
    FROM bireports_ec2.tbl_ecr_wise_daily_bi_summary s
    JOIN base_players p ON s.c_ecr_id = p.ecr_id
    WHERE s.c_created_date = DATE '{dt}'
    """

def extract_channel_day(channel, ids, dt):
    aff = _aff_sql(ids)
    log.info(f"  Extraindo {channel} | {dt} | affiliates: {ids}")
    df_reg = query_athena(query_reg(dt, aff), database="bireports_ec2")
    reg = int(df_reg.iloc[0]["reg"])
    df_ftd = query_athena(query_ftd(dt, aff), database="bireports_ec2")
    ftd = int(df_ftd.iloc[0]["ftd"])
    ftd_amount = float(df_ftd.iloc[0]["ftd_amount"])
    df_fin = query_athena(query_financeiro(dt, aff), database="ps_bi")
    r = df_fin.iloc[0]
    dep = float(r["dep_amount"])
    saq = float(r["saques"])
    ggr_c = float(r["ggr_casino"])
    ggr_s = float(r["ggr_sport"])
    bonus = float(r["bonus_cost"])
    players = int(r["players_ativos"])
    conv_pct = round(ftd / max(reg, 1) * 100, 1)
    ticket_medio = round(ftd_amount / max(ftd, 1), 2)
    ngr = round(ggr_c + ggr_s - bonus, 2)
    net_dep = round(dep - saq, 2)
    return {
        "dt": dt, "channel": channel, "registros": reg, "ftd_count": ftd,
        "ftd_amount": round(ftd_amount, 2), "conv_reg_ftd_pct": conv_pct,
        "ftd_ticket_medio": ticket_medio, "depositos_amount": round(dep, 2),
        "saques_amount": round(saq, 2), "net_deposit": net_dep,
        "ggr_casino": round(ggr_c, 2), "ggr_sport": round(ggr_s, 2),
        "bonus_cost": round(bonus, 2), "ngr": ngr, "players_ativos": players,
    }

def persist_rows(rows):
    if not rows:
        log.warning("Nenhuma linha para persistir")
        return
    tunnel, conn = get_supernova_connection()
    try:
        with conn.cursor() as cur:
            for row in rows:
                cur.execute("DELETE FROM multibet.aquisicao_trafego_diario WHERE dt = %s AND channel = %s", (row["dt"], row["channel"]))
                cur.execute("""
                    INSERT INTO multibet.aquisicao_trafego_diario
                        (dt, channel, registros, ftd_count, ftd_amount, conv_reg_ftd_pct,
                         ftd_ticket_medio, depositos_amount, saques_amount, net_deposit,
                         ggr_casino, ggr_sport, bonus_cost, ngr, players_ativos, refreshed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (row["dt"], row["channel"], row["registros"], row["ftd_count"],
                      row["ftd_amount"], row["conv_reg_ftd_pct"], row["ftd_ticket_medio"],
                      row["depositos_amount"], row["saques_amount"], row["net_deposit"],
                      row["ggr_casino"], row["ggr_sport"], row["bonus_cost"],
                      row["ngr"], row["players_ativos"]))
            conn.commit()
            log.info(f"Persistidas {len(rows)} linhas no Super Nova DB")
    finally:
        conn.close()
        tunnel.stop()

def run(days=1, include_today=True):
    ensure_table()
    end_date = date.today() - timedelta(days=1)
    start_date = end_date - timedelta(days=days - 1)
    final_date = date.today() if include_today else end_date
    log.info(f"ETL Aquisicao Trafego: {start_date} a {final_date}")
    log.info(f"Canais: {list(CHANNEL_MAP.keys())}")
    all_rows = []
    current = start_date
    while current <= final_date:
        dt_str = current.isoformat()
        is_today = current == date.today()
        for channel, ids in CHANNEL_MAP.items():
            try:
                row = extract_channel_day(channel, ids, dt_str)
                all_rows.append(row)
                parcial = " (PARCIAL)" if is_today else ""
                log.info(f"  {channel} {dt_str}{parcial}: REG={row['registros']} FTD={row['ftd_count']} NGR=R${row['ngr']:,.2f}")
            except Exception as e:
                log.error(f"  ERRO {channel} {dt_str}: {e}", exc_info=True)
        try:
            row_all = extract_channel_day("Consolidado", ALL_MAPPED_IDS, dt_str)
            all_rows.append(row_all)
            parcial = " (PARCIAL)" if is_today else ""
            log.info(f"  Consolidado {dt_str}{parcial}: REG={row_all['registros']} FTD={row_all['ftd_count']} NGR=R${row_all['ngr']:,.2f}")
        except Exception as e:
            log.error(f"  ERRO Consolidado {dt_str}: {e}", exc_info=True)
        current += timedelta(days=1)
    persist_rows(all_rows)
    log.info("=" * 60)
    log.info(f"ETL concluido: {len(all_rows)} linhas processadas")
    for row in all_rows:
        parcial = " *" if row["dt"] == date.today().isoformat() else ""
        log.info(f"  {row['dt']}{parcial} | {row['channel']:>12} | REG={row['registros']:>5} FTD={row['ftd_count']:>4} Conv={row['conv_reg_ftd_pct']:>5.1f}% NGR=R${row['ngr']:>12,.2f}")
    if include_today:
        log.info("  * = dados parciais (dia em andamento)")
    log.info("=" * 60)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="ETL Aquisicao Trafego Diario")
    parser.add_argument("--days", type=int, default=1, help="Dias retroativos (default: 1)")
    parser.add_argument("--no-today", action="store_true", help="Nao incluir dia atual")
    args = parser.parse_args()
    run(days=args.days, include_today=not args.no_today)
PYEOF
echo "  OK: pipeline criado"

# 5. Criar wrapper do cron
echo "[5/6] Criando run_etl_aquisicao_trafego.sh..."
cat > run_etl_aquisicao_trafego.sh << 'SHEOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/pipelines/logs"
mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/etl_aquisicao_trafego_$(date +%Y-%m-%d).log"
echo "=========================================" >> "$LOGFILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOGFILE"
cd "$SCRIPT_DIR"
source venv/bin/activate
python3 pipelines/etl_aquisicao_trafego_diario.py --days 2 >> "$LOGFILE" 2>&1
EXIT_CODE=$?
echo "Fim: $(date '+%Y-%m-%d %H:%M:%S') | Exit: $EXIT_CODE" >> "$LOGFILE"
echo "" >> "$LOGFILE"
exit $EXIT_CODE
SHEOF
chmod +x run_etl_aquisicao_trafego.sh
echo "  OK: wrapper criado"

# 6. Agendar cron (ADICIONA sem alterar existentes)
echo "[6/6] Agendando cron horario..."
CRON_LINE="10 * * * * /home/ec2-user/multibet/run_etl_aquisicao_trafego.sh"
if crontab -l 2>/dev/null | grep -q "etl_aquisicao_trafego"; then
    echo "  SKIP: cron ja existe, nao duplicando"
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
echo "  python3 pipelines/etl_aquisicao_trafego_diario.py --days 1"
echo ""
echo "Verificar logs:"
echo "  tail -f pipelines/logs/etl_aquisicao_trafego_$(date +%Y-%m-%d).log"
echo "========================================="
