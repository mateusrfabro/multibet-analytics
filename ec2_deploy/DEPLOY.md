# Deploy — Pipelines MultiBet (EC2)

## Estrutura da pasta
```
multibet/
├── .env                          ← copiar do .env.example e preencher credenciais
├── bigquery_credentials.json     ← chave de serviço do BigQuery (pedir pro Mateus)
├── bastion-analytics-key.pem     ← chave SSH do bastion (pedir pro Mateus)
├── requirements.txt
├── run_grandes_ganhos.sh         ← cron: pipeline Grandes Ganhos (diário)
├── run_anti_abuse.sh             ← loop: bot Anti-Abuse Multiverso (a cada 5 min)
├── db/
│   ├── bigquery.py
│   ├── redshift.py
│   └── supernova.py
├── logs/                         ← criada automaticamente
└── pipelines/
    ├── grandes_ganhos.py         ← pipeline Grandes Ganhos
    ├── anti_abuse_multiverso.py  ← bot Anti-Abuse Campanha Multiverso
    └── ddl_grandes_ganhos.sql
```

## Setup na EC2

```bash
# 1. Criar pasta e copiar os arquivos
mkdir -p /home/ec2-user/multibet
# (copiar todos os arquivos desta pasta para /home/ec2-user/multibet/)

# 2. Instalar Python 3.12+ (se não tiver)
sudo yum install python3.12 -y  # Amazon Linux 2023
# ou
sudo yum install python3 -y     # Amazon Linux 2

# 3. Criar venv e instalar dependências
cd /home/ec2-user/multibet
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Configurar .env
cp .env.example .env
nano .env  # preencher REDSHIFT_PASSWORD e ajustar paths

# 5. Copiar credenciais
# - bigquery_credentials.json → /home/ec2-user/multibet/
# - bastion-analytics-key.pem → /home/ec2-user/multibet/
chmod 600 bastion-analytics-key.pem

# 6. Testar manualmente
source venv/bin/activate
python3 pipelines/grandes_ganhos.py

# 7. Dar permissão de execução ao script
chmod +x run_grandes_ganhos.sh
```

## Agendar no cron (diário às 00:30 BRT)

```bash
crontab -e
```

Adicionar a linha:
```
30 3 * * * /home/ec2-user/multibet/run_grandes_ganhos.sh
```

> **Nota:** 3:00 UTC = 0:00 BRT. Então `30 3` = 00:30 BRT.

## Logs
Os logs ficam em `pipelines/logs/grandes_ganhos_YYYY-MM-DD.log`.

## ETL Aquisicao Trafego — cron horario (a cada 60 min)

Alimenta `multibet.aquisicao_trafego_diario` no Super Nova DB.
Consumido pela aba "Aquisicao Trafego" do front `db.supernovagaming.com.br`.

### Deploy

```bash
# 1. Copiar pipeline e wrapper (ja estao no ec2_deploy/)
# pipelines/etl_aquisicao_trafego_diario.py
# run_etl_aquisicao_trafego.sh
# db/athena.py

# 2. Instalar pyathena (se ainda nao tiver)
source venv/bin/activate
pip install pyathena>=3.0

# 3. Garantir variaveis no .env
# ATHENA_AWS_ACCESS_KEY_ID=...
# ATHENA_AWS_SECRET_ACCESS_KEY=...
# ATHENA_S3_STAGING=s3://aws-athena-query-results-803633136520-sa-east-1/
# ATHENA_REGION=sa-east-1
# BASTION_HOST=...
# SUPERNOVA_HOST=...
# SUPERNOVA_PASS=...

# 4. Testar manualmente
python3 pipelines/etl_aquisicao_trafego_diario.py --days 1

# 5. Dar permissao e agendar
chmod +x run_etl_aquisicao_trafego.sh
crontab -e
```

### Crontab

```
# ETL Aquisicao Trafego — a cada hora (minuto 10, evita colisao com outros ETLs)
10 * * * * /home/ec2-user/multibet/run_etl_aquisicao_trafego.sh
```

> Roda a cada hora no minuto 10 (ex: 00:10, 01:10, ..., 23:10 UTC).
> Reprocessa D-2 + D-1 + hoje (parcial). Idempotente (DELETE + INSERT).

### Logs
```bash
tail -f pipelines/logs/etl_aquisicao_trafego_$(date +%Y-%m-%d).log
```

## Bot Anti-Abuse — Campanha Multiverso

Monitora os 6 Fortune games (PG Soft) em tempo real, detecta fraude e alerta no Slack.

### Variáveis de ambiente necessárias (adicionar ao `.env`)
```
SLACK_WEBHOOK_MULTIVERSO=https://hooks.slack.com/services/...
```

### Iniciar / Parar / Status
```bash
chmod +x run_anti_abuse.sh

./run_anti_abuse.sh           # inicia em background (loop a cada 5 min)
./run_anti_abuse.sh status    # verifica se está rodando
./run_anti_abuse.sh stop      # para o bot
```

### Logs
```bash
tail -f logs/anti_abuse_$(date +%Y-%m-%d).log
```

### Reiniciar após reboot da EC2
Adicionar ao crontab para iniciar automaticamente:
```bash
crontab -e
# adicionar:
@reboot /home/ec2-user/multibet/run_anti_abuse.sh
```

---

## Matriz de Risco — cron diario (02:00 BRT = 05:00 UTC)

Classifica 100% da base de jogadores em 5 tiers de risco/saude via 21 tags comportamentais.
Executa 21 queries no Athena, calcula scores, persiste snapshots historicos no PostgreSQL.

### Arquivos

```
multibet/
├── pipelines/
│   └── risk_matrix_pipeline.py   ← pipeline principal (21 tags)
├── sql/
│   └── risk_matrix/
│       ├── REGULAR_DEPOSITOR.sql  ← 1 SQL por tag (21 total)
│       ├── PROMO_ONLY.sql
│       ├── ... (21 arquivos)
│       └── ROLLBACK_PLAYER.sql
├── run_risk_matrix.sh            ← wrapper do cron
└── output/                       ← CSVs + legendas (gerados automaticamente)
```

### Deploy (passo a passo)

```bash
# 1. Na maquina LOCAL — copiar arquivos para EC2 ETL via SCP
#    (ajustar IP se necessario — EC2 ETL nao tem Elastic IP)

EC2_IP="54.197.63.138"
KEY="C:/Users/NITRO/Downloads/etl-key.pem"

# Pipeline
scp -i "$KEY" ec2_deploy/pipelines/risk_matrix_pipeline.py \
    ec2-user@$EC2_IP:/home/ec2-user/multibet/pipelines/

# SQLs (21 arquivos)
ssh -i "$KEY" ec2-user@$EC2_IP "mkdir -p /home/ec2-user/multibet/sql/risk_matrix"
scp -i "$KEY" ec2_deploy/sql/risk_matrix/*.sql \
    ec2-user@$EC2_IP:/home/ec2-user/multibet/sql/risk_matrix/

# Wrapper cron
scp -i "$KEY" ec2_deploy/run_risk_matrix.sh \
    ec2-user@$EC2_IP:/home/ec2-user/multibet/

# Deploy script
scp -i "$KEY" ec2_deploy/deploy_risk_matrix.sh \
    ec2-user@$EC2_IP:/home/ec2-user/multibet/

# 2. Conectar na EC2 via SSH
ssh -i "$KEY" ec2-user@$EC2_IP

# 3. Rodar o deploy
cd /home/ec2-user/multibet
chmod +x deploy_risk_matrix.sh
./deploy_risk_matrix.sh
```

### Teste manual

```bash
cd /home/ec2-user/multibet
source venv/bin/activate

# Dry-run (apenas CSV, sem gravar PostgreSQL)
python3 pipelines/risk_matrix_pipeline.py --dry-run

# Execucao completa (CSV + PostgreSQL)
python3 pipelines/risk_matrix_pipeline.py

# Apenas tags especificas
python3 pipelines/risk_matrix_pipeline.py --only VIP_WHALE_PLAYER FAST_CASHOUT
```

### Crontab

```
# Matriz de Risco — diario 02:00 BRT (05:00 UTC)
0 5 * * * /home/ec2-user/multibet/run_risk_matrix.sh
```

> Roda diariamente as 02:00 BRT. Executa 21 tags, pivota, calcula scores,
> persiste snapshot no PostgreSQL (multibet.risk_tags). Idempotente por data.
> Tempo estimado: 15-30 minutos (21 queries Athena + COPY PostgreSQL).

### Logs

```bash
tail -f pipelines/logs/risk_matrix_$(date +%Y-%m-%d).log
```

### Output

```bash
ls -la output/risk_matrix_*
# risk_matrix_YYYY-MM-DD_FINAL.csv   — CSV com todos os jogadores
# risk_matrix_YYYY-MM-DD_legenda.txt — dicionario de colunas
```

### Destino PostgreSQL

- **Schema:** multibet
- **Tabela:** risk_tags
- **Chave:** (label_id, user_id, snapshot_date) — snapshots historicos
- Idempotente: DELETE do snapshot do dia + INSERT

---

## Observações
- A senha do Redshift expira periodicamente — atualizar no `.env` quando necessário.
- O IP do bastion (`supernova.py`) pode mudar se a EC2 não tiver Elastic IP.
- Se o pipeline rodar NA PRÓPRIA EC2 do bastion, o tunnel SSH não é necessário
  (conectar direto no RDS). Nesse caso, simplificar `supernova.py`.
