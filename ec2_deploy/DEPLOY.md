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

## Observações
- A senha do Redshift expira periodicamente — atualizar no `.env` quando necessário.
- O IP do bastion (`supernova.py`) pode mudar se a EC2 não tiver Elastic IP.
- Se o pipeline rodar NA PRÓPRIA EC2 do bastion, o tunnel SSH não é necessário
  (conectar direto no RDS). Nesse caso, simplificar `supernova.py`.
