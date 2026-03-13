# Deploy — Anti-Abuse Bot Multiverso (EC2)

Bot que monitora os 6 Fortune games (PG Soft) a cada 5 minutos e alerta no Slack
quando detecta jogadores suspeitos durante a Campanha Multiverso.

## Estrutura
```
anti_abuse/
├── .env                         ← preencher com credenciais reais (pedir pro Mateus)
├── bigquery_credentials.json    ← chave BigQuery (pedir pro Mateus)
├── requirements.txt
├── run_anti_abuse.sh            ← start / stop / status
├── db/
│   └── bigquery.py
└── pipelines/
    └── anti_abuse_multiverso.py
```

## Setup na EC2

```bash
# 1. Criar pasta
mkdir -p /home/ec2-user/anti_abuse

# 2. Copiar todos os arquivos desta pasta para lá
# (exceto credenciais — receber separado do Mateus)

# 3. Instalar Python e dependências
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

# 4. Configurar credenciais
cp .env.example .env
nano .env   # preencher BIGQUERY_CREDENTIALS_PATH e SLACK_WEBHOOK_MULTIVERSO

# 5. Dar permissão ao script de controle
chmod +x run_anti_abuse.sh
```

## Iniciar / Parar / Status

```bash
./run_anti_abuse.sh           # inicia em background (loop a cada 5 min)
./run_anti_abuse.sh status    # verifica se está rodando
./run_anti_abuse.sh stop      # para o bot
```

## Ver logs em tempo real

```bash
tail -f logs/anti_abuse_$(date +%Y-%m-%d).log
```

## Reiniciar automaticamente após reboot da EC2

```bash
crontab -e
# adicionar a linha abaixo:
@reboot /home/ec2-user/anti_abuse/run_anti_abuse.sh
```
