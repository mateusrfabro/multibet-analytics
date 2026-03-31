# Deploy — Dashboard de Trafego Pago (EC2)

## Visao Geral
Dashboard Flask multi-canal (Google Ads + Meta) para gestores de trafego.
Consulta Athena Data Lake a cada 1h (cache), exibe KPIs, graficos e insights.

## Arquitetura
```
Internet → Nginx (HTTPS, porta 443) → Gunicorn (porta 5050) → Flask App
                                                                  ↓
                                                          Athena Data Lake
                                                        (read-only, BRT)
```

## Atualizacao dos dados
| Aspecto | Detalhe |
|---------|---------|
| Banco | AWS Athena (Iceberg Data Lake) — read-only |
| Databases | bireports_ec2, ps_bi (pre-agregado) |
| Cache | Em memoria Flask — TTL 1 hora |
| Refresh | Automatico a cada 30min (JS) + botao manual |
| Custo | ~R$ 0,02-0,05 por acesso (Athena cobra por scan S3) |
| Credenciais | .env (ATHENA_AWS_ACCESS_KEY_ID, ATHENA_AWS_SECRET_ACCESS_KEY) |

## Estrutura de arquivos
```
/home/ec2-user/multibet/dashboard/
├── .env                      ← credenciais Athena + senha do dashboard
├── requirements.txt
├── run_dashboard.sh           ← script de start/stop
├── gunicorn.conf.py           ← config Gunicorn
├── db/
│   └── athena.py
├── dashboards/
│   └── google_ads/
│       ├── app.py             ← Flask app principal
│       ├── config.py          ← canais, cache, auth
│       ├── queries.py         ← queries Athena (D-1, D-2, trend, hourly)
│       ├── queries_hourly.py  ← comparativo hora a hora
│       ├── static/
│       │   ├── css/theme.css
│       │   └── js/dashboard.js
│       └── templates/
│           ├── dashboard.html
│           └── login.html
└── logs/
    └── dashboard_YYYY-MM-DD.log
```

## Setup na EC2

### 1. Criar pasta isolada (NAO misturar com /home/ec2-user/multibet/)
```bash
mkdir -p /home/ec2-user/multibet/dashboard
cd /home/ec2-user/multibet/dashboard
```

### 2. Copiar arquivos (via scp ou git clone)
```bash
# Opcao A: scp do local
scp -i bastion-key.pem -r dashboard_deploy/* ec2-user@<bastion-ip>:/home/ec2-user/multibet/dashboard/

# Opcao B: git clone (se EC2 tiver acesso ao GitHub)
cd /home/ec2-user/multibet/dashboard
git clone https://github.com/mateusrfabro/multibet-analytics.git .
```

### 3. Criar venv SEPARADO (isolamento das outras apps)
```bash
cd /home/ec2-user/multibet/dashboard
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 4. Configurar .env
```bash
cat > .env << 'EOF'
# Athena
ATHENA_AWS_ACCESS_KEY_ID=<preencher>
ATHENA_AWS_SECRET_ACCESS_KEY=<preencher>

# Dashboard
DASHBOARD_HOST=127.0.0.1
DASHBOARD_PORT=5050
DASHBOARD_DEBUG=false
DASHBOARD_SECRET_KEY=<gerar com: python3 -c "import secrets; print(secrets.token_hex(32))">
DASHBOARD_USER=multibet
DASHBOARD_PASS=<senha forte aqui>
EOF
chmod 600 .env
```

### 5. Testar manualmente
```bash
source venv/bin/activate
cd /home/ec2-user/multibet/dashboard
python3 dashboards/google_ads/app.py
# Deve mostrar: "Dashboard Trafego Pago iniciando em 127.0.0.1:5050"
# Ctrl+C para parar
```

### 6. Configurar Gunicorn (producao)
```bash
# gunicorn.conf.py (ja incluso no pacote)
pip install gunicorn
gunicorn -c gunicorn.conf.py "dashboards.google_ads.app:app"
```

### 7. Configurar systemd (auto-restart)
```bash
sudo tee /etc/systemd/system/dashboard-trafego.service << 'EOF'
[Unit]
Description=Dashboard Trafego Pago MultiBet
After=network.target

[Service]
Type=simple
User=ec2-user
WorkingDirectory=/home/ec2-user/multibet/dashboard
ExecStart=/home/ec2-user/multibet/dashboard/venv/bin/gunicorn -c gunicorn.conf.py "dashboards.google_ads.app:app"
Restart=always
RestartSec=5
EnvironmentFile=/home/ec2-user/multibet/dashboard/.env

[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload
sudo systemctl enable dashboard-trafego
sudo systemctl start dashboard-trafego
sudo systemctl status dashboard-trafego
```

### 8. Configurar Nginx (reverse proxy + HTTPS)
```bash
# Verificar se Nginx ja esta instalado
sudo nginx -v
# Se nao: sudo yum install nginx -y

# Adicionar config do dashboard (NAO substituir configs existentes!)
sudo tee /etc/nginx/conf.d/dashboard-trafego.conf << 'EOF'
server {
    listen 443 ssl;
    server_name dashboard.multibet.com;  # ajustar dominio

    # SSL (ajustar paths dos certificados existentes)
    ssl_certificate /etc/nginx/ssl/cert.pem;
    ssl_certificate_key /etc/nginx/ssl/key.pem;

    location / {
        proxy_pass http://127.0.0.1:5050;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
    }
}
EOF

# Testar config antes de recarregar (seguranca!)
sudo nginx -t
# Se OK:
sudo systemctl reload nginx
```

## Isolamento (requisito do Gusta)
- **Porta propria:** 5050 (nao conflita com outras apps)
- **Venv separado:** /home/ec2-user/multibet/dashboard/venv/
- **Systemd separado:** dashboard-trafego.service
- **Nginx:** config em arquivo separado (conf.d/dashboard-trafego.conf)
- **Bind localhost:** Gunicorn escuta em 127.0.0.1 (nao expoe direto)
- **Sem root:** roda como ec2-user

## Monitoramento
```bash
# Status do servico
sudo systemctl status dashboard-trafego

# Logs
journalctl -u dashboard-trafego -f

# Health check
curl http://127.0.0.1:5050/health
```

## Comandos uteis
```bash
# Reiniciar apos atualizacao de codigo
sudo systemctl restart dashboard-trafego

# Parar
sudo systemctl stop dashboard-trafego

# Ver logs do dia
journalctl -u dashboard-trafego --since today
```

## Login
- Usuario: definido no .env (DASHBOARD_USER)
- Senha: definida no .env (DASHBOARD_PASS)
- Rate limit: 30 requests/minuto por IP
