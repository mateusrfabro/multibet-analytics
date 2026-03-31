# Gunicorn config — Dashboard Trafego Pago
# Producao: 2 workers, bind localhost, logs com rotacao

import os

# Bind somente em localhost (Nginx faz o proxy)
bind = f"127.0.0.1:{os.getenv('DASHBOARD_PORT', '5050')}"

# Workers (2 e suficiente para dashboard interno)
workers = 2
worker_class = "sync"
timeout = 120  # queries Athena podem levar ate 60s

# Logging
accesslog = "-"  # stdout (capturado pelo systemd/journalctl)
errorlog = "-"
loglevel = "info"

# Seguranca
limit_request_line = 4094
limit_request_fields = 100
