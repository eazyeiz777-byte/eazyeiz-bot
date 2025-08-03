. Create the Web Service
 • Source → GitHub repo with the single file above (or your repo).
 • Environment → Python 3.11 (Render provides).
 • Build Command → leave empty  (pip install is automatic via  requirements.txt ).
 • Start Command →  python bot.py  (or  gunicorn bot:app --workers=1 --bind=0.0.0.0:$PORT  if you separate Flask).
 Add Environment Variables in Render Dashboard
 FINNHUB_API_KEY=xxxxxxxxxxxxxxxxxxx
TELEGRAM_BOT_TOKEN=xxxxxxxxxxxxx
TELEGRAM_CHAT_ID=xxxxxxxxxxxxx
ACCOUNT_EQUITY=1000
RISK_PER_TRADE=0.01
SIGNAL_CONF_THRESHOLD=0.5
PORT=10000          # Render injects this
requirements.txt
aiohttp==3.9.5
websockets==12.0
pandas==2.2.2
numpy==1.26.4
flask==3.0.3
Disk (optional)
 • Mount a 1 GB “Disk” at  /opt/render/project/src  if you want  signal_log.csv  to survive restarts.
 • Otherwise log to stdout → use  logging.StreamHandler  and collect via Render → Log Streams.
 Health & Auto-Deploy
 • Health-check path:  /health .
 • Auto-deploy: OFF if you want to test locally first; ON for CI/CD later.6. Resource Plan
 • Starter (512 MB RAM, 0.1 CPU) is enough for 100 pairs × 3 TFs with the bounded deque.
 • Watch memory on Dashboard; bump to Standard (1 GB) if you add 1-minute timeframes or hundreds more symbols.7. Alerts
 • Connect your Telegram bot to a private channel → instant alerts.
 • Add a free UptimeRobot ping to  https://your-app.onrender.com/health  every 5 min → email/SMS if down.
