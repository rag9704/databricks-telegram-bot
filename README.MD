# Databricks-Telegram Bot

A lightweight Python bot that brings your Databricks Jobs into a Telegram chat.
Get instant notifications, check today’s run status, repair failed jobs, or pause schedules—all from your phone.

| Command   | What it does                                                   |
| --------- | -------------------------------------------------------------- |
| `/jobs`   | List every job you own → tap “Check Status” to see today’s run |
| `/failed` | Show only **today’s failures** → tap “Repair” to re-run        |
| `/pause`  | Toggle the schedule trigger of any job (pause / resume)        |
| `/help`   | Quick reference                                                |



## Quick-start
### 1. Prerequisites
 1. Python 3.8+
 2. A Databricks personal access token
 3. A Telegram bot token from @BotFather

### 2. Clone & install
 ```
 git clone git@github.com:rag9704/databricks-telegram-bot.git
 cd databricks-telegram-bot/telegram-bot
 python -m venv venv
 source venv/bin/activate
 pip install -r requirements.txt   # see file below
```

### 3. Configure
Create a .env file in telegram-bot/:
```
BOT_TOKEN=123456:ABC...
CHAT_ID=-1001234567890
DATABRICKS_SERVER=https://dbc-1234.cloud.databricks.com
DATABRICKS_TOKEN=dapi...
EMAIL=you@company.com
```

### 4. Run
``` 
python app.py
```

### requirements.txt

```
telebot
databricks-sdk
python-dotenv
pytz
schedule
certifi
```

## Security notes
1. Never commit .env or any token.
2. The bot only lists jobs created by the e-mail in EMAIL.
3. Repair uses your personal token and respects Databricks RBAC.

