#!/usr/bin/env python3
"""
author : anurag pal
"""

import os
import logging
import json
import time
from datetime import date, datetime
from collections import defaultdict

import certifi
import pytz
import requests
import schedule
import telebot
from telebot import types
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunResultState
from dotenv import load_dotenv

os.environ["SSL_CERT_FILE"] = certifi.where()
load_dotenv()

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
BOT_TOKEN         = os.environ["BOT_TOKEN"]
CHAT_ID           = int(os.environ["CHAT_ID"])
DATABRICKS_SERVER = os.environ["DATABRICKS_SERVER"]
DATABRICKS_TOKEN  = os.environ["DATABRICKS_TOKEN"]
EMAIL             = os.environ["EMAIL"]

bot = telebot.TeleBot(BOT_TOKEN, threaded=False)
TZ   = pytz.timezone("Asia/Kolkata")

# ------------------------------------------------------------------
# Helper: common workspace client
# ------------------------------------------------------------------
def _ws():
    return WorkspaceClient(host=DATABRICKS_SERVER, token=DATABRICKS_TOKEN)

# ------------------------------------------------------------------
# /help
# ------------------------------------------------------------------
@bot.message_handler(commands=["help", "Help"])
def send_welcome(message):
    bot.reply_to(
        message,
        "Available commands:\n"
        "/jobs  – list all jobs\n"
        "/failed – list failed runs today\n"
        "/pause – pause / resume job schedules\n"
        "/help  – this help",
    )

# ------------------------------------------------------------------
# /jobs
# ------------------------------------------------------------------
@bot.message_handler(commands=["jobs"])
def jobs_cmd(message):
    send_job_list()

def send_job_list():
    """List jobs with a ‘check status’ button for each."""
    w = _ws()
    jobs = [
        {"name": j.settings.name, "id": j.job_id}
        for j in w.jobs.list()
        if j.creator_user_name == EMAIL
    ]
    if not jobs:
        bot.send_message(CHAT_ID, "No jobs found for your account.")
        return

    bot.send_message(CHAT_ID, f"📋 Found {len(jobs)} job(s). Tap to check today’s run:")
    for j in jobs:
        kb = types.InlineKeyboardMarkup()
        kb.add(
            types.InlineKeyboardButton(
                text="📊 Check Status",
                callback_data=json.dumps({"action": "check_status", "job_id": j["id"]}),
            )
        )
        bot.send_message(
            CHAT_ID,
            f'{j["name"]}\nJob ID: `{j["id"]}`',
            reply_markup=kb,
        )

# ------------------------------------------------------------------
# /failed
# ------------------------------------------------------------------
@bot.message_handler(commands=["failed"])
def failed_cmd(message):
    databricks_job_notification()

def databricks_job_notification():
    """Send today’s failed runs with ‘repair’ buttons."""
    w = _ws()
    today = date.today()
    failed = []

    for job in w.jobs.list():
        if job.creator_user_name != EMAIL:
            continue
        for run in w.jobs.list_runs(job_id=job.job_id, expand_tasks=False):
            if (
                run.state.result_state == RunResultState.FAILED
                and run.end_time
                and datetime.fromtimestamp(run.end_time / 1000, tz=TZ).date() == today
            ):
                failed.append(
                    {
                        "job": job.settings.name,
                        "run_id": run.run_id,
                        "start": run.start_time,
                        "end": run.end_time,
                    }
                )

    if not failed:
        bot.send_message(CHAT_ID, "🎉 No failures today!")
        return

    bot.send_message(CHAT_ID, f"❌ Found {len(failed)} failure(s) today:")
    for f in failed:
        print(f)
        kb = types.InlineKeyboardMarkup()
        kb.add(
            types.InlineKeyboardButton(
                text=f"🔧 Repair {f['job'][:25]}",
                callback_data=json.dumps(
                    {"action": "repair", "run_id": f["run_id"]}
                ),
            )
        )
        start = datetime.fromtimestamp(f["start"] / 1000, tz=TZ).strftime("%H:%M")
        end   = datetime.fromtimestamp(f["end"]   / 1000, tz=TZ).strftime("%H:%M")
        bot.send_message(
            CHAT_ID,
            f"🔴 **{f['job']}**\n`{f['run_id']}`\n⏰ {start} – {end}",
            reply_markup=kb,
            parse_mode="Markdown",
        )

# ------------------------------------------------------------------
# /pause
# ------------------------------------------------------------------
@bot.message_handler(commands=["pause"])
def pause_cmd(message):
    send_pause_job_list()

def send_pause_job_list():
    """List jobs with Pause / Resume buttons for their schedule."""
    w = _ws()
    jobs = [
        {
            "name": j.settings.name,
            "id": j.job_id,
            "schedule": j.settings.schedule,
        }
        for j in w.jobs.list()
        if j.creator_user_name == EMAIL
    ]

    if not jobs:
        bot.send_message(CHAT_ID, "No jobs found for your account.")
        return

    bot.send_message(CHAT_ID, f"📋 Found {len(jobs)} job(s). Tap to pause / resume schedule:")
    for j in jobs:
        kb = types.InlineKeyboardMarkup()
        if j["schedule"] and j["schedule"].pause_status != "PAUSED":
            action, label = "pause", "⏸ Pause"
        else:
            action, label = "resume", "▶️ Resume"

        kb.add(
            types.InlineKeyboardButton(
                text=label,
                callback_data=json.dumps({"action": action, "job_id": j["id"]}),
            )
        )
        bot.send_message(
            CHAT_ID,
            f'{j["name"]}\nJob ID: `{j["id"]}`',
            reply_markup=kb,
        )

def toggle_job_schedule(job_id: int, pause: bool):
    """Pause or resume the schedule trigger of a job."""
    w = _ws()
    try:
        job = w.jobs.get(job_id=job_id)
        settings = job.settings
        if not settings.schedule:
            bot.send_message(CHAT_ID, f"Job `{job_id}` has no schedule.")
            return

        settings.schedule.pause_status = "PAUSED" if pause else "UNPAUSED"
        w.jobs.update(job_id=job_id, new_settings=settings)

        verb = "paused" if pause else "resumed"
        bot.send_message(CHAT_ID, f"✅ Schedule for `{settings.name}` has been {verb}.")
    except Exception as e:
        bot.send_message(CHAT_ID, f"❌ Could not toggle schedule: {e}")

# ------------------------------------------------------------------
# Callback dispatcher
# ------------------------------------------------------------------
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    try:
        data = json.loads(call.data)
        action = data.get("action")

        if action == "check_status":
            job_id = data["job_id"]
            bot.answer_callback_query(call.id, "🔍 Checking…")
            check_job_today_status(job_id)

        elif action == "repair":
            run_id = data["run_id"]
            bot.answer_callback_query(call.id, "🔧 Repairing…")
            repair_databricks_job(run_id)

        elif action == "pause":
            job_id = data["job_id"]
            bot.answer_callback_query(call.id, "⏸ Pausing…")
            toggle_job_schedule(job_id, pause=True)

        elif action == "resume":
            job_id = data["job_id"]
            bot.answer_callback_query(call.id, "▶️ Resuming…")
            toggle_job_schedule(job_id, pause=False)

    except Exception as e:
        bot.answer_callback_query(call.id, "❌ Error processing request")
        logging.exception("callback error")

# ------------------------------------------------------------------
# Status checker (used by /jobs)
# ------------------------------------------------------------------
def check_job_today_status(job_id):
    today = date.today()
    w = _ws()
    try:
        job = w.jobs.get(job_id=job_id)
        runs_today = [
            r
            for r in w.jobs.list_runs(job_id=job_id, expand_tasks=False)
            if (
                r.start_time
                and datetime.fromtimestamp(r.start_time / 1000, tz=TZ).date() == today
            )
        ]
        if not runs_today:
            bot.send_message(
                CHAT_ID,
                f"📅 **{job.settings.name}**\nNo runs today.",
                parse_mode="Markdown",
            )
            return

        r = max(runs_today, key=lambda x: x.start_time)
        start = datetime.fromtimestamp(r.start_time / 1000, tz=TZ).strftime("%H:%M")
        if r.end_time:
            end = datetime.fromtimestamp(r.end_time / 1000, tz=TZ).strftime("%H:%M")
            dur = f"{start} – {end}"
        else:
            dur = f"Started {start} (still running)"

        if r.state.result_state == RunResultState.SUCCESS:
            msg = f"✅ **{job.settings.name}**\nSUCCESS\n⏰ {dur}\nRun `{r.run_id}`"
        elif r.state.result_state == RunResultState.FAILED:
            kb = types.InlineKeyboardMarkup()
            kb.add(
                types.InlineKeyboardButton(
                    "🔧 Repair",
                    callback_data=json.dumps({"action": "repair", "run_id": r.run_id}),
                )
            )
            msg = (
                f"❌ **{job.settings.name}**\nFAILED\n⏰ {dur}\n"
                f"Run `{r.run_id}`\n{r.state.state_message or ''}"
            )
            bot.send_message(CHAT_ID, msg, reply_markup=kb, parse_mode="Markdown")
            return
        else:
            msg = f"🔄 **{job.settings.name}**\nRUNNING\n⏰ {dur}\nRun `{r.run_id}`"

        bot.send_message(CHAT_ID, msg, parse_mode="Markdown")

    except Exception as e:
        bot.send_message(CHAT_ID, f"❌ Error: {e}")

# ------------------------------------------------------------------
# Repair helper
# ------------------------------------------------------------------
def repair_databricks_job(run_id):
    w = _ws()
    try:
        resp = w.jobs.repair_run(run_id, rerun_all_failed_tasks=True)
        bot.send_message(
            CHAT_ID,
            f"✅ Repair started!\nOriginal: `{run_id}`\nRepair run: `{resp.run_id}`",
        )
    except Exception as e:
        bot.send_message(CHAT_ID, f"❌ Repair failed: {e}")

# ------------------------------------------------------------------
# Scheduler
# ------------------------------------------------------------------
times = ("07:45","08:30","09:30","11:00","12:00","13:00","15:00","18:00","20:00","23:30")
for t in times:
    schedule.every().day.at(t).do(databricks_job_notification)

# ------------------------------------------------------------------
# Entry-point
# ------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    databricks_job_notification()  # first run
    while True:
        schedule.run_pending()
        bot.polling(none_stop=True)
        time.sleep(1)
