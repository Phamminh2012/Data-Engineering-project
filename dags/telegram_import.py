import asyncio
import datetime as dt
import os
import json
from dotenv import load_dotenv
from datetime import timedelta, datetime
from telethon import TelegramClient
from telethon.sync import TelegramClient as SyncTelegramClient

load_dotenv(".env")

TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_CHANNELS = [
    "t.me/JobHitchpt",
    "t.me/SearchForJob",
    "t.me/nextjobs",
    "t.me/SgJobsFlash",
    "t.me/SgPartTimeAgency",
]


def fetch_telegram_jobs(
    channels: list = TELEGRAM_CHANNELS,
    hours_back: int = 24,
    session_name: str = "session",
    output_path: str = None,
) -> list:
    if output_path is None:
        output_path = f"telescraping_output-{datetime.now()}.json"

    messages = []
    since = dt.datetime.now(dt.timezone.utc) - timedelta(hours=hours_back)

    # telethon.sync makes all calls synchronous — no async/await needed
    with SyncTelegramClient(session_name, TELEGRAM_API_ID, TELEGRAM_API_HASH) as client:
        for channel in channels:
            count = 0
            for message in client.iter_messages(channel, offset_date=since, reverse=True):
                msg = message.to_dict()
                msg["channel_found"] = channel
                messages.append(msg)
                count += 1
            print(f"[Telegram] {channel}: {count} messages fetched.")

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=4, default=str)

    print(f"[Telegram] Saved {len(messages)} messages to {output_path}")
    return messages


# --- Airflow DAG usage example ---
# from airflow.operators.python import PythonOperator
# task = PythonOperator(
#     task_id="scrape_telegram",
#     python_callable=scrape_telegram,
# )

if __name__ == "__main__":
    fetch_telegram_jobs()