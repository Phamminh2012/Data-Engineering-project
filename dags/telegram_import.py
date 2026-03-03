import asyncio
import datetime as dt
import os
import json
import requests
from dotenv import load_dotenv
from datetime import timedelta, datetime
from telethon import TelegramClient

#get apip key
load_dotenv(".env") 

# load Telegram API credentials
TELEGRAM_API_ID   = int(os.getenv("TELEGRAM_API_ID"))
TELEGRAM_API_HASH = os.getenv("TELEGRAM_API_HASH")
TELEGRAM_CHANNELS = ["t.me/JobHitchpt", "t.me/SearchForJob", "t.me/nextjobs", "t.me/SgJobsFlash", "t.me/SgPartTimeAgency"]

# Run scraping session
async def fetch_telegram_jobs(
    channels: list[str] = TELEGRAM_CHANNELS,
    hours_back: int = 24,
    session_name: str = "session",
    output_path: str = f"telescraping_output-{datetime.now()}.json",
) -> list[dict]:
  
    messages = []
    since = dt.datetime.now() - timedelta(hours=hours_back)

    client = TelegramClient(session_name, TELEGRAM_API_ID, TELEGRAM_API_HASH)
    await client.start()

    for channel in channels:
        count = 0
        async for message in client.iter_messages(channel, offset_date=since, reverse=True):
            msg = message.to_dict()
            msg["channel_found"] = channel
            messages.append(msg)
            count += 1
        print(f"[Telegram] {channel}: {count} messages fetched.")

    await client.disconnect()

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(messages, f, ensure_ascii=False, indent=4, default=str)
    print(f"[Telegram] Saved {len(messages)} messages to {output_path}")

    return messages