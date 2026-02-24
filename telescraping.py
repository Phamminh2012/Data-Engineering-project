import asyncio
import datetime as dt
from datetime import timedelta
import json
from telethon import TelegramClient

MESSAGE_LIST = []
CHANNELS = ['t.me/JobHitchpt', 't.me/SearchForJob', 't.me/nextjobs']

LAST_24_HOURS = dt.datetime.now() - timedelta(hours=24)

api_id = 20323297
api_hash = "440827e1c1301fd5ec8f97e41bf089e6"

client = TelegramClient("session", api_id, api_hash)

async def main():
    await client.start()

    for channel in CHANNELS:
        # Use the channel variable here (not hardcoded)
        async for message in client.iter_messages(
            channel,
            offset_date=LAST_24_HOURS,
            reverse=True
        ):
            msg = message.to_dict()
            msg["channel_found"] = channel
            MESSAGE_LIST.append(msg)

    with open("telescraping_output_1.json", "w", encoding="utf-8") as f:
        json.dump(MESSAGE_LIST, f, ensure_ascii=False, indent=4, default=str)

    await client.disconnect()
    print(f"Done. Saved {len(MESSAGE_LIST)} messages.")

if __name__ == "__main__":
    asyncio.run(main())
