import os
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import datetime as dt
import json

# PLEASE KEEP THIS ENVIRONMENT VARIABLE SECRET!!!
api_id = int(os.getenv("api_id"))
api_hash = os.getenv("TELEGRAM_API_HASH")
string_session = os.getenv("string_session")
offset_date = dt.datetime.now() - dt.timedelta(hours=24)
def telegram_import():
    messages = []
    f_path = "/opt/airflow/data/raw/telegram_search.json"
    with TelegramClient(StringSession(string_session), api_id, api_hash) as client:
        channels = ["t.me/JobHitchpt", 
                    "t.me/SearchForJob", 
                    "t.me/nextjobs", 
                    "t.me/SgJobsFlash", 
                    "t.me/SgSecurityOfficer",
                    "t.me/sgpttempjobs"]
        for channel in channels:
            for message in client.iter_messages(channel, offset_date=offset_date, reverse = True):
                msg = message.to_dict()
                msg_text = msg['message'] if 'message' in msg else "" # This is because some messages will NOT have text!
                msg_date = msg['date'] # But it's a datetime object!
                msg_source = channel
                messages.append({
                    "msg_id": msg['id'],
                    "text": msg_text, 
                    "date": msg_date.isoformat(), # Convert to ISO string for JSON serialization
                    "source_channel": msg_source.strip("t.me/"),
                    "source": "telegram" # To delcare accordingly
                })
    with open(f_path, 'w') as f:
        json.dump(messages, f, indent = 4)
    
    return f_path