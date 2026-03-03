import os
from dotenv import load_dotenv
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
import datetime as dt

# PLEASE KEEP THIS ENVIRONMENT VARIABLE SECRET!!!
api_id = 20323297
api_hash = '440827e1c1301fd5ec8f97e41bf089e6'
string_session = "1BVtsOJMBu1hB_wTzxSCqPBxu5FHfr_cRWCTxOMxXbfiG0QP-uBI9NBS52tcTW5BQpddSLkT1vRLWt_CfE5ZYyhRaAoch1zmmAaidt7AVeUBdF3RxR6yVMC59lwOOZFtqKcVAL9WGt7In2KfrtIvwfAl-xQaGf08H-1mM0l4GsYIFLtPEYTVTnJIospMBtzoeKShWmHxJYwlZdGj_hDHlEiRyRuTMZhOXw_m2XmlQ-txnc3WxDaKjh8MLoBLu6wuFbJ_nsNtVcse0tkcRqJ8ZLOmUYfYa4SjV5_owj_ECMjV0p34NWFRdjATMcMY-hzsxzN95YcdoUN0VXdQck3TfWDo6impt12E="
offset_date = dt.datetime.now() - dt.timedelta(hours=24)
def telegram_import():
    messages = []
    with TelegramClient(StringSession(string_session), api_id, api_hash) as client:
        channels = ["t.me/JobHitchpt", "t.me/SearchForJob", "t.me/nextjobs", "t.me/SgJobsFlash", "t.me/SgSecurityOfficer"]
        for channel in channels:
            for message in client.iter_messages(channel, offset_date=offset_date, reverse = True):
                msg = message.to_dict()
                msg_text = msg['message'] if 'message' in msg else "" # This is because some messages will NOT have text!
                msg_date = msg['date'] # But it's a datetime object!
                msg_source = channel
                messages.append({
                    "text": msg_text, 
                    "date": msg_date.isoformat(), # Convert to ISO string for JSON serialization
                    "source_channel": msg_source.strip("t.me/"),
                    "source": "telegram" # To delcare accordingly
                })
    return messages