from telethon.sync import TelegramClient
from telethon.sessions import StringSession

# PLEASE KEEP THIS ENVIRONMENT VARIABLE SECRET!!!
api_id = 20323297
api_hash = '440827e1c1301fd5ec8f97e41bf089e6'
string_session = "1BVtsOJMBu1hB_wTzxSCqPBxu5FHfr_cRWCTxOMxXbfiG0QP-uBI9NBS52tcTW5BQpddSLkT1vRLWt_CfE5ZYyhRaAoch1zmmAaidt7AVeUBdF3RxR6yVMC59lwOOZFtqKcVAL9WGt7In2KfrtIvwfAl-xQaGf08H-1mM0l4GsYIFLtPEYTVTnJIospMBtzoeKShWmHxJYwlZdGj_hDHlEiRyRuTMZhOXw_m2XmlQ-txnc3WxDaKjh8MLoBLu6wuFbJ_nsNtVcse0tkcRqJ8ZLOmUYfYa4SjV5_owj_ECMjV0p34NWFRdjATMcMY-hzsxzN95YcdoUN0VXdQck3TfWDo6impt12E="

def send_myself(message):
    with TelegramClient(StringSession(string_session), api_id, api_hash) as client:
        client.send_message("me", message)