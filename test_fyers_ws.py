import os
import time
import threading
from fyers_apiv3.FyersWebsocket import data_ws
from dotenv import load_dotenv

load_dotenv()

client_id = os.getenv("FYERS_CLIENT_ID")
access_token = os.getenv("FYERS_ACCESS_TOKEN")

def on_message(message):
    print(f"🔥 DATA RECEIVED: {message}")

def on_error(message):
    print(f"❌ ERROR: {message}")

def on_close(message):
    print("🏠 CONNECTION CLOSED")

def on_open():
    print("📡 CONNECTION OPENED. Waiting 3 seconds...")
    time.sleep(3)
    
    # Try multiple subscription formats just in case
    symbols = ["NSE:RELIANCE-EQ", "NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]
    print(f"🚀 Subscribing to: {symbols}")
    
    # Try symbolData
    ws.subscribe(symbols=symbols, data_type="symbolData")
    
    # Try some depth just to see if ANY packet comes
    # ws.subscribe(symbols=["NSE:RELIANCE-EQ"], data_type="depthData")

# Try with the FULL ID format first
token_str = f"{client_id}:{access_token}"

ws = data_ws.FyersDataSocket(
    access_token=token_str,
    log_path=os.getcwd(),
    litemode=True,
    reconnect=True,
    on_connect=on_open,
    on_close=on_close,
    on_error=on_error,
    on_message=on_message
)

print(f"🚀 Starting Deep Test for {client_id}...")
print(f"Using Token Prefix: {token_str[:20]}...")

def run_ws():
    ws.connect()

t = threading.Thread(target=run_ws)
t.start()

# Keep main thread alive for 60 seconds
for i in range(60):
    time.sleep(1)
    if i % 10 == 0:
        print(f"⏱️ Waiting... ({i}s/60s)")
