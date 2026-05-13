import os
import asyncio
import time
import threading
from fyers_apiv3 import fyersModel
from fyers_apiv3.FyersWebsocket import data_ws
from scanner.analysis_engine import AnalysisEngine
from scanner.notifier import TelegramNotifier
from dotenv import load_dotenv

load_dotenv()

class FyersDataStream:
    def __init__(self, client_id: str, access_token: str, loop: asyncio.AbstractEventLoop, callback: callable):
        self.client_id = client_id
        self.access_token = access_token
        self.loop = loop
        self.broadcast_callback = callback
        self.symbols = []
        self.engine = AnalysisEngine()
        
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.notifier = TelegramNotifier(token=token, chat_id=chat_id)
        
        self.ws = None
        self.is_connected = False
        self.sub_queue = []

    def on_message(self, message):
        """Full Mode Parser (v6.3)"""
        try:
            if not message: return
            ticks = message if isinstance(message, list) else [message]
            
            for tick in ticks:
                # DEBUG: Inspect the first few packets
                if time.time() % 60 < 2:
                    print(f"DEBUG RAW: {tick}")

                if isinstance(tick, dict) and tick.get("type") in ["cn", "lit", "ful", "sub"]:
                    print(f"DEBUG System: {tick}")
                    continue
                
                # In Full Mode, V3 uses 'symbol' and 'ltp' / 'ltq'
                sym = tick.get("symbol") or tick.get("n")
                if not sym: continue
                
                ltp = float(tick.get("ltp") or tick.get("lp") or tick.get("v", {}).get("lp") or 0.0)
                ltq = int(tick.get("ltq") or tick.get("last_traded_qty") or tick.get("v", {}).get("ltq") or 0)
                
                if ltp == 0.0: continue

                raw_tick = {
                    "symbol": sym, "ltp": ltp, "last_traded_qty": ltq,
                    "bid_price": ltp, "ask_price": ltp
                }
                
                deal = self.engine.analyze_tick(raw_tick)
                if deal:
                    asyncio.run_coroutine_threadsafe(self.broadcast_callback(deal), self.loop)
                    
        except Exception as e:
            print(f"❌ WS Parser Error: {e}")

    def on_error(self, message):
        print(f"❌ Fyers WS Error: {message}")

    def on_close(self, message):
        print("🏠 Fyers WS Connection Closed")
        self.is_connected = False

    def on_open(self):
        print("✅ Fyers WS Connected!")
        self.is_connected = True
        
        # PROVEN NAMES FOR V3
        proven_symbols = ["NSE:RELIANCE-EQ", "NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]
        print(f"🚀 Subscribing to PROVEN symbols: {proven_symbols}")
        self.ws.subscribe(symbols=proven_symbols, data_type="symbolData")
        
        def delayed_sub():
            time.sleep(5)
            if self.sub_queue:
                others = [s for s in self.sub_queue if s not in proven_symbols]
                print(f"🚀 Now Subscribing to remaining {len(others)} symbols in chunks...")
                self.subscribe_symbols(others)
                self.sub_queue = []
        
        threading.Thread(target=delayed_sub, daemon=True).start()

    def subscribe_symbols(self, symbols: list):
        if not symbols: return
        valid_symbols = list(set([s for s in symbols if s and isinstance(s, str)]))
        
        # SAFETY: Chunk size of 10 with 1s delay prevents buffer scrambling
        CHUNK_SIZE = 10
        for i in range(0, len(valid_symbols), CHUNK_SIZE):
            chunk = valid_symbols[i : i + CHUNK_SIZE]
            if self.is_connected and self.ws:
                self.ws.subscribe(symbols=chunk, data_type="symbolData")
                time.sleep(1)
            else:
                self.sub_queue.extend(chunk)
        self.symbols = list(set(self.symbols + valid_symbols))

    def start(self):
        token_str = f"{self.client_id}:{self.access_token}"
        print(f"🚀 Launching WebSocket for: {self.client_id}")
        
        self.ws = data_ws.FyersDataSocket(
            access_token=token_str,
            log_path=os.getcwd(),
            litemode=False, # BACK TO FULL MODE FOR VOLUME
            reconnect=True,
            on_connect=self.on_open,
            on_close=self.on_close,
            on_error=self.on_error,
            on_message=self.on_message
        )
        self.ws.connect()
