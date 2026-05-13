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
        self.prev_volumes = {} # Track volume for delta
        
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.notifier = TelegramNotifier(token=token, chat_id=chat_id)
        
        self.ws = None
        self.is_connected = False
        self.sub_queue = []

    def on_message(self, message):
        """Lite Mode Parser with Volume Delta (v5.8)"""
        try:
            if not message: return
            ticks = message if isinstance(message, list) else [message]
            
            for tick in ticks:
                # DEBUG: Print raw tick for the first few seconds
                if time.time() % 30 < 2:
                    print(f"DEBUG RAW TICK: {tick}")
                
                if isinstance(tick, dict) and tick.get("type") in ["cn", "lit", "ful", "sub"]:
                    print(f"DEBUG System: {tick}")
                    continue
                
                sym = tick.get("symbol") or tick.get("n")
                if not sym: continue
                
                # In Lite Mode, ltp is in 'lp' or 'v' -> 'lp'
                ltp = float(tick.get("ltp") or tick.get("lp") or 0.0)
                total_vol = int(tick.get("vol") or tick.get("v", {}).get("vol") or 0)
                
                # Calculate LTQ (Delta)
                last_vol = self.prev_volumes.get(sym, 0)
                ltq = total_vol - last_vol if last_vol > 0 else 0
                self.prev_volumes[sym] = total_vol
                if ltq < 0: ltq = 0 

                if ltp == 0.0: continue
                
                # Proof of life every 10s
                if time.time() % 10 < 0.2:
                    print(f"🔥 TICK: {sym} @ {ltp} (V-Delta: {ltq})")

                raw_tick = {
                    "symbol": sym, "ltp": ltp, "last_traded_qty": ltq,
                    "bid_price": ltp, "ask_price": ltp
                }
                
                deal = self.engine.analyze_tick(raw_tick)
                if deal and deal.get("score", 0) >= 4:
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
        
        proven_symbols = ["NSE:RELIANCE-EQ", "NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]
        print(f"🚀 Subscribing to PROVEN symbols: {proven_symbols}")
        self.ws.subscribe(symbols=proven_symbols, data_type="symbolData")
        
        def delayed_sub():
            time.sleep(10)
            if self.sub_queue:
                others = [s for s in self.sub_queue if s not in proven_symbols]
                print(f"🚀 Now Subscribing to remaining {len(others)} symbols...")
                self.subscribe_symbols(others)
                self.sub_queue = []
        
        threading.Thread(target=delayed_sub, daemon=True).start()

    def subscribe_symbols(self, symbols: list):
        if not symbols: return
        valid_symbols = list(set([s for s in symbols if s and isinstance(s, str)]))
        
        CHUNK_SIZE = 20
        for i in range(0, len(valid_symbols), CHUNK_SIZE):
            chunk = valid_symbols[i : i + CHUNK_SIZE]
            print(f"📡 Sending Batch ({len(chunk)} symbols): {chunk[:5]}...")
            if self.is_connected and self.ws:
                self.ws.subscribe(symbols=chunk, data_type="symbolData")
                time.sleep(2)
            else:
                self.sub_queue.extend(chunk)
        self.symbols = list(set(self.symbols + valid_symbols))

    def start(self):
        token_str = f"{self.client_id}:{self.access_token}"
        print(f"🚀 Launching WebSocket for: {self.client_id}")
        
        self.ws = data_ws.FyersDataSocket(
            access_token=token_str,
            log_path=os.getcwd(),
            litemode=True, # LITE MODE FOR STABILITY
            reconnect=True,
            on_connect=self.on_open,
            on_close=self.on_close,
            on_error=self.on_error,
            on_message=self.on_message
        )
        self.ws.connect()
