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
        self.prev_volumes = {} # Fallback for qty
        
        token = os.getenv("TELEGRAM_BOT_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")
        self.notifier = TelegramNotifier(token=token, chat_id=chat_id)
        
        self.index_ws = None
        self.data_ws = None
        self.is_connected = False
        self.sub_queue = []

    def on_message(self, message):
        """Dual-Socket Parser (v7.1)"""
        try:
            if not message: return
            ticks = message if isinstance(message, list) else [message]
            
            for tick in ticks:
                if isinstance(tick, dict) and tick.get("type") in ["cn", "lit", "ful", "sub"]:
                    continue
                
                sym = tick.get("symbol") or tick.get("n")
                if not sym: continue
                
                # PRICE
                ltp = float(tick.get("ltp") or tick.get("lp") or tick.get("v", {}).get("lp") or 0.0)
                
                # QUANTITY (Primary: ltq, Fallback: Volume Delta)
                ltq = int(tick.get("ltq") or tick.get("last_traded_qty") or 0)
                total_vol = int(tick.get("vol") or tick.get("v", {}).get("vol") or 0)
                
                if ltq == 0 and total_vol > 0:
                    last_vol = self.prev_volumes.get(sym, 0)
                    ltq = total_vol - last_vol if last_vol > 0 else 0
                    self.prev_volumes[sym] = total_vol
                
                if ltp == 0.0: continue

                raw_tick = {
                    "symbol": sym, "ltp": ltp, "last_traded_qty": ltq,
                    "bid_price": ltp, "ask_price": ltp, "avg_trade_price": ltp
                }
                
                deal = self.engine.analyze_tick(raw_tick)
                if deal:
                    # Update qty in deal if calculated from delta
                    if deal.get("qty") == 0 and ltq > 0: deal["qty"] = ltq
                    asyncio.run_coroutine_threadsafe(self.broadcast_callback(deal), self.loop)
                    
        except Exception as e:
            print(f"❌ WS Parser Error: {e}")

    def on_error(self, message):
        print(f"❌ Fyers WS Error: {message}")

    def on_close(self, message):
        print("🏠 Fyers WS Connection Closed")

    def on_open_index(self):
        print("✅ Index Socket Connected!")
        indices = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]
        self.index_ws.subscribe(symbols=indices, data_type="symbolData")

    def on_open_data(self):
        print("✅ Data Socket Connected!")
        self.is_connected = True
        if self.sub_queue:
            print(f"🚀 Subscribing to {len(self.sub_queue)} Symbols...")
            self.subscribe_symbols(self.sub_queue)
            self.sub_queue = []

    def subscribe_symbols(self, symbols: list):
        if not symbols: return
        valid_symbols = list(set([s for s in symbols if s and isinstance(s, str)]))
        
        # Bypassing the Index Infection by sending only non-indices here
        clean_symbols = [s for s in valid_symbols if "INDEX" not in s]
        
        CHUNK_SIZE = 20
        for i in range(0, len(clean_symbols), CHUNK_SIZE):
            chunk = clean_symbols[i : i + CHUNK_SIZE]
            if self.is_connected and self.data_ws:
                self.data_ws.subscribe(symbols=chunk, data_type="symbolData")
                time.sleep(1)
            else:
                self.sub_queue.extend(chunk)
        self.symbols = list(set(self.symbols + clean_symbols))

    def start(self):
        token_str = f"{self.client_id}:{self.access_token}"
        print(f"🚀 Launching Isolated Dual-Socket for: {self.client_id}")
        
        # 1. INDEX SOCKET
        self.index_ws = data_ws.FyersDataSocket(
            access_token=token_str, log_path=os.getcwd(), litemode=False,
            on_connect=self.on_open_index, on_close=self.on_close,
            on_error=self.on_error, on_message=self.on_message
        )
        
        # 2. DATA SOCKET
        self.data_ws = data_ws.FyersDataSocket(
            access_token=token_str, log_path=os.getcwd(), litemode=False,
            on_connect=self.on_open_data, on_close=self.on_close,
            on_error=self.on_error, on_message=self.on_message
        )
        
        self.index_ws.connect()
        self.data_ws.connect()
