import asyncio
import os
import threading
import time
import re
from datetime import datetime, timedelta
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from dotenv import load_dotenv
from collections import deque

from scanner.fyers_auth import get_access_token
from scanner.fyers_client import FyersDataStream
from scanner.symbol_manager import get_fyers_data_client, get_symbol_pool, get_all_symbols_to_subscribe

load_dotenv()

# Global State
connected_clients = set()
activity_stats = {} 
deal_cache = deque(maxlen=20000) # Increased capacity
fyers_stream = None
nifty_strikes_to_sub = []

def get_ist_time():
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%H:%M:%S")

def extract_strike_type(sym: str):
    if not sym or not isinstance(sym, str): return None, None
    # Matches patterns like NIFTY2651423500CE
    match = re.search(r'(\d{5})(CE|PE)$', sym)
    if match: return int(match.group(1)), match.group(2)
    # Generic match for any digits followed by CE/PE
    match = re.search(r'(\d+)(CE|PE)$', sym)
    if match: return int(match.group(1)), match.group(2)
    return None, None

async def broadcast_deal(deal: dict):
    if isinstance(deal, dict) and "symbol" in deal:
        # Diagnostic: Only print if significant
        if deal.get("score", 0) >= 4:
            print(f"🔥 INSTITUTIONAL: {deal.get('symbol')} score={deal.get('score')} qty={deal.get('qty')}", flush=True)
        
        activity_stats[deal["symbol"]] = activity_stats.get(deal["symbol"], 0) + 1
        deal_cache.append(deal)
        
    if not connected_clients: return
    disconnected = set()
    for client in connected_clients:
        try: await client.send_json(deal)
        except Exception: disconnected.add(client)
    for c in disconnected: connected_clients.remove(c)

async def odx_cycle_loop():
    global fyers_stream, deal_cache, nifty_strikes_to_sub
    
    while True:
        if fyers_stream and fyers_stream.engine:
            try:
                # FIX 4: 60-second Sliding Window
                now_ts = time.time()
                current_deals = [d for d in list(deal_cache) if now_ts - float(d.get("timestamp", 0)) <= 60]
                
                print(f"ODX CYCLE: Processing {len(current_deals)} deals from last 60s", flush=True)

                strikes_data = []
                client_id = os.getenv("FYERS_CLIENT_ID")
                access_token = os.getenv("FYERS_ACCESS_TOKEN") or (fyers_stream.access_token if fyers_stream else None)
                
                if not client_id or not access_token:
                    await asyncio.sleep(5)
                    continue

                client = get_fyers_data_client(client_id, access_token)
                
                quotes_res = client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
                if quotes_res.get("s") == "ok":
                    spot = float(quotes_res["d"][0]["v"].get("lp") or 0.0)
                    atm = round(spot / 50) * 50
                    
                    oc_payload = {"symbol": "NSE:NIFTY50-INDEX", "strikecount": 5, "timestamp": ""}
                    response = client.optionchain(data=oc_payload)
                    
                    if response.get("s") == "ok":
                        raw_chain = response.get("data", {}).get("optionsChain", [])
                        current_time = get_ist_time()
                        
                        strike_map = {}
                        symbol_lookup = {}
                        
                        for item in raw_chain:
                            strike = int(item.get("strike_price") or 0)
                            if strike <= 0: continue
                            opt_type = item.get("option_type")
                            sym = item.get("symbol")
                            if strike not in strike_map: strike_map[strike] = {"ce": {}, "pe": {}}
                            
                            if opt_type == "CE":
                                strike_map[strike]["ce"] = {"symbol": sym, "oi": item.get("oi", 0)}
                                symbol_lookup[sym] = (strike, "CE")
                            elif opt_type == "PE":
                                strike_map[strike]["pe"] = {"symbol": sym, "oi": item.get("oi", 0)}
                                symbol_lookup[sym] = (strike, "PE")

                        temp_nifty_strikes = []
                        ce_buy_l, ce_sell_l, pe_buy_l, pe_sell_l = 0.0, 0.0, 0.0, 0.0
                        
                        # FIX 2: Fuzzy Symbol Matching Fallback
                        deals_by_strike = {} 
                        for d in current_deals:
                            sym = d.get("symbol", "")
                            if not sym: continue
                            
                            if sym in symbol_lookup:
                                d_strike, d_type = symbol_lookup[sym]
                            else:
                                d_strike, d_type = extract_strike_type(sym)
                            
                            if d_strike is not None:
                                if d_strike not in deals_by_strike: deals_by_strike[d_strike] = {"CE": [], "PE": []}
                                deals_by_strike[d_strike][d_type].append(d)

                        for strike in sorted(strike_map.keys()):
                            row = strike_map[strike]
                            ce_row, pe_row = row["ce"], row["pe"]
                            if not ce_row or not pe_row: continue
                            
                            ce_sym, pe_sym = ce_row["symbol"], pe_row["symbol"]
                            ce_oi, pe_oi = float(ce_row.get("oi") or 0.0), float(pe_row.get("oi") or 0.0)
                            
                            if abs(strike - atm) <= 250: temp_nifty_strikes.extend([ce_sym, pe_sym])

                            strike_deals = deals_by_strike.get(strike, {"CE": [], "PE": []})
                            ce_deals = strike_deals["CE"]
                            pe_deals = strike_deals["PE"]
                            
                            def get_agg_and_flow(deals, sym_type):
                                nonlocal ce_buy_l, ce_sell_l, pe_buy_l, pe_sell_l
                                # FIX 1: Remove score filter from ODX aggregator
                                sig_deals = [d for d in deals if d.get("symbol", "")]
                                if not sig_deals: return "----", "----", 0.0
                                
                                # FIX 3: Lakhs Math (100,000 Divisor)
                                d_buy_l  = sum(float(d.get("qty", 0)) * float(d.get("ltp", 0)) for d in sig_deals if d.get("direction") == "BUY") / 100000
                                d_sell_l = sum(float(d.get("qty", 0)) * float(d.get("ltp", 0)) for d in sig_deals if d.get("direction") == "SELL") / 100000
                                
                                net_flow_l = d_buy_l - d_sell_l
                                
                                if sym_type == "CE":
                                    ce_buy_l += d_buy_l
                                    ce_sell_l += d_sell_l
                                else:
                                    pe_buy_l += d_buy_l
                                    pe_sell_l += d_sell_l
                                
                                buys = sum(float(d.get("qty") or 0.0) for d in sig_deals if d.get("direction") == "BUY")
                                sells = sum(float(d.get("qty") or 0.0) for d in sig_deals if d.get("direction") == "SELL")
                                agg = "BUY" if buys > sells else "SELL" if sells > buys else "----"
                                participants = [d.get("participant") for d in sig_deals if d.get("participant") and d.get("participant") != "----"]
                                who = max(set(participants), key=participants.count) if participants else "----"
                                return agg, who, net_flow_l

                            ce_agg, ce_who, ce_pulse_l = get_agg_and_flow(ce_deals, "CE")
                            pe_agg, pe_who, pe_pulse_l = get_agg_and_flow(pe_deals, "PE")
                            final_who = ce_who if ce_who != "----" else pe_who

                            label = "Straddle" if ce_pulse_l != 0 and pe_pulse_l != 0 else "Call write" if ce_pulse_l < 0 else "Put write" if pe_pulse_l < 0 else "Accumulate"
                            if abs(ce_pulse_l) > 1.0 or abs(pe_pulse_l) > 1.0: label += "⚡"
                            
                            strikes_data.append({
                                "strike": int(strike), "is_atm": strike == atm,
                                "ce_delta_l": float(ce_pulse_l), "pe_delta_l": float(pe_pulse_l),
                                "ce_oi_l": float(ce_oi * 50 / 100000), "pe_oi_l": float(pe_oi * 50 / 100000),
                                "ce_aggressor": str(ce_agg), "pe_aggressor": str(pe_agg),
                                "who": str(final_who), "label": str(label)
                            })

                        new_strikes = list(set(s for s in temp_nifty_strikes if s and isinstance(s, str)))
                        if set(new_strikes) != set(nifty_strikes_to_sub):
                            nifty_strikes_to_sub = new_strikes
                            if fyers_stream: 
                                fyers_stream.subscribe_symbols(nifty_strikes_to_sub)

                        bias_l = (ce_buy_l + pe_sell_l) - (ce_sell_l + pe_buy_l)
                        
                        odx_payload = {
                            "time": str(current_time), "spot": float(spot), "atm": int(atm), "pcr": 0.91,
                            "is_first_cycle": False, "strikes": strikes_data,
                            "aggregator": {"aggregate_bias_l": float(bias_l), "ce_buy": round(float(ce_buy_l), 2), "ce_sell": round(float(ce_sell_l), 2), "pe_buy": round(float(pe_buy_l), 2), "pe_sell": round(float(pe_sell_l), 2)}
                        }
                        fyers_stream.notifier.send_odx_heartbeat(odx_payload)

            except Exception as e: print(f"ODX Error: {e}")
        await asyncio.sleep(60)

async def rotation_cycle_loop():
    global fyers_stream, activity_stats, nifty_strikes_to_sub
    while True:
        if fyers_stream and hasattr(fyers_stream, 'symbols'):
            try:
                client_id, access_token = os.getenv("FYERS_CLIENT_ID"), os.getenv("FYERS_ACCESS_TOKEN")
                client = get_fyers_data_client(client_id, access_token)
                reserved = list(set(nifty_strikes_to_sub))[:15]
                current_symbols = list(fyers_stream.symbols) if fyers_stream.symbols else []
                rotating_symbols = [s for s in current_symbols if s not in reserved]
                stats = sorted([(s, activity_stats.get(s, 0)) for s in rotating_symbols], key=lambda x: x[1])
                cold_symbols = [x[0] for x in stats[:3]] if len(stats) >= 3 else []
                pool, _ = get_symbol_pool(client)
                available_pool = [s for s in pool if s not in current_symbols and s not in reserved]
                new_symbols = available_pool[:len(cold_symbols)]
                if cold_symbols: fyers_stream.unsubscribe_symbols(cold_symbols)
                missing_reserved = [s for s in reserved if s not in current_symbols]
                final_to_sub = list(set(new_symbols + missing_reserved))
                if final_to_sub: fyers_stream.subscribe_symbols(final_to_sub)
                activity_stats = {s: 0 for s in fyers_stream.symbols}
            except Exception as e: print(f"Rotation Error: {e}")
        await asyncio.sleep(3600) 

async def oi_polling_loop():
    global fyers_stream, nifty_strikes_to_sub
    while True:
        if fyers_stream:
            try:
                client_id, access_token = os.getenv("FYERS_CLIENT_ID"), os.getenv("FYERS_ACCESS_TOKEN")
                client = get_fyers_data_client(client_id, access_token)
                current_subbed = list(fyers_stream.symbols) if fyers_stream.symbols else []
                all_to_poll = list(set(current_subbed + nifty_strikes_to_sub))
                opt_syms = [s for s in all_to_poll if "CE" in s or "PE" in s]
                if opt_syms:
                    for i in range(0, len(opt_syms), 50):
                        batch = ",".join(opt_syms[i:i+50])
                        q = client.quotes({"symbols": batch})
                        if q.get("s") == "ok":
                            for d in q.get("d", []):
                                sym, oi = d.get("n"), d.get("v", {}).get("oi")
                                if sym and oi is not None and oi > 0:
                                    fyers_stream.engine.update_oi(sym, oi)
            except Exception as e: print(f"OI Poll Error: {e}")
        await asyncio.sleep(10)

@asynccontextmanager
async def lifespan(app: FastAPI):
    client_id = os.getenv("FYERS_CLIENT_ID")
    access_token = os.getenv("FYERS_ACCESS_TOKEN")
    
    if client_id and not access_token:
        try:
            from scanner.fyers_auth import get_access_token
            access_token = get_access_token()
            os.environ["FYERS_ACCESS_TOKEN"] = access_token
        except Exception as e: print(f"Auth Error: {e}")

    if client_id and access_token:
        global fyers_stream
        loop = asyncio.get_running_loop()
        fyers_stream = FyersDataStream(client_id, access_token, loop, broadcast_deal)

        client = get_fyers_data_client(client_id, access_token)
        initial_symbols, _ = get_all_symbols_to_subscribe(client)
        fyers_stream.sub_queue = initial_symbols

        print("🚀 Launching Fyers WebSocket Stream thread...", flush=True)
        threading.Thread(target=fyers_stream.start, daemon=True).start()
    
    asyncio.create_task(oi_polling_loop())
    asyncio.create_task(rotation_cycle_loop())
    asyncio.create_task(odx_cycle_loop())
    yield

app = FastAPI(title="Institutional ODX Platform", lifespan=lifespan)

@app.get("/")
async def root(): return {"status": "online"}

@app.websocket("/ws/stream")
async def websocket_endpoint(websocket: WebSocket):
    await websocket.accept()
    connected_clients.add(websocket)
    try:
        while True:
            data = await websocket.receive_text()
            if data == "ping": await websocket.send_text("pong")
    except WebSocketDisconnect: connected_clients.remove(websocket)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
