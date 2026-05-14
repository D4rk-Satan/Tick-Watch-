import asyncio
import os
import threading
import time
import re
import traceback
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
deal_cache = deque(maxlen=20000) 
fyers_stream = None
nifty_strikes_to_sub = []

def get_ist_time():
    return (datetime.utcnow() + timedelta(hours=5, minutes=30)).strftime("%H:%M:%S")

def extract_strike_type(sym: str):
    if not sym or not isinstance(sym, str): return None, None
    match = re.search(r'(\d+)(CE|PE)$', sym)
    if match: return int(match.group(1)), match.group(2)
    return None, None

async def broadcast_deal(deal: dict):
    if isinstance(deal, dict) and "symbol" in deal:
        activity_stats[deal["symbol"]] = activity_stats.get(deal["symbol"], 0) + 1
        deal_cache.append(deal)
        
    if not connected_clients: return
    disconnected = set()
    for client in connected_clients:
        try: await client.send_json(deal)
        except Exception: disconnected.add(client)
    for c in disconnected: connected_clients.remove(c)

async def check_instant_alerts(notifier, strikes_data: list, spot: float, atm: int):
    """Fires immediate Telegram alert if any strike hits >50L in current window"""
    THRESHOLD_L = 50.0
    
    for strike in strikes_data:
        ce_flow = abs(float(strike.get("ce_delta_l", 0)))
        pe_flow = abs(float(strike.get("pe_delta_l", 0)))
        total   = ce_flow + pe_flow
        
        if total >= THRESHOLD_L:
            strike_price = strike.get("strike")
            is_atm = strike.get("is_atm", False)
            atm_label = " [ATM]" if is_atm else f" [{'+' if strike_price > atm else ''}{strike_price - atm}]"
            
            ce_str = f"+{ce_flow:.1f}L" if ce_flow > 0 else "----"
            pe_str = f"+{pe_flow:.1f}L" if pe_flow > 0 else "----"
            who    = strike.get("who", "----")
            label  = strike.get("label", "----")
            
            msg = (
                f"⚡ FLOW SPIKE ALERT\n"
                f"Strike: {strike_price}{atm_label}\n"
                f"CE: {ce_str} | PE: {pe_str}\n"
                f"Total: {total:.1f}L in 60s\n"
                f"Who: {who} | {label}\n"
                f"Spot: {spot:.1f} | ATM: {atm}"
            )
            notifier.send_message(msg)
            print(f"⚡ INSTANT ALERT FIRED: {strike_price} total={total:.1f}L", flush=True)

async def odx_cycle_loop():
    global fyers_stream, deal_cache, nifty_strikes_to_sub
    
    # v9.9: Clear Startup Warmup
    print("ODX: Waiting 30s for WS warmup...", flush=True)
    await asyncio.sleep(30)

    while True:
        # v9.9: Clear Root Heartbeat
        print(f"ODX LOOP TICK: {get_ist_time()}", flush=True)
        
        try:
            if fyers_stream and fyers_stream.engine:
                ce_buy_cr = ce_sell_cr = pe_buy_cr = pe_sell_cr = 0.0
                bias_cr = 0.0
                strikes_data = []
                live_pcr = 0.91
                spot = 0.0
                atm = 0
                
                now_ts = time.time()
                current_deals = [d for d in list(deal_cache) if now_ts - float(d.get("timestamp", 0)) <= 60]
                
                client_id = os.getenv("FYERS_CLIENT_ID")
                access_token = fyers_stream.access_token if fyers_stream else os.getenv("FYERS_ACCESS_TOKEN")
                
                if client_id and access_token:
                    client = get_fyers_data_client(client_id, access_token)
                    quotes_res = client.quotes({"symbols": "NSE:NIFTY50-INDEX"})
                    
                    if quotes_res.get("s") == "ok":
                        spot = float(quotes_res["d"][0]["v"].get("lp") or 0.0)
                        atm = round(spot / 50) * 50
                        print(f"ODX SPOT: {spot}", flush=True)
                        
                        oc_payload = {"symbol": "NSE:NIFTY50-INDEX", "strikecount": 5, "timestamp": ""}
                        response = client.optionchain(data=oc_payload)
                        
                        if response.get("s") == "ok":
                            raw_chain = response.get("data", {}).get("optionsChain", [])
                            print(f"ODX CHAIN RESPONSE: {response.get('s')} strikes={len(raw_chain)}", flush=True)
                            
                            current_time = get_ist_time()
                            strike_map = {}
                            symbol_lookup = {}
                            total_pe_oi, total_ce_oi = 0, 0
                            
                            for item in raw_chain:
                                strike = int(item.get("strike_price") or 0)
                                if strike <= 0: continue
                                opt_type = item.get("option_type")
                                sym = item.get("symbol")
                                oi = int(item.get("oi") or 0)
                                
                                if strike not in strike_map: strike_map[strike] = {"ce": {}, "pe": {}}
                                if opt_type == "CE":
                                    strike_map[strike]["ce"] = {"symbol": sym, "oi": oi}
                                    symbol_lookup[sym] = (strike, "CE")
                                    total_ce_oi += oi
                                elif opt_type == "PE":
                                    strike_map[strike]["pe"] = {"symbol": sym, "oi": oi}
                                    symbol_lookup[sym] = (strike, "PE")
                                    total_pe_oi += oi

                            live_pcr = round(total_pe_oi / total_ce_oi, 2) if total_ce_oi > 0 else 0.91

                            temp_nifty_strikes = []
                            deals_by_strike = {} 
                            for d in current_deals:
                                sym = d.get("symbol", "")
                                if not sym: continue
                                d_strike, d_type = symbol_lookup[sym] if sym in symbol_lookup else extract_strike_type(sym)
                                if d_strike is not None:
                                    if d_strike not in deals_by_strike: deals_by_strike[d_strike] = {"CE": [], "PE": []}
                                    deals_by_strike[d_strike][d_type].append(d)

                            ce_buy_cr = ce_sell_cr = pe_buy_cr = pe_sell_cr = 0.0

                            for strike in sorted(strike_map.keys()):
                                row = strike_map[strike]
                                ce_row, pe_row = row.get("ce", {}), row.get("pe", {})
                                if not ce_row or not pe_row: continue
                                
                                ce_sym, pe_sym = ce_row.get("symbol"), pe_row.get("symbol")
                                if abs(strike - atm) <= 250: temp_nifty_strikes.extend([ce_sym, pe_sym])

                                strike_deals = deals_by_strike.get(strike, {"CE": [], "PE": []})
                                ce_agg = pe_agg = ce_who = pe_who = "----"
                                ce_pulse_cr = pe_pulse_cr = 0.0

                                for sym_type, sym_deals in [("CE", strike_deals["CE"]), ("PE", strike_deals["PE"])]:
                                    if not sym_deals: continue

                                    buy_deals  = [d for d in sym_deals if d.get("direction") == "BUY"]
                                    sell_deals = [d for d in sym_deals if d.get("direction") == "SELL"]

                                    d_buy_l  = sum(float(d.get("qty", 0)) * float(d.get("ltp", 0)) for d in buy_deals)  / 100000
                                    d_sell_l = sum(float(d.get("qty", 0)) * float(d.get("ltp", 0)) for d in sell_deals) / 100000
                                    net_cr = d_buy_l - d_sell_l

                                    buys = sum(float(d.get("qty", 0)) for d in buy_deals)
                                    sells = sum(float(d.get("qty", 0)) for d in sell_deals)
                                    agg = "BUY" if buys > sells else "SELL" if sells > buys else "----"

                                    parts = [d.get("participant") for d in sym_deals 
                                            if d.get("participant") and d.get("participant") not in ("----", None, "")]
                                    who = max(set(parts), key=parts.count) if parts else "----"

                                    if sym_type == "CE":
                                        ce_buy_cr += d_buy_l; ce_sell_cr += d_sell_l; ce_agg, ce_who, ce_pulse_cr = agg, who, net_cr
                                    else:
                                        pe_buy_cr += d_buy_l; pe_sell_cr += d_sell_l; pe_agg, pe_who, pe_pulse_cr = agg, who, net_cr

                                if ce_pulse_cr != 0 and pe_pulse_cr != 0:
                                    if ce_pulse_cr > 0 and pe_pulse_cr < 0:   label = "Bull Spread⚡"
                                    elif ce_pulse_cr < 0 and pe_pulse_cr > 0: label = "Bear Spread⚡"
                                    elif ce_pulse_cr > 0 and pe_pulse_cr > 0: label = "Straddle Buy⚡"
                                    else:                                   label = "Straddle Write⚡"
                                elif ce_pulse_cr > 0:  label = "Call Buy⚡"
                                elif ce_pulse_cr < 0:  label = "Call Write⚡"
                                elif pe_pulse_cr > 0:  label = "Put Buy⚡"
                                elif pe_pulse_cr < 0:  label = "Put Write⚡"
                                else:                  label = "Accumulate"

                                strikes_data.append({
                                    "strike": int(strike), "is_atm": strike == atm,
                                    "ce_delta_l": float(ce_pulse_cr), "pe_delta_l": float(pe_pulse_cr),
                                    "ce_aggressor": str(ce_agg), "pe_aggressor": str(pe_agg),
                                    "who": str(who if (ce_who!="----" or pe_who!="----") else "----"), "label": str(label)
                                })

                            print(f"ODX LOOP DONE: {len(strikes_data)} strikes built", flush=True)

                            ce_net = ce_buy_cr - ce_sell_cr
                            pe_net = pe_buy_cr - pe_sell_cr
                            bias_cr = ce_net - pe_net

                            print(f"AGGREGATOR DEBUG: ce_buy={ce_buy_cr:.1f} ce_sell={ce_sell_cr:.1f} pe_buy={pe_buy_cr:.1f} pe_sell={pe_sell_cr:.1f} bias={bias_cr:.1f}", flush=True)

                            odx_payload = {
                                "time": str(current_time), "spot": float(spot), "atm": int(atm), "pcr": live_pcr,
                                "strikes": strikes_data,
                                "aggregator": {
                                    "aggregate_bias_cr": float(bias_cr),
                                    "ce_buy":  round(float(ce_buy_cr),  1),
                                    "ce_sell": round(float(ce_sell_cr), 1),
                                    "pe_buy":  round(float(pe_buy_cr),  1),
                                    "pe_sell": round(float(pe_sell_cr), 1)
                                }
                            }
                            print(f"ODX PAYLOAD READY: sending to TG", flush=True)

                            new_strikes = list(set(s for s in temp_nifty_strikes if s and isinstance(s, str)))
                            if set(new_strikes) != set(nifty_strikes_to_sub):
                                nifty_strikes_to_sub = new_strikes
                                if fyers_stream: fyers_stream.subscribe_symbols(nifty_strikes_to_sub)

                            await check_instant_alerts(fyers_stream.notifier, strikes_data, spot, atm)
                            fyers_stream.notifier.send_odx_heartbeat(odx_payload)

                            cutoff = time.time() - 60
                            remaining = [d for d in list(deal_cache) if float(d.get("timestamp", 0)) >= cutoff]
                            deal_cache.clear()
                            deal_cache.extend(remaining)

        except Exception as e:
            import traceback
            print(f"ODX CRASH: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()

        # v9.9: Clear Root Sleep
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
