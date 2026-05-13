import os
import requests
import pandas as pd
from datetime import datetime
from fyers_apiv3 import fyersModel

# Standard Nifty 50 Symbols
NIFTY_50_SYMBOLS = [
    "NSE:RELIANCE-EQ", "NSE:TCS-EQ", "NSE:HDFCBANK-EQ", "NSE:ICICIBANK-EQ", "NSE:INFY-EQ",
    "NSE:ITC-EQ", "NSE:SBIN-EQ", "NSE:BHARTIARTL-EQ", "NSE:LT-EQ", "NSE:BAJFINANCE-EQ",
    "NSE:HCLTECH-EQ", "NSE:ASIANPAINT-EQ", "NSE:AXISBANK-EQ", "NSE:MARUTI-EQ", "NSE:KOTAKBANK-EQ",
    "NSE:SUNPHARMA-EQ", "NSE:TITAN-EQ", "NSE:ULTRACEMCO-EQ", "NSE:TATAMOTORS-EQ", "NSE:NTPC-EQ",
    "NSE:BAJAJFINSV-EQ", "NSE:M&M-EQ", "NSE:TATASTEEL-EQ", "NSE:POWERGRID-EQ", "NSE:NESTLEIND-EQ",
    "NSE:TECHM-EQ", "NSE:HINDUNILVR-EQ", "NSE:WIPRO-EQ", "NSE:GRASIM-EQ", "NSE:INDUSINDBK-EQ",
    "NSE:HINDALCO-EQ", "NSE:JSWSTEEL-EQ", "NSE:ADANIENT-EQ", "NSE:ADANIPORTS-EQ", "NSE:ONGC-EQ",
    "NSE:DRREDDY-EQ", "NSE:SBILIFE-EQ", "NSE:CIPLA-EQ", "NSE:COALINDIA-EQ", "NSE:BRITANNIA-EQ",
    "NSE:APOLLOHOSP-EQ", "NSE:TATACONSUM-EQ", "NSE:EICHERMOT-EQ", "NSE:BAJAJ-AUTO-EQ",
    "NSE:DIVISLAB-EQ", "NSE:HEROMOTOCO-EQ", "NSE:HDFCLIFE-EQ", "NSE:LTIM-EQ", "NSE:UPL-EQ",
    "NSE:BPCL-EQ"
]

def get_fyers_data_client(client_id, access_token):
    return fyersModel.FyersModel(client_id=client_id, is_async=False, token=access_token, log_path="")

def get_current_spot_prices(data_client):
    res = data_client.quotes(data={"symbols": "NSE:NIFTY50-INDEX,NSE:NIFTYBANK-INDEX"})
    if res.get("s") == "ok":
        return {d["n"]: d["v"]["lp"] for d in res["d"]}
    return {"NSE:NIFTY50-INDEX": 23500, "NSE:NIFTYBANK-INDEX": 54000}

def get_all_symbols_to_subscribe(data_client):
    """
    Stabilized Symbol Generation (v6.0)
    Bypasses broken Fyers Master by constructing weekly symbols manually.
    """
    spots = get_current_spot_prices(data_client)
    nifty_spot = spots.get("NSE:NIFTY50-INDEX", 23500)
    bn_spot = spots.get("NSE:NIFTYBANK-INDEX", 54000)
    
    # 1. Construct Nifty Weekly Symbols (Tomorrow's Expiry: May 14)
    nifty_atm = round(nifty_spot / 50) * 50
    forced_nifty = []
    for i in range(-7, 8): # 15 strikes
        strike = int(nifty_atm + (i * 50))
        forced_nifty.append(f"NSE:NIFTY26514{strike}CE")
        forced_nifty.append(f"NSE:NIFTY26514{strike}PE")
    
    # 2. Construct BankNifty Weekly Symbols (Next Week Expiry: May 21)
    bn_atm = round(bn_spot / 100) * 100
    forced_bn = []
    for i in range(-5, 6): # 11 strikes
        strike = int(bn_atm + (i * 100))
        forced_bn.append(f"NSE:BANKNIFTY26521{strike}CE")
        forced_bn.append(f"NSE:BANKNIFTY26521{strike}PE")
        
    print(f"🛠️ Stable Symbols Ready: {len(forced_nifty)} Nifty & {len(forced_bn)} BankNifty.")
    
    total = list(set(forced_nifty + forced_bn + NIFTY_50_SYMBOLS + ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]))
    return total, set(total)

def get_symbol_pool(data_client):
    symbols, pool = get_all_symbols_to_subscribe(data_client)
    return symbols, pool
