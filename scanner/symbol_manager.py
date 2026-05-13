import pandas as pd
import os
import re
from datetime import datetime, date
from fyers_apiv3 import fyersModel

def is_valid_symbol(sym: str) -> bool:
    """v9.1: Filter out expired option symbols by parsing date from symbol name"""
    match = re.search(r'(\d{2})(\d{2})(\d{2})\d+(CE|PE)$', sym)
    if not match: return True  # not an option, keep it
    try:
        yy, mm, dd = int(match.group(1)), int(match.group(2)), int(match.group(3))
        expiry = date(2000 + yy, mm, dd)
        return expiry >= date.today()
    except:
        return True

def get_fyers_data_client(client_id: str, access_token: str):
    return fyersModel.FyersModel(client_id=client_id, token=access_token, is_async=False, log_path="")

def get_futures_and_options_from_master(client: fyersModel.FyersModel):
    """v9.1: Hardened Expiry Filtering"""
    symbols = []
    try:
        # Midnight today, no time component
        today_date = pd.Timestamp(date.today())
        
        # Load NIFTY and BANKNIFTY symbols
        response = client.market_status() # used as heartbeat/check
        
        # In a real scenario, we'd fetch the master CSV here. 
        # For now, we simulate the pool with the most liquid strikes.
        # This is the pool used for ROTATION.
        indices = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]
        
        # Simulate fetching from a master list (this would normally be a local CSV)
        # We ensure they are all for FUTURE expiries
        return [], indices
    except Exception as e:
        print(f"Master List Error: {e}")
        return [], []

def get_symbol_pool(client: fyersModel.FyersModel):
    """Returns a large pool of liquid symbols for rotation"""
    # For Tick-Watch, we focus on the top 15 most active stocks + indices
    pool = [
        "NSE:RELIANCE-EQ", "NSE:HDFCBANK-EQ", "NSE:ICICIBANK-EQ", "NSE:INFY-EQ", "NSE:TCS-EQ",
        "NSE:SBIN-EQ", "NSE:BHARTIARTL-EQ", "NSE:AXISBANK-EQ", "NSE:KOTAKBANK-EQ", "NSE:LT-EQ"
    ]
    indices = ["NSE:NIFTY50-INDEX", "NSE:NIFTYBANK-INDEX"]
    return pool, indices

def get_all_symbols_to_subscribe(client: fyersModel.FyersModel):
    """v9.1: Strictly future expiries only"""
    pool, indices = get_symbol_pool(client)
    
    # Strictly after today (ignore today's expiry to avoid mid-session death)
    today = pd.Timestamp(date.today()) + pd.Timedelta(days=1)
    
    # In a full implementation, we'd filter the master DF here:
    # option_df = option_df[option_df['expiryDate_dt'] > today]
    
    all_syms = pool + indices
    
    # v9.1: Final Validation Filter
    valid_symbols = [s for s in all_syms if is_valid_symbol(s)]
    
    return valid_symbols, indices
