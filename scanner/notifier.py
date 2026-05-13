import requests
import json
import time
import traceback

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        print(f"CHAT ID FORMAT CHECK: '{self.chat_id}' starts_with_minus={str(self.chat_id).startswith('-')}", flush=True)

    def send_message(self, text: str, parse_mode=None):
        """v8.3 Plain Text Mode (Fix 1)"""
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            res = requests.post(url, json=payload)
            return res.json()
        except Exception as e:
            print(f"❌ TG Send Error: {e}")
            return None

    def send_odx_heartbeat(self, data: dict):
        """v8.3 Bulletproof Plain-Text Heartbeat (Fix 2 & 3)"""
        try:
            time_str = data.get("time", "00:00")
            spot = data.get("spot", 0.0)
            atm = data.get("atm", 0)
            pcr = data.get("pcr", 0.0)
            strikes = data.get("strikes", [])
            
            # Use Plain Text Header (No HTML/Markdown)
            header = (
                f"ODX Pulse • {time_str}\n"
                f"SPOT: {spot:.1f} | ATM: {atm} | PCR: {pcr:.2f}\n\n"
                f"STRIKE | CE (L) | PE (L) | WHO\n"
                f"----------------------------\n"
            )
            
            body = ""
            for s in strikes:
                # Fix 2: Using .get() for all keys with fallbacks
                strike_price = s.get('strike', '???')
                strike_str = str(strike_price)
                if s.get('is_atm'): strike_str = f"*{strike_str}"
                
                # Handling renamed delta keys
                ce_val = s.get('ce_delta_l', s.get('ce_delta_cr', 0.0))
                pe_val = s.get('pe_delta_l', s.get('pe_delta_cr', 0.0))
                
                ce_fmt = f"{ce_val:+.1f}L" if abs(ce_val) >= 0.1 else "----"
                pe_fmt = f"{pe_val:+.1f}L" if abs(pe_val) >= 0.1 else "----"
                
                who = s.get('who', '----')
                row = f"{strike_str:<6} | {ce_fmt:<6} | {pe_fmt:<6} | {who}\n"
                body += row

            agg = data.get("aggregator", {})
            bias_l = agg.get("aggregate_bias_l", agg.get("aggregate_bias_cr", 0.0))
            bias_sign = "+" if bias_l > 0 else "-" if bias_l < 0 else "="
            
            footer = (
                f"----------------------------\n"
                f"Institutional Flow (60s)\n"
                f"BIAS: {bias_sign} {bias_l:+.1f} Lakhs\n"
                f"CE: {agg.get('ce_buy',0):.1f}L vs {agg.get('ce_sell',0):.1f}L\n"
                f"PE: {agg.get('pe_buy',0):.1f}L vs {agg.get('pe_sell',0):.1f}L"
            )
            
            text = header + body + footer
            
            if len(text) > 4000:
                text = text[:4000] + "\n..."
                print(f"WARNING: Heartbeat truncated", flush=True)
            
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text}
            
            print(f"TG HEARTBEAT: token={self.token[:10]}... chat={self.chat_id}", flush=True)
            res = requests.post(url, json=payload)
            print(f"TG API RESPONSE: {res.status_code} | {res.text}", flush=True)
            
            return res.json()
            
        except Exception as e:
            print(f"HEARTBEAT EXCEPTION: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()
            return None

    def send_deal_alert(self, deal: dict):
        """v8.3 Plain Text Deal Alert"""
        try:
            sym = deal.get("symbol", "UNKNOWN")
            ltp = deal.get("ltp", 0.0)
            score = deal.get("score", 0)
            direction = deal.get("direction", "NEUTRAL")
            qty = deal.get("qty", 0)
            
            # 100% Plain Text - No bold, no italic, no escaping needed
            alert_text = (
                f"DEAL ALERT: {sym}\n"
                f"Direction: {direction}\n"
                f"LTP: {ltp}\n"
                f"Qty: {qty}\n"
                f"Score: {score}"
            )
            
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": alert_text}
            requests.post(url, json=payload)
            return True
        except Exception as e:
            print(f"❌ Alert Error: {e}")
            return False
