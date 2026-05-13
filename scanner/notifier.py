import requests
import json
import time
import traceback

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"
        
        # FIX 4: Chat ID Format Check
        print(f"CHAT ID FORMAT CHECK: '{self.chat_id}' starts_with_minus={str(self.chat_id).startswith('-')}", flush=True)

    def send_message(self, text: str):
        try:
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            res = requests.post(url, json=payload)
            return res.json()
        except Exception as e:
            print(f"❌ TG Send Error: {e}")
            return None

    def send_odx_heartbeat(self, data: dict):
        """v8.2 Hardened Heartbeat with Truncation & Logging"""
        try:
            time_str = data.get("time", "00:00")
            spot = data.get("spot", 0.0)
            atm = data.get("atm", 0)
            pcr = data.get("pcr", 0.0)
            strikes = data.get("strikes", [])
            
            header = (
                f"<b>ODX Pulse • {time_str}</b>\n"
                f"<code>SPOT: {spot:.1f} | ATM: {atm} | PCR: {pcr:.2f}</code>\n\n"
                f"<code>STRIKE | CE (L) | PE (L) | WHO</code>\n"
                f"<code>-------|--------|--------|-----</code>\n"
            )
            
            body = ""
            for s in strikes:
                strike_str = f"{s['strike']}"
                if s.get('is_atm'): strike_str = f"*{strike_str}"
                
                ce_val = s.get('ce_delta_l', 0.0)
                pe_val = s.get('pe_delta_l', 0.0)
                
                ce_fmt = f"{ce_val:+.1f}" if abs(ce_val) >= 0.1 else "----"
                pe_fmt = f"{pe_val:+.1f}" if abs(pe_val) >= 0.1 else "----"
                
                row = f"<code>{strike_str:<6} | {ce_fmt:<6} | {pe_fmt:<6} | {s.get('who', '----')}</code>\n"
                body += row

            agg = data.get("aggregator", {})
            bias_l = agg.get("aggregate_bias_l", 0.0)
            bias_sign = "🟢" if bias_l > 0 else "🔴" if bias_l < 0 else "⚪"
            
            footer = (
                f"\n<b>Institutional Flow (60s)</b>\n"
                f"<code>BIAS: {bias_sign} {bias_l:+.1f} Lakhs</code>\n"
                f"<code>CE: {agg.get('ce_buy',0):.1f} L vs {agg.get('ce_sell',0):.1f} L</code>\n"
                f"<code>PE: {agg.get('pe_buy',0):.1f} L vs {agg.get('pe_sell',0):.1f} L</code>"
            )
            
            text = header + body + footer
            
            # FIX 3: Safety Truncation (4096 char limit)
            if len(text) > 4000:
                text = text[:4000] + "\n..."
                print(f"WARNING: Heartbeat truncated to 4000 chars", flush=True)
            
            # FIX 1: Detailed Response Logging
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            
            print(f"TG HEARTBEAT: token={self.token[:10]}... chat={self.chat_id}", flush=True)
            res = requests.post(url, json=payload)
            print(f"TG API RESPONSE: {res.status_code} | {res.text}", flush=True)
            
            return res.json()
            
        # FIX 6: Exception Traceback
        except Exception as e:
            print(f"HEARTBEAT EXCEPTION: {type(e).__name__}: {e}", flush=True)
            traceback.print_exc()
            return None
