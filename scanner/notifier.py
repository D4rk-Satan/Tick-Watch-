import requests
import json
import time
import traceback

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"

    def send_message(self, text: str, parse_mode=None):
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
        """v8.5 Precise Table Heartbeat"""
        try:
            time_str = data.get("time", "00:00")
            spot = data.get("spot", 0.0)
            atm = data.get("atm", 0)
            pcr = data.get("pcr", 0.91)
            strikes = data.get("strikes", [])
            
            # FIX 3: Precise Header Alignment
            header = (
                f"ODX Pulse • {time_str}\n"
                f"SPOT: {spot:.1f} | ATM: {atm} | PCR: {pcr:.2f}\n\n"
                f"{'STRIKE':<7} | {'CE(L)':<8} {'CA':<5}| {'PE(L)':<8} {'PA':<5}| {'WHO':<5}| SIG\n"
                f"----------------------------------------------------------\n"
            )
            
            body = ""
            for s in strikes:
                strike_price = s.get('strike', '???')
                strike_str = str(strike_price)
                if s.get('is_atm'): strike_str = f"*{strike_str}"
                
                ce_val = s.get('ce_delta_l', 0.0)
                pe_val = s.get('pe_delta_l', 0.0)
                ce_fmt = f"{ce_val:+.1f}L" if abs(ce_val) >= 0.1 else "----"
                pe_fmt = f"{pe_val:+.1f}L" if abs(pe_val) >= 0.1 else "----"
                
                c_agg = s.get('ce_aggressor', '----')
                p_agg = s.get('pe_aggressor', '----')
                who = s.get('who', '----')
                signal = s.get('label', '----')
                
                # FIX 3: Precise Row Alignment
                row = f"{strike_str:<7} | {ce_fmt:<8} {c_agg:<5}| {pe_fmt:<8} {p_agg:<5}| {who:<5}| {signal}\n"
                body += row

            agg = data.get("aggregator", {})
            bias_l = agg.get("aggregate_bias_l", 0.0)
            bias_sign = "+" if bias_l > 0 else "-" if bias_l < 0 else "="
            
            footer = (
                f"----------------------------------------------------------\n"
                f"Institutional Flow (60s)\n"
                f"BIAS: {bias_sign} {bias_l:+.1f} Lakhs\n"
                f"CE: {agg.get('ce_buy',0):.1f}L vs {agg.get('ce_sell',0):.1f}L\n"
                f"PE: {agg.get('pe_buy',0):.1f}L vs {agg.get('pe_sell',0):.1f}L"
            )
            
            text = header + body + footer
            
            if len(text) > 4000:
                text = text[:4000] + "\n..."
            
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text}
            res = requests.post(url, json=payload)
            return res.json()
            
        except Exception as e:
            print(f"HEARTBEAT EXCEPTION: {e}")
            return None

    def send_deal_alert(self, deal: dict):
        try:
            sym = deal.get("symbol", "UNKNOWN")
            ltp = deal.get("ltp", 0.0)
            score = deal.get("score", 0)
            direction = deal.get("direction", "NEUTRAL")
            qty = deal.get("qty", 0)
            
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
