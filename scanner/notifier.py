import requests
import os
from dotenv import load_dotenv

load_dotenv()

class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token = token
        self.chat_id = chat_id
        self.base_url = f"https://api.telegram.org/bot{token}"

    def send_message(self, text: str, parse_mode="MarkdownV2"):
        url = f"{self.base_url}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": parse_mode
        }
        try:
            res = requests.post(url, json=payload)
            return res.json()
        except Exception as e:
            print(f"Telegram Error: {e}")
            return None

    def send_deal_alert(self, deal: dict):
        """Standard Deal Alert for 1000+ Lot Trades"""
        sym = deal.get("symbol", "NIFTY")
        ltp = deal.get("ltp", 0)
        qty = deal.get("qty", 0)
        direction = deal.get("direction", "BUY")
        participant = deal.get("participant", "HNI")
        
        # Format for MarkdownV2
        sym = sym.replace("-", "\\-").replace(".", "\\.")
        
        emoji = "🚀" if direction == "BUY" else "🔥"
        text = (
            f"{emoji} *INSTITUTIONAL FLOW DETECTED*\n\n"
            f"Symbol: `{sym}`\n"
            f"Action: *{direction}*\n"
            f"Quantity: `{qty:,}`\n"
            f"Price: `{ltp:.2f}`\n"
            f"Who: *{participant}*"
        )
        return self.send_message(text)

    def send_odx_heartbeat(self, data: dict):
        """Detailed ODX Pulse Table (60s Delta)"""
        try:
            current_time = data.get("time", "00:00")
            spot = data.get("spot", 0)
            atm = data.get("atm", 0)
            pcr = data.get("pcr", 0.9)
            
            # Table Header
            text = f"🧠 NIFTY ODX — {current_time}\n"
            text += f"Spot: {spot:.2f} | ATM: {atm} | PCR: {pcr}\n"
            text += "```copy\n"
            text += f"{'STRIKE':<8} {'CALL Δ':<10} {'C-AGG':<8} {'PUT Δ':<10} {'P-AGG':<8} {'WHO':<8} {'SIGNAL':<12}\n\n"
            
            for strike in data.get("strikes", []):
                s_price = str(strike["strike"])
                if strike.get("is_atm"): s_price = f"▸{s_price}"
                
                c_delta = f"{strike['ce_delta_cr']:+.2f}Cr"
                p_delta = f"{strike['pe_delta_cr']:+.2f}Cr"
                
                # Replace zeros with cleaner text
                if strike['ce_delta_cr'] == 0: c_delta = " +0.00Cr"
                if strike['pe_delta_cr'] == 0: p_delta = " +0.00Cr"
                
                c_agg = strike.get("ce_aggressor", "----")
                p_agg = strike.get("pe_aggressor", "----")
                who = strike.get("who", "----")
                signal = strike.get("label", "Straddle")
                
                # Format line
                text += f"{s_price:<8} {c_delta:<10} {c_agg:<8} {p_delta:<10} {p_agg:<8} {who:<8} {signal:<12}\n"
            
            text += "```\n"
            
            # Aggregator Bias Logic (v5.2 Sensitivity)
            agg = data.get("aggregator", {})
            bias = agg.get("aggregate_bias_cr", 0)
            
            # ADJUSTED THRESHOLDS FOR 60s PULSE
            if bias >= 1.0: emoji, trend = "🟢", "BULLISH"
            elif bias <= -1.0: emoji, trend = "🔴", "BEARISH"
            else: emoji, trend = "🟡", "NEUTRAL"
            
            text += f"\nBIAS: {bias:+.2f}Cr {emoji} {trend}\n"
            text += f"CE: Buy {agg.get('ce_buy', 0)}Cr | Sell {agg.get('ce_sell', 0)}Cr\n"
            text += f"PE: Buy {agg.get('pe_buy', 0)}Cr | Sell {agg.get('pe_sell', 0)}Cr"
            
            # Bug 3 Fix: Remove MarkdownV2 escaping and send as plain text
            url = f"{self.base_url}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text}  # no parse_mode
            requests.post(url, json=payload)
            return True
        except Exception as e:
            print(f"Heartbeat Error: {e}")
            return None
