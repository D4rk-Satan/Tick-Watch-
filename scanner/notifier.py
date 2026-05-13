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
            
            strikes = data.get("strikes", [])
            
            header = (
                f"<b>ODX Pulse • {current_time}</b>\n"
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
            
            url = f"{self.base_url}/sendMessage"
            payload = {
                "chat_id": self.chat_id, 
                "text": text,
                "parse_mode": "HTML"
            }
            requests.post(url, json=payload)
            return True
        except Exception as e:
            print(f"Heartbeat Error: {e}")
            return None
