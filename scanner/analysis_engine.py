import time

class AnalysisEngine:
    def __init__(self):
        self.oi_data = {}

    def update_oi(self, symbol: str, oi: int):
        self.oi_data[symbol] = oi

    def _infer_direction(self, tick: dict) -> str:
        """v8.4 Sensitive Direction Inference"""
        bid = tick.get("bid_price", 0)
        ask = tick.get("ask_price", 0)
        ltp = tick.get("ltp", 0)
        atp = tick.get("avg_trade_price", ltp)
        
        if bid > 0 and ask > 0:
            mid = (bid + ask) / 2
            if ltp >= ask: return "BUY"
            if ltp <= bid: return "SELL"
            if ltp > mid: return "BUY"
            if ltp < mid: return "SELL"
            
        # FIX 1: ATP Fallback (Primary for Options)
        if atp > 0:
            if ltp > atp * 1.0002: return "BUY"
            if ltp < atp * 0.9998: return "SELL"
            
        return "BUY" # Default to BUY to ensure flow is counted

    def analyze_tick(self, tick: dict):
        try:
            sym = tick.get("symbol")
            ltp = tick.get("ltp")
            ltq = tick.get("last_traded_qty", 0)
            
            if not sym or not ltp or ltq <= 0: return None
            
            direction = self._infer_direction(tick)
            value_lakhs = (ltp * ltq) / 100000
            is_option = "CE" in sym or "PE" in sym
            
            participant = "----"
            # FIX 2: Sensitive Option Thresholds
            if is_option:
                if direction == "BUY":
                    if value_lakhs >= 5: participant = "INST"
                    elif value_lakhs >= 1: participant = "PROP"
                    elif value_lakhs >= 0.1: participant = "HNI"
                else:
                    if value_lakhs >= 10: participant = "INST"
                    elif value_lakhs >= 2: participant = "PROP"
                    elif value_lakhs >= 0.5: participant = "HNI"
            else:
                if value_lakhs >= 10: participant = "INST"
                elif value_lakhs >= 2: participant = "PROP"
                elif value_lakhs >= 1: participant = "HNI"

            deal = {
                "symbol": sym,
                "ltp": ltp,
                "qty": ltq,
                "timestamp": int(time.time()),
                "direction": direction,
                "value_l": value_lakhs,
                "participant": participant,
                "score": 5 if participant != "----" else 3,
                "label": "ACCUMULATION" if direction == "BUY" else "DISTRIBUTION"
            }
            
            return deal
        except Exception as e:
            print(f"❌ Analysis Error: {e}")
            return None
