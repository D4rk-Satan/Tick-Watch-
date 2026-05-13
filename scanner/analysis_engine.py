import time

class AnalysisEngine:
    def __init__(self):
        self.oi_data = {}

    def update_oi(self, symbol: str, oi: int):
        self.oi_data[symbol] = oi

    def _infer_direction(self, tick: dict) -> str:
        """v8.5 Double-Signal Direction Engine"""
        bid = tick.get("bid_price", 0)
        ask = tick.get("ask_price", 0)
        ltp = tick.get("ltp", 0)
        tot_buy = tick.get("tot_buy_qty", 0)
        tot_sell = tick.get("tot_sell_qty", 0)
        
        # Diagnostic Print
        # print(f"DIRECTION DEBUG: {tick.get('symbol')} ltp={ltp} bid={bid} ask={ask} tbq={tot_buy} tsq={tot_sell}", flush=True)

        # Signal 1: Spread Comparison (Primary)
        if bid > 0 and ask > 0:
            if ltp >= ask: return "BUY"
            if ltp <= bid: return "SELL"
            mid = (bid + ask) / 2
            return "BUY" if ltp >= mid else "SELL"
            
        # Signal 2: Order Book Imbalance (Fallback)
        if tot_buy > 0 and tot_sell > 0:
            return "BUY" if tot_buy >= tot_sell else "SELL"
            
        return "BUY" 

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
            # FIX 2: Verified Option Thresholds
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

            # Diagnostic Print
            if participant != "----":
                print(f"PARTICIPANT: {sym} dir={direction} val={value_lakhs:.3f}L → {participant}", flush=True)

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
