import time

class AnalysisEngine:
    def __init__(self):
        self.oi_data = {}

    def update_oi(self, symbol: str, oi: int):
        self.oi_data[symbol] = oi

    def _infer_direction(self, tick: dict) -> str:
        """v8.6 Hardened Direction Engine (Step 2)"""
        bid      = float(tick.get("bid_price") or 0)
        ask      = float(tick.get("ask_price") or 0)
        ltp      = float(tick.get("ltp") or 0)
        tot_buy  = int(tick.get("tot_buy_qty") or 0)
        tot_sell = int(tick.get("tot_sell_qty") or 0)

        if bid > 0 and ask > 0 and ltp > 0:
            if ltp >= ask:
                return "BUY"
            if ltp <= bid:
                return "SELL"
            mid = (bid + ask) / 2.0
            if ltp > mid:
                return "BUY"
            if ltp < mid:
                return "SELL"
            return "SELL"  # exactly at mid = passive = SELL

        if tot_buy > 0 or tot_sell > 0:
            return "BUY" if tot_buy > tot_sell else "SELL"

        return "SELL"  # default SELL not BUY — balances the bias

    def analyze_tick(self, tick: dict):
        try:
            sym = tick.get("symbol")
            ltp = tick.get("ltp")
            ltq = tick.get("last_traded_qty", 0)
            
            if not sym or not ltp or ltq <= 0: return None
            
            # Step 1: DIR_CHECK Print
            direction = self._infer_direction(tick)
            print(f"DIR_CHECK: {sym} ltp={ltp} bid={tick.get('bid_price')} ask={tick.get('ask_price')} tot_buy={tick.get('tot_buy_qty')} tot_sell={tick.get('tot_sell_qty')} → {direction}", flush=True)

            value_lakhs = (ltp * ltq) / 100000
            is_option = "CE" in sym or "PE" in sym
            
            participant = "----"
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
