import time
import statistics
import re
from collections import deque
from typing import Dict, Optional

class AnalysisEngine:
    def __init__(self):
        self.oi_data = {}
        # Feature 1: OI History for spike detection
        self.oi_history: Dict[str, deque] = {}
        # Feature 2: Pinning history tracking
        self.pin_history: Dict[str, deque] = {}  # {symbol: deque([ltp1, ltp2...], maxlen=30)}
        self.pin_oi_start: Dict[str, int] = {}   # OI at start of pin window
        # Iceberg tracking
        self.active_sequences = {}

    def update_oi(self, symbol: str, oi: int):
        self.oi_data[symbol] = oi
        if symbol not in self.oi_history:
            self.oi_history[symbol] = deque(maxlen=6) # 60 seconds of history (10s intervals)
        self.oi_history[symbol].append(oi)

    def detect_oi_spike(self, symbol: str) -> dict:
        """v9.0 Feature 1: Detects sudden OI buildup at a strike"""
        if symbol not in self.oi_history or len(self.oi_history[symbol]) < 2:
            return {"spike": False}
        
        history = list(self.oi_history[symbol])
        latest  = history[-1]
        oldest  = history[0]
        
        if oldest == 0: return {"spike": False}
        
        pct_change = ((latest - oldest) / oldest) * 100
        abs_change = latest - oldest
        
        # Spike = >15% OI increase across last 60 seconds
        if pct_change >= 15 and abs_change > 500:
            return {
                "spike": True,
                "pct_change": round(pct_change, 1),
                "abs_change": abs_change,
                "signal": "OI_SPIKE 🔥",
                "meaning": "Fresh positioning — potential support/resistance"
            }
        return {"spike": False}

    def detect_pin(self, symbol: str, ltp: float, current_oi: int) -> dict:
        """v9.0 Feature 2: Detects MM defending a strike level — price stuck + OI growing"""
        if symbol not in self.pin_history:
            self.pin_history[symbol] = deque(maxlen=30)
            self.pin_oi_start[symbol] = current_oi
        
        self.pin_history[symbol].append(ltp)
        
        if len(self.pin_history[symbol]) < 10:
            return {"pin": False}
        
        prices = list(self.pin_history[symbol])
        price_range_pct = (max(prices) - min(prices)) / min(prices) * 100
        oi_growth = current_oi - self.pin_oi_start.get(symbol, current_oi)
        
        # Pin = price oscillating within 0.5% for 10+ ticks AND OI increasing
        if price_range_pct <= 0.5 and oi_growth > 0:
            strike = None
            match = re.search(r'(\d+)(CE|PE)$', symbol)
            if match: strike = int(match.group(1))
            
            return {
                "pin": True,
                "strike": strike,
                "price_range_pct": round(price_range_pct, 3),
                "oi_growth": oi_growth,
                "signal": "PIN_ALERT 📌",
                "meaning": f"MM defending {strike} — strong magnet level"
            }
        
        # Reset OI baseline every 30 ticks
        if len(self.pin_history[symbol]) == 30:
            self.pin_oi_start[symbol] = current_oi
        
        return {"pin": False}

    def _process_iceberg(self, symbol: str, ltp: float, ltq: int, current_time: float) -> Optional[dict]:
        """v9.0 Feature 3: Catching quantity rotation across slightly different prices"""
        seq = self.active_sequences.get(symbol)
        
        if not seq:
            self.active_sequences[symbol] = {
                "price": ltp, "qtys": [ltq], "times": [current_time],
                "last_time": current_time, "price_range": [ltp, ltp]
            }
            return None
        
        time_gap = current_time - seq["last_time"]
        price_deviation = abs(seq["price"] - ltp) / seq["price"]
        
        # Extended: allow up to 0.1% price deviation (quantity rotation)
        if time_gap > 2.0 or price_deviation > 0.001:
            self.active_sequences[symbol] = {
                "price": ltp, "qtys": [ltq], "times": [current_time],
                "last_time": current_time, "price_range": [ltp, ltp]
            }
            return None
        
        seq["qtys"].append(ltq)
        seq["times"].append(current_time)
        seq["last_time"] = current_time
        seq["price_range"] = [min(seq["price_range"][0], ltp), max(seq["price_range"][1], ltp)]
        
        if len(seq["qtys"]) >= 5:
            qty_mean   = statistics.mean(seq["qtys"])
            qty_std    = statistics.stdev(seq["qtys"]) if len(seq["qtys"]) > 1 else 0
            max_gap    = max([seq["times"][i] - seq["times"][i-1] for i in range(1, len(seq["times"]))]) if len(seq["times"]) > 1 else 0
            total_qty  = sum(seq["qtys"])
            price_span = seq["price_range"][1] - seq["price_range"][0]
            
            return {
                "mean": qty_mean,
                "std_dev": qty_std,
                "max_gap": max_gap,
                "total_qty": total_qty,
                "ticks": len(seq["qtys"]),
                "price_span": price_span,
                "is_rotation": price_span > 0 
            }
        return None

    def _infer_direction(self, tick: dict) -> str:
        bid = float(tick.get("bid_price") or 0)
        ask = float(tick.get("ask_price") or 0)
        ltp = float(tick.get("ltp") or 0)
        atp = float(tick.get("avg_trade_price") or ltp)

        if bid > 0 and ask > 0 and ltp > 0:
            if ltp >= ask: return "BUY"
            elif ltp <= bid: return "SELL"
            else:
                mid = (bid + ask) / 2.0
                return "BUY" if ltp > mid else "SELL"
        else:
            if ltp > atp: return "BUY"
            elif ltp < atp: return "SELL"
            else: return "NEUTRAL"
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
            
            score = 3
            details = []
            
            # Iceberg Detection
            iceberg = self._process_iceberg(sym, ltp, ltq, time.time())
            if iceberg:
                score += 2
                details.append("Iceberg Pattern")
                if iceberg['std_dev'] < (iceberg['mean'] * 0.1):
                    score += 2
                    details.append("Low Qty Variance")
                if iceberg['max_gap'] < 0.05:
                    score += 1
                    details.append("Rapid Cluster")
                if iceberg.get('is_rotation'):
                    score += 2
                    details.append("Qty Rotation")
                if iceberg.get('ticks', 0) >= 10:
                    score += 1
                    details.append("Deep Iceberg")

            participant = "----"
            if is_option:
                if direction == "BUY":
                    if value_lakhs >= 5: participant = "INST"; score += 5
                    elif value_lakhs >= 1: participant = "PROP"; score += 3
                    elif value_lakhs >= 0.1: participant = "HNI"; score += 1
                else:
                    if value_lakhs >= 10: participant = "INST"; score += 5
                    elif value_lakhs >= 2: participant = "PROP"; score += 3
                    elif value_lakhs >= 0.5: participant = "HNI"; score += 1
            else:
                if value_lakhs >= 10: participant = "INST"; score += 5
                elif value_lakhs >= 2: participant = "PROP"; score += 3
                elif value_lakhs >= 1: participant = "HNI"; score += 1

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
                "score": score,
                "details": details,
                "label": "ACCUMULATION" if direction == "BUY" else "DISTRIBUTION"
            }
            
            return deal
        except Exception as e:
            print(f"❌ Analysis Error: {e}")
            return None
