import time
import math
from typing import Dict, List, Optional
from collections import deque
import statistics

class AnalysisEngine:
    def __init__(self):
        # Layer 1 & 3: Baselines & Pressure
        self.rolling_qty: Dict[str, deque] = {}
        self.rolling_ticks: Dict[str, deque] = {}
        self.buy_sell_pressure: Dict[str, deque] = {} # [(timestamp, net_vol), ...]
        
        # Layer 4: OI & PCR
        self.oi_history: Dict[str, deque] = {}
        self.last_oi_poll_time: Dict[str, float] = {}
        self.pcr_history: Dict[str, deque] = {} # {expiry: deque([pcr1, pcr2], maxlen=5)}
        
        # Iceberg tracking
        self.active_sequences: Dict[str, dict] = {}

    def is_market_window(self) -> bool:
        """9:15-9:30 or 15:00-15:30"""
        now = time.localtime()
        current_time = now.tm_hour * 100 + now.tm_min
        return (915 <= current_time <= 930) or (1500 <= current_time <= 1530)

    def get_threshold_multiplier(self) -> float:
        return 1.5 if self.is_market_window() else 1.0

    def update_oi(self, symbol: str, oi_value: int):
        if symbol not in self.oi_history:
            self.oi_history[symbol] = deque(maxlen=3)
        self.oi_history[symbol].append(oi_value)
        self.last_oi_poll_time[symbol] = time.time()
        
        # Calculate Local PCR if possible (Nifty/BankNifty ATM)
        self._update_pcr(symbol)

    def _update_pcr(self, symbol: str):
        # Extract expiry from symbol (e.g. NSE:NIFTY24MAY24000CE)
        if "CE" not in symbol and "PE" not in symbol: return
        
        root = "NIFTY" if "NIFTY" in symbol else "BANKNIFTY"
        # Simple PCR: Sum of monitored PE OI / Sum of monitored CE OI
        # This is a 'Local PCR' for the ATM zone
        pe_oi = sum(h[-1] for s, h in self.oi_history.items() if root in s and "PE" in s and h)
        ce_oi = sum(h[-1] for s, h in self.oi_history.items() if root in s and "CE" in s and h)
        
        if ce_oi > 0:
            pcr = pe_oi / ce_oi
            if root not in self.pcr_history: self.pcr_history[root] = deque(maxlen=5)
            self.pcr_history[root].append(pcr)

    def analyze_tick(self, tick: dict) -> Optional[dict]:
        symbol = tick["symbol"]
        ltp = tick["ltp"]
        ltq = tick["last_traded_qty"]
        atp = tick.get("avg_trade_price", ltp)
        current_time = time.time()
        
        # Initialize
        if symbol not in self.rolling_qty:
            self.rolling_qty[symbol] = deque(maxlen=60)
            self.rolling_ticks[symbol] = deque(maxlen=60)
            self.buy_sell_pressure[symbol] = deque(maxlen=300) # 5 mins
        
        self.rolling_qty[symbol].append(ltq)
        self.rolling_ticks[symbol].append(current_time)

        # Baseline stats
        avg_qty = statistics.mean(self.rolling_qty[symbol]) if len(self.rolling_qty[symbol]) > 10 else ltq
        multiplier = self.get_threshold_multiplier()
        score = 0
        details = []

        # Direction Inference
        direction = self._infer_direction(tick)
        net_vol = ltq if direction == "BUY" else -ltq
        self.buy_sell_pressure[symbol].append((current_time, net_vol))

        # --- Layer 1: Size & Price ---
        if ltq >= (avg_qty * 3 * multiplier):
            score += 2
            details.append("Size Spike")
        if abs(ltp - atp) / atp > (0.0015 * multiplier):
            score += 1
            details.append("Price Deviation")
        recent_ticks = [t for t in self.rolling_ticks[symbol] if current_time - t <= 1.0]
        if len(recent_ticks) >= (5 * multiplier):
            score += 2
            details.append("Frequency Spike")

        # --- Layer 2: Iceberg ---
        iceberg = self._process_iceberg(symbol, ltp, ltq, current_time)
        if iceberg:
            score += 2 
            details.append("Iceberg Pattern")
            if iceberg['std_dev'] < (iceberg['mean'] * 0.1):
                score += 2
                details.append("Low Qty Variance")
            if iceberg['max_gap'] < 0.05:
                score += 1
                details.append("Rapid Cluster")

        # --- Layer 3: Context ---
        # Net buy pressure (5-min)
        five_min_vol = sum(v for t, v in self.buy_sell_pressure[symbol] if current_time - t <= 300)
        if five_min_vol > 0:
            score += 1
            details.append("Buy Pressure")
        # Absorption
        if ltq > (avg_qty * 5) and abs(ltp - atp) / atp < 0.0005:
            score += 2
            details.append("Absorption")
        if not self.is_market_window():
            score += 1
            details.append("Window Context")

        # --- Layer 4: OI Correlation ---
        is_option = "CE" in symbol or "PE" in symbol
        oi_status = "oi_unconfirmed"
        
        if is_option:
            if symbol in self.oi_history and len(self.oi_history[symbol]) >= 2:
                time_since_poll = current_time - self.last_oi_poll_time[symbol]
                if time_since_poll <= 10:
                    oi_delta = self.oi_history[symbol][-1] - self.oi_history[symbol][-2]
                    oi_status = "confirmed"
                    
                    if oi_delta > 0:
                        score += 3
                        details.append("OI Increase")
                        if (direction == "BUY" and "CE" in symbol) or (direction == "SELL" and "PE" in symbol):
                            score += 2
                            details.append("OI Directional Confirm")
                    elif oi_delta < 0:
                        score -= 2
                        details.append("OI Unwinding")
                    else:
                        details.append("Price play not positional")
                else:
                    oi_status = "oi_stale"
            
            # PCR Shift
            root = "NIFTY" if "NIFTY" in symbol else "BANKNIFTY"
            if root in self.pcr_history and len(self.pcr_history[root]) >= 2:
                pcr_shift = abs(self.pcr_history[root][-1] - self.pcr_history[root][-2])
                if pcr_shift > 0.15:
                    score += 1
                    details.append("PCR Shift")

        # --- Alert Tiers ---
        tier = "DISCARD"
        if is_option and oi_status != "confirmed":
            if score >= 10: tier = "Tier 2 MEDIUM (OI Unconfirmed)"
            elif score >= 7: tier = "Tier 2 MEDIUM"
            elif score >= 4: tier = "Tier 3 LOW"
        else:
            if score >= 10: tier = "Tier 1 HIGH"
            elif score >= 7: tier = "Tier 2 MEDIUM"
            elif score >= 4: tier = "Tier 3 LOW"

        # Calculate PCR Shift for summary
        pcr_shift = 0
        root = "NIFTY" if "NIFTY" in symbol else "BANKNIFTY"
        if is_option and root in self.pcr_history and len(self.pcr_history[root]) >= 2:
            pcr_shift = self.pcr_history[root][-1] - self.pcr_history[root][-2]

        # Final Position Type & Margin Context
        pos_type = "price play"
        margin_label = ""
        value_lakhs = (ltp * ltq) / 100000
        
        if is_option:
            # Estimate margin: Sellers need ~1.5L per lot, Buyers need only Premium
            # Lot size: 50 (Nifty), 15 (BankNifty)
            lot_size = 50 if "NIFTY" in symbol else 15
            lots = ltq / lot_size
            
            if direction == "SELL":
                pos_type = "INSTITUTIONAL WRITING 🛡️"
                est_margin = (lots * 150000) / 10000000 # In Cr
                margin_label = f" (Est. Margin: {est_margin:.2f} Cr)"
            elif direction == "BUY":
                pos_type = "AGGRESSIVE BUYING 🚀"
                margin_label = f" (Premium Paid: {value_lakhs:.2f} L)"

        # Participant Logic (WHO)
        participant = None
        if is_option:
            if direction == "BUY":
                if value_lakhs >= 50: participant = "INST"
                elif value_lakhs >= 10: participant = "PROP"
                elif value_lakhs >= 2: participant = "HNI"
            elif direction == "SELL":
                if value_lakhs >= 100: participant = "INST"
                elif value_lakhs >= 10: participant = "PROP"

        result = {
            "symbol": symbol,
            "ltp": ltp,
            "qty": ltq,
            "timestamp": int(current_time),
            "score": score,
            "tier": tier,
            "signals_triggered": details,
            "direction": direction,
            "oi_delta": self.oi_history[symbol][-1] - self.oi_history[symbol][-2] if (is_option and symbol in self.oi_history and len(self.oi_history[symbol]) >= 2) else 0,
            "oi_confirmed": oi_status == "confirmed",
            "pcr_shift": round(pcr_shift, 4),
            "position_type": pos_type + margin_label,
            "participant": participant,
            "slot_status": "tbt_active",
            "discard_reason": "score < 4" if score < 4 else None
        }

        if score < 4:
            return result # Still return for logging/rotation stats if needed
            
        return result

    def _process_iceberg(self, symbol: str, ltp: float, ltq: int, current_time: float) -> Optional[dict]:
        seq = self.active_sequences.get(symbol)
        if not seq or abs(seq["price"] - ltp) > (ltp * 0.0005) or (current_time - seq["last_time"]) > 1.5:
            self.active_sequences[symbol] = {"price": ltp, "qtys": [ltq], "times": [current_time], "last_time": current_time}
            return None
        seq["qtys"].append(ltq); seq["times"].append(current_time); seq["last_time"] = current_time
        if len(seq["qtys"]) >= 5:
            return {"mean": statistics.mean(seq["qtys"]), "std_dev": statistics.stdev(seq["qtys"]) if len(seq["qtys"]) > 1 else 0, "max_gap": max([seq["times"][i] - seq["times"][i-1] for i in range(1, len(seq["times"]))]) if len(seq["times"]) > 1 else 0}
        return None

    def _infer_direction(self, tick: dict) -> str:
        bid, ask, ltp = tick.get("bid_price", 0), tick.get("ask_price", 0), tick.get("ltp", 0)
        if bid > 0 and ask > 0:
            if ltp >= ask: return "BUY"
            if ltp <= bid: return "SELL"
            return "BUY" if ltp > (bid + ask) / 2 else "SELL"
        return "NEUTRAL"
