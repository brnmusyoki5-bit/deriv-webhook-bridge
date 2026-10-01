import asyncio
import json
import math
import datetime
import websockets

# =====================================================
# CONFIGURATION
# =====================================================

API_TOKEN = "YOUR_DERIV_API_TOKEN"
APP_ID = "1089"
SYMBOL = "1HZ75V"

DRY_RUN = True

HALVING_STEPS = 4
USE_INT_EQ = True
MA_PERIOD = 200

ENTRY_ZONE_PCT = 15.0
PULLBACK_ZONE_PCT = 35.0

STAKE_AMOUNT = 1.0
DURATION_TICKS = 5

# Cache intervals
CANDLE_REFRESH_INTERVAL = 30  # Fetch candles every 30s instead of every tick

# =====================================================
# STATE TRACKERS
# =====================================================

prev_buy_entry_cond = False
prev_sell_entry_cond = False
prev_buy_pullback_cond = False
prev_sell_pullback_cond = False
prev_buy_exhausted_cond = False
prev_sell_exhausted_cond = False

last_market_state = None
req_id_counter = 1

# =====================================================
# DERIV API HELPERS
# =====================================================

def get_next_req_id():
    global req_id_counter
    req_id_counter += 1
    return req_id_counter

async def send_json(ws, request):
    req_id = get_next_req_id()
    request["req_id"] = req_id
    await ws.send(json.dumps(request))

    while True:
        response = await ws.recv()
        data = json.loads(response)
        
        # Match response to specific req_id
        if data.get("req_id") == req_id:
            return data

async def fetch_candles(ws, symbol, granularity, count=250):
    request = {
        "ticks_history": symbol,
        "adjust_start_time": 1,
        "count": count,
        "end": "latest",
        "style": "candles",
        "granularity": granularity
    }
    response = await send_json(ws, request)
    if "error" in response:
        print("Candle error:", response["error"].get("message", "Unknown error"))
        return []
    return response.get("candles", [])

async def place_order(ws, contract_type, amount, symbol, duration):
    if DRY_RUN:
        print("\n========================================")
        print("           DRY RUN TRADE")
        print("========================================")
        print(f"Contract : {contract_type}")
        print(f"Symbol   : {symbol}")
        print(f"Stake    : ${amount}")
        print(f"Duration : {duration} ticks")
        print("STATUS   : NOT EXECUTED")
        print("========================================\n")
        return

    proposal_request = {
        "proposal": 1,
        "amount": amount,
        "basis": "stake",
        "contract_type": contract_type,
        "currency": "USD",
        "duration": duration,
        "duration_unit": "t",
        "symbol": symbol
    }

    proposal_response = await send_json(ws, proposal_request)
    if "error" in proposal_response or "proposal" not in proposal_response:
        print("Proposal error:", proposal_response.get("error", {}).get("message", "No proposal"))
        return

    proposal_id = proposal_response["proposal"]["id"]
    buy_request = {"buy": proposal_id, "price": amount}
    buy_response = await send_json(ws, buy_request)

    if "error" in buy_response:
        print("BUY error:", buy_response["error"].get("message", "Unknown error"))
        return

    print("\n========================================")
    print("          REAL TRADE EXECUTED")
    print("========================================")
    print(buy_response.get("buy", {}))
    print("========================================\n")

def fmt(value):
    return f"{value:.5f}"

# =====================================================
# MAIN ENGINE
# =====================================================

async def main():
    global prev_buy_entry_cond, prev_sell_entry_cond
    global prev_buy_pullback_cond, prev_sell_pullback_cond
    global prev_buy_exhausted_cond, prev_sell_exhausted_cond
    global last_market_state

    url = f"wss://ws.derivws.com/websockets/v3?app_id={APP_ID}"

    async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
        auth_response = await send_json(ws, {"authorize": API_TOKEN})
        if "error" in auth_response:
            print("Authorization failed:", auth_response["error"].get("message"))
            return

        print("Connected and authorized successfully.")

        now = datetime.datetime.now(datetime.timezone.utc)
        weekday = now.weekday()
        is_weekly_group = weekday in [6, 0, 2, 3]

        tf1_sec = 604800 if is_weekly_group else 86400
        tf2_sec = 86400 if is_weekly_group else 43200
        tf1_name = "W1" if is_weekly_group else "D1"
        tf2_name = "D1" if is_weekly_group else "H12"

        print(f"Active HTFs: {tf1_name} + {tf2_name}")

        await ws.send(json.dumps({"ticks": SYMBOL, "subscribe": 1}))

        # Cache control variables
        last_candle_fetch = 0
        tf1_candles, tf2_candles = [], []

        while True:
            message = await ws.recv()
            data = json.loads(message)

            if "tick" not in data:
                continue

            spot_price = float(data["tick"]["quote"])
            current_time = datetime.datetime.now(datetime.timezone.utc).timestamp()

            # Refresh candle data periodically rather than on every tick
            if current_time - last_candle_fetch > CANDLE_REFRESH_INTERVAL or not tf1_candles:
                tf1_candles = await fetch_candles(ws, SYMBOL, tf1_sec, count=3)
                tf2_candles = await fetch_candles(ws, SYMBOL, tf2_sec, count=MA_PERIOD + 2)
                last_candle_fetch = current_time

            if len(tf1_candles) < 2 or len(tf2_candles) < MA_PERIOD + 1:
                continue

            tf1_completed = tf1_candles[-2]
            tf2_completed = tf2_candles[-2]

            tf1_open, tf1_close = float(tf1_completed["open"]), float(tf1_completed["close"])
            tf2_open, tf2_close = float(tf2_completed["open"]), float(tf2_completed["close"])

            tf1_bull, tf1_bear = tf1_close > tf1_open, tf1_close < tf1_open
            tf2_bull, tf2_bear = tf2_close > tf2_open, tf2_close < tf2_open

            buy_match = tf1_bull and tf2_bull
            sell_match = tf1_bear and tf2_bear
            no_match = not buy_match and not sell_match

            htf_direction = "BUY MATCH" if buy_match else "SELL MATCH" if sell_match else "NO MATCH"

            avg_close = (tf1_close + tf2_close) / 2.0
            equilibrium = math.floor(avg_close) if USE_INT_EQ else avg_close
            divisor = math.pow(2.0, HALVING_STEPS)
            offset = equilibrium / divisor

            upper_exhaustion = equilibrium + offset
            lower_exhaustion = equilibrium - offset
            total_range = upper_exhaustion - lower_exhaustion

            completed_tf2 = tf2_candles[:-1]
            ma_closes = [float(c["close"]) for c in completed_tf2[-MA_PERIOD:]]
            trend_ma = sum(ma_closes) / len(ma_closes)

            uptrend = spot_price > trend_ma
            downtrend = spot_price < trend_ma

            range_position_pct = ((spot_price - lower_exhaustion) / total_range * 100.0) if total_range != 0 else 50.0

            buy_entry_zone = lower_exhaustion + total_range * (ENTRY_ZONE_PCT / 100.0)
            sell_entry_zone = upper_exhaustion - total_range * (ENTRY_ZONE_PCT / 100.0)

            buy_pullback_cond = buy_match and uptrend and spot_price < equilibrium and spot_price > buy_entry_zone
            sell_pullback_cond = sell_match and downtrend and spot_price > equilibrium and spot_price < sell_entry_zone

            buy_entry_cond = buy_match and uptrend and spot_price <= buy_entry_zone and spot_price >= lower_exhaustion
            sell_entry_cond = sell_match and downtrend and spot_price >= sell_entry_zone and spot_price <= upper_exhaustion

            buy_exhausted_cond = buy_match and uptrend and spot_price >= upper_exhaustion
            sell_exhausted_cond = sell_match and downtrend and spot_price <= lower_exhaustion

            buy_entry_event = buy_entry_cond and not prev_buy_entry_cond
            sell_entry_event = sell_entry_cond and not prev_sell_entry_cond

            prev_buy_entry_cond = buy_entry_cond
            prev_sell_entry_cond = sell_entry_cond

            # Market State Determination
            if no_match:
                market_state = "NO HTF MATCH"
            elif buy_exhausted_cond:
                market_state = "BUY EXHAUSTED"
            elif sell_exhausted_cond:
                market_state = "SELL EXHAUSTED"
            elif buy_entry_cond:
                market_state = "BUY ENTRY"
            elif sell_entry_cond:
                market_state = "SELL ENTRY"
            elif buy_pullback_cond:
                market_state = "BUY PULLBACK WATCH"
            elif sell_pullback_cond:
                market_state = "SELL PULLBACK WATCH"
            elif buy_match and uptrend:
                market_state = "BUY CONTINUATION"
            elif sell_match and downtrend:
                market_state = "SELL CONTINUATION"
            else:
                market_state = "WAIT"

            # Print status log only when market state changes
            if market_state != last_market_state:
                timestamp = datetime.datetime.now().strftime("%H:%M:%S")
                print(f"\n========================================")
                print(f"[{timestamp}] MARKET STATE CHANGED: {market_state}")
                print(f"Price: {fmt(spot_price)} | Bias: {htf_direction} | Range: {range_position_pct:.1f}%")
                print(f"========================================\n")
                last_market_state = market_state

            # Order Execution Triggers
            if buy_entry_event:
                print(">>> BUY ENTRY TRIGGERED")
                await place_order(ws, "CALL", STAKE_AMOUNT, SYMBOL, DURATION_TICKS)

            elif sell_entry_event:
                print(">>> SELL ENTRY TRIGGERED")
                await place_order(ws, "PUT", STAKE_AMOUNT, SYMBOL, DURATION_TICKS)

if __name__ == "__main__":
    while True:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            print("\nBot stopped manually.")
            break
        except Exception as e:
            print(f"\nConnection or execution error: {e}")
            print("Reconnecting in 5 seconds...")
            asyncio.sleep(5)
