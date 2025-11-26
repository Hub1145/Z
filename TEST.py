"""
================================================================================
PHEMEX SOLUSDT LIVE TRADING BOT - LONG STRATEGY WITH CPR
================================================================================

STRATEGY OVERVIEW:
------------------
This bot implements a daily CPR (Central Pivot Range) based long strategy for
SOLUSDT perpetual futures on Phemex.

ENTRY CONDITIONS (Daily at 00:00:05 UTC):
- Price must open ABOVE TC (Top Central Pivot)
- Places a LIMIT order at TC level
- Entry only executes if price comes down to TC during the day

EMA FILTER (Fixed - Mode 2):
- If 21 EMA < 50 EMA: Reduces take profit from 0.7% to 0.2%
- Does NOT skip entry, just reduces profit target

EXIT CONDITIONS:
- Take Profit: 0.7% (or 0.2% if EMA bearish)
- Stop Loss: 20% below entry
- End of Day: Closes position at 23:50 UTC if not already exited

RISK MANAGEMENT:
- Uses 99.9% of available balance for position sizing
- Leverage: 1x (adjustable)
- Real-time tracking with 1-minute precision



TEST MODES:
1. MANUAL_TEST_MODE: Immediate order placement with static values near current price
- Skips entry check
- Uses current price ±1% for testing
- Places orders immediately on startup

2. AUTO_TEST_MODE: Accelerated realistic simulation
- Entry check: 1 minute after start
- EOD exit: 2 minutes after start
- Full workflow testing

3. PRODUCTION_MODE: Real trading with actual schedule
- Entry: 00:00:05 UTC daily
- EOD Exit: 23:50 UTC

USAGE:
- Set TEST_MODE = "MANUAL" for immediate testing
- Set TEST_MODE = "AUTO" for accelerated simulation
- Set TEST_MODE = "PRODUCTION" for live trading
================================================================================
"""

import os
import datetime
import hashlib
import hmac
import json
import requests
import websocket
import pandas as pd
import ta
import numpy as np
from time import time, sleep
import threading
import traceback
import gc # Added for garbage collection

# ================================================================================
# TEST CONFIGURATION
# ================================================================================
# TEST MODE SELECTION
TEST_MODE = "AUTO" # Options: "MANUAL", "AUTO", "PRODUCTION"

"""
TEST MODE GUIDE:
----------------
MANUAL:
- Places order immediately on startup
- Uses static TC value near current price
- Skips daily entry check
- Best for: Testing order placement, TP/SL, exit protocols

AUTO:
- Entry check: 1 minute after start
- EOD exit: 2 minutes after start
- Best for: Testing full workflow quickly

PRODUCTION:
- Real schedule (00:00:05 UTC entry, 23:50 UTC exit)
- Best for: Live trading
"""

# Manual test configuration
MANUAL_TEST_CONFIG = {
'use_current_price': True, # Use current market price
'price_offset_percent': 0.1, # Entry at current price - 0.5%
'force_entry': True, # Skip entry conditions check
'immediate_order': True, # Place order on startup
'test_tp_after_seconds': 6000, # Test TP hit after 30 seconds
'test_eod_after_seconds': 6000, # Test EOD exit after 60 seconds
}

# ================================================================================
# LOGGING SETUP
# ================================================================================
LOG_DIR = "trading_logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file_path = os.path.join(LOG_DIR, f"bot_{TEST_MODE.lower()}_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.txt")

def log_message(message, timestamp=None, section="GENERAL"):
    """Enhanced logging with sections and error suppression"""
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc)
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=datetime.timezone.utc)

    timestamp_str = timestamp.strftime("%Y-%m-%d %H:%M:%S %Z")
    section_tag = f"[{section:8s}]" if section != "GENERAL" else ""
    # Sanitize message for console output to avoid encoding errors
    sanitized_message = message.replace('✓', '(OK)').replace('🛑', '(STOP)').replace('⚠️', '(WARNING)').replace('✅', '(COMPLETE)').replace('🔔', '(BELL)').replace('🟢', '(GREEN_CIRCLE)').replace('📁', '(FOLDER)').replace('🔴', '(RED_CIRCLE)').replace('🟢', '(GREEN_CIRCLE)').replace('🟡', '(YELLOW_CIRCLE)').replace('⚪', '(WHITE_CIRCLE)').replace('🎉', '(PARTY)').replace('❌', '(CROSS_MARK)').replace('🛑', '(STOP_SIGN)').replace('🎯', '(TARGET)').replace('🕐', '(CLOCK)').replace('🤖', '(BOT)').replace('🧪', '(TEST_TUBE)').replace('📅', '(CALENDAR)').replace('🔔', '(BELL)').replace('📁', '(FOLDER)')
    log_entry_for_print = f"[{timestamp_str}] {section_tag} {sanitized_message}\n"
    print(log_entry_for_print, end='')

    # Original log_entry for file writing (full unicode support)
    full_log_entry = f"[{timestamp_str}] {section_tag} {message}\n"

    try:
        with open(log_file_path, "a", encoding="utf-8") as log_file:
            log_file.write(full_log_entry)
    except Exception:
        pass

# ================================================================================
# API CONFIGURATION
# ================================================================================
# Testnet Configuration
API_KEY_TESTNET = "c4bae1e3-5fb3-405f-ad1c-4d9104a7bdfa"
API_SECRET_TESTNET = "RSA0gICcrZjePI37SqRxFEV2TUnErsCL1QDuEJ28bao0NmUwNDk1ZC1jNzE2LTRhMDctYmFjYS0xYzgxNjI5NmFkMGQ"
REST_API_BASE_URL_TESTNET = "https://testnet-api.phemex.com"
WEBSOCKET_BASE_URL_TESTNET = "wss://testnet-api.phemex.com/ws"

# Live Configuration
API_KEY_LIVE = "8c9a553f-8f06-4a42-96b6-4f1ab0f4227a"
API_SECRET_LIVE = "lZldz0rqfLV54hgjo8fHavfit4qayWroU0a20mISApxjNDlkMDJjNS1lOTkyLTRkNzItOTE5MC1mMGZhZmNhMDk2OWU"
REST_API_BASE_URL_LIVE = "https://api.phemex.com"
WEBSOCKET_BASE_URL_LIVE = "wss://ws.phemex.com"

# Active Configuration
USE_TESTNET = False # Always use testnet for testing purposes Set False if ready for live trading

if USE_TESTNET:
    API_KEY = API_KEY_TESTNET
    API_SECRET = API_SECRET_TESTNET
    REST_API_BASE_URL = REST_API_BASE_URL_TESTNET
    WEBSOCKET_BASE_URL = WEBSOCKET_BASE_URL_TESTNET
else:
    API_KEY = API_KEY_LIVE
    API_SECRET = API_SECRET_LIVE
    REST_API_BASE_URL = REST_API_BASE_URL_LIVE
    WEBSOCKET_BASE_URL = WEBSOCKET_BASE_URL_LIVE

# ================================================================================
# TRADING CONFIGURATION
# ================================================================================
SYMBOL = 'SOLUSDT'
CURRENCY = 'USDT'
POSITION_SIDE = "Merged" # Or "Long" / "Short" if not using hedge mode and want to specify
TIMEFRAME_CPR = '1d'
TIMEFRAME_TRADE = '1h'

# Risk Parameters
RISK_PERCENT = 2.0
LEVERAGE = 10

# Exit Parameters
SL_PERCENT = 5
TP_PERCENT = 0.7
TP_PERCENT_REDUCED = 0.2

# Historical Data
HISTORICAL_DATA_MONTHS = 3
MIN_CONTRACT_QTY = 1

# Add these global variables at the top with your other globals
ws_authenticated = threading.Event()
ws_subscriptions_ready = threading.Event()
bot_startup_complete = False

entry_sl_price = 0.0 # Add this with other globals
# 1. ADD GLOBAL VARIABLE (with other globals at top)
sl_hit_triggered = False
sl_hit_lock = threading.Lock()

# Track entry order ID for SL detection
entry_order_with_sl = None
entry_order_sl_lock = threading.Lock()
# Schedule Configuration based on TEST_MODE
now_utc = datetime.datetime.now(datetime.timezone.utc)

if TEST_MODE == "MANUAL":
    # Manual mode - no scheduled entry check
    DAILY_ENTRY_CHECK_HOUR = 0 # Invalid hour to skip scheduled checks
    DAILY_ENTRY_CHECK_MINUTE = 0
    DAILY_ENTRY_CHECK_SECOND = 0
    EOD_EXIT_HOUR_UTC = 22
    EOD_EXIT_MINUTE_UTC = 59 # EOD in 2 minutes
    log_message("MANUAL TEST MODE - Immediate order placement", section="SYSTEM")

elif TEST_MODE == "AUTO":
    # Auto mode - accelerated schedule
    DAILY_ENTRY_CHECK_HOUR = now_utc.hour
    DAILY_ENTRY_CHECK_MINUTE = (now_utc.minute + 1) % 60 # Entry in 1 minute
    DAILY_ENTRY_CHECK_SECOND = 0
    EOD_EXIT_HOUR_UTC = now_utc.hour
    EOD_EXIT_MINUTE_UTC = (now_utc.minute + 2) % 60 # EOD in 2 minutes
    log_message("🧪 AUTO TEST MODE - Accelerated schedule", section="SYSTEM")

else: # PRODUCTION
    DAILY_ENTRY_CHECK_HOUR = 0
    DAILY_ENTRY_CHECK_MINUTE = 0
    DAILY_ENTRY_CHECK_SECOND = 5
    EOD_EXIT_HOUR_UTC = 23
    EOD_EXIT_MINUTE_UTC = 50
    log_message("🔴 PRODUCTION MODE - Live trading schedule", section="SYSTEM")

# TP Hit Detection Flag
tp_hit_triggered = False
tp_hit_lock = threading.Lock()

# Test flags
manual_test_order_placed = False
manual_test_tp_scheduled = False
manual_test_eod_scheduled = False

# ================================================================================
# PRODUCT INFO
# ================================================================================
PRODUCT_INFO = {
    "pricePrecision": None, # New: Number of decimal places for price
    "qtyPrecision": None, # New: Number of decimal places for quantity
    "priceTickSize": None, # New: Smallest price increment
    "qtyStepSize": None, # New: Smallest quantity increment
    "contractSize": None, # Value of one contract in quote currency (e.g., 1 for linear)
    "priceScale": None, # Can be used as 10^pricePrecision
    "qtyScale": None, # Can be used as 10^qtyPrecision
}

# ================================================================================
# GLOBAL STATE VARIABLES
# ================================================================================
# WebSocket data
latest_trade_price = None
latest_trade_timestamp = None
trade_data_lock = threading.Lock()

# Account info
account_balance = 0.0
available_balance = 0.0
account_info_lock = threading.Lock()

# WebSocket connection
ws = None

# Position tracking
in_position = False
position_entry_price = 0.0
position_qty = 0.0
current_stop_loss = 0.0
current_take_profit = 0.0
position_lock = threading.Lock()

# Order tracking
pending_entry_order_id = None
pending_entry_order_details = {}
position_exit_orders = {}
entry_reduced_tp_flag = False

# Historical data storage
historical_data_store = {'1d': None, '1h': None}
data_lock = threading.Lock()

# Thread references
trading_logic_thread = None
position_manager_thread = None
live_monitor_thread = None
manual_test_thread = None

# Shutdown flag
shutdown_flag = threading.Event()

# Timeframe mapping
intervals = {
    '1m': 60, '3m': 180, '5m': 300, '15m': 900, '30m': 1800,
    '1h': 3600, '2h': 7200, '4h': 14400, '6h': 21600, '8h': 28800,
    '12h': 43200, '1d': 86400, '1w': 604800, '1M': 2592000
}
interval_to_timeframe_str = {v: k for k, v in intervals.items()}

# ================================================================================
# HELPER FUNCTIONS
# ================================================================================
def safe_float(value, default=0.0):
    """Safely convert to float"""
    try:
        return float(value)
    except (ValueError, TypeError):
        return default

# ================================================================================
# AUTHENTICATION FUNCTIONS
# ================================================================================
def generate_signature(path, query_string_for_sign, expiry_int, body=None):
    """Generates the Phemex API signature using the canonical query string."""
    if body:
        body_str = json.dumps(body, separators=(',', ':'), sort_keys=True)
        message = path + query_string_for_sign + str(expiry_int) + body_str
    else:
        message = path + query_string_for_sign + str(expiry_int)
    message = message.encode('utf-8')
    secret = API_SECRET.encode('utf-8')
    signature = hmac.new(secret, message, hashlib.sha256).hexdigest()
    return signature

def phemex_request(method, path, params=None, body_dict=None):
    """
    Generic function to make Phemex REST API requests.
    Handles signature generation correctly for GET and POST/PUT requests.
    """
    query_string_for_signature = ""
    query_params_for_request = params

    if params:
        sorted_params = sorted(params.items())
        query_string_for_signature = "&".join([f"{k}={v}" for k, v in sorted_params])

    url = f"{REST_API_BASE_URL}{path}"
    if method.upper() == 'PUT' and params:
        url += "?" + query_string_for_signature

    expiry_int = int(time()) + 60

    signature = generate_signature(path, query_string_for_signature, expiry_int, body_dict)

    headers = {
        "x-phemex-access-token": API_KEY,
        "x-phemex-request-expiry": str(expiry_int),
        "x-phemex-request-signature": signature,
        "Content-Type": "application/json"
    }

    try:
        req_func = getattr(requests, method.lower(), None)
        if not req_func:
             log_message(f"Unsupported HTTP method: {method}")
             return None

        kwargs = {'headers': headers, 'timeout': 15}
        if query_params_for_request and method.upper() in ['GET', 'DELETE']:
             kwargs['params'] = query_params_for_request
        elif body_dict and method.upper() in ['POST', 'PUT']:
             kwargs['data'] = json.dumps(body_dict, separators=(',', ':'), sort_keys=True)

        response = req_func(url, **kwargs)

        if response.status_code == 200:
            try:
                json_response = response.json()
                if json_response.get('code') != 0 and json_response.get('code') is not None:
                     log_message(f"Phemex API returned non-zero code: {json_response.get('code')} Msg: {json_response.get('msg')} for {method} {path}")
                return json_response
            except json.JSONDecodeError:
                 log_message(f"Failed to decode JSON response for {method} {path}. Status: {response.status_code}, Response: {response.text[:200]}")
                 return None
        else:
             response.raise_for_status()

    except requests.exceptions.Timeout:
        log_message(f"Phemex API request timeout: {method} {path}")
        return None
    except requests.exceptions.RequestException as e:
        status_code = e.response.status_code if e.response is not None else "N/A"
        err_text = e.response.text[:200] if e.response is not None else 'No response text'
        log_message(f"Phemex API HTTP Error ({method} {path}): Status={status_code}, Error={e}. Response: {err_text}")
        if e.response is not None:
            try:
                error_data = e.response.json()
                log_message(f"Phemex API Error Body: Code={error_data.get('code')}, Msg={error_data.get('msg')}")
            except json.JSONDecodeError:
                pass
        return None
    except Exception as e:
        log_message(f"Unexpected error during Phemex API request ({method} {path}): {e}")
        import traceback
        traceback.print_exc()
        return None
    return None
# ================================================================================
# PRODUCT INFO AND CURRENT PRICE FETCHING
# ================================================================================
def get_current_market_price():
    """Fetches current market price from Phemex"""
    try:
        # Use linear specific ticker endpoint
        path = "/g-public/md/v1/ticker/24hr"
        params = {"symbol": SYMBOL}
        response = phemex_request("GET", path, params=params)

        if response and response.get('code') == 0:
            result = response.get('result', [])
            if result and isinstance(result, list):
                data = result[0]
                last_rp = data.get('lastRp') # Raw price for linear
                if last_rp is not None:
                    current_price = safe_float(last_rp)
                    log_message(f"Current linear market price: ${current_price:.2f}", section="SYSTEM")
                    return current_price
            else:
                log_message("lastRp not found in linear ticker response. Falling back to WS.", section="ERROR")
                with trade_data_lock:
                    if latest_trade_price:
                        log_message(f"Current market price (from WS): ${latest_trade_price:.2f}", section="SYSTEM")
                        return latest_trade_price

        log_message("Failed to fetch current linear price from REST. Falling back to WS.", section="ERROR")
        with trade_data_lock:
            if latest_trade_price:
                log_message(f"Current market price (from WS): ${latest_trade_price:.2f}", section="SYSTEM")
                return latest_trade_price

        log_message("Failed to fetch current price from any source", section="ERROR")
        return None
    except Exception as e:
        log_message(f"Exception in get_current_market_price: {e}", section="ERROR")
        return None

def fetch_product_info(target_symbol):
    """Fetches and stores product information for linear contracts (and leverage limits if present)"""
    global PRODUCT_INFO

    try:
        path = "/public/products"
        response = phemex_request("GET", path)  # No params required

        if response and response.get('code') == 0:
            product_data = None
            perp_products = response.get('data', {}).get('perpProductsV2', [])

            for product in perp_products:
                if product.get('symbol') == target_symbol:
                    product_data = product
                    break

            if not product_data:
                log_message(f"Product {target_symbol} not found in perpProductsV2 response.", section="ERROR")
                return False

            price_precision = product_data.get('pricePrecision')
            qty_precision = product_data.get('qtyPrecision')

            if price_precision is None or qty_precision is None:
                log_message(f"Missing pricePrecision or qtyPrecision for {target_symbol}.", section="ERROR")
                return False

            PRODUCT_INFO['pricePrecision'] = price_precision
            PRODUCT_INFO['qtyPrecision'] = qty_precision
            PRODUCT_INFO['priceTickSize'] = 1 / (10 ** price_precision)
            PRODUCT_INFO['qtyStepSize'] = 1 / (10 ** qty_precision)
            PRODUCT_INFO['priceScale'] = price_precision
            PRODUCT_INFO['qtyScale'] = qty_precision
            PRODUCT_INFO['contractSize'] = safe_float(product_data.get('contractSize', "1"), 1.0)

            # Attempt to read leverage limits if provided by the product metadata
            # Different API versions may provide different field names; check common ones.
            # Fallback to sensible defaults if not provided.
            max_lev = None
            min_lev = None

            # Common possible fields in response:
            # 'maxLeverage' or nested leverage object
            if 'maxLeverage' in product_data:
                max_lev = product_data.get('maxLeverage')
            else:
                # Some API responses include a leverage range or map
                lev_info = product_data.get('leverage') or product_data.get('leverageRange') or {}
                if isinstance(lev_info, dict):
                    # try common keys
                    max_lev = lev_info.get('max') or lev_info.get('maxLeverage')
                    min_lev = lev_info.get('min') or lev_info.get('minLeverage')

            # Ensure sensible defaults
            try:
                PRODUCT_INFO['maxLeverage'] = int(max_lev) if max_lev is not None else 100
            except (ValueError, TypeError):
                PRODUCT_INFO['maxLeverage'] = 100

            try:
                PRODUCT_INFO['minLeverage'] = int(min_lev) if min_lev is not None else 1
            except (ValueError, TypeError):
                PRODUCT_INFO['minLeverage'] = 1

            log_message(f"Product info loaded for linear: {PRODUCT_INFO}", section="SYSTEM")
            log_message(f"Leverage bounds for {target_symbol}: min={PRODUCT_INFO['minLeverage']} max={PRODUCT_INFO['maxLeverage']}", section="SYSTEM")
            return True
        else:
            msg = response.get('msg') if response else 'No response'
            log_message(f"Failed to fetch product info for linear contracts (code: {response.get('code') if response else 'N/A'}, msg: {msg})", section="ERROR")
            return False
    except Exception as e:
        log_message(f"Exception in fetch_product_info (linear): {e}", section="ERROR")
        return False

def phemex_set_leverage(symbol, leverage_val):
    """Sets leverage for the trading symbol for linear contracts with validation/clamping.

    Issues addressed:
    - Validate leverage within product min/max (if available)
    - Only send posSide when it's 'Long' or 'Short' (do NOT send 'Merged')
    - Log adjustments clearly
    """
    try:
        path = "/g-positions/leverage"

        # Ensure integer leverage
        try:
            lev_int = int(float(leverage_val))
        except Exception:
            log_message(f"Invalid leverage value provided: {leverage_val}", section="ERROR")
            return False

        # Read product leverage bounds if available
        min_lev = PRODUCT_INFO.get('minLeverage', 1)
        max_lev = PRODUCT_INFO.get('maxLeverage', 100)

        if lev_int < min_lev:
            log_message(f"Requested leverage {lev_int} < product min {min_lev}. Clamping to min.", section="SYSTEM")
            lev_int = min_lev
        if lev_int > max_lev:
            log_message(f"Requested leverage {lev_int} > product max {max_lev}. Clamping to max.", section="SYSTEM")
            lev_int = max_lev

        # Build params: posSide is required only in hedged mode if setting one side.
        params = {
            "symbol": symbol,
            "leverage": int(lev_int)
        }

        # Only include posSide if explicitly Long or Short. Do NOT send 'Merged' as posSide.
        if POSITION_SIDE and POSITION_SIDE.lower() in ["long", "short"]:
            params["posSide"] = POSITION_SIDE.capitalize()
        else:
            # omit posSide for merged/unified mode; log it
            log_message(f"Not including posSide in leverage request (POSITION_SIDE='{POSITION_SIDE}')", section="SYSTEM")

        log_message(f"Setting leverage to {lev_int}x for {symbol} (params: {params})", section="SYSTEM")
        response = phemex_request("PUT", path, params=params)

        if response and response.get('code') == 0:
            log_message(f"✓ Leverage set successfully for {symbol} -> {lev_int}x", section="SYSTEM")
            return True
        else:
            # Provide extra logging for API error
            msg = response.get('msg') if response else 'No response'
            code = response.get('code') if response else 'N/A'
            log_message(f"Failed to set leverage for {symbol}: code={code} msg={msg}", section="ERROR")
            return False
    except Exception as e:
        log_message(f"Exception in phemex_set_leverage: {e}", section="ERROR")
        return False

# ================================================================================
# ORDER PLACEMENT FUNCTIONS
# ================================================================================
def phemex_place_order(symbol, side, qty, price=None, order_type="Market", time_in_force=None, reduce_only=False):
    """Places an order on Phemex."""
    path = "/g-orders" # POST endpoint

    price_precision = PRODUCT_INFO.get('pricePrecision', 4)
    qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)

    body = {"symbol": symbol, "side": side.capitalize(), "orderQtyRq": f"{qty:.{qty_precision}f}",
            "ordType": order_type.capitalize(), "posSide": POSITION_SIDE,
            "reduceOnly": reduce_only}
    if time_in_force: body["timeInForce"] = time_in_force.capitalize()
    if order_type.capitalize() == "Limit" and price:
        body["priceRp"] = f"{price:.{price_precision}f}"
    if order_type.capitalize() == "Stop" and price:
        body["stopPxRp"] = f"{price:.{price_precision}f}"
    body = {k: v for k, v in body.items() if v is not None} # Clean Nones
    log_message(f"Placing Order Request: {body}", section="TRADE")
    response = phemex_request("POST", path, body_dict=body)
    if response and response.get('code') == 0:
        log_message(f"Order placement successful: {response.get('data', {})}", section="TRADE")
        return response.get('data')
    elif response:
        log_message(f"Order placement failed: Code={response.get('code')}, Msg={response.get('msg')}", section="ERROR")
        return None
    else:
        log_message("Order placement failed: No response/network error.", section="ERROR")
        return None
# ================================================================================
# VERIFICATION FUNCTION - Check SL/TP on Order (Not applicable for linear with current order placement)
# ================================================================================
def verify_order_sl_tp(order_id):
    """
    Fetch order details to verify SL/TP are set for linear contracts.
    For linear, SL/TP are separate orders, so this function is mainly to check if they exist.
    """
    try:
        path = "/g-orders/detail" # Linear order detail endpoint
        params = {"symbol": SYMBOL, "orderID": order_id}
        response = phemex_request("GET", path, params=params)

        if response and response.get('code') == 0:
            order = response.get('data', {})
            if order.get('orderID') == order_id:
                # For linear, SL/TP are separate orders, not embedded within the entry order.
                # This function is now mainly to confirm the entry order was placed.
                log_message(f"✓ Verified entry order {order_id[:12]}... exists.", section="TRADE")
                return True

        log_message(f"Could not verify entry order {order_id[:12]}...", section="ERROR")
        return False
    except Exception as e:
        log_message(f"Exception in verify_order_sl_tp (linear): {e}", section="ERROR")
        return False

# ================================================================================
# CLOSE UNFILLED ENTRY ORDERS
# ================================================================================
def close_all_entry_orders():
    """Cancels all unfilled active entry orders (Buy orders) for linear contracts"""
    try:
        log_message("Attempting to close unfilled linear entry orders...", section="TRADE")

        # Use linear activeList endpoint
        path = "/g-orders/active"
        params = {"symbol": SYMBOL}
        response = phemex_request("GET", path, params=params)

        if not response or response.get('code') != 0:
            log_message("No linear orders found or API error (OK if no orders)", section="TRADE")
            return True

        orders = response.get('data', {}).get('rows', [])
        cancelled_count = 0

        for order in orders:
            try:
                order_id = order.get('orderID')
                status = order.get('ordStatus')
                side = order.get('side')

                if side == 'Buy' and status not in ['Filled', 'FullyFilled', 'Canceled', 'Cancelled']:
                    if phemex_cancel_order(SYMBOL, order_id):
                        cancelled_count += 1
                    sleep(0.1)
            except Exception as e:
                log_message(f"Error processing linear order: {e}", section="ERROR")

        if cancelled_count > 0:
            log_message(f"✓ Closed {cancelled_count} unfilled linear entry orders", section="TRADE")
        else:
            log_message(f"No unfilled linear entry orders to close (OK)", section="TRADE")

        return True
    except Exception as e:
        log_message(f"Exception in close_all_entry_orders (linear): {e} (continuing)", section="ERROR")
        return True

# ================================================================================
# HANDLE TP HIT (0.7%)
# ================================================================================
def handle_tp_hit():
    """
    TP HIT PROTOCOL:
    1. Close all unfilled entry orders
    2. Wait 3 seconds (3 x 1-second candles)
    3. If position still open (partial fill), market close it
    """
    global tp_hit_triggered, in_position, position_qty

    try:
        log_message("=" * 80, section="TRADE")
        log_message("🎯 TP HIT (0.7%) - EXECUTING PROTOCOL", section="TRADE")
        log_message("=" * 80, section="TRADE")

        log_message("Step 1: Closing unfilled entry orders...", section="TRADE")
        close_all_entry_orders()

        sleep(1)

        log_message("Step 2: Checking linear position status...", section="TRADE")
        path = "/g-accounts/positions" # Linear endpoint
        params = {"currency": CURRENCY}
        response = phemex_request("GET", path, params=params)

        position_still_open = False
        open_qty = 0.0

        if response and response.get('code') == 0:
            positions = response.get('data', {}).get('positions', [])
            for pos in positions:
                if pos.get('symbol') == SYMBOL and pos.get('side') == 'Buy':
                    size_rv = safe_float(pos.get('sizeRv', 0)) # Raw value size for linear
                    if size_rv > 0:
                        position_still_open = True
                        open_qty = size_rv
                        log_message(f"Linear position still open: {open_qty} {SYMBOL} (partial fill)", section="TRADE")
                        break

        if position_still_open and open_qty > 0:
            log_message("Step 3: Waiting 3 seconds (monitoring 3 x 1-second candles)...", section="TRADE")
            for i in range(3):
                log_message(f" [{i+1}/3 seconds elapsed]", section="TRADE")
                sleep(1)

            log_message("Step 4: Market closing remaining linear position...", section="TRADE")
            exit_order = phemex_place_order(
                SYMBOL,
                "Sell",
                open_qty, # Pass float quantity for linear
                order_type="Market",
                reduce_only=True
            )

            if exit_order and exit_order.get('orderID'):
                log_message(f"✓ Market close order placed for {open_qty} {SYMBOL}", section="TRADE")
            else:
                log_message(f"⚠ Market close order may have failed (OK if already closed)", section="TRADE")

            sleep(1)
            cancel_all_exit_orders_and_reset("TP hit - linear position closed")
        else:
            log_message("Linear position fully closed by TP order. No market close needed.", section="TRADE")
            cancel_all_exit_orders_and_reset("TP hit - fully closed")

        with tp_hit_lock:
            tp_hit_triggered = False

        log_message("=" * 80, section="TRADE")
        log_message("✓ TP HIT PROTOCOL COMPLETE (Linear)", section="TRADE")
        log_message("=" * 80, section="TRADE")

    except Exception as e:
        log_message(f"Exception in handle_tp_hit (linear): {e} (continuing)", section="ERROR")
        with tp_hit_lock:
            tp_hit_triggered = False

# ================================================================================
# HANDLE EOD EXIT (23:50 UTC)
# ================================================================================
def handle_eod_exit():
    """
    EOD EXIT PROTOCOL (23:50 UTC) for LINEAR contracts:
    1. Market close all open positions
    2. Close all unfilled entry orders
    3. Force cancel ALL remaining orders
    """
    try:
        log_message("=" * 80, section="TRADE")
        log_message("🕐 EOD EXIT TRIGGERED (Linear)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        with position_lock:
            is_in_pos = in_position
            pos_qty = position_qty

        log_message("Step 1: Checking for open linear positions...", section="TRADE")

        try:
            path = "/g-accounts/positions" # Linear endpoint
            params = {"currency": CURRENCY}
            response = phemex_request("GET", path, params=params)

            if response and response.get('code') == 0:
                positions = response.get('data', {}).get('positions', [])
                for pos in positions:
                    if pos.get('symbol') == SYMBOL:
                        size_rv = safe_float(pos.get('sizeRv', 0)) # Raw value size for linear
                        side = pos.get('side')

                        if size_rv > 0:
                            log_message(f"Found open {side} linear position: {size_rv} {SYMBOL} - closing...", section="TRADE")
                            close_side = "Sell" if side == "Buy" else "Buy"

                            exit_order = phemex_place_order(
                                SYMBOL,
                                close_side,
                                size_rv, # Pass float quantity for linear
                                order_type="Market",
                                reduce_only=True
                            )

                            if exit_order and exit_order.get('orderID'):
                                log_message(f"✓ Market close order placed", section="TRADE")
                            else:
                                log_message(f"⚠ Market close failed (OK if already closed)", section="TRADE")

                            sleep(1)
                            break
            else:
                log_message("No linear positions found or API error (OK)", section="TRADE")
        except Exception as e:
            log_message(f"Error closing linear position: {e} (OK, continuing)", section="TRADE")

        log_message("Step 2: Closing unfilled entry orders...", section="TRADE")
        try:
            close_all_entry_orders()
        except Exception as e:
            log_message(f"Error closing entry orders: {e} (OK, continuing)", section="TRADE")

        sleep(0.5)

        log_message("Step 3: Force cancelling all remaining linear orders...", section="TRADE")
        try:
            path = "/g-orders/all" # Use linear cancel endpoint for all orders
            params = {
                "symbol": SYMBOL,
                "untriggered": "true" # Cancel untriggered SL/TP orders
            }
            response = phemex_request("DELETE", path, params=params)
            if response and response.get('code') == 0:
                log_message(f"✓ All linear orders cancelled", section="TRADE")
            else:
                log_message(f"⚠ All linear orders cancel response: {response} (OK)", section="TRADE")
        except Exception as e:
            log_message(f"Error force cancelling linear orders: {e} (OK, continuing)", section="TRADE")

        log_message("=" * 80, section="TRADE")
        log_message("✓ EOD EXIT COMPLETE (Linear)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        cancel_all_exit_orders_and_reset("EOD Exit")

    except Exception as e:
        log_message(f"Exception in handle_eod_exit (linear): {e} (continuing)", section="ERROR")
        cancel_all_exit_orders_and_reset("EOD Exit - forced")

# ================================================================================
# ACCOUNT MANAGEMENT
# ================================================================================
def update_account_info():
    """Updates global account balance for linear contracts"""
    global account_balance, available_balance

    try:
        # Use linear specific endpoint for account and position info
        path = "/g-accounts/positions"
        params = {"currency": CURRENCY} # Filter by currency if needed
        response = phemex_request("GET", path, params=params)

        if response and response.get('code') == 0:
            data = response.get('data', {})
            # For linear, account data might be under 'account' or directly in 'data'
            # Look for 'accountBalanceRv' and 'totalUsedBalanceRv'
            account_details = data.get('account')

            if account_details:
                with account_info_lock:
                    # Raw value (Rv) is already unscaled for linear
                    new_total_balance_rv = safe_float(account_details.get('accountBalanceRv', 0))
                    total_used_balance_rv = safe_float(account_details.get('totalUsedBalanceRv', 0))
                    new_avail_balance_rv = max(0.0, new_total_balance_rv - total_used_balance_rv)

                    if (abs(new_total_balance_rv - account_balance) > 1e-9 or
                            abs(new_avail_balance_rv - available_balance) > 1e-9):
                        log_message(f"Balance: {new_total_balance_rv:.8f} {CURRENCY} | Available: {new_avail_balance_rv:.8f} {CURRENCY}", section="CALC")

                    account_balance = new_total_balance_rv
                    available_balance = new_avail_balance_rv
                return True
            else:
                log_message("Linear account details missing", section="ERROR")
                return False
        else:
            log_message(f"Failed to fetch linear account info: {response.get('msg') if response else 'No response'}", section="ERROR")
            return False
    except Exception as e:
        log_message(f"Exception in update_account_info (linear): {e}", section="ERROR")
        return False

# ================================================================================
# ORDER CANCELLATION
# ================================================================================
def phemex_cancel_order(symbol, order_id):
    """Cancels an order on Phemex for linear contracts"""
    try:
        # Linear cancel order endpoint uses DELETE with query params
        path = "/g-orders/cancel"
        params = {
            "symbol": symbol,
            "orderID": order_id
        }

        log_message(f"Cancelling linear order {order_id[:12]}...", section="TRADE")
        response = phemex_request("DELETE", path, params=params)

        if response and response.get('code') == 0:
            log_message(f"✓ Linear order cancelled", section="TRADE")
            return True
        elif response and response.get('code') == 10002: # Common Phemex error for already cancelled/filled
            log_message(f"Linear order already filled/cancelled (OK)", section="TRADE")
            return True
        else:
            log_message(f"Failed to cancel linear order (OK, continuing): {response.get('msg') if response else 'No response'}", section="TRADE")
            return False
    except Exception as e:
        log_message(f"Exception in phemex_cancel_order (linear): {e} (continuing)", section="ERROR")
        return False

# ================================================================================
# HISTORICAL DATA FETCHING
# ================================================================================
def fetch_historical_data_phemex(symbol, timeframe, start_date_str, end_date_str):
    """Fetches historical kline data from Phemex REST API"""
    try:
        path = "/exchange/public/md/v2/kline/list"
        resolution_seconds = intervals.get(timeframe)

        if not resolution_seconds:
            log_message(f"Invalid timeframe: {timeframe}", section="ERROR")
            return []

        start_dt = datetime.datetime.strptime(start_date_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
        end_dt = datetime.datetime.strptime(end_date_str, '%Y-%m-%d').replace(tzinfo=datetime.timezone.utc)
        start_ts = int(start_dt.timestamp())
        end_ts = int(end_dt.timestamp())

        all_data = []
        current_from = start_ts
        max_candles = 1000

        while current_from < end_ts:
            current_to = min(current_from + (max_candles * resolution_seconds) - 1, end_ts)

            params = {
                "symbol": symbol,
                "resolution": resolution_seconds,
                "from": current_from,
                "to": current_to
            }

            response = phemex_request("GET", path, params=params)

            if response and response.get('code') == 0:
                rows = response.get('data', {}).get('rows', [])
                if rows:
                    for kline in rows:
                        try:
                            all_data.append([
                                int(kline[0]) * 1000,
                                float(kline[3]),
                                float(kline[4]),
                                float(kline[5]),
                                float(kline[6]),
                                float(kline[7])
                            ])
                        except (ValueError, TypeError, IndexError) as e:
                            continue

                    last_ts = rows[-1][0]
                    current_from = int(last_ts) + resolution_seconds
                else:
                    current_from = current_to + resolution_seconds

                sleep(0.3)
            else:
                log_message(f"Error fetching klines", section="ERROR")
                return []

        return all_data
    except Exception as e:
        log_message(f"Exception in fetch_historical_data_phemex: {e}", section="ERROR")
        return []

def fetch_initial_historical_data(symbol, timeframe, start_date_str, end_date_str):
    """Fetches initial historical data and stores it"""
    global historical_data_store

    try:
        raw_data = fetch_historical_data_phemex(symbol, timeframe, start_date_str, end_date_str)

        if raw_data:
            df = pd.DataFrame(raw_data, columns=['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume'])
            df.dropna(subset=['Open', 'High', 'Low', 'Close', 'Volume'], inplace=True)

            if df.empty:
                log_message(f"No valid data for {timeframe}", section="ERROR")
                return False

            invalid_rows = df[(df['Low'] > df['High']) |
                            (df['Open'] < df['Low']) | (df['Open'] > df['High']) |
                            (df['Close'] < df['Low']) | (df['Close'] > df['High'])]

            if not invalid_rows.empty:
                log_message(f"WARNING: Found {len(invalid_rows)} invalid OHLC rows", section="ERROR")
                df = df[(df['Low'] <= df['High'])]

            df['Datetime'] = pd.to_datetime(df['Timestamp'], unit='ms', utc=True)
            df = df.set_index('Datetime')
            df = df[~df.index.duplicated(keep='first')]
            df = df.sort_index()

            with data_lock:
                historical_data_store[timeframe] = df

            log_message(f"Loaded {len(df)} candles for {timeframe}", section="SYSTEM")
            return True
        else:
            log_message(f"Failed to fetch data for {timeframe}", section="ERROR")
            return False
    except Exception as e:
        log_message(f"Exception in fetch_initial_historical_data: {e}", section="ERROR")
        return False

# ================================================================================
# WEBSOCKET HANDLING
# ================================================================================
def update_historical_data_from_ws(timeframe_key, klines_ws):
    """Updates historical data storage with new klines from WebSocket"""
    global historical_data_store

    try:
        if not klines_ws:
            return

        with data_lock:
            df = historical_data_store.get(timeframe_key)
            if df is None:
                return

            new_data_points = []
            for kline in klines_ws:
                try:
                    ts_sec = int(kline[0])
                    o = float(kline[3])
                    h = float(kline[4])
                    l = float(kline[5])
                    c = float(kline[6])
                    v = float(kline[7])

                    if not (l <= h):
                        continue

                    dt_utc = datetime.datetime.fromtimestamp(ts_sec, tz=datetime.timezone.utc)

                    if not df.empty and dt_utc == df.index[-1]:
                        df.at[dt_utc, 'Open'] = o
                        df.at[dt_utc, 'High'] = h
                        df.at[dt_utc, 'Low'] = l
                        df.at[dt_utc, 'Close'] = c
                        df.at[dt_utc, 'Volume'] = v
                        continue

                    new_data_points.append({
                        'Datetime': dt_utc,
                        'Open': o,
                        'High': h,
                        'Low': l,
                        'Close': c,
                        'Volume': v
                    })

                except (ValueError, TypeError, IndexError):
                    continue

            if not new_data_points:
                return

            temp_df = pd.DataFrame(new_data_points).set_index('Datetime')
            original_last_time = df.index[-1] if not df.empty else None

            combined_df = pd.concat([df, temp_df])
            combined_df = combined_df[~combined_df.index.duplicated(keep='last')]
            combined_df = combined_df.sort_index()

            # Add this immediately after concat:
            if len(combined_df) > 1000: # Keep only last 1000 candles
                combined_df = combined_df.iloc[-1000:]

            historical_data_store[timeframe_key] = combined_df

            if original_last_time is not None and not combined_df.empty:
                current_last_time = combined_df.index[-1]
                if current_last_time > original_last_time:
                    log_message(f"New {timeframe_key} candle: {current_last_time}", section="SYSTEM")
    except Exception as e:
        log_message(f"Exception in update_historical_data_from_ws: {e}", section="ERROR")

# ================================================================================
# IMPROVED WEBSOCKET MESSAGE HANDLER
# ================================================================================
def on_message(ws_app, message):
    """WebSocket message handler with minimal logging"""
    global latest_trade_price, latest_trade_timestamp, bot_startup_complete

    try:
        msg = json.loads(message)

        # Handle responses (auth, subscriptions)
        if 'result' in msg or 'error' in msg:
            # Skip pong messages
            if msg.get('result') == 'pong':
                return

            # Check for authentication response
            if msg.get('id') == 12345: # Auth request ID
                result = msg.get('result')
                if isinstance(result, dict) and result.get('status') == 'success':
                    log_message("✓ WebSocket authenticated", section="WS")
                    ws_authenticated.set()
                else:
                    log_message(f"✗ Auth failed: code={msg.get('error', {}).get('code')}", section="ERROR")

            # Check for subscription confirmations
            elif 'result' in msg:
                result = msg.get('result')
                if isinstance(result, dict) and result.get('status') == 'success':
                    log_message(f"✓ Subscription OK (ID:{msg.get('id')})", section="WS")
                elif msg.get('error'):
                    error_code = msg.get('error', {}).get('code')
                    if error_code != 6001: # Ignore "invalid argument" for legacy orders.subscribe
                        log_message(f"Subscription error: code={error_code}", section="WS")

        # Handle linear trade data
        elif 'trades_p' in msg and msg.get('symbol') == SYMBOL: # Linear trade topic
            with trade_data_lock:
                trades = msg['trades_p']
                if trades:
                    # Assuming same structure: [timestamp, symbol, price, qty, trade_type]
                    latest_trade_timestamp = trades[-1][0]
                    latest_trade_price = float(trades[-1][2])

        # Handle linear kline data
        elif 'kline_p' in msg and msg.get('symbol') == SYMBOL: # Linear kline topic
            klines = msg['kline_p']
            if klines:
                interval_sec = int(klines[0][1])
                timeframe_key = interval_to_timeframe_str.get(interval_sec)
                if timeframe_key:
                    update_historical_data_from_ws(timeframe_key, klines)

        # Handle linear ORDER updates - ONLY after bot startup complete
        elif 'orders_p' in msg: # Linear order topic
            # Skip initial order dump on startup
            if not bot_startup_complete:
                return

            orders = msg['orders_p']
            if isinstance(orders, list):
                handle_order_update(orders)
            else:
                handle_order_update([orders])
        # Handle linear POSITION updates for SL detection
        elif 'positions_p' in msg: # Linear position topic
            # CRITICAL: Process position updates immediately
            # Don't skip during startup - needed for SL detection
            positions = msg['positions_p']
            if isinstance(positions, list):
                detect_sl_from_position_update(positions)
            else:
                detect_sl_from_position_update([positions])
        # Handle linear AOP updates (silent)
        elif 'accounts_p' in msg: # Linear account topic
            pass
    except json.JSONDecodeError:
        pass
    except Exception as e:
        log_message(f"Exception in on_message: {e}", section="ERROR")

# ================================================================================
# IMPROVED AUTHENTICATION AND SUBSCRIPTION
# ================================================================================
def send_auth(ws_instance):
    """Sends authentication message to WebSocket"""
    try:
        expiry = int(time()) + 60
        auth_string = API_KEY + str(expiry)
        signature = hmac.new(API_SECRET.encode('utf-8'),
                             auth_string.encode('utf-8'),
                             hashlib.sha256).hexdigest()

        auth_msg = {
            "method": "user.auth",
            "params": ["API", API_KEY, signature, expiry],
            "id": 12345
        }

        log_message(f"Sending authentication...", section="WS")
        ws_instance.send(json.dumps(auth_msg))
    except Exception as e:
        log_message(f"Error sending WS auth: {e}", section="ERROR")

def on_open(ws_instance):
    """Enhanced WebSocket open handler for linear contracts"""
    try:
        log_message("=" * 80, section="WS")
        log_message("WebSocket connection opened", section="WS")
        log_message("=" * 80, section="WS")

        ws_authenticated.clear()
        ws_subscriptions_ready.clear()

        # Step 1: Authenticate
        log_message("Step 1: Authenticating...", section="WS")
        send_auth(ws_instance)

        # Wait for the authentication confirmation
        if not ws_authenticated.wait(timeout=10):
            log_message("✗ Authentication timeout", section="ERROR")
            # Optional: try to re-authenticate or close the connection
            log_message("Retrying authentication...", section="WS")
            send_auth(ws_instance)
            if not ws_authenticated.wait(timeout=10):
                log_message("✗ Authentication failed on retry. Closing connection.", section="ERROR")
                ws_instance.close()
                return

        log_message("✓ Authentication confirmed", section="WS")

        # Step 2: Subscribe to trades (linear)
        log_message("Step 2: Subscribing to linear trades...", section="WS")
        trade_sub = {"method": "trade_p.subscribe", "params": [SYMBOL], "id": 1} # Linear trade subscription
        ws_instance.send(json.dumps(trade_sub))
        sleep(0.5)

        # Step 3: Subscribe to AOP (orders/positions/accounts - linear)
        log_message("Step 3: Subscribing to AOP (orders/positions/accounts - linear)...", section="WS")
        # Phemex linear uses "aop_p.subscribe" for perpetuals
        aop_sub = {"method": "aop_p.subscribe", "params": [], "id": 100}
        ws_instance.send(json.dumps(aop_sub))
        sleep(0.5)

        # Step 4: Subscribe to klines (linear)
        subscribed_intervals = set()
        tf_id = 200

        for tf in [TIMEFRAME_CPR, TIMEFRAME_TRADE]:
            if tf and intervals.get(tf) not in subscribed_intervals:
                interval_sec = intervals[tf]
                kline_sub = {
                    "method": "kline_p.subscribe", # Linear kline subscription
                    "params": [SYMBOL, interval_sec],
                    "id": tf_id
                }
                log_message(f"Step {tf_id-196}: Subscribing to {tf} linear klines...", section="WS")
                ws_instance.send(json.dumps(kline_sub))
                subscribed_intervals.add(interval_sec)
                tf_id += 1
                sleep(0.5)

        ws_subscriptions_ready.set()

        log_message("=" * 80, section="WS")
        log_message("✓ All WebSocket linear subscriptions complete", section="WS")
        log_message("🔔 Position updates are now monitored for SL detection", section="WS")
        log_message("=" * 80, section="WS")
    except Exception as e:
        log_message(f"Error in on_open: {e}", section="ERROR")
        import traceback
        traceback.print_exc()

def on_error(ws_app, error):
    """WebSocket error handler"""
    log_message(f"WebSocket error: {error}", section="ERROR")
    if not shutdown_flag.is_set():
        sleep(5)
        try:
            initialize_websocket()
        except Exception:
            pass

def on_close(ws_app, status_code, msg):
    """WebSocket close handler"""
    log_message(f"WebSocket closed: Status {status_code}", section="SYSTEM")
    if not shutdown_flag.is_set():
        sleep(5)
        try:
            initialize_websocket()
        except Exception:
            pass

def initialize_websocket():
    """Initializes and starts WebSocket connection"""
    global ws

    try:
        if ws and hasattr(ws, 'sock') and ws.sock and ws.sock.connected:
            try:
                ws.close()
                sleep(2)
            except Exception:
                pass

        ws = websocket.WebSocketApp(
            WEBSOCKET_BASE_URL,
            on_open=on_open,
            on_message=on_message,
            on_error=on_error,
            on_close=on_close
        )

        wst = threading.Thread(target=ws.run_forever, name="PhemexWS", daemon=True)
        wst.start()
        log_message("WebSocket thread started", section="SYSTEM")

        return ws
    except Exception as e:
        log_message(f"Exception in initialize_websocket: {e}", section="ERROR")
        return None

def send_heartbeat():
    """Sends periodic heartbeat to keep WebSocket alive"""
    global ws

    while not shutdown_flag.is_set():
        sleep(30) # CHANGE FROM 5 TO 30
        try:
            if ws and hasattr(ws, 'sock') and ws.sock and ws.sock.connected:
                ping_msg = json.dumps({"method": "server.ping", "params": [], "id": 99})
                ws.send(ping_msg)
        except Exception:
            pass

# ================================================================================
# CPR AND INDICATOR CALCULATIONS
# ================================================================================
def calculate_cpr_levels(H, L, C):
    """Calculates CPR (Central Pivot Range) levels"""
    try:
        if H < L:
            H, L = L, H

        P = (H + L + C) / 3.0
        TC = (H + L) / 2.0
        BC = P - TC + P

        if TC < BC:
            TC, BC = BC, TC

        R1 = (2 * P) - L
        S1 = (2 * P) - H
        R2 = P + (H - L)
        S2 = P - (H - L)
        R3 = P + 2 * (H - L)
        S3 = P - 2 * (H - L)
        R4 = R3 + (R2 - R1)

        return {
            'P': P, 'TC': TC, 'BC': BC,
            'R1': R1, 'S1': S1,
            'R2': R2, 'S2': S2,
            'R3': R3, 'S3': S3,
            'R4': R4
        }
    except Exception as e:
        log_message(f"Exception in calculate_cpr_levels: {e}", section="ERROR")
        return None

def calculate_indicators(df):
    """Calculates technical indicators (EMA21, EMA50, RSI, MACD)"""
    try:
        if df is None or df.empty:
            return None

        if not isinstance(df.index, pd.DatetimeIndex):
            return None

        df_copy = df.copy()
        price_data_ep = pd.to_numeric(df_copy['Close'], errors='coerce').dropna()

        if price_data_ep.empty:
            return None

        window_ema21 = 21
        if len(price_data_ep) >= window_ema21:
            df_copy['EMA_21'] = ta.trend.ema_indicator(price_data_ep, window=window_ema21)
        else:
            df_copy['EMA_21'] = np.nan

        window_ema50 = 50
        if len(price_data_ep) >= window_ema50:
            df_copy['EMA_50'] = ta.trend.ema_indicator(price_data_ep, window=window_ema50)
        else:
            df_copy['EMA_50'] = np.nan

        window_rsi = 14
        if len(price_data_ep) >= (window_rsi + 1):
            df_copy['RSI'] = ta.momentum.rsi(price_data_ep, window=window_rsi)
        else:
            df_copy['RSI'] = np.nan

        min_macd_candles = 35
        if len(price_data_ep) >= min_macd_candles:
            macd = ta.trend.MACD(price_data_ep, window_slow=26, window_fast=12, window_sign=9)
            df_copy['MACD'] = macd.macd()
            df_copy['MACD_Signal'] = macd.macd_signal()
            df_copy['MACD_Histo'] = macd.macd_diff()
        else:
            df_copy['MACD'], df_copy['MACD_Signal'], df_copy['MACD_Histo'] = np.nan, np.nan, np.nan

        return df_copy

    except Exception as e:
        log_message(f"Exception in calculate_indicators: {e}", section="ERROR")
        return None

# ================================================================================
# ENTRY CONDITIONS CHECK
# ================================================================================
def check_entry_conditions(calculated_data):
    """Checks if entry conditions are met"""
    try:
        with position_lock:
            if in_position:
                log_message("Entry check: Already in position", section="TRADE")
                return False, 0.0, False
            if pending_entry_order_id:
                log_message("Entry check: Pending entry order exists", section="TRADE")
                return False, 0.0, False

        if not calculated_data:
            log_message("Entry check: No calculated data", section="ERROR")
            return False, 0.0, False

        daily_cpr = calculated_data['daily_cpr']
        daily_open = calculated_data['daily_open']
        tc_level = daily_cpr['TC']

        ema_21 = calculated_data['ema_21']
        ema_50 = calculated_data['ema_50']

        # Manual test mode - force entry
        if TEST_MODE == "MANUAL" and MANUAL_TEST_CONFIG.get('force_entry'):
            log_message(f"🧪 MANUAL TEST: Forcing entry (skipping conditions)", section="TEST")
            reduced_tp = False
            return True, tc_level, reduced_tp

        # Normal entry check
        if daily_open <= tc_level:
            log_message(f"Entry check FAIL: Open ({daily_open:.2f}) not above TC ({tc_level:.2f})", section="TRADE")
            return False, 0.0, False

        log_message(f"✓ Entry check PASS: Open ({daily_open:.2f}) above TC ({tc_level:.2f})", section="TRADE")

        reduced_tp = False
        if pd.notna(ema_21) and pd.notna(ema_50):
            if ema_21 < ema_50:
                reduced_tp = True
                log_message(f"⚠ EMA Filter: 21 EMA < 50 EMA - TP reduced to {TP_PERCENT_REDUCED}%", section="TRADE")
            else:
                log_message(f"✓ EMA Filter: 21 EMA >= 50 EMA - Normal TP {TP_PERCENT}%", section="TRADE")
        else:
            log_message("Warning: EMA values not available, using normal TP", section="TRADE")

        return True, tc_level, reduced_tp
    except Exception as e:
        log_message(f"Exception in check_entry_conditions: {e}", section="ERROR")
        return False, 0.0, False

# ================================================================================
# POSITION SIZING
# ================================================================================
def calculate_position_size(entry_price, current_avail_balance):
    """Calculates position size for USD-M (linear) contracts"""
    try:
        if current_avail_balance <= 0:
            log_message("Position sizing: Insufficient available balance", section="ERROR")
            return 0.0

        if not entry_price or entry_price <= 0:
            log_message(f"Position sizing: Invalid entry price {entry_price}", section="ERROR")
            return 0.0

        # current_avail_balance is already unscaled USDT for linear contracts from update_account_info

        # Calculate the amount of USDT to risk
        risk_amount_usdt = current_avail_balance * (RISK_PERCENT / 100.0)

        # Calculate the total value of the position in USDT with leverage
        position_value_usdt = risk_amount_usdt * LEVERAGE

        # Calculate quantity in base asset (e.g., SOL for SOLUSDT)
        # quantity = USDT value / entry price
        qty_base_asset = position_value_usdt / entry_price

        # Apply quantity precision from PRODUCT_INFO
        qty_precision = PRODUCT_INFO.get('qtyPrecision', 8)
        qty_base_asset = round(qty_base_asset, qty_precision)

        # Ensure minimum contract quantity (if applicable for linear)
        # For linear contracts, MIN_CONTRACT_QTY usually refers to the base asset quantity
        if qty_base_asset < MIN_CONTRACT_QTY:
            log_message(f"Position sizing: Calculated quantity {qty_base_asset} < minimum {MIN_CONTRACT_QTY}", section="ERROR")
            return 0.0

        log_message(f"Position size: {qty_base_asset:.{qty_precision}f} {SYMBOL} (Available USDT: {current_avail_balance:.2f})", section="CALC")

        return qty_base_asset
    except Exception as e:
        log_message(f"Exception in calculate_position_size: {e}", section="ERROR")
        return 0.0

# ================================================================================
# TAKE PROFIT AND STOP LOSS CALCULATION
# ================================================================================
def calculate_tp_sl(entry_price, reduced_tp_flag):
    """Calculates take profit and stop loss prices"""
    try:
        if reduced_tp_flag:
            tp_price = entry_price * (1 + TP_PERCENT_REDUCED / 100.0)
            tp_reason = f"Reduced TP {TP_PERCENT_REDUCED}%"
        else:
            tp_price = entry_price * (1 + TP_PERCENT / 100.0)
            tp_reason = f"Normal TP {TP_PERCENT}%"

        sl_price = entry_price * (1 - SL_PERCENT / 100.0)

        log_message(f"TP/SL: TP={tp_price:.2f} ({tp_reason}), SL={sl_price:.2f}", section="CALC")

        return tp_price, sl_price, tp_reason
    except Exception as e:
        log_message(f"Exception in calculate_tp_sl: {e}", section="ERROR")
        return 0, 0, "Error"

# ================================================================================
# ENTRY EXECUTION
# ================================================================================
def initiate_entry_sequence(calculated_data, limit_entry_price, reduced_tp_flag):
    """Places entry order (limit) WITH embedded SL, TP placed AFTER fill"""
    global pending_entry_order_id, entry_reduced_tp_flag, pending_entry_order_details
    global position_manager_thread, entry_sl_price, entry_order_with_sl

    try:
        if not update_account_info():
            log_message("Entry aborted: Could not update account info", section="ERROR")
            return

        with account_info_lock:
            current_avail = available_balance

        qty = calculate_position_size(limit_entry_price, current_avail)
        if qty == 0:
            log_message("Entry aborted: Position size is 0", section="ERROR")
            return

        # Calculate SL price
        sl_price = limit_entry_price * (1 - SL_PERCENT / 100.0)

        log_message("=" * 80, section="TRADE")
        log_message(f"PLACING ENTRY ORDER", section="TRADE")
        log_message(f"Entry Price: ${limit_entry_price:.2f}", section="TRADE")
        log_message(f"Stop Loss: ${sl_price:.2f} (separate order)", section="TRADE") # Updated log
        log_message(f"Quantity: {qty} {SYMBOL}", section="TRADE") # Updated log
        log_message("=" * 80, section="TRADE")

        # Place LIMIT BUY order
        entry_order = phemex_place_order(
            SYMBOL,
            "Buy",
            qty,
            price=limit_entry_price,
            order_type="Limit",
            time_in_force="GoodTillCancel"
        )

        if entry_order and entry_order.get('orderID'):
            order_id = entry_order['orderID']

            with position_lock:
                pending_entry_order_id = order_id
                entry_reduced_tp_flag = reduced_tp_flag
                entry_sl_price = sl_price # Track SL price for separate order placement

            pending_entry_order_details = {
                'order_id': order_id,
                'side': 'Buy',
                'qty': qty,
                'limit_price': limit_entry_price,
                'sl_price': sl_price,
                'order_type': 'Limit',
                'status': 'New',
                'placed_at': datetime.datetime.now(datetime.timezone.utc)
            }

            # Track the entry order for position confirmation
            with entry_order_sl_lock:
                entry_order_with_sl = order_id

            log_message(f"✓ Entry order placed (SL will be placed separately)", section="TRADE")

            # No verify_order_sl_tp needed as SL is not embedded

            if position_manager_thread is None or not position_manager_thread.is_alive():
                position_manager_thread = threading.Thread(
                    target=manage_position_lifecycle,
                    name="PositionManager",
                    daemon=True
                )
                position_manager_thread.start()
                log_message("✓ Position manager started", section="SYSTEM")
        else:
            log_message(f"Entry order placement failed", section="ERROR")
    except Exception as e:
        log_message(f"Exception in initiate_entry_sequence: {e}", section="ERROR")
        import traceback
        traceback.print_exc()

# ================================================================================
# 3. ADD SL HIT HANDLER FUNCTION
# ================================================================================
def handle_sl_hit():
    """
    SIMPLIFIED SL HIT PROTOCOL:
    Since position is already closed by exchange, just cleanup orders and reset state
    """
    global sl_hit_triggered, in_position, position_qty, entry_order_with_sl

    try:
        log_message("=" * 80, section="TRADE")
        log_message("🛑 STOP LOSS HIT - EXECUTING CLEANUP", section="TRADE")
        log_message("=" * 80, section="TRADE")

        # Position already closed by exchange, just cleanup
        log_message("Position already closed by exchange SL", section="TRADE")

        # Step 1: Close any unfilled entry orders (shouldn't exist, but safety check)
        try:
            close_all_entry_orders()
        except Exception as e:
            log_message(f"Entry order cleanup: {e} (OK)", section="TRADE")

        sleep(0.5)

        # Step 2: Cancel TP order and reset state
        log_message("Cancelling TP order and resetting state...", section="TRADE")
        cancel_all_exit_orders_and_reset("SL hit - position closed by exchange")

        # Clear tracked entry order
        with entry_order_sl_lock:
            entry_order_with_sl = None

        with sl_hit_lock:
            sl_hit_triggered = False

        log_message("=" * 80, section="TRADE")
        log_message("✓ SL CLEANUP COMPLETE", section="TRADE")
        log_message("=" * 80, section="TRADE")

    except Exception as e:
        log_message(f"Exception in handle_sl_hit: {e}", section="ERROR")
        # Force reset even on error
        try:
            cancel_all_exit_orders_and_reset("SL hit - forced reset")
        except:
            pass
        with sl_hit_lock:
            sl_hit_triggered = False
        with entry_order_sl_lock:
            entry_order_with_sl = None
# ================================================================================
# ENHANCED ORDER UPDATE HANDLER
# ================================================================================
def handle_order_update(orders_data):
    """
    ENHANCED: Detect SL hits through order status changes
    Handles: Entry fills, TP hits, SL hits, manual closes
    """
    global pending_entry_order_id, in_position, tp_hit_triggered, sl_hit_triggered
    global entry_order_with_sl, position_qty

    try:
        if not isinstance(orders_data, list):
            orders_data = [orders_data]

        with position_lock:
            current_pending_id = pending_entry_order_id
            is_in_pos = in_position
            active_exit_orders = dict(position_exit_orders)
            tracked_qty = position_qty

        with entry_order_sl_lock:
            tracked_entry_order = entry_order_with_sl

        for order in orders_data:
            if not isinstance(order, dict):
                continue

            order_id = order.get('orderID')
            status = order.get('ordStatus')
            symbol = order.get('symbol')
            side = order.get('side', '')
            cum_qty = order.get('cumQty', 0)
            order_qty = order.get('orderQty', 0)
            exec_status = order.get('execStatus', '')
            order_type = order.get('ordType', '')

            if not order_id or not status:
                continue

            if symbol and symbol != SYMBOL:
                continue

            # =====================================================================
            # 🆕 CRITICAL: Detect SL Hit via Order Status (for separate SL order)
            # For linear, SL is a separate order. When it fills, it means SL hit.
            # =====================================================================
            if order_id == active_exit_orders.get('sl') and status in ['Filled', 'FullyFilled']:
                log_message("=" * 80, section="TRADE")
                log_message(f"🛑 SL HIT DETECTED via SL Order Fill!", section="TRADE")
                log_message(f"Order ID: {order_id[:12]}...", section="TRADE")
                log_message(f"Status: {status} | ExecStatus: {exec_status}", section="TRADE")
                log_message("=" * 80, section="TRADE")

                with sl_hit_lock:
                    if not sl_hit_triggered:
                        sl_hit_triggered = True
                        threading.Timer(0.5, handle_sl_hit).start()
                return

            # =====================================================================
            # Handle pending ENTRY order (fills)
            # =====================================================================
            if current_pending_id and order_id == current_pending_id:
                with position_lock:
                    if pending_entry_order_details:
                        pending_entry_order_details['status'] = status
                        pending_entry_order_details['cumQty'] = cum_qty

                # Check for fill
                if status in ['Filled', 'FullyFilled', 'PartiallyFilled'] or cum_qty > 0:
                    log_message("=" * 80, section="TRADE")
                    log_message(f"🎉 ENTRY FILLED: {cum_qty}/{order_qty} {SYMBOL}", section="TRADE")
                    log_message("=" * 80, section="TRADE")

                    if status in ['Filled', 'FullyFilled']:
                        threading.Timer(2.0, lambda: confirm_and_set_active_position(order_id)).start()
                    else:
                        threading.Timer(5.0, lambda: confirm_and_set_active_position(order_id)).start()
                    return

                elif status in ['Canceled', 'Cancelled', 'Rejected'] and not is_in_pos:
                    log_message(f"❌ Entry order {status}", section="TRADE")
                    reset_entry_state(f"Entry order {status}")
                    with entry_order_sl_lock:
                        entry_order_with_sl = None
                    return

            # =====================================================================
            # Handle EXIT orders (TP)
            # =====================================================================
            elif is_in_pos and order_id == active_exit_orders.get('tp'): # Only TP for now
                if status in ['Filled', 'FullyFilled', 'PartiallyFilled'] or cum_qty > 0:
                    log_message("=" * 80, section="TRADE")
                    log_message(f"!!! TP HIT !!! {cum_qty}/{order_qty} {SYMBOL}", section="TRADE")
                    log_message("=" * 80, section="TRADE")

                    with tp_hit_lock:
                        if not tp_hit_triggered:
                            tp_hit_triggered = True
                            threading.Timer(0.5, handle_tp_hit).start()
                    break

    except Exception as e:
        log_message(f"Exception in handle_order_update: {e}", section="ERROR")
        import traceback
        traceback.print_exc()
# ================================================================================
# 5. IMPROVED SL HIT DETECTION - Alternative Method Using Position Updates
# ================================================================================
def detect_sl_from_position_update(positions_msg):
    """
    BACKUP: Detect SL hit from WebSocket position updates
    This runs in parallel with order update detection
    """
    global in_position, position_qty, sl_hit_triggered, entry_order_with_sl

    try:
        if not isinstance(positions_msg, list):
            positions_msg = [positions_msg]

        with position_lock:
            was_in_position = in_position
            expected_qty = position_qty

        if not was_in_position or expected_qty == 0:
            return # Not tracking a position

        # Check current position size from WebSocket update
        current_position_size = 0
        for pos in positions_msg:
            # For linear, check 'sizeRv'
            if pos.get('symbol') == SYMBOL and pos.get('side') == 'Buy':
                size_rv = safe_float(pos.get('sizeRv', 0))
                current_position_size = size_rv
                break

        # CRITICAL: Position was open, now it's closed
        if was_in_position and current_position_size == 0 and expected_qty > 0:
            log_message("=" * 80, section="TRADE")
            log_message("🛑 SL/CLOSURE DETECTED via Position Update!", section="TRADE")
            log_message(f"Expected Qty: {expected_qty} → Current Qty: 0", section="TRADE")
            log_message("=" * 80, section="TRADE")

            with sl_hit_lock:
                if not sl_hit_triggered:
                    sl_hit_triggered = True
                    with entry_order_sl_lock:
                        entry_order_with_sl = None
                    threading.Timer(0.1, handle_sl_hit).start()

    except Exception as e:
        log_message(f"Exception in detect_sl_from_position_update: {e}", section="ERROR")
# ================================================================================
# TEST FUNCTION TO VERIFY ORDER SUBSCRIPTIONS
# ================================================================================
def test_order_subscription():
    """Test function to verify order subscriptions are working"""
    try:
        log_message("=" * 80, section="TEST")
        log_message("Testing order subscription...", section="TEST")
        log_message("=" * 80, section="TEST")

        if ws_subscriptions_ready.wait(timeout=10):
            log_message("✓ Subscriptions ready", section="TEST")
        else:
            log_message("✗ Subscription timeout", section="TEST")
            return

        # Fetch current orders via REST API
        log_message("Fetching active orders...", section="TEST")
        path = "/g-orders/activeList" # Use linear activeList endpoint
        params = {"symbol": SYMBOL, "bizType": "PERPETUAL"} # Add bizType for linear
        response = phemex_request("GET", path, params=params)

        if response and response.get('code') == 0:
            orders = response.get('data', {}).get('rows', [])
            log_message(f"Found {len(orders)} active orders", section="TEST")

            for order in orders[:3]: # Show max 3 orders
                log_message(f" Order: {order.get('orderID')[:12]}... - {order.get('ordStatus')} - {order.get('side')}", section="TEST")
        else:
            log_message("No active orders found", section="TEST")

        log_message("=" * 80, section="TEST")
    except Exception as e:
        log_message(f"Exception in test_order_subscription: {e}", section="ERROR")

# ================================================================================
# POSITION CONFIRMATION AND TP/SL PLACEMENT
# ================================================================================
def confirm_and_set_active_position(filled_order_id):
    """
    Confirms position and places separate TP order after entry fills for linear contracts.
    SL is handled by a separate order or through position monitoring.
    """
    global in_position, position_entry_price, position_qty
    global current_take_profit, current_stop_loss, pending_entry_order_id
    global position_exit_orders, entry_sl_price

    try:
        log_message(f"Confirming linear position...", section="TRADE")

        # Fetch current position details for linear contracts
        path = "/g-accounts/positions"
        params = {"currency": CURRENCY}
        response = phemex_request("GET", path, params=params)

        entry_confirmed = False
        actual_entry_price = 0.0
        actual_qty = 0.0

        if response and response.get('code') == 0:
            positions = response.get('data', {}).get('positions', [])
            for pos in positions:
                if pos.get('symbol') == SYMBOL and pos.get('side') == 'Buy': # Assuming 'Buy' for long position
                    size_rv = safe_float(pos.get('sizeRv', 0)) # Raw value size for linear
                    if size_rv > 0:
                        avg_entry_price_rv = safe_float(pos.get('avgEntryPriceRv', 0)) # Raw value avg entry price

                        actual_entry_price = avg_entry_price_rv
                        actual_qty = size_rv
                        entry_confirmed = True
                        break

        if not entry_confirmed or actual_entry_price <= 0:
            log_message("CRITICAL: Could not confirm linear position!", section="ERROR")
            return

        # Calculate TP price
        global entry_reduced_tp_flag
        reduced_tp = entry_reduced_tp_flag if 'entry_reduced_tp_flag' in globals() else False

        if reduced_tp:
            tp_price = actual_entry_price * (1 + TP_PERCENT_REDUCED / 100.0)
            tp_reason = f"Reduced TP {TP_PERCENT_REDUCED}%"
        else:
            tp_price = actual_entry_price * (1 + TP_PERCENT / 100.0)
            tp_reason = f"Normal TP {TP_PERCENT}%"

        # For linear contracts, SL is usually a separate STOP_MARKET order
        # We need to place it after the position is confirmed.
        sl_price = actual_entry_price * (1 - SL_PERCENT / 100.0)

        with position_lock:
            in_position = True
            position_entry_price = actual_entry_price
            position_qty = actual_qty
            current_take_profit = tp_price
            current_stop_loss = sl_price
            pending_entry_order_id = None
            position_exit_orders = {}

        log_message("=" * 80, section="TRADE")
        log_message("LINEAR POSITION OPENED", section="TRADE")
        log_message(f"Entry: ${actual_entry_price:.2f} | Qty: {actual_qty}", section="TRADE")
        log_message(f"TP: ${tp_price:.2f} ({tp_reason})", section="TRADE")
        log_message(f"SL: ${sl_price:.2f} (separate order)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        # Place TP Limit order
        log_message(f"Placing TP Limit order at ${tp_price:.2f}", section="TRADE")
        tp_order = phemex_place_order(
            SYMBOL,
            "Sell", # To close a long position
            actual_qty, # Use actual_qty for linear
            price=tp_price, # Unscaled price for linear
            order_type="Limit",
            time_in_force="GoodTillCancel",
            reduce_only=True
        )

        if tp_order and tp_order.get('orderID'):
            with position_lock:
                position_exit_orders['tp'] = tp_order['orderID']
            log_message(f"✓ TP order placed", section="TRADE")
        else:
            log_message(f"CRITICAL: TP order failed! Closing position", section="ERROR")
            execute_trade_exit("Failed to place TP")
            return

        # Place SL Stop Market order
        log_message(f"Placing SL Stop Market order at ${sl_price:.2f}", section="TRADE")
        sl_order = phemex_place_order(
            SYMBOL,
            "Sell", # To close a long position
            actual_qty,
            price=sl_price, # Trigger price for stop market
            order_type="Stop", # For stop market orders
            time_in_force="GoodTillCancel",
            reduce_only=True
        )

        if sl_order and sl_order.get('orderID'):
            with position_lock:
                position_exit_orders['sl'] = sl_order['orderID']
            log_message(f"✓ SL order placed", section="TRADE")
        else:
            log_message(f"CRITICAL: SL order failed! Closing position", section="ERROR")
            execute_trade_exit("Failed to place SL")
            return

        log_message("=" * 80, section="TRADE")
        log_message("✓ LINEAR POSITION CONFIGURED (SL and TP active)", section="TRADE")
        log_message("=" * 80, section="TRADE")

        update_account_info()
    except Exception as e:
        log_message(f"Exception in confirm_and_set_active_position (linear): {e}", section="ERROR")

# ================================================================================
# POSITION LIFECYCLE MANAGEMENT
# ================================================================================
def manage_position_lifecycle():
    """Manages pending orders and active positions - monitors for EOD exit"""
    try:
        log_message("Position lifecycle manager started", section="SYSTEM")
        log_message(f"EOD exit time: {EOD_EXIT_HOUR_UTC:02d}:{EOD_EXIT_MINUTE_UTC:02d} UTC", section="SYSTEM")

        eod_triggered = False

        while not shutdown_flag.is_set():
            sleep(5)

            now_utc = datetime.datetime.now(datetime.timezone.utc)
            current_hour = now_utc.hour
            current_minute = now_utc.minute

            with position_lock:
                is_in_pos = in_position
                has_pending_entry = (pending_entry_order_id is not None)

            if not eod_triggered and (current_hour == EOD_EXIT_HOUR_UTC and current_minute >= EOD_EXIT_MINUTE_UTC):
                eod_triggered = True
                log_message(f"⚠️ EOD EXIT TIME REACHED: {EOD_EXIT_HOUR_UTC:02d}:{EOD_EXIT_MINUTE_UTC:02d} UTC", section="SYSTEM")

                handle_eod_exit()

                sleep(5)

                with position_lock:
                    final_pos = in_position
                    final_pending = (pending_entry_order_id is not None)

                if not final_pos and not final_pending:
                    log_message("✓ EOD cleanup complete. Position manager exiting.", section="SYSTEM")
                    break

            if current_hour == 0 and current_minute == 0:
                eod_triggered = False

            if not is_in_pos and not has_pending_entry and not eod_triggered:
                log_message("Position manager exiting (no active position/order)", section="SYSTEM")
                break

        log_message("Position manager thread finished", section="SYSTEM")
    except Exception as e:
        log_message(f"Exception in manage_position_lifecycle: {e}", section="ERROR")

# ================================================================================
# MANUAL EXIT EXECUTION
# ================================================================================
def execute_trade_exit(reason):
    """Manually exits position with market order"""
    try:
        log_message(f"=== MANUAL EXIT === Reason: {reason}", section="TRADE")

        with position_lock:
            if not in_position:
                log_message("Exit aborted: Not in position", section="TRADE")
                return
            qty_to_close = position_qty

        with position_lock:
            orders_to_cancel = list(position_exit_orders.values())

        for order_id in orders_to_cancel:
            if order_id:
                try:
                    phemex_cancel_order(SYMBOL, order_id)
                    sleep(0.2)
                except Exception as e:
                    log_message(f"Error cancelling order: {e} (OK, continuing)", section="ERROR")

        try:
            log_message(f"Placing market sell for {qty_to_close} {SYMBOL}", section="TRADE")
            exit_order = phemex_place_order(
                SYMBOL,
                "Sell",
                qty_to_close, # Pass float quantity for linear
                order_type="Market",
                reduce_only=True
            )

            if not (exit_order and exit_order.get('orderID')):
                log_message(f"WARNING: Market exit order may have failed (OK if already closed)", section="TRADE")
        except Exception as e:
            log_message(f"Exception during market exit: {e} (OK, continuing)", section="ERROR")

        sleep(1)
        cancel_all_exit_orders_and_reset(reason)
    except Exception as e:
        log_message(f"Exception in execute_trade_exit: {e}", section="ERROR")

# ================================================================================
# CHECK AND CLOSE FUNCTIONS
# ================================================================================
def check_and_close_any_open_position():
    """Checks for any open linear position and closes it"""
    try:
        log_message("Checking for any open linear positions...", section="SYSTEM")

        path = "/g-accounts/positions" # Linear endpoint
        params = {"currency": CURRENCY}
        response = phemex_request("GET", path, params=params)

        if response and response.get('code') == 0:
            positions = response.get('data', {}).get('positions', [])
            for pos in positions:
                if pos.get('symbol') == SYMBOL:
                    size_rv = safe_float(pos.get('sizeRv', 0)) # Raw value size for linear
                    side = pos.get('side')

                    if size_rv > 0:
                        log_message(f"⚠️ Found open {side} linear position: {size_rv} {SYMBOL}", section="SYSTEM")

                        close_side = "Sell" if side == "Buy" else "Buy"

                        log_message(f"Closing {size_rv} {SYMBOL} with market {close_side} order", section="SYSTEM")
                        close_order = phemex_place_order(
                            SYMBOL,
                            close_side,
                            size_rv, # Pass float quantity for linear
                            order_type="Market",
                            reduce_only=True
                        )

                        if close_order:
                            log_message(f"✓ Position close order placed", section="SYSTEM")
                            return True
                        else:
                            log_message(f"❌ Failed to place close order", section="ERROR")
                            return False

        log_message("No open linear positions found", section="SYSTEM")
        return False
    except Exception as e:
        log_message(f"Exception in check_and_close_any_open_position (linear): {e}", section="ERROR")
        return False
# ================================================================================
# STATE RESET FUNCTIONS
# ================================================================================
def reset_entry_state(reason):
    """Resets entry-related state including SL tracking"""
    global pending_entry_order_id, entry_reduced_tp_flag, pending_entry_order_details
    global entry_order_with_sl

    try:
        with position_lock:
            pending_entry_order_id = None
            entry_reduced_tp_flag = False
            pending_entry_order_details = {}

        with entry_order_sl_lock:
            entry_order_with_sl = None

        log_message(f"Entry state reset. Reason: {reason}", section="SYSTEM")
    except Exception as e:
        log_message(f"Exception in reset_entry_state: {e}", section="ERROR")


def cancel_all_exit_orders_and_reset(reason):
    """Cancels all exit orders and resets position state including SL tracking"""
    global in_position, position_entry_price, position_qty
    global current_take_profit, current_stop_loss, position_exit_orders
    global pending_entry_order_id, entry_reduced_tp_flag, entry_order_with_sl

    try:
        with position_lock:
            # orders_to_cancel = list(position_exit_orders.values()) # Not needed for bulk cancel

            in_position = False
            position_entry_price = 0.0
            position_qty = 0.0
            current_take_profit = 0.0
            current_stop_loss = 0.0
            position_exit_orders = {}
            pending_entry_order_id = None
            entry_reduced_tp_flag = False

        with entry_order_sl_lock:
            entry_order_with_sl = None

        log_message("=" * 80, section="TRADE")
        log_message(f"POSITION CLOSED - Reason: {reason}", section="TRADE")
        log_message("=" * 80, section="TRADE")

        # Use bulk cancellation endpoint
        path = "/g-orders/all"
        params = {
            "symbol": SYMBOL,
            "untriggered": "true" # Cancel untriggered SL/TP orders
        }
        response = phemex_request("DELETE", path, params=params)

        # Also cancel active orders
        params = {
            "symbol": SYMBOL,
            "untriggered": "false"
        }
        response = phemex_request("DELETE", path, params=params)

        if response and response.get('code') == 0:
            log_message("✓ All exit orders cancelled via bulk API", section="TRADE")
        else:
            log_message(f"⚠ Failed to bulk cancel orders: {response.get('msg') if response else 'No response'} (OK if no orders)", section="TRADE")

        update_account_info()
    except Exception as e:
        log_message(f"Exception in cancel_all_exit_orders_and_reset: {e}", section="ERROR")

# ================================================================================
# DATA RETRIEVAL AND CPR CALCULATION
# ================================================================================
def get_latest_data_and_indicators():
    """Fetches latest data and calculates CPR levels and indicators"""
    try:
        with data_lock:
            daily_df = historical_data_store.get(TIMEFRAME_CPR)

        if daily_df is None or len(daily_df) < 52:
            log_message(f"Insufficient daily data", section="ERROR")
            return None

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        today = now_utc.date()

        df_dates = daily_df.index.date
        yesterday = today - datetime.timedelta(days=1)

        yesterday_mask = df_dates == yesterday

        if yesterday_mask.any():
            prev_candle = daily_df[yesterday_mask].iloc[-1]
            prev_date = yesterday
        else:
            prev_candle = daily_df.iloc[-1]
            prev_date = daily_df.index[-1].date()
            log_message(f"Warning: Yesterday's data not found, using last available: {prev_date}", section="CALC")

        today_mask = df_dates == today

        if today_mask.any():
            current_candle = daily_df[today_mask].iloc[-1]
            current_date = today
        else:
            current_candle = daily_df.iloc[-1]
            current_date = daily_df.index[-1].date()

        prev_open_scaled = prev_candle['Open']
        prev_high_scaled = prev_candle['High']
        prev_low_scaled = prev_candle['Low']
        prev_close_scaled = prev_candle['Close']
        daily_open_scaled = current_candle['Open']

        prev_open = prev_open_scaled
        prev_high = prev_high_scaled
        prev_low = prev_low_scaled
        prev_close = prev_close_scaled
        daily_open = daily_open_scaled

        if prev_high < prev_low:
            log_message(f"CRITICAL: Data corruption! Prev High < Low", section="ERROR")
            return None

        daily_cpr = calculate_cpr_levels(prev_high, prev_low, prev_close)

        if not daily_cpr:
            log_message("Failed to calculate CPR levels", section="ERROR")
            return None

        indicator_results_df = calculate_indicators(daily_df.copy())

        if indicator_results_df is None or indicator_results_df.empty:
            log_message("Failed to calculate indicators", section="ERROR")
            return None

        latest_indicators = indicator_results_df.iloc[-1]
        ema_21 = latest_indicators['EMA_21']
        ema_50 = latest_indicators['EMA_50']

        log_message("=" * 80, section="CALC")
        log_message("MARKET DATA CALCULATION", section="CALC")
        log_message("=" * 80, section="CALC")
        log_message(f"Previous Day ({prev_date}):", section="CALC")
        log_message(f" Open={prev_open:.2f}, High={prev_high:.2f}, Low={prev_low:.2f}, Close={prev_close:.2f}", section="CALC")
        log_message(f"Current Day ({current_date}): Open={daily_open:.2f}", section="CALC")
        log_message(f"CPR TC={daily_cpr['TC']:.2f}, P={daily_cpr['P']:.2f}, BC={daily_cpr['BC']:.2f}", section="CALC")
        log_message(f"EMA 21={ema_21:.2f}, EMA 50={ema_50:.2f}", section="CALC")
        log_message("=" * 80, section="CALC")

        return {
            'daily_cpr': daily_cpr,
            'daily_open': daily_open,
            'ema_21': ema_21,
            'ema_50': ema_50,
            'prev_daily_high': prev_high,
            'prev_daily_low': prev_low,
            'prev_daily_close': prev_close,
        }

    except Exception as e:
        log_message(f"Exception in get_latest_data_and_indicators: {e}", section="ERROR")
        return None

# ================================================================================
# MANUAL TEST MODE FUNCTIONS
# ================================================================================
def manual_test_entry():
    """Manual test mode - places order immediately with current price"""
    global manual_test_order_placed, manual_test_tp_scheduled, manual_test_eod_scheduled

    try:
        log_message("=" * 80, section="TEST")
        log_message("🧪 MANUAL TEST MODE - INITIATING IMMEDIATE ENTRY", section="TEST")
        log_message("=" * 80, section="TEST")

        # Get current market price
        current_price = get_current_market_price()

        if not current_price:
            log_message("❌ Failed to get current price. Cannot proceed with test.", section="TEST")
            return

        # Calculate entry price with offset
        offset_percent = MANUAL_TEST_CONFIG.get('price_offset_percent', -0.5)
        entry_price = current_price * (1 + offset_percent / 100.0)

        log_message(f"Current Price: ${current_price:.2f}", section="TEST")
        log_message(f"Entry Price (offset {offset_percent}%): ${entry_price:.2f}", section="TEST")

        # Create minimal calculated data for manual test
        calculated_data = {
            'daily_cpr': {'TC': entry_price, 'P': entry_price, 'BC': entry_price * 0.99},
            'daily_open': entry_price * 1.01, # Above TC to pass check
            'ema_21': entry_price,
            'ema_50': entry_price * 0.99,
            'prev_daily_high': entry_price * 1.05,
            'prev_daily_low': entry_price * 0.95,
            'prev_daily_close': entry_price,
        }

        # Place entry order
        log_message("Placing test entry order...", section="TEST")
        initiate_entry_sequence(calculated_data, entry_price, False)

        manual_test_order_placed = True

        # Schedule TP test
        if MANUAL_TEST_CONFIG.get('test_tp_after_seconds'):
            test_tp_delay = MANUAL_TEST_CONFIG['test_tp_after_seconds']
            log_message(f"📅 TP hit test scheduled in {test_tp_delay} seconds", section="TEST")
            manual_test_tp_scheduled = True

        # Schedule EOD test
        if MANUAL_TEST_CONFIG.get('test_eod_after_seconds'):
            test_eod_delay = MANUAL_TEST_CONFIG['test_eod_after_seconds']
            log_message(f"📅 EOD exit test scheduled in {test_eod_delay} seconds", section="TEST")
            manual_test_eod_scheduled = True

        log_message("=" * 80, section="TEST")
        log_message("✓ MANUAL TEST ORDER PLACED", section="TEST")
        log_message("Monitor the logs for order fills and TP/SL placement", section="TEST")
        log_message("=" * 80, section="TEST")

    except Exception as e:
        log_message(f"Exception in manual_test_entry: {e}", section="ERROR")

def manual_test_monitor():
    """Monitor for manual test triggers"""
    global manual_test_order_placed, manual_test_tp_scheduled, manual_test_eod_scheduled

    try:
        start_time = time()
        tp_triggered = False
        eod_triggered = False

        log_message("🧪 Manual test monitor started", section="TEST")

        while not shutdown_flag.is_set():
            sleep(1)
            elapsed = time() - start_time

            # Check if position filled
            with position_lock:
                is_in_pos = in_position

            # TP test trigger
            if (manual_test_tp_scheduled and not tp_triggered and
                    elapsed >= MANUAL_TEST_CONFIG.get('test_tp_after_seconds', 30) and is_in_pos):
                tp_triggered = True
                log_message("=" * 80, section="TEST")
                log_message("🧪 MANUAL TEST: Simulating TP HIT", section="TEST")
                log_message("=" * 80, section="TEST")

                # Trigger TP hit manually
                threading.Timer(1.0, handle_tp_hit).start()

            # EOD test trigger
            if (manual_test_eod_scheduled and not eod_triggered and
                    elapsed >= MANUAL_TEST_CONFIG.get('test_eod_after_seconds', 60)):
                eod_triggered = True
                log_message("=" * 80, section="TEST")
                log_message("🧪 MANUAL TEST: Simulating EOD EXIT", section="TEST")
                log_message("=" * 80, section="TEST")

                # Trigger EOD exit manually
                threading.Timer(1.0, handle_eod_exit).start()

                # Exit test monitor after EOD
                sleep(10)
                break

        log_message("🧪 Manual test monitor finished", section="TEST")
    except Exception as e:
        log_message(f"Exception in manual_test_monitor: {e}", section="ERROR")

# ================================================================================
# SIMPLIFIED LIVE MONITORING
# ================================================================================
def live_position_monitor():
    """Simplified live monitor showing next entry/exit times and current status"""
    try:
        log_message("Live monitor started", section="SYSTEM")

        while not shutdown_flag.is_set():
            sleep(60)

            now_utc = datetime.datetime.now(datetime.timezone.utc)

            with position_lock:
                is_in_pos = in_position
                has_pending = (pending_entry_order_id is not None)
                pos_entry = position_entry_price
                pos_qty_val = position_qty
                tp_val = current_take_profit
                sl_val = current_stop_loss

            # Calculate next entry check time
            next_entry_check = datetime.datetime(
                now_utc.year, now_utc.month, now_utc.day,
                DAILY_ENTRY_CHECK_HOUR, DAILY_ENTRY_CHECK_MINUTE, DAILY_ENTRY_CHECK_SECOND,
                tzinfo=datetime.timezone.utc
            )

            if now_utc >= next_entry_check:
                next_entry_check += datetime.timedelta(days=1)

            time_until_entry = next_entry_check - now_utc
            hours_entry = int(time_until_entry.total_seconds() // 3600)
            minutes_entry = int((time_until_entry.total_seconds() % 3600) // 60)
            seconds_entry = int(time_until_entry.total_seconds() % 60)

            # Calculate next EOD exit time
            next_eod_exit = datetime.datetime(
                now_utc.year, now_utc.month, now_utc.day,
                EOD_EXIT_HOUR_UTC, EOD_EXIT_MINUTE_UTC, 0,
                tzinfo=datetime.timezone.utc
            )

            if now_utc >= next_eod_exit:
                next_eod_exit += datetime.timedelta(days=1)

            time_until_exit = next_eod_exit - now_utc
            hours_exit = int((time_until_exit.total_seconds() % 3600) // 60)
            minutes_exit = int((time_until_exit.total_seconds() % 3600) // 60)
            seconds_exit = int(time_until_exit.total_seconds() % 60)

            # Get current price
            with trade_data_lock:
                current_price = latest_trade_price

            # Display status
            log_message("=" * 80, section="MONITOR")
            log_message("🤖 BOT STATUS", section="MONITOR")
            log_message("=" * 80, section="MONITOR")
            log_message(f"Time: {now_utc.strftime('%Y-%m-%d %H:%M:%S UTC')}", section="MONITOR")
            log_message(f"Mode: {TEST_MODE}", section="MONITOR")

            if current_price:
                # For linear contracts, current_price is already unscaled
                log_message(f"Current Price: ${current_price:.2f}", section="MONITOR")

            log_message("-" * 80, section="MONITOR")
            log_message(f"Position: {'🟢 ACTIVE' if is_in_pos else '⚪ NONE'}", section="MONITOR")
            log_message(f"Pending Entry Order: {'🟡 YES' if has_pending else '⚪ NO'}", section="MONITOR")

            if is_in_pos:
                log_message(f"Entry Price: ${pos_entry:.2f} | Qty: {pos_qty_val:.{PRODUCT_INFO.get('qtyPrecision', 8)}f}", section="MONITOR")
                log_message(f"TP: ${tp_val:.2f} | SL: ${sl_val:.2f}", section="MONITOR")

                if current_price and pos_entry > 0:
                    pnl_pct = (current_price - pos_entry) / pos_entry * 100
                    log_message(f"Unrealized P&L: {pnl_pct:+.2f}%", section="MONITOR")

            if TEST_MODE != "MANUAL":
                log_message("-" * 80, section="MONITOR")
                log_message(f"Next Entry Check: {next_entry_check.strftime('%H:%M:%S UTC')}", section="MONITOR")
                log_message(f"Time Until: {hours_entry:02d}h {minutes_entry:02d}m {seconds_entry:02d}s", section="MONITOR")
                log_message("-" * 80, section="MONITOR")
                log_message(f"Next EOD Exit: {next_eod_exit.strftime('%H:%M:%S UTC')}", section="MONITOR")
                log_message(f"Time Until: {hours_exit:02d}h {minutes_exit:02d}m {seconds_exit:02d}s", section="MONITOR")

            log_message("=" * 80, section="MONITOR")

    except Exception as e:
        log_message(f"Exception in live_position_monitor: {e}", section="ERROR")

# ================================================================================
# GARBAGE COLLECTION
# ================================================================================
def cleanup_memory():
    """Forces garbage collection periodically to prevent memory leaks"""
    while not shutdown_flag.is_set():
        sleep(3600) # Every hour
        gc.collect() # Force garbage collection
        log_message(f"Memory cleaned: {gc.get_count()[0]} objects collected", section="SYSTEM")

# ================================================================================
# MAIN TRADING LOGIC
# ================================================================================
def process_new_candle_and_check_entry():
    """Checks entry conditions when new daily candle starts"""
    try:
        log_message("=" * 80, section="TRADE")
        log_message(f"🕐 NEW DAILY CANDLE - Entry Check @ {datetime.datetime.now(datetime.timezone.utc)}", section="TRADE")
        log_message("=" * 80, section="TRADE")

        calculated_data = get_latest_data_and_indicators()

        if not calculated_data:
            log_message("Failed to calculate data. Skipping entry check.", section="ERROR")
            return

        with position_lock:
            is_in_pos = in_position
            has_pending = (pending_entry_order_id is not None)

        if is_in_pos or has_pending:
            log_message("Skipping entry: Already in position or pending order exists", section="TRADE")
            return

        log_message("Checking entry conditions...", section="TRADE")
        entry_signal, limit_price, reduced_tp = check_entry_conditions(calculated_data)

        if entry_signal:
            log_message(f">>> ENTRY SIGNAL <<< Limit order at TC: ${limit_price:.2f}", section="TRADE")
            initiate_entry_sequence(calculated_data, limit_price, reduced_tp)
        else:
            log_message("No entry signal. Waiting for next check.", section="TRADE")
    except Exception as e:
        log_message(f"Exception in process_new_candle_and_check_entry: {e}", section="ERROR")

def main_trading_logic():
    """Main trading loop - handles daily entry checks"""
    try:
        log_message("=== MAIN TRADING LOGIC STARTED ===", section="SYSTEM")

        if TEST_MODE != "MANUAL":
            log_message(f"Daily entry check time: {DAILY_ENTRY_CHECK_HOUR:02d}:{DAILY_ENTRY_CHECK_MINUTE:02d}:{DAILY_ENTRY_CHECK_SECOND:02d} UTC", section="SYSTEM")
        else:
            log_message("Manual test mode - entry check handled by test thread", section="SYSTEM")

        last_check_date = None

        while not shutdown_flag.is_set():
            try:
                now_utc = datetime.datetime.now(datetime.timezone.utc)
                current_date = now_utc.date()

                # Skip scheduled checks in manual mode
                if TEST_MODE == "MANUAL":
                    sleep(5)
                    continue

                # Check if it's time for daily entry check
                if (now_utc.hour == DAILY_ENTRY_CHECK_HOUR and
                        now_utc.minute == DAILY_ENTRY_CHECK_MINUTE and
                        now_utc.second >= DAILY_ENTRY_CHECK_SECOND and
                        current_date != last_check_date):

                    log_message(f"Daily entry check triggered at {now_utc.strftime('%H:%M:%S UTC')}", section="SYSTEM")
                    process_new_candle_and_check_entry()
                    last_check_date = current_date

                sleep(1)

            except Exception as e:
                log_message(f"Error in trading logic loop: {e} (continuing)", section="ERROR")
                sleep(30)

    except Exception as e:
        log_message(f"CRITICAL ERROR in main_trading_logic: {e}", section="ERROR")

# ================================================================================
# MAIN EXECUTION
# ================================================================================
if __name__ == "__main__":
    log_message("=" * 80, section="SYSTEM")
    log_message(f"PHEMEX SOLUSDT TRADING BOT - {TEST_MODE} MODE", section="SYSTEM")
    log_message("=" * 80, section="SYSTEM")
    log_message(f"Symbol: {SYMBOL} | Leverage: {LEVERAGE}x | Risk: {RISK_PERCENT}%", section="SYSTEM")
    log_message(f"TP: {TP_PERCENT}% / {TP_PERCENT_REDUCED}% | SL: {SL_PERCENT}%", section="SYSTEM")

    if TEST_MODE == "MANUAL":
        log_message("MANUAL TEST MODE", section="SYSTEM")
        log_message(f"- Immediate order placement: {MANUAL_TEST_CONFIG.get('immediate_order')}", section="SYSTEM")
        log_message(f"- Force entry: {MANUAL_TEST_CONFIG.get('force_entry')}", section="SYSTEM")
        log_message(f"- TP test after: {MANUAL_TEST_CONFIG.get('test_tp_after_seconds')}s", section="SYSTEM")
        log_message(f"- EOD test after: {MANUAL_TEST_CONFIG.get('test_eod_after_seconds')}s", section="SYSTEM")
    elif TEST_MODE == "AUTO":
        log_message("🧪 AUTO TEST MODE", section="SYSTEM")
        log_message(f"Entry Check: {DAILY_ENTRY_CHECK_HOUR:02d}:{DAILY_ENTRY_CHECK_MINUTE:02d} UTC", section="SYSTEM")
        log_message(f"EOD Exit: {EOD_EXIT_HOUR_UTC:02d}:{EOD_EXIT_MINUTE_UTC:02d} UTC", section="SYSTEM")
    else:
        log_message("🔴 PRODUCTION MODE", section="SYSTEM")
        log_message(f"Entry Check: {DAILY_ENTRY_CHECK_HOUR:02d}:{DAILY_ENTRY_CHECK_MINUTE:02d}:{DAILY_ENTRY_CHECK_SECOND:02d} UTC", section="SYSTEM")
        log_message(f"EOD Exit: {EOD_EXIT_HOUR_UTC:02d}:{EOD_EXIT_MINUTE_UTC:02d} UTC", section="SYSTEM")

    log_message("=" * 80, section="SYSTEM")

    try:
        # Validate credentials
        if not API_KEY or not API_SECRET:
            log_message("FATAL: API credentials not set", section="ERROR")
            exit()

        # Fetch product info
        if not fetch_product_info(SYMBOL):
            log_message(f"FATAL: Could not fetch product info for {SYMBOL}", section="ERROR")
            exit()

        # Set leverage
        if not phemex_set_leverage(SYMBOL, LEVERAGE):
            log_message(f"WARNING: Failed to set leverage (continuing)", section="SYSTEM")

        sleep(2)

        # Fetch account balance
        if not update_account_info():
            log_message("WARNING: Could not fetch initial account info", section="SYSTEM")

        # Fetch historical data
        log_message(f"Fetching {HISTORICAL_DATA_MONTHS} months of historical data...", section="SYSTEM")

        today_str = datetime.datetime.now(datetime.timezone.utc).strftime('%Y-%m-%d')
        start_str = (datetime.datetime.now(datetime.timezone.utc) -
                     pd.DateOffset(months=HISTORICAL_DATA_MONTHS + 1)).strftime('%Y-%m-%d')

        for tf in [TIMEFRAME_CPR, TIMEFRAME_TRADE]:
            if not fetch_initial_historical_data(SYMBOL, tf, start_str, today_str):
                log_message(f"FATAL: Failed to fetch {tf} data", section="ERROR")
                exit()
            sleep(1)

        log_message("✓ Historical data loaded", section="SYSTEM")

        # Validate initial data
        initial_data = get_latest_data_and_indicators()
        if not initial_data:
            log_message("WARNING: Could not calculate initial indicators", section="SYSTEM")

        # Initialize WebSocket
        ws_instance = initialize_websocket()
        if not ws_instance:
            log_message("FATAL: Failed to initialize WebSocket", section="ERROR")
            exit()

        sleep(5) # Wait for WebSocket to fully connect and authenticate

        # Test order subscriptions after WebSocket is ready
        test_thread = threading.Thread(target=test_order_subscription, name="SubTest", daemon=True)
        test_thread.start()
        log_message("✓ Order subscription test scheduled", section="SYSTEM")

        sleep(2) # Give test time to run

        # Start heartbeat
        heartbeat_thread = threading.Thread(target=send_heartbeat, name="Heartbeat", daemon=True)
        heartbeat_thread.start()
        log_message("✓ Heartbeat started", section="SYSTEM")

        # Start live monitor
        live_monitor_thread = threading.Thread(target=live_position_monitor, name="LiveMonitor", daemon=True)
        live_monitor_thread.start()
        log_message("✓ Live monitor started", section="SYSTEM")

        # Start trading logic
        trading_logic_thread = threading.Thread(target=main_trading_logic, name="TradingLogic", daemon=True)
        trading_logic_thread.start()
        log_message("✓ Trading logic started", section="SYSTEM")

        # Start garbage collection thread
        cleanup_thread = threading.Thread(target=cleanup_memory, name="MemoryCleanup", daemon=True)
        cleanup_thread.start()
        log_message("✓ Memory cleanup thread started", section="SYSTEM")

        # Mark startup complete - enable order update handling
        sleep(3)
        bot_startup_complete = True
        log_message("✓ Bot startup complete - order monitoring active", section="SYSTEM")

        # Manual test mode - start test sequence
        if TEST_MODE == "MANUAL" and MANUAL_TEST_CONFIG.get('immediate_order'):
            sleep(5) # Wait for WebSocket to stabilize
            manual_test_thread = threading.Thread(target=manual_test_entry, name="ManualTest", daemon=True)
            manual_test_thread.start()
            log_message("✓ Manual test thread started", section="SYSTEM")

            # Start test monitor
            test_monitor_thread = threading.Thread(target=manual_test_monitor, name="TestMonitor", daemon=True)
            test_monitor_thread.start()
            log_message("✓ Test monitor started", section="SYSTEM")

        log_message("=" * 80, section="SYSTEM")
        log_message(f"🟢 BOT IS NOW LIVE - {TEST_MODE} MODE", section="SYSTEM")
        log_message("Strategy: Long at TC (limit) when price opens above TC", section="SYSTEM")
        log_message("TP/SL: Placed AFTER entry fills (separate orders for linear)", section="SYSTEM")
        log_message("Press Ctrl+C to stop", section="SYSTEM")
        log_message("=" * 80, section="SYSTEM")

        # Main monitoring loop
        while True:
            try:
                # Monitor thread health
                if trading_logic_thread and not trading_logic_thread.is_alive():
                    log_message("FATAL: Trading logic thread died! Restarting...", section="ERROR")
                    trading_logic_thread = threading.Thread(target=main_trading_logic, name="TradingLogic", daemon=True)
                    trading_logic_thread.start()

                # Check WebSocket
                ws_connected = False
                if ws and hasattr(ws, 'sock') and ws.sock:
                    try:
                        ws_connected = ws.sock.connected
                    except Exception:
                        pass

                if not ws_connected:
                    log_message("WARNING: WebSocket disconnected! Reconnecting...", section="SYSTEM")
                    sleep(10)
                    ws_instance = initialize_websocket()
                    sleep(10)

                # Periodic account update
                if datetime.datetime.now().second == 0:
                    update_account_info()

                sleep(30)

            except Exception as e:
                log_message(f"Error in main loop: {e} (continuing)", section="ERROR")
                sleep(30)

    except KeyboardInterrupt:
        log_message("=" * 80, section="SYSTEM")
        log_message("⚠️ KEYBOARD INTERRUPT - SHUTTING DOWN", section="SYSTEM")
        log_message("=" * 80, section="SYSTEM")

    except Exception as e:
        log_message(f"!!! UNHANDLED EXCEPTION: {e} !!!", section="ERROR")
        traceback.print_exc()

    finally:
        # Enhanced cleanup
        log_message("=" * 80, section="SYSTEM")
        log_message("🛑 SHUTDOWN INITIATED - Starting cleanup", section="SYSTEM")
        log_message("=" * 80, section="SYSTEM")

        shutdown_flag.set()

        # Close WebSocket
        if ws and hasattr(ws, 'sock') and ws.sock:
            try:
                log_message("Closing WebSocket connection...", section="SYSTEM")
                ws.close()
                sleep(1)
            except Exception as e:
                log_message(f"Error closing WebSocket: {e} (OK, continuing)", section="ERROR")

        # Check current state
        with position_lock:
            has_pending = (pending_entry_order_id is not None)
            pending_id = pending_entry_order_id
            is_in_pos = in_position
            pos_qty = position_qty

        log_message(f"Current state: has_pending={has_pending}, is_in_pos={is_in_pos}", section="SYSTEM")

        # Cancel pending entry orders
        if has_pending:
            log_message(f"Cancelling pending entry order", section="SYSTEM")
            try:
                phemex_cancel_order(SYMBOL, pending_id)
                sleep(0.5)
                reset_entry_state("Shutdown")
            except Exception as e:
                log_message(f"Error cancelling pending order: {e} (OK, continuing)", section="ERROR")
                reset_entry_state("Shutdown - forced")

        # Close known active position
        if is_in_pos and pos_qty > 0:
            log_message(f"Closing known active position ({pos_qty} contracts)", section="SYSTEM")
            try:
                exit_thread = threading.Thread(target=execute_trade_exit, args=("Bot shutting down",))
                exit_thread.start()
                exit_thread.join(timeout=10)

                if exit_thread.is_alive():
                    log_message("WARNING: Position exit timed out after 10 seconds", section="ERROR")
                    cancel_all_exit_orders_and_reset("Forced shutdown after timeout")
            except Exception as e:
                log_message(f"Error during position exit: {e} (OK, continuing)", section="ERROR")
                cancel_all_exit_orders_and_reset("Forced shutdown after error")

        sleep(1)

        # Check for any missed positions
        log_message("Performing final position check...", section="SYSTEM")
        try:
            check_and_close_any_open_position()
            sleep(1)
        except Exception as e:
            log_message(f"Error in final position check: {e} (OK, continuing)", section="ERROR")

        # Force cancel ALL open orders
        log_message("Cancelling all remaining orders...", section="SYSTEM")
        try:
            path = "/g-orders/all" # Use linear cancel ALL endpoint
            params = {
                "symbol": SYMBOL,
                "untriggered": "true" # Cancel untriggered SL/TP orders
            }
            response = phemex_request("DELETE", path, params=params)
            # Also cancel active orders
            params = {
                "symbol": SYMBOL,
                "untriggered": "false"
            }
            response = phemex_request("DELETE", path, params=params)
            if response and response.get('code') == 0:
                log_message("✓ All orders cancelled", section="SYSTEM")
            else:
                log_message(f"Order cancellation response: {response} (OK if no orders): {response.get('msg') if response else 'No response'}", section="SYSTEM")
        except Exception as e:
            log_message(f"Error cancelling all orders: {e} (OK, continuing)", section="ERROR")

        log_message("=" * 80, section="SYSTEM")
        log_message("✅ BOT STOPPED - Cleanup complete. Goodbye!", section="SYSTEM")
        log_message(f"📁 Log file: {log_file_path}", section="SYSTEM")
        log_message("=" * 80, section="SYSTEM")