import os, time, json, requests, csv
from datetime import datetime, timezone, date
import ccxt
from ccxt.base.errors import AuthenticationError

###############################################################################
# SETTINGS (안정형 성장: 월 3~7% 목표)
# - 일부 파라미터는 config JSON으로 덮어쓰기 가능 (아래 load_config 참고)
###############################################################################

# 기본 설정값 (config 파일 없을 때 사용, 있으면 덮어씀)
CONFIG = {
    "DRY_RUN": True,
    "MAIN_LOOP_INTERVAL": 45,          # 초
    "MAX_DAILY_LOSS_RATIO": 0.03,      # 자본의 3%

    # 김프/역프 스프레드
    "TIER1_THR_MIN": 0.8,
    "TIER1_THR_MAX": 1.2,
    "TIER2_THR": 0.5,

    "BASE_RATIO_MIN": 0.35,
    "BASE_RATIO_MAX": 0.60,
    "TIER2_RATIO_FACTOR": 0.35,

    "MIN_NOTIONAL_KRW": 60000,
    "MAX_TRADES_1H": 35,
    "MAX_NOTIONAL_PER_TRADE_KRW": 2_000_000,  # 1순위: per-trade 절대 상한

    # 업↔빗 KRW 크로스
    "KRW_ARB_THR": 0.12,
    "KRW_ARB_RATIO": 0.25,

    # 펀딩 아비트
    "FUTURES_SYMBOL": "BTC/USDT:USDT",
    "FUNDING_SPREAD_THR_OPEN": 0.008,
    "FUNDING_SPREAD_THR_CLOSE": 0.003,
    "FUNDING_ARB_RATIO": 0.12,
    "FUNDING_MIN_NOTIONAL_USDT": 80.0,
    "FUNDING_TARGET_PAYMENTS": 3,
    "FUNDING_INTERVAL_HOURS": 8.0,

    # 프리미엄 예측
    "VOL_THRESHOLD_BORDER": 10.0,
    "PREMIUM_PRED_WEIGHTS": {
        "upbit_speed": 0.3,
        "bithumb_speed": 0.3,
        "volatility": 0.2,
        "orderbook_imbalance": 0.2,
    },

    # 순엣지 제한
    "EDGE_BUFFER_FEE_PCT": 0.12,
    "EDGE_BUFFER_SLIPPAGE_PCT": 0.05,
    "EDGE_MIN_NET_PCT": 0.08,

    # 슬리피지 제한 (2순위): VWAP vs top price 차이가 이 비율 이상이면 거래 스킵
    "SLIPPAGE_LIMIT_PCT": 0.001,  # 0.1%

    # z-score 기반 진입 필터 (3순위)
    "Z_SCORE_ENABLED": True,
    "Z_SCORE_WINDOW": 100,
    "Z_SCORE_THR": 1.5,

    # 레이어별 일일 드로다운 제한 (3순위)
    "LAYER_DD_LIMIT_KRW": 300_000,  # 한 레이어에서 하루 -30만 넘으면 해당 레이어 OFF (DRY_RUN 기준)

    # 에러 감지/쿨다운 (1순위)
    "ERROR_THRESHOLD": 3,
    "ERROR_COOLDOWN_SEC": 300,

    # 환율 fallback
    "FX_FALLBACK_USDT_KRW": 1450.0,
}

CONFIG_FILE = "kimchi_bot_config.json"


def load_config():
    """외부 JSON 설정이 있으면 CONFIG 값 덮어쓰기"""
    global CONFIG
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                user_cfg = json.load(f)
            for k, v in user_cfg.items():
                if k in CONFIG:
                    CONFIG[k] = v
            print(f"[CONFIG] Loaded override from {CONFIG_FILE}")
        except Exception as e:
            print(f"[CONFIG] load ERR {e}")


load_config()

# 설정값 언팩
DRY_RUN = CONFIG["DRY_RUN"]
MAIN_LOOP_INTERVAL = CONFIG["MAIN_LOOP_INTERVAL"]
MAX_DAILY_LOSS_RATIO = CONFIG["MAX_DAILY_LOSS_RATIO"]

TIER1_THR_MIN = CONFIG["TIER1_THR_MIN"]
TIER1_THR_MAX = CONFIG["TIER1_THR_MAX"]
TIER2_THR = CONFIG["TIER2_THR"]

BASE_RATIO_MIN = CONFIG["BASE_RATIO_MIN"]
BASE_RATIO_MAX = CONFIG["BASE_RATIO_MAX"]
TIER2_RATIO_FACTOR = CONFIG["TIER2_RATIO_FACTOR"]

MIN_NOTIONAL_KRW = CONFIG["MIN_NOTIONAL_KRW"]
MAX_TRADES_1H = CONFIG["MAX_TRADES_1H"]
MAX_NOTIONAL_PER_TRADE_KRW = CONFIG["MAX_NOTIONAL_PER_TRADE_KRW"]

KRW_ARB_THR = CONFIG["KRW_ARB_THR"]
KRW_ARB_RATIO = CONFIG["KRW_ARB_RATIO"]

FUTURES_SYMBOL = CONFIG["FUTURES_SYMBOL"]
FUNDING_SPREAD_THR_OPEN = CONFIG["FUNDING_SPREAD_THR_OPEN"]
FUNDING_SPREAD_THR_CLOSE = CONFIG["FUNDING_SPREAD_THR_CLOSE"]
FUNDING_ARB_RATIO = CONFIG["FUNDING_ARB_RATIO"]
FUNDING_MIN_NOTIONAL_USDT = CONFIG["FUNDING_MIN_NOTIONAL_USDT"]
FUNDING_TARGET_PAYMENTS = CONFIG["FUNDING_TARGET_PAYMENTS"]
FUNDING_INTERVAL_HOURS = CONFIG["FUNDING_INTERVAL_HOURS"]
FUNDING_MAX_HOURS_HOLD = FUNDING_TARGET_PAYMENTS * FUNDING_INTERVAL_HOURS

VOL_THRESHOLD_BORDER = CONFIG["VOL_THRESHOLD_BORDER"]
PREMIUM_PRED_WEIGHTS = CONFIG["PREMIUM_PRED_WEIGHTS"]

EDGE_BUFFER_FEE_PCT = CONFIG["EDGE_BUFFER_FEE_PCT"]
EDGE_BUFFER_SLIPPAGE_PCT = CONFIG["EDGE_BUFFER_SLIPPAGE_PCT"]
EDGE_MIN_NET_PCT = CONFIG["EDGE_MIN_NET_PCT"]

SLIPPAGE_LIMIT_PCT = CONFIG["SLIPPAGE_LIMIT_PCT"]

Z_SCORE_ENABLED = CONFIG["Z_SCORE_ENABLED"]
Z_SCORE_WINDOW = CONFIG["Z_SCORE_WINDOW"]
Z_SCORE_THR = CONFIG["Z_SCORE_THR"]

LAYER_DD_LIMIT_KRW = CONFIG["LAYER_DD_LIMIT_KRW"]

ERROR_THRESHOLD = CONFIG["ERROR_THRESHOLD"]
ERROR_COOLDOWN_SEC = CONFIG["ERROR_COOLDOWN_SEC"]

FX_FALLBACK_USDT_KRW = CONFIG["FX_FALLBACK_USDT_KRW"]

# 레이어 ON/OFF
ENABLE_LAYER_SPREAD_ARB = True
ENABLE_LAYER_KRW_CROSS = True
ENABLE_LAYER_FUNDING_SIG = True
ENABLE_LAYER_TRI_MONITOR = True

# 수수료율
FEE_RATES = {
    "binance": 0.0004,
    "upbit": 0.0005,
    "bithumb": 0.0005,
    "bybit": 0.0006,
    "okx": 0.0005,
}
DEFAULT_FEE_RATE = 0.0005

# 로그 파일
STATE_FILE = "kimchi_bot_state.json"
TRADE_LOG_FILE = "kimchi_bot_trades.csv"

###############################################################################
# ENV
###############################################################################

def env(k: str) -> str:
    if k not in os.environ:
        raise Exception(f"[ENV] Missing: {k}")
    return os.environ[k]


BINANCE_API = env("BINANCE_API_KEY")
BINANCE_SECRET = env("BINANCE_SECRET")
UPBIT_API = env("UPBIT_API_KEY")
UPBIT_SECRET = env("UPBIT_SECRET")
BITHUMB_API = env("BITHUMB_API_KEY")
BITHUMB_SECRET = env("BITHUMB_SECRET")
BYBIT_API = env("BYBIT_API_KEY")
BYBIT_SECRET = env("BYBIT_SECRET")
OKX_API = env("OKX_API_KEY")
OKX_SECRET = env("OKX_SECRET")
OKX_PASSWORD = env("OKX_PASSWORD")
TELEGRAM_TOKEN = env("TELEGRAM_TOKEN")
CHAT_ID = env("CHAT_ID")

###############################################################################
# GLOBAL STATE
###############################################################################

ex, ex_fut = {}, {}
TRADE_TIMES = []
price_history = {"upbit": [], "bithumb": []}
disable_trading = False
LAST_EQUITY_KRW = 21500000.0

# 거래소 에러 카운터 및 쿨다운 (1순위)
ERROR_COUNT = {}
DISABLED_UNTIL = {}

# 프리미엄 히스토리 (3순위: z-score)
SPREAD_PREM_HISTORY = {
    "BTC": [],
    "ETH": [],
}
KRW_PREM_HISTORY = {
    "BTC": [],
    "ETH": [],
}

STATE = {
    "date": None,  # "YYYY-MM-DD" UTC
    "realized_pnl_krw": 0.0,
    "realized_pnl_krw_daily": 0.0,
    "realized_pnl_krw_weekly": 0.0,
    "fees_krw": 0.0,
    "fees_krw_daily": 0.0,
    "fees_krw_weekly": 0.0,
    "num_trades": 0,
    "num_trades_daily": 0,
    "num_trades_weekly": 0,
    "weekly_start_date": None,
    # 레이어별 일일 PnL (3순위)
    "spread_pnl_daily": 0.0,
    "krw_pnl_daily": 0.0,
    "funding_pnl_daily": 0.0,
    # 레이어 비활성 플래그
    "spread_disabled_today": False,
    "krw_disabled_today": False,
    "funding_disabled_today": False,
}

FUNDING_POS = {
    "active": False,
    "short_ex": None,
    "long_ex": None,
    "symbol": FUTURES_SYMBOL,
    "amount": 0.0,
    "open_spread": 0.0,
    "open_time": 0.0,
}

###############################################################################
# TELEGRAM / STATE / TRADE LOG
###############################################################################

def send_telegram(msg: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=5)
    except Exception as e:
        print(f"[TELEGRAM] ERR {e}")


DEFAULT_STATE = STATE.copy()


def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE] save ERR {e}")


def load_state():
    global STATE
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            STATE = DEFAULT_STATE.copy()
            STATE.update(data)
            print(f"[STATE] Loaded: {STATE}")
        else:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            STATE["date"] = today
            STATE["weekly_start_date"] = today
            save_state()
    except Exception as e:
        print(f"[STATE] load ERR {e}")


def init_trade_log():
    """트레이드 로그 CSV가 없으면 헤더 생성"""
    if not os.path.exists(TRADE_LOG_FILE):
        try:
            with open(TRADE_LOG_FILE, "w", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                w.writerow([
                    "ts",
                    "date_utc",
                    "layer",
                    "symbol",
                    "venue",
                    "side",
                    "tier",
                    "prem_pct",
                    "notional_krw",
                    "amount",
                    "gross_pnl_krw",
                    "fee_krw",
                    "net_pnl_krw",
                    "dry_run",
                ])
            print(f"[TRADE LOG] Created {TRADE_LOG_FILE}")
        except Exception as e:
            print(f"[TRADE LOG INIT ERR] {e}")


def log_trade(layer, symbol, venue, side, tier,
              prem_pct, notional_krw, amount,
              gross_pnl_krw, fee_krw, net_pnl_krw):
    """각 트레이드를 CSV로 한 줄씩 기록"""
    ts = time.time()
    dt = datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    row = [
        f"{ts:.3f}",
        dt,
        layer,
        symbol,
        venue,
        side,
        tier if tier is not None else "",
        f"{prem_pct:.6f}" if prem_pct is not None else "",
        int(notional_krw) if notional_krw is not None else "",
        f"{amount:.8f}" if amount is not None else "",
        f"{gross_pnl_krw:.0f}" if gross_pnl_krw is not None else "",
        f"{fee_krw:.0f}" if fee_krw is not None else "",
        f"{net_pnl_krw:.0f}" if net_pnl_krw is not None else "",
        str(DRY_RUN),
    ]
    try:
        with open(TRADE_LOG_FILE, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(row)
    except Exception as e:
        print(f"[TRADE LOG ERR] {e}")

###############################################################################
# ERROR HANDLING (1순위)
###############################################################################


def record_exchange_error(ex_id: str):
    now = time.time()
    cnt = ERROR_COUNT.get(ex_id, 0) + 1
    ERROR_COUNT[ex_id] = cnt
    if cnt >= ERROR_THRESHOLD:
        DISABLED_UNTIL[ex_id] = now + ERROR_COOLDOWN_SEC
        ERROR_COUNT[ex_id] = 0
        msg = f"[RISK] {ex_id} 에러 {ERROR_THRESHOLD}회 이상 → {ERROR_COOLDOWN_SEC}s 동안 비활성화"
        print(msg)
        send_telegram(msg)


def is_exchange_disabled(ex_id: str) -> bool:
    until = DISABLED_UNTIL.get(ex_id)
    if until is None:
        return False
    if time.time() >= until:
        DISABLED_UNTIL.pop(ex_id, None)
        return False
    return True

###############################################################################
# FX / EQUITY
###############################################################################


def now_ts() -> float:
    return time.time()


def safe_ticker(e, symbol: str):
    if is_exchange_disabled(e.id):
        raise Exception(f"exchange {e.id} disabled")
    try:
        t = e.fetch_ticker(symbol)
        bid = t.get("bid") or t.get("last")
        ask = t.get("ask") or t.get("last")
        if not bid or not ask:
            raise Exception(f"invalid ticker {e.id} {symbol} {t}")
        t["bid"], t["ask"] = bid, ask
        return t
    except Exception as e2:
        record_exchange_error(e.id)
        raise


def safe_orderbook(e, symbol: str, depth: int = 10):
    if is_exchange_disabled(e.id):
        print(f"[OB] {e.id} disabled")
        return None
    try:
        ob = e.fetch_order_book(symbol, depth)
        if not ob["bids"] or not ob["asks"]:
            raise Exception("empty ob")
        return ob
    except Exception as e2:
        print(f"[OB] {e.id} {symbol} ERR {str(e2)[:80]}")
        record_exchange_error(e.id)
        return None


def get_usdt_krw() -> float:
    for name in ["upbit", "bithumb"]:
        inst = ex.get(name)
        if not inst or is_exchange_disabled(name):
            continue
        try:
            t = safe_ticker(inst, "USDT/KRW")
            return float(t["bid"])
        except Exception as e2:
            print(f"[FX] {name} USDT/KRW ERR {e2}")
    print(f"[FX] 환율 실패 → {FX_FALLBACK_USDT_KRW} 사용")
    return FX_FALLBACK_USDT_KRW


def estimate_total_equity_krw() -> float:
    global LAST_EQUITY_KRW
    try:
        usdt_krw = get_usdt_krw()
        b = ex.get("binance")
        if not b or is_exchange_disabled("binance"):
            return LAST_EQUITY_KRW
        t_btc = safe_ticker(b, "BTC/USDT")
        t_eth = safe_ticker(b, "ETH/USDT")
        btc_usdt = float(t_btc["last"])
        eth_usdt = float(t_eth["last"])
        total_krw = 0.0

        # 업비트/빗썸
        for name in ["upbit", "bithumb"]:
            inst = ex.get(name)
            if not inst or is_exchange_disabled(name):
                continue
            try:
                bal = inst.fetch_balance()
            except Exception as e:
                print(f"[EQ] {name} balance ERR {e}")
                record_exchange_error(name)
                continue
            krw = float(bal.get("KRW", {}).get("total", 0) or 0)
            btc = float(bal.get("BTC", {}).get("total", 0) or 0)
            eth = float(bal.get("ETH", {}).get("total", 0) or 0)
            total_krw += krw + btc * btc_usdt * usdt_krw + eth * eth_usdt * usdt_krw

        # 바이낸스
        if b and not is_exchange_disabled("binance"):
            try:
                bal = b.fetch_balance()
                usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
                btc = float(bal.get("BTC", {}).get("total", 0) or 0)
                eth = float(bal.get("ETH", {}).get("total", 0) or 0)
                total_krw += usdt * usdt_krw + btc * btc_usdt * usdt_krw + eth * eth_usdt * usdt_krw
            except Exception as e:
                print(f"[EQ] binance balance ERR {e}")
                record_exchange_error("binance")

        # OKX
        inst = ex.get("okx")
        if inst and not is_exchange_disabled("okx"):
            try:
                bal = inst.fetch_balance()
                usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
                total_krw += usdt * usdt_krw
            except Exception as e:
                print(f"[EQ] okx balance ERR {e}")
                record_exchange_error("okx")

        # Bybit
        inst = ex.get("bybit")
        if inst and not is_exchange_disabled("bybit"):
            try:
                bal = inst.fetch_balance()
                usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
                usd = float(bal.get("USD", {}).get("total", 0) or 0)
                total_krw += (usdt + usd) * usdt_krw
            except Exception as e:
                print(f"[EQ] bybit balance ERR {e}")
                record_exchange_error("bybit")

        if total_krw <= 0:
            return LAST_EQUITY_KRW
        LAST_EQUITY_KRW = total_krw
        return total_krw
    except Exception as e:
        print(f"[EQ] estimate ERR {e}")
        return LAST_EQUITY_KRW


def get_daily_volatility() -> float:
    try:
        b = ex["binance"]
        if is_exchange_disabled("binance"):
            return 0.0
        ohlcv = b.fetch_ohlcv("BTC/USDT", "1d", limit=2)
        if len(ohlcv) < 2:
            return 0.0
        p0 = ohlcv[0][4]
        p1 = ohlcv[1][4]
        return abs((p1 - p0) / p0 * 100)
    except Exception as e:
        print(f"[VOL] ERR {e}")
        record_exchange_error("binance")
        return 0.0

###############################################################################
# PRICE SPEED / IMBALANCE
###############################################################################


def record_price(source: str, price: float):
    ph = price_history[source]
    ph.append(price)
    if len(ph) > 50:
        ph.pop(0)


def price_speed(source: str) -> float:
    ph = price_history[source]
    if len(ph) < 3:
        return 0.0
    return (ph[-1] - ph[0]) / (abs(ph[0]) + 1e-9)


def orderbook_imbalance(ob) -> float:
    if not ob:
        return 0.0
    bid_vol = sum(v for p, v in ob["bids"])
    ask_vol = sum(v for p, v in ob["asks"])
    tot = bid_vol + ask_vol
    if tot == 0:
        return 0.0
    return (bid_vol - ask_vol) / tot


def predict_premium_prob(vol: float) -> float:
    up_speed = price_speed("upbit")
    bt_speed = price_speed("bithumb")
    if "upbit" not in ex or is_exchange_disabled("upbit"):
        return 0.0
    ob = safe_orderbook(ex["upbit"], "BTC/KRW", depth=5)
    imbal = orderbook_imbalance(ob)
    score = (
        PREMIUM_PRED_WEIGHTS["upbit_speed"] * up_speed +
        PREMIUM_PRED_WEIGHTS["bithumb_speed"] * bt_speed +
        PREMIUM_PRED_WEIGHTS["volatility"] * (vol / 15.0) +
        PREMIUM_PRED_WEIGHTS["orderbook_imbalance"] * imbal
    )
    return max(0.0, min(1.0, score))

###############################################################################
# VWAP / AUTO PARAMS / Z-SCORE
###############################################################################


def calc_vwap(ob, amount: float, is_buy: bool):
    if not ob:
        return None
    side = ob["asks"] if is_buy else ob["bids"]
    remain, cost = amount, 0.0
    for price, vol in side:
        use = min(vol, remain)
        cost += price * use
        remain -= use
        if remain <= 0:
            break
    if remain > 0:
        return None
    return cost / amount


def update_premium_history(history_dict, symbol: str, prem: float):
    arr = history_dict[symbol]
    arr.append(prem)
    if len(arr) > Z_SCORE_WINDOW:
        arr.pop(0)


def z_score_filter(history_dict, symbol: str, prem: float) -> bool:
    """z-score 기준 필터: True면 통과, False면 스킵"""
    if not Z_SCORE_ENABLED:
        return True
    arr = history_dict[symbol]
    if len(arr) < 10:
        # 데이터가 충분치 않으면 필터 적용 안함
        return True
    mean = sum(arr) / len(arr)
    var = sum((x - mean) ** 2 for x in arr) / len(arr)
    std = var ** 0.5
    if std <= 1e-9:
        return False
    z = abs((prem - mean) / std)
    return z >= Z_SCORE_THR


def auto_tier1_params(vol: float, trade_times):
    tc = len([t for t in trade_times if now_ts() - t <= 3600])
    v = min(max(vol, 0.0), VOL_THRESHOLD_BORDER)
    thr = TIER1_THR_MIN + (TIER1_THR_MAX - TIER1_THR_MIN) * (v / VOL_THRESHOLD_BORDER)
    prob = predict_premium_prob(vol)
    thr -= prob * 0.2
    if tc > MAX_TRADES_1H * 0.7:
        thr += 0.2
    thr = max(0.6, min(2.0, thr))

    base_ratio = 0.45
    vol_factor = v / VOL_THRESHOLD_BORDER if VOL_THRESHOLD_BORDER > 0 else 1.0
    base_ratio -= vol_factor * 0.05
    base_ratio += prob * 0.10
    base_ratio = max(BASE_RATIO_MIN, min(BASE_RATIO_MAX, base_ratio))
    return thr, base_ratio

###############################################################################
# CORE HELPERS / PnL
###############################################################################


def place_market_order(inst, symbol, side, amount) -> float:
    """
    부분체결 대응을 위한 wrapper.
    return: 실제 filled amount (best-effort).
    DRY_RUN=True면 요청 수량 그대로 리턴.
    """
    print(f"[ORDER] {inst.id} {side.upper()} {symbol} {amount} DRY_RUN={DRY_RUN}")
    if DRY_RUN:
        return amount
    if is_exchange_disabled(inst.id):
        raise Exception(f"exchange {inst.id} disabled")

    try:
        if side.lower() == "buy":
            order = inst.create_market_buy_order(symbol, amount)
        else:
            order = inst.create_market_sell_order(symbol, amount)
        filled = order.get("filled") or order.get("amount") or amount
        return float(filled)
    except Exception as e:
        print(f"[ORDER ERR] {inst.id} {symbol} {side} {e}")
        record_exchange_error(inst.id)
        raise


def can_trade_more(trade_times):
    now = now_ts()
    recent = [t for t in trade_times if now - t <= 3600]
    return len(recent) < MAX_TRADES_1H


def estimate_fee_krw(exchange_id: str, notional_krw: float) -> float:
    rate = FEE_RATES.get(exchange_id, DEFAULT_FEE_RATE)
    return notional_krw * rate


def send_daily_report(prev_date: str, pnl: float, trades: int, fees: float):
    msg = (
        f"[DAILY REPORT {prev_date}]\n"
        f"- 실현손익: {int(pnl)} KRW\n"
        f"- 트레이드 수: {trades} 건\n"
        f"- 수수료: {int(fees)} KRW\n"
        f"- 누적 손익: {int(STATE['realized_pnl_krw'])} KRW"
    )
    print(msg)
    send_telegram(msg)


def send_weekly_report(start_date: str, end_date: str, pnl: float, trades: int, fees: float):
    msg = (
        f"[WEEKLY REPORT {start_date} ~ {end_date}]\n"
        f"- 실현손익: {int(pnl)} KRW\n"
        f"- 트레이드 수: {trades} 건\n"
        f"- 수수료: {int(fees)} KRW\n"
        f"- 누적 손익: {int(STATE['realized_pnl_krw'])} KRW"
    )
    print(msg)
    send_telegram(msg)


def rollover_daily_pnl():
    today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    prev_date = STATE["date"]
    if prev_date is None:
        STATE["date"] = today_str
        if STATE["weekly_start_date"] is None:
            STATE["weekly_start_date"] = today_str
        save_state()
        return
    if prev_date == today_str:
        return

    # 데일리 리포트
    prev_pnl = STATE["realized_pnl_krw_daily"]
    prev_tr = STATE["num_trades_daily"]
    prev_fees = STATE["fees_krw_daily"]
    send_daily_report(prev_date, prev_pnl, prev_tr, prev_fees)

    # 일일 값 초기화
    STATE["realized_pnl_krw_daily"] = 0.0
    STATE["fees_krw_daily"] = 0.0
    STATE["num_trades_daily"] = 0

    # 레이어별 일일 PnL / disable 플래그 리셋
    STATE["spread_pnl_daily"] = 0.0
    STATE["krw_pnl_daily"] = 0.0
    STATE["funding_pnl_daily"] = 0.0
    STATE["spread_disabled_today"] = False
    STATE["krw_disabled_today"] = False
    STATE["funding_disabled_today"] = False

    # 주간 리포트
    if STATE["weekly_start_date"] is None:
        STATE["weekly_start_date"] = prev_date

    try:
        ws = date.fromisoformat(STATE["weekly_start_date"])
        pe = date.fromisoformat(prev_date)
        days_diff = (pe - ws).days
    except Exception:
        days_diff = 0

    if days_diff >= 6:
        weekly_pnl = STATE["realized_pnl_krw_weekly"]
        weekly_tr = STATE["num_trades_weekly"]
        weekly_fees = STATE["fees_krw_weekly"]
        send_weekly_report(STATE["weekly_start_date"], prev_date, weekly_pnl, weekly_tr, weekly_fees)
        STATE["realized_pnl_krw_weekly"] = 0.0
        STATE["fees_krw_weekly"] = 0.0
        STATE["num_trades_weekly"] = 0
        STATE["weekly_start_date"] = today_str

    STATE["date"] = today_str
    save_state()


def update_pnl(trade_name: str, pnl_krw: float, fee_krw: float, layer: str = None):
    """PnL/수수료/트레이드 누적 + 일/주간 업데이트 + 동적 3% 손실 한도 & 레이어별 드로다운 체크"""
    global disable_trading
    STATE["realized_pnl_krw"] += pnl_krw
    STATE["realized_pnl_krw_daily"] += pnl_krw
    STATE["realized_pnl_krw_weekly"] += pnl_krw

    STATE["fees_krw"] += fee_krw
    STATE["fees_krw_daily"] += fee_krw
    STATE["fees_krw_weekly"] += fee_krw

    STATE["num_trades"] += 1
    STATE["num_trades_daily"] += 1
    STATE["num_trades_weekly"] += 1

    # 레이어별 PnL
    if layer == "SPREAD":
        STATE["spread_pnl_daily"] += pnl_krw
    elif layer == "KRW":
        STATE["krw_pnl_daily"] += pnl_krw
    elif layer == "FUNDING":
        STATE["funding_pnl_daily"] += pnl_krw

    save_state()

    print(
        f"[PNL] {trade_name} pnl={pnl_krw:.0f} fee={fee_krw:.0f} "
        f"day_pnl={STATE['realized_pnl_krw_daily']:.0f} "
        f"week_pnl={STATE['realized_pnl_krw_weekly']:.0f} "
        f"total={STATE['realized_pnl_krw']:.0f}"
    )

    # 전체 일일 손실 한도
    equity_krw = estimate_total_equity_krw()
    loss_limit = equity_krw * MAX_DAILY_LOSS_RATIO
    if STATE["realized_pnl_krw_daily"] <= -loss_limit and not disable_trading:
        disable_trading = True
        msg = (
            f"[RISK] 일일 손실 한도 초과(안정형 모드): PnL={STATE['realized_pnl_krw_daily']:.0f} krw "
            f"<= -{int(loss_limit)} (자본 {int(equity_krw)}의 {MAX_DAILY_LOSS_RATIO*100:.1f}%)\n"
            f"→ 자동 매매 중단."
        )
        print(msg)
        send_telegram(msg)

    # 레이어별 드로다운 제한 (3순위 - soft layer disable)
    if layer == "SPREAD" and not STATE["spread_disabled_today"]:
        if STATE["spread_pnl_daily"] <= -LAYER_DD_LIMIT_KRW:
            STATE["spread_disabled_today"] = True
            msg = f"[RISK] SPREAD 레이어 일일 손실 {STATE['spread_pnl_daily']:.0f} → 오늘 SPREAD 중지"
            print(msg)
            send_telegram(msg)

    if layer == "KRW" and not STATE["krw_disabled_today"]:
        if STATE["krw_pnl_daily"] <= -LAYER_DD_LIMIT_KRW:
            STATE["krw_disabled_today"] = True
            msg = f"[RISK] KRW 레이어 일일 손실 {STATE['krw_pnl_daily']:.0f} → 오늘 KRW 중지"
            print(msg)
            send_telegram(msg)

###############################################################################
# EXCHANGE INIT
###############################################################################


def init_exchanges():
    global ex, ex_fut
    ex, ex_fut = {}, {}

    spot_cfg = [
        ("binance", ccxt.binance, BINANCE_API, BINANCE_SECRET, None),
        ("upbit", ccxt.upbit, UPBIT_API, UPBIT_SECRET, None),
        ("bithumb", ccxt.bithumb, BITHUMB_API, BITHUMB_SECRET, None),
        ("bybit", ccxt.bybit, BYBIT_API, BYBIT_SECRET, None),
        ("okx", ccxt.okx, OKX_API, OKX_SECRET, OKX_PASSWORD),
    ]
    for name, cls, key, sec, pwd in spot_cfg:
        try:
            params = {"apiKey": key, "secret": sec, "enableRateLimit": True}
            if name == "okx":
                params["password"] = pwd
            inst = cls(params)
            inst.load_markets()
            ex[name] = inst
            print(f"[INIT] {name} spot 연결 성공")
        except Exception as e:
            print(f"[INIT] {name} spot ERR {e}")
            record_exchange_error(name)

    try:
        bin_fut = ccxt.binanceusdm({
            "apiKey": BINANCE_API,
            "secret": BINANCE_SECRET,
            "enableRateLimit": True,
        })
        bin_fut.load_markets()
        ex_fut["binance_fut"] = bin_fut
        print("[INIT] binance_fut 연결 성공")
    except Exception as e:
        print(f"[INIT] binance_fut ERR {e}")
        record_exchange_error("binance_fut")

    try:
        bybit_fut = ccxt.bybit({
            "apiKey": BYBIT_API,
            "secret": BYBIT_SECRET,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })
        bybit_fut.load_markets()
        ex_fut["bybit_fut"] = bybit_fut
        print("[INIT] bybit_fut 연결 성공")
    except Exception as e:
        print(f"[INIT] bybit_fut ERR {e}")
        record_exchange_error("bybit_fut")

    try:
        okx_fut = ccxt.okx({
            "apiKey": OKX_API,
            "secret": OKX_SECRET,
            "password": OKX_PASSWORD,
            "enableRateLimit": True,
            "options": {"defaultType": "swap"},
        })
        okx_fut.load_markets()
        ex_fut["okx_fut"] = okx_fut
        print("[INIT] okx_fut 연결 성공")
    except Exception as e:
        print(f"[INIT] okx_fut ERR {e}")
        record_exchange_error("okx_fut")

###############################################################################
# ARB LAYERS
###############################################################################


def run_spread_arbitrage(symbol: str, tier1_thr: float, base_ratio: float, trade_times):
    global disable_trading
    if disable_trading or not ENABLE_LAYER_SPREAD_ARB or STATE["spread_disabled_today"]:
        print(f"[ARB] SPREAD skip {symbol}")
        return
    if is_exchange_disabled("binance"):
        print("[ARB] binance disabled, skip SPREAD")
        return
    try:
        b = ex["binance"]
        usdt_krw = get_usdt_krw()
        base_pair = f"{symbol}/USDT"
        t_base = safe_ticker(b, base_pair)
        base_usdt = float(t_base["bid"])
        ref_krw = base_usdt * usdt_krw
        bal_b = b.fetch_balance()
        free_usdt = float(bal_b.get("USDT", {}).get("free", 0) or 0)
        free_sym = float(bal_b.get(symbol, {}).get("free", 0) or 0)

        for venue in ["upbit", "bithumb"]:
            if venue not in ex or is_exchange_disabled(venue):
                continue
            e = ex[venue]
            try:
                t_krw = safe_ticker(e, f"{symbol}/KRW")
                last_price = t_krw["last"]
                record_price(venue, last_price)
            except Exception as e2:
                print(f"[ARB] {venue} ticker ERR {e2}")
                continue

            test_amount = 0.01 if symbol == "BTC" else 0.05
            ob = safe_orderbook(e, f"{symbol}/KRW", depth=10)
            if not ob:
                continue

            # 슬리피지 제한 체크용 top price
            top_bid = ob["bids"][0][0] if ob["bids"] else None
            top_ask = ob["asks"][0][0] if ob["asks"] else None

            vwap_sell_krw = calc_vwap(ob, test_amount, is_buy=False)
            vwap_buy_krw = calc_vwap(ob, test_amount, is_buy=True)

            sell_prem = None
            buy_prem = None

            if vwap_sell_krw and top_bid:
                # 슬리피지 체크 (2순위)
                if abs(vwap_sell_krw - top_bid) / top_bid > SLIPPAGE_LIMIT_PCT:
                    print(f"[SLIP] {symbol} {venue} SELL vwap slippage too large, skip")
                else:
                    sell_usdt = vwap_sell_krw / usdt_krw
                    sell_prem = (sell_usdt / base_usdt - 1) * 100

            if vwap_buy_krw and top_ask:
                if abs(vwap_buy_krw - top_ask) / top_ask > SLIPPAGE_LIMIT_PCT:
                    print(f"[SLIP] {symbol} {venue} BUY vwap slippage too large, skip")
                else:
                    buy_usdt = vwap_buy_krw / usdt_krw
                    buy_prem = (buy_usdt / base_usdt - 1) * 100

            print(f"[REAL {symbol} {venue}] sell={sell_prem} buy={buy_prem} thr={tier1_thr:.2f} base_ratio={base_ratio:.2f}")

            # 프리미엄 히스토리 업데이트 (3순위 z-score)
            if sell_prem is not None:
                update_premium_history(SPREAD_PREM_HISTORY, symbol, sell_prem)
            if buy_prem is not None:
                update_premium_history(SPREAD_PREM_HISTORY, symbol, buy_prem)

            try:
                bal_k = e.fetch_balance()
            except AuthenticationError as ae:
                print(f"[ARB] {venue} balance auth ERR {ae}")
                record_exchange_error(venue)
                continue
            except Exception as e3:
                print(f"[ARB] {venue} balance ERR {e3}")
                record_exchange_error(venue)
                continue

            ex_krw = float(bal_k.get("KRW", {}).get("free", 0) or 0)
            ex_sym = float(bal_k.get(symbol, {}).get("free", 0) or 0)

            def net_edge_ok(prem: float) -> bool:
                if prem is None:
                    return False
                gross = abs(prem)
                needed = EDGE_BUFFER_FEE_PCT + EDGE_BUFFER_SLIPPAGE_PCT + EDGE_MIN_NET_PCT
                return gross >= needed

            # 김프: 국내 SELL / 바이낸스 BUY
            if sell_prem is not None and can_trade_more(trade_times) and net_edge_ok(sell_prem):
                # z-score 필터
                if not z_score_filter(SPREAD_PREM_HISTORY, symbol, sell_prem):
                    print(f"[Z] {symbol} {venue} SELL z-score 부족, skip")
                else:
                    trade_tier, trade_ratio = None, 0.0
                    if sell_prem >= tier1_thr:
                        trade_tier, trade_ratio = "TIER1", base_ratio
                    elif sell_prem >= TIER2_THR:
                        trade_tier, trade_ratio = "TIER2", base_ratio * TIER2_RATIO_FACTOR

                    if trade_tier and ex_sym > 0 and free_usdt > 0:
                        max_from_k = ex_sym * trade_ratio
                        max_from_b = (free_usdt * trade_ratio) / base_usdt
                        amt = min(max_from_k, max_from_b)
                        # 절대 노출 상한 (1순위)
                        vwap = vwap_sell_krw or t_krw["bid"]
                        notional_krw_est = amt * vwap
                        if notional_krw_est > MAX_NOTIONAL_PER_TRADE_KRW:
                            amt = MAX_NOTIONAL_PER_TRADE_KRW / vwap
                            notional_krw_est = MAX_NOTIONAL_PER_TRADE_KRW

                        if notional_krw_est >= MIN_NOTIONAL_KRW and amt > 0:
                            # 실제 진입
                            bin_filled = place_market_order(b, base_pair, "buy", amt)
                            dom_filled = place_market_order(e, f"{symbol}/KRW", "sell", amt)
                            effective_amt = min(bin_filled, dom_filled)
                            if effective_amt <= 0:
                                continue

                            notional_krw_sell = effective_amt * vwap
                            notional_krw_buy = effective_amt * ref_krw
                            notional_krw = min(notional_krw_sell, notional_krw_buy)

                            fee_sell = estimate_fee_krw(venue, notional_krw_sell)
                            fee_buy = estimate_fee_krw("binance", notional_krw_buy)
                            gross_pnl = (vwap - ref_krw) * effective_amt
                            total_fee = fee_sell + fee_buy
                            net_pnl = gross_pnl - total_fee
                            print(f"[ARB {symbol}] {venue} SELL {trade_tier} amt={effective_amt} notional={int(notional_krw)} net_pnl={net_pnl:.0f}")
                            trade_times.append(now_ts())

                            log_trade(
                                layer="SPREAD_ARB",
                                symbol=symbol,
                                venue=venue,
                                side="KRW_SELL_BIN_BUY",
                                tier=trade_tier,
                                prem_pct=sell_prem,
                                notional_krw=notional_krw,
                                amount=effective_amt,
                                gross_pnl_krw=gross_pnl,
                                fee_krw=total_fee,
                                net_pnl_krw=net_pnl,
                            )

                            update_pnl(f"{symbol}-{venue}-SELL-{trade_tier}", net_pnl, total_fee, layer="SPREAD")
                            send_telegram(f"[{symbol}] {venue} SELL {trade_tier} prem={sell_prem:.2f}% amt={effective_amt:.6f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}")

            # 역프: 국내 BUY / 바이낸스 SELL
            if buy_prem is not None and can_trade_more(trade_times) and net_edge_ok(buy_prem):
                if not z_score_filter(SPREAD_PREM_HISTORY, symbol, buy_prem):
                    print(f"[Z] {symbol} {venue} BUY z-score 부족, skip")
                else:
                    trade_tier, trade_ratio = None, 0.0
                    if buy_prem <= -tier1_thr:
                        trade_tier, trade_ratio = "TIER1", base_ratio
                    elif buy_prem <= -TIER2_THR:
                        trade_tier, trade_ratio = "TIER2", base_ratio * TIER2_RATIO_FACTOR

                    if trade_tier and ex_krw > 0 and free_sym > 0:
                        vwap = vwap_buy_krw or t_krw["ask"]
                        max_from_krw = (ex_krw * trade_ratio) / vwap
                        max_from_sym = free_sym * trade_ratio
                        amt = min(max_from_krw, max_from_sym)
                        notional_krw_est = amt * vwap
                        if notional_krw_est > MAX_NOTIONAL_PER_TRADE_KRW:
                            amt = MAX_NOTIONAL_PER_TRADE_KRW / vwap
                            notional_krw_est = MAX_NOTIONAL_PER_TRADE_KRW

                        if notional_krw_est >= MIN_NOTIONAL_KRW and amt > 0:
                            dom_filled = place_market_order(e, f"{symbol}/KRW", "buy", amt)
                            bin_filled = place_market_order(b, base_pair, "sell", amt)
                            effective_amt = min(dom_filled, bin_filled)
                            if effective_amt <= 0:
                                continue

                            notional_krw_buy = effective_amt * vwap
                            notional_krw_sell = effective_amt * ref_krw
                            notional_krw = min(notional_krw_buy, notional_krw_sell)

                            fee_buy = estimate_fee_krw(venue, notional_krw_buy)
                            fee_sell = estimate_fee_krw("binance", notional_krw_sell)
                            gross_pnl = (ref_krw - vwap) * effective_amt
                            total_fee = fee_buy + fee_sell
                            net_pnl = gross_pnl - total_fee
                            print(f"[ARB {symbol}] {venue} BUY {trade_tier} amt={effective_amt} notional={int(notional_krw)} net_pnl={net_pnl:.0f}")
                            trade_times.append(now_ts())

                            log_trade(
                                layer="SPREAD_ARB",
                                symbol=symbol,
                                venue=venue,
                                side="KRW_BUY_BIN_SELL",
                                tier=trade_tier,
                                prem_pct=buy_prem,
                                notional_krw=notional_krw,
                                amount=effective_amt,
                                gross_pnl_krw=gross_pnl,
                                fee_krw=total_fee,
                                net_pnl_krw=net_pnl,
                            )

                            update_pnl(f"{symbol}-{venue}-BUY-{trade_tier}", net_pnl, total_fee, layer="SPREAD")
                            send_telegram(f"[{symbol}] {venue} BUY {trade_tier} prem={buy_prem:.2f}% amt={effective_amt:.6f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}")
    except Exception as e:
        print(f"[ARB ERR] {symbol} {e}")
        send_telegram(f"[ARB ERR] {symbol}: {e}")


def run_krw_cross_arb(symbol: str):
    if disable_trading or not ENABLE_LAYER_KRW_CROSS or STATE["krw_disabled_today"]:
        print(f"[KRW-ARB] skip {symbol}")
        return
    if "upbit" not in ex or "bithumb" not in ex:
        return
    if is_exchange_disabled("upbit") or is_exchange_disabled("bithumb"):
        print("[KRW-ARB] upbit or bithumb disabled")
        return
    try:
        u, bth = ex["upbit"], ex["bithumb"]
        t_u = safe_ticker(u, f"{symbol}/KRW")
        t_b = safe_ticker(bth, f"{symbol}/KRW")
        price_u, price_b = float(t_u["last"]), float(t_b["last"])
        diff, mid = price_u - price_b, (price_u + price_b) / 2
        prem = (diff / mid) * 100
        print(f"[KRW-ARB {symbol}] up={price_u} bt={price_b} prem={prem:.3f}%")
        if abs(prem) < KRW_ARB_THR:
            return

        # z-score 히스토리 업데이트 & 필터
        update_premium_history(KRW_PREM_HISTORY, symbol, prem)
        if not z_score_filter(KRW_PREM_HISTORY, symbol, prem):
            print(f"[Z] KRW-ARB {symbol} prem z-score 부족, skip")
            return

        gross = abs(prem)
        needed = EDGE_BUFFER_FEE_PCT + EDGE_BUFFER_SLIPPAGE_PCT + EDGE_MIN_NET_PCT
        if gross < needed:
            print(f"[KRW-ARB {symbol}] prem={prem:.3f}% but net edge 부족(need {needed:.2f}%)")
            return

        bal_u, bal_b = u.fetch_balance(), bth.fetch_balance()
        free_u_sym = float(bal_u.get(symbol, {}).get("free", 0) or 0)
        free_b_sym = float(bal_b.get(symbol, {}).get("free", 0) or 0)
        free_u_krw = float(bal_u.get("KRW", {}).get("free", 0) or 0)
        free_b_krw = float(bal_b.get("KRW", {}).get("free", 0) or 0)
        max_notional = KRW_ARB_RATIO * min(
            free_u_sym * price_u + free_u_krw,
            free_b_sym * price_b + free_b_krw,
        )
        if max_notional < MIN_NOTIONAL_KRW:
            return
        if max_notional > MAX_NOTIONAL_PER_TRADE_KRW:
            max_notional = MAX_NOTIONAL_PER_TRADE_KRW

        # 업비트 고가, 빗썸 저가 → 업 SELL / 빗 BUY
        if prem > 0:
            amt = max_notional / price_u
            amt = min(amt, free_u_sym * 0.9, (free_b_krw * 0.9) / price_b)
            if amt <= 0:
                return
            notional_sell = amt * price_u
            notional_buy = amt * price_b
            fee_sell = estimate_fee_krw("upbit", notional_sell)
            fee_buy = estimate_fee_krw("bithumb", notional_buy)
            gross_pnl = (price_u - price_b) * amt
            total_fee = fee_sell + fee_buy
            net_pnl = gross_pnl - total_fee
            print(f"[KRW-ARB {symbol}] upbit SELL, bithumb BUY amt={amt} net_pnl={net_pnl:.0f}")

            up_filled = place_market_order(u, f"{symbol}/KRW", "sell", amt)
            bt_filled = place_market_order(bth, f"{symbol}/KRW", "buy", amt)
            effective_amt = min(up_filled, bt_filled)
            if effective_amt <= 0:
                return

            log_trade(
                layer="KRW_ARB",
                symbol=symbol,
                venue="upbit_bithumb",
                side="UP_SELL_BT_BUY",
                tier="NONE",
                prem_pct=prem,
                notional_krw=max_notional,
                amount=effective_amt,
                gross_pnl_krw=gross_pnl,
                fee_krw=total_fee,
                net_pnl_krw=net_pnl,
            )

            update_pnl(f"{symbol}-KRW-ARB-up-sell", net_pnl, total_fee, layer="KRW")
            send_telegram(f"[KRW ARB {symbol}] upbit SELL / bithumb BUY prem={prem:.3f}% amt={effective_amt:.5f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}")
        # 빗썸 고가, 업비트 저가 → 빗 SELL / 업 BUY
        else:
            amt = max_notional / price_b
            amt = min(amt, free_b_sym * 0.9, (free_u_krw * 0.9) / price_u)
            if amt <= 0:
                return
            notional_sell = amt * price_b
            notional_buy = amt * price_u
            fee_sell = estimate_fee_krw("bithumb", notional_sell)
            fee_buy = estimate_fee_krw("upbit", notional_buy)
            gross_pnl = (price_b - price_u) * amt
            total_fee = fee_sell + fee_buy
            net_pnl = gross_pnl - total_fee
            print(f"[KRW-ARB {symbol}] bithumb SELL, upbit BUY amt={amt} net_pnl={net_pnl:.0f}")

            bt_filled = place_market_order(bth, f"{symbol}/KRW", "sell", amt)
            up_filled = place_market_order(u, f"{symbol}/KRW", "buy", amt)
            effective_amt = min(bt_filled, up_filled)
            if effective_amt <= 0:
                return

            log_trade(
                layer="KRW_ARB",
                symbol=symbol,
                venue="bithumb_upbit",
                side="BT_SELL_UP_BUY",
                tier="NONE",
                prem_pct=prem,
                notional_krw=max_notional,
                amount=effective_amt,
                gross_pnl_krw=gross_pnl,
                fee_krw=total_fee,
                net_pnl_krw=net_pnl,
            )

            update_pnl(f"{symbol}-KRW-ARB-bt-sell", net_pnl, total_fee, layer="KRW")
            send_telegram(f"[KRW ARB {symbol}] bithumb SELL / upbit BUY prem={prem:.3f}% amt={effective_amt:.5f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}")
    except Exception as e:
        print(f"[KRW-ARB ERR {symbol}] {e}")


def funding_arbitrage_signals():
    if not ENABLE_LAYER_FUNDING_SIG:
        return
    global FUNDING_POS
    try:
        if not ex_fut:
            print("[FUND] futures exchanges not initialized")
            return

        rates = {}
        try:
            bin_fut = ex_fut.get("binance_fut")
            if bin_fut:
                fr = bin_fut.fetch_funding_rate(FUTURES_SYMBOL)
                rates["binance_fut"] = fr["fundingRate"]
        except Exception as e:
            print(f"[FUND] binance_fut ERR {e}")
            record_exchange_error("binance_fut")
        try:
            bybit_fut = ex_fut.get("bybit_fut")
            if bybit_fut:
                fr = bybit_fut.fetch_funding_rate(FUTURES_SYMBOL)
                rates["bybit_fut"] = fr["fundingRate"]
        except Exception as e:
            print(f"[FUND] bybit_fut ERR {e}")
            record_exchange_error("bybit_fut")
        try:
            okx_fut = ex_fut.get("okx_fut")
            if okx_fut:
                fr = okx_fut.fetch_funding_rate(FUTURES_SYMBOL)
                rates["okx_fut"] = fr["fundingRate"]
        except Exception as e:
            print(f"[FUND] okx_fut ERR {e}")
            record_exchange_error("okx_fut")

        print(f"[FUND RATES] {rates}")
        if len(rates) < 2:
            return

        max_ex = max(rates, key=rates.get)
        min_ex = min(rates, key=rates.get)
        spread = rates[max_ex] - rates[min_ex]
        print(f"[FUND SPREAD] max={max_ex}({rates[max_ex]:.5f}) min={min_ex}({rates[min_ex]:.5f}) diff={spread:.5f}")

        now = now_ts()

        # 기존 포지션 청산
        if FUNDING_POS["active"]:
            hold_hours = (now - FUNDING_POS["open_time"]) / 3600.0
            close_reason = None
            if hold_hours >= FUNDING_MAX_HOURS_HOLD:
                close_reason = f"TIME: {hold_hours:.2f}h >= {FUNDING_MAX_HOURS_HOLD:.2f}h"
            elif spread <= FUNDING_SPREAD_THR_CLOSE:
                close_reason = f"SPREAD: {spread:.5f} <= {FUNDING_SPREAD_THR_CLOSE}"

            if close_reason:
                short_key, long_key = FUNDING_POS["short_ex"], FUNDING_POS["long_ex"]
                symbol, amount = FUNDING_POS["symbol"], FUNDING_POS["amount"]
                short_ex, long_ex = ex_fut.get(short_key), ex_fut.get(long_key)
                if not short_ex or not long_ex:
                    print("[FUND CLOSE] missing fut instance")
                    return
                print(f"[FUND ARB CLOSE] reason={close_reason}, short={short_key}, long={long_key}, amt={amount:.4f}")
                place_market_order(short_ex, symbol, "buy", amount)
                place_market_order(long_ex, symbol, "sell", amount)

                # 로그 (청산)
                log_trade(
                    layer="FUNDING_ARB",
                    symbol=symbol,
                    venue=f"{short_key}_{long_key}",
                    side="CLOSE",
                    tier="NONE",
                    prem_pct=spread * 100,
                    notional_krw=None,
                    amount=amount,
                    gross_pnl_krw=None,
                    fee_krw=None,
                    net_pnl_krw=None,
                )

                msg = (
                    "[FUND ARB CLOSE]\n"
                    f"- short: {short_key}\n- long : {long_key}\n- amt  : {amount:.4f} BTC\n"
                    f"- reason: {close_reason}\n- open_spread={FUNDING_POS['open_spread']:.5f}\n"
                    f"- current_spread={spread:.5f}\n- hold_hours={hold_hours:.2f}\n- DRY_RUN={DRY_RUN}"
                )
                print(msg)
                send_telegram(msg)
                FUNDING_POS.update({
                    "active": False,
                    "short_ex": None,
                    "long_ex": None,
                    "amount": 0.0,
                    "open_spread": 0.0,
                    "open_time": 0.0,
                })
            return

        # 새 포지션 진입
        if disable_trading:
            print("[FUND] trading disabled, skip open")
            return
        if spread < FUNDING_SPREAD_THR_OPEN:
            return

        high_key, low_key = max_ex, min_ex
        high_ex, low_ex = ex_fut.get(high_key), ex_fut.get(low_key)
        if not high_ex or not low_ex:
            print("[FUND] missing fut instance for open")
            return
        symbol = FUTURES_SYMBOL
        t_high = high_ex.fetch_ticker(symbol)
        t_low = low_ex.fetch_ticker(symbol)
        price_high = float(t_high["last"] or t_high["bid"])
        price_low = float(t_low["last"] or t_low["bid"])
        mid_price = (price_high + price_low) / 2.0

        bal_high, bal_low = high_ex.fetch_balance(), low_ex.fetch_balance()
        free_high_usdt = float(bal_high.get("USDT", {}).get("free", 0) or 0)
        free_low_usdt = float(bal_low.get("USDT", {}).get("free", 0) or 0)
        max_usable_usdt = min(free_high_usdt, free_low_usdt) * FUNDING_ARB_RATIO
        if max_usable_usdt < FUNDING_MIN_NOTIONAL_USDT:
            print(f"[FUND] not enough USDT: {max_usable_usdt:.1f}")
            return

        amount = max_usable_usdt / mid_price
        if amount <= 0:
            return
        print(f"[FUND ARB OPEN] short {high_key}, long {low_key}, amt={amount:.4f}, notional≈{max_usable_usdt:.1f}, spread={spread:.5f}")
        place_market_order(high_ex, symbol, "sell", amount)
        place_market_order(low_ex, symbol, "buy", amount)
        FUNDING_POS.update({
            "active": True,
            "short_ex": high_key,
            "long_ex": low_key,
            "symbol": symbol,
            "amount": amount,
            "open_spread": float(spread),
            "open_time": now,
        })

        # 로그 (오픈)
        log_trade(
            layer="FUNDING_ARB",
            symbol=symbol,
            venue=f"{high_key}_{low_key}",
            side="OPEN",
            tier="NONE",
            prem_pct=spread * 100,
            notional_krw=None,
            amount=amount,
            gross_pnl_krw=None,
            fee_krw=None,
            net_pnl_krw=None,
        )

        msg = (
            "[FUND ARB OPEN]\n"
            f"- short: {high_key} (funding={rates[high_key]:.5f})\n"
            f"- long : {low_key} (funding={rates[low_key]:.5f})\n"
            f"- spread={spread:.5f} >= {FUNDING_SPREAD_THR_OPEN}\n"
            f"- amt={amount:.4f} BTC, notional≈{max_usable_usdt:.1f} USDT\n"
            f"- hold_target={FUNDING_TARGET_PAYMENTS} payments (≈ {FUNDING_MAX_HOURS_HOLD:.1f}h)\n"
            f"- DRY_RUN={DRY_RUN}"
        )
        print(msg)
        send_telegram(msg)
    except Exception as e:
        print(f"[FUND ARB ERR] {e}")
        send_telegram(f"[FUND ARB ERR] {e}")


def triangular_monitor(name: str):
    if not ENABLE_LAYER_TRI_MONITOR:
        return
    try:
        inst = ex.get(name)
        if not inst or is_exchange_disabled(name):
            return
        t1 = safe_ticker(inst, "BTC/USDT")
        t2 = safe_ticker(inst, "ETH/USDT")
        t3 = safe_ticker(inst, "ETH/BTC")
        spread = (t2["bid"] / (t1["bid"] * t3["bid"]) - 1) * 100
        print(f"[TRI {name}] {spread:.3f}%")
    except Exception as e:
        print(f"[TRI ERR {name}] {e}")

###############################################################################
# MAIN
###############################################################################


def main():
    global disable_trading
    load_state()
    init_exchanges()
    init_trade_log()
    equity_krw = estimate_total_equity_krw()
    msg = (
        f"김프봇 안정형 성장 시작 (DRY_RUN={DRY_RUN})\n"
        f"- 추정 자본: 약 {int(equity_krw):,} KRW\n"
        f"- 일일 손실 한도: 자본의 {MAX_DAILY_LOSS_RATIO*100:.1f}% (동적)\n"
        f"- 목표: 월 3~7% 수준의 안정적 성장 + 레이어별 드로다운 관리"
    )
    print(msg)
    send_telegram(msg)

    trade_times = []

    while True:
        loop_start = now_ts()
        try:
            rollover_daily_pnl()
            vol = get_daily_volatility()
            tier1_thr, base_ratio = auto_tier1_params(vol, trade_times)
            trades_1h = len([t for t in trade_times if now_ts() - t <= 3600])
            print(
                f"\n[LOOP] vol={vol:.2f}% tier1_thr={tier1_thr:.2f}% base_ratio={base_ratio:.2f} "
                f"trades_1h={trades_1h} day_pnl={STATE['realized_pnl_krw_daily']:.0f}"
            )

            if not disable_trading:
                if ENABLE_LAYER_SPREAD_ARB:
                    run_spread_arbitrage("BTC", tier1_thr, base_ratio, trade_times)
                    run_spread_arbitrage("ETH", tier1_thr, base_ratio, trade_times)
                if ENABLE_LAYER_KRW_CROSS:
                    run_krw_cross_arb("BTC")
                    run_krw_cross_arb("ETH")
                if ENABLE_LAYER_FUNDING_SIG:
                    funding_arbitrage_signals()
                if ENABLE_LAYER_TRI_MONITOR:
                    for name in ["bybit", "okx"]:
                        triangular_monitor(name)
            else:
                print("[LOOP] trading disabled – 매매 중단 상태")
        except Exception as e:
            print(f"[MAIN ERR] {e}")
            send_telegram(f"[MAIN ERR] {e}")
        elapsed = now_ts() - loop_start
        sleep_time = max(5, MAIN_LOOP_INTERVAL - elapsed)
        print(f"[LOOP] sleep {sleep_time:.1f}s")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
