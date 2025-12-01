import os
import time
import json
import requests
from datetime import datetime
import ccxt
from ccxt.base.errors import AuthenticationError

###############################################################################
# SETTINGS
###############################################################################

DRY_RUN = True                 # 실매매 전에는 반드시 True 유지
MAIN_LOOP_INTERVAL = 60        # 1분 루프

# ─ 리스크 관리 ─
MAX_DAILY_LOSS_KRW = 300000    # 일일 허용 손실 한도 (예: 30만 원). 초과 시 자동 정지
STATE_FILE = "kimchi_bot_state.json"

# ─ Layer ON/OFF ─
ENABLE_LAYER_SPREAD_ARB   = True   # Binance vs KRW 김프/역프
ENABLE_LAYER_KRW_CROSS    = True   # 업비트 vs 빗썸 KRW 차익
ENABLE_LAYER_FUNDING_SIG  = True   # 펀딩 아비트 (실제 포지션 + 자동 청산)
ENABLE_LAYER_TRI_MONITOR  = True   # 삼각차익 모니터

# ─ 기본 김프/역프 레이어 ─
# Tier1: 굵은 김프 (강하게 진입)
TIER1_THR_MIN = 1.2            # 변동성 낮을 때 최소 1.2%
TIER1_THR_MAX = 1.6            # 변동성 높을 때 1.6%까지

# Tier2: 얕은 김프 (소액 비중으로 진입)
TIER2_THR = 0.7                # 0.7% 이상이면 Tier2 후보

# 기본 ratio (한 번에 잔고 몇 %까지 사용할지)
BASE_RATIO_MIN = 0.4           # 최소 40%
BASE_RATIO_MAX = 0.8           # 최대 80%
TIER2_RATIO_FACTOR = 0.3       # Tier2는 기본 ratio의 30%만 사용

# per-trade 최소 노치널 (KRW)
MIN_NOTIONAL_KRW = 50000       # 5만 미만은 거래 안 함

# 1시간 거래 횟수 제한
MAX_TRADES_1H = 50             # 공격형: 1시간 최대 50회까지 허용

# ─ 업비트 ↔ 빗썸 KRW 차익 레이어 ─
KRW_ARB_THR = 0.25             # 0.25% 이상이면 차익거래 후보 (net edge 필터로 추가 필터링)
KRW_ARB_RATIO = 0.2            # 업빗/빗썸 사이 KRW 차익거래는 계좌의 20% 정도만

# ─ Funding 레이어(실제 포지션 진입 + 자동 청산) ─
FUNDING_SPREAD_THR_OPEN  = 0.02   # 진입 기준: 스프레드 2% 이상일 때 진입 후보
FUNDING_SPREAD_THR_CLOSE = 0.005  # 청산 기준: 스프레드가 0.5% 이하로 줄어들면 청산 후보

FUNDING_ARB_RATIO            = 0.10    # 각 선물 계좌 USDT의 몇 %를 펀딩 아비트에 사용할지
FUNDING_MIN_NOTIONAL_USDT    = 100.0   # 이 미만이면 진입 안 함
FUNDING_TARGET_PAYMENTS      = 3       # 목표 펀딩 횟수 (예: 3번 펀딩 받으면 청산)
FUNDING_INTERVAL_HOURS       = 8.0     # 펀딩 간격(보통 8시간)
FUNDING_MAX_HOURS_HOLD       = FUNDING_TARGET_PAYMENTS * FUNDING_INTERVAL_HOURS

# 변동성 기준 (%)
VOL_THRESHOLD_BORDER = 10.0    # BTC/USDT 일간 변동성 10% 기준

# 김프 예측 엔진 가중치 (단순화)
PREMIUM_PRED_WEIGHTS = {
    "upbit_speed":          0.3,
    "bithumb_speed":        0.3,
    "volatility":           0.2,
    "orderbook_imbalance":  0.2,
}

# ─ 수수료 / 슬리피지 / 최소 순엣지 설정 ─
# 각 거래소 taker fee (대략값. 실제 계정 수수료에 맞게 반드시 수정)
FEE_RATES = {
    "binance": 0.0004,     # 0.04%
    "upbit":   0.0005,     # 0.05%
    "bithumb": 0.0005,     # 0.05%
    "bybit":   0.0006,
    "okx":     0.0005,
}
DEFAULT_FEE_RATE = 0.0005

# 수수료 외에 슬리피지 등을 고려한 쿠션 (단위: %)
EDGE_BUFFER_FEE_PCT       = 0.20   # 왕복 수수료 추정치 (0.20%)
EDGE_BUFFER_SLIPPAGE_PCT  = 0.10   # 슬리피지 여유 (0.10%)
EDGE_MIN_NET_PCT          = 0.15   # 위 두 개 빼고도 최소 0.15% 이상 남아야 진입

###############################################################################
# ENV
###############################################################################

def env(k: str) -> str:
    if k not in os.environ:
        raise Exception(f"[ENV] Missing: {k}")
    return os.environ[k]

BINANCE_API     = env("BINANCE_API_KEY")
BINANCE_SECRET  = env("BINANCE_SECRET")

UPBIT_API       = env("UPBIT_API_KEY")
UPBIT_SECRET    = env("UPBIT_SECRET")

BITHUMB_API     = env("BITHUMB_API_KEY")
BITHUMB_SECRET  = env("BITHUMB_SECRET")

BYBIT_API       = env("BYBIT_API_KEY")
BYBIT_SECRET    = env("BYBIT_SECRET")

OKX_API         = env("OKX_API_KEY")
OKX_SECRET      = env("OKX_SECRET")
OKX_PASSWORD    = env("OKX_PASSWORD")

TELEGRAM_TOKEN  = env("TELEGRAM_TOKEN")
CHAT_ID         = env("CHAT_ID")

###############################################################################
# GLOBAL STATE
###############################################################################

# 현물/스팟 계정
ex = {}
# 선물/스왑 계정
ex_fut = {}

TRADE_TIMES = []                     # 최근 1시간 트레이드 타임스탬프
price_history = {"upbit": [], "bithumb": []}

disable_trading = False

STATE = {
    "date": None,                   # "YYYY-MM-DD" (UTC 기준)
    "realized_pnl_krw": 0.0,
    "realized_pnl_krw_daily": 0.0,
    "fees_krw": 0.0,
    "num_trades": 0,
}

# 현재 열린 펀딩 아비트 포지션(1쌍만 운용)
FUNDING_POS = {
    "active": False,
    "short_ex": None,         # "binance_fut" / "bybit_fut" / "okx_fut"
    "long_ex":  None,
    "symbol":   "BTC/USDT",
    "amount":   0.0,
    "open_spread": 0.0,       # 진입 당시 funding spread
    "open_time":   0.0,       # 진입 시각 (timestamp)
}

###############################################################################
# TELEGRAM
###############################################################################

def send_telegram(msg: str):
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        requests.post(url, data={"chat_id": CHAT_ID, "text": msg}, timeout=5)
    except Exception as e:
        print(f"[TELEGRAM] ERR {e}")

###############################################################################
# STATE PERSISTENCE
###############################################################################

def load_state():
    global STATE
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                STATE.update(data)
                print(f"[STATE] Loaded from {STATE_FILE}: {STATE}")
        else:
            today = datetime.utcnow().strftime("%Y-%m-%d")
            STATE["date"] = today
            save_state()
    except Exception as e:
        print(f"[STATE] load ERR {e}")

def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE] save ERR {e}")

def rollover_daily_pnl():
    """UTC 기준 날짜가 바뀌면 일일 실현손익을 리셋."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if STATE["date"] != today:
        print(f"[STATE] New day detected: {STATE['date']} -> {today}. Daily PnL reset.")
        STATE["date"] = today
        STATE["realized_pnl_krw_daily"] = 0.0
        save_state()

def estimate_fee_krw(exchange_id: str, notional_krw: float) -> float:
    rate = FEE_RATES.get(exchange_id, DEFAULT_FEE_RATE)
    return notional_krw * rate

def update_pnl(trade_name: str, pnl_krw: float, fee_krw: float):
    """
    개별 트레이드의 net PnL과 fee를 상태에 반영하고, 리스크 한도 체크.
    (펀딩 아비트는 여기 안 넣고, 현물/김프/크로스만 집계)
    """
    global disable_trading
    STATE["realized_pnl_krw"] += pnl_krw
    STATE["realized_pnl_krw_daily"] += pnl_krw
    STATE["fees_krw"] += fee_krw
    STATE["num_trades"] += 1
    save_state()

    print(
        f"[PNL] {trade_name} pnl={pnl_krw:.0f} krw fee={fee_krw:.0f} krw "
        f"day_pnl={STATE['realized_pnl_krw_daily']:.0f} total={STATE['realized_pnl_krw']:.0f}"
    )

    # 일일 손실 한도 체크
    if STATE["realized_pnl_krw_daily"] <= -MAX_DAILY_LOSS_KRW and not disable_trading:
        disable_trading = True
        msg = (
            f"[RISK] 일일 손실 한도 초과: {STATE['realized_pnl_krw_daily']:.0f} krw "
            f"<= -{MAX_DAILY_LOSS_KRW}. 자동 매매 중단."
        )
        print(msg)
        send_telegram(msg)

###############################################################################
# EXCHANGE INIT
###############################################################################

def init_exchanges():
    global ex, ex_fut
    ex = {}
    ex_fut = {}

    # Spot / 일반 계정
    spot_config = [
        ("binance", ccxt.binance,   BINANCE_API,   BINANCE_SECRET, None),
        ("upbit",   ccxt.upbit,     UPBIT_API,     UPBIT_SECRET,   None),
        ("bithumb", ccxt.bithumb,   BITHUMB_API,   BITHUMB_SECRET, None),
        ("bybit",   ccxt.bybit,     BYBIT_API,     BYBIT_SECRET,   None),
        ("okx",     ccxt.okx,       OKX_API,       OKX_SECRET,     OKX_PASSWORD),
    ]
    for name, cls, key, sec, pwd in spot_config:
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

    # Futures / swap 계정
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

###############################################################################
# UTIL: TIME / TICKER / ORDERBOOK
###############################################################################

def now_ts() -> float:
    return time.time()

def safe_ticker(e, symbol: str):
    t = e.fetch_ticker(symbol)
    bid = t.get("bid") or t.get("last")
    ask = t.get("ask") or t.get("last")
    if not bid or not ask:
        raise Exception(f"invalid ticker {e.id} {symbol} {t}")
    t["bid"], t["ask"] = bid, ask
    return t

def safe_orderbook(e, symbol: str, depth: int = 10):
    try:
        ob = e.fetch_order_book(symbol, depth)
        if not ob["bids"] or not ob["asks"]:
            raise Exception("empty ob")
        return ob
    except Exception as e2:
        print(f"[OB] {e.id} {symbol} ERR {str(e2)[:80]}")
        return None

###############################################################################
# FX / VOL / SPEED
###############################################################################

def get_usdt_krw() -> float:
    for name in ["upbit", "bithumb"]:
        inst = ex.get(name)
        if not inst:
            continue
        try:
            t = safe_ticker(inst, "USDT/KRW")
            return float(t["bid"])
        except Exception as e2:
            print(f"[FX] {name} USDT/KRW ERR {e2}")
    print("[FX] 환율 실패 → 1350 사용")
    return 1350.0

def get_daily_volatility() -> float:
    try:
        b = ex["binance"]
        ohlcv = b.fetch_ohlcv("BTC/USDT", "1d", limit=2)
        if len(ohlcv) < 2:
            return 0.0
        p0 = ohlcv[0][4]
        p1 = ohlcv[1][4]
        return abs((p1 - p0) / p0 * 100)
    except Exception as e:
        print(f"[VOL] ERR {e}")
        return 0.0

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
# VWAP (호가창 기반 평균 체결가)
###############################################################################

def calc_vwap(ob, amount: float, is_buy: bool):
    if not ob:
        return None
    side = ob["asks"] if is_buy else ob["bids"]
    remain = amount
    cost = 0.0
    for price, vol in side:
        use = min(vol, remain)
        cost += price * use
        remain -= use
        if remain <= 0:
            break
    if remain > 0:
        return None
    return cost / amount

###############################################################################
# AUTO PARAMS (TIER1 threshold + ratio)
###############################################################################

def auto_tier1_params(vol: float, trade_times) -> (float, float):
    tc = len([t for t in trade_times if now_ts() - t <= 3600])
    v = min(max(vol, 0.0), VOL_THRESHOLD_BORDER)
    # threshold
    if VOL_THRESHOLD_BORDER > 0:
        thr = TIER1_THR_MIN + (TIER1_THR_MAX - TIER1_THR_MIN) * (v / VOL_THRESHOLD_BORDER)
    else:
        thr = TIER1_THR_MAX
    prob = predict_premium_prob(vol)
    thr -= prob * 0.3
    if tc > MAX_TRADES_1H * 0.7:
        thr += 0.3
    thr = max(1.0, min(2.0, thr))
    # ratio
    base_ratio = 0.5
    vol_factor = v / VOL_THRESHOLD_BORDER if VOL_THRESHOLD_BORDER > 0 else 1.0
    base_ratio -= vol_factor * 0.1
    base_ratio += prob * 0.1
    base_ratio = max(BASE_RATIO_MIN, min(BASE_RATIO_MAX, base_ratio))
    return thr, base_ratio

###############################################################################
# CORE MARKET HELPERS
###############################################################################

def create_order(inst, symbol, side, amount):
    print(f"[ORDER] {inst.id} {side.upper()} {symbol} {amount} DRY_RUN={DRY_RUN}")
    if DRY_RUN:
        return
    if side.lower() == "buy":
        inst.create_market_buy_order(symbol, amount)
    else:
        inst.create_market_sell_order(symbol, amount)

def can_trade_more(trade_times):
    now = now_ts()
    recent = [t for t in trade_times if now - t <= 3600]
    return len(recent) < MAX_TRADES_1H

###############################################################################
# LAYER 1: 김프/역프 재정거래 (TIER1 + TIER2)
###############################################################################

def run_spread_arbitrage(symbol: str, tier1_thr: float, base_ratio: float, trade_times):
    global disable_trading
    if disable_trading or not ENABLE_LAYER_SPREAD_ARB:
        print(f"[ARB] trading disabled or layer off, skip {symbol}")
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
        free_sym  = float(bal_b.get(symbol, {}).get("free", 0) or 0)

        for venue in ["upbit", "bithumb"]:
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
            vwap_sell_krw = calc_vwap(ob, test_amount, is_buy=False)
            vwap_buy_krw  = calc_vwap(ob, test_amount, is_buy=True)

            if vwap_sell_krw:
                sell_usdt = vwap_sell_krw / usdt_krw
                sell_prem = (sell_usdt / base_usdt - 1) * 100
            else:
                sell_prem = None

            if vwap_buy_krw:
                buy_usdt = vwap_buy_krw / usdt_krw
                buy_prem = (buy_usdt / base_usdt - 1) * 100
            else:
                buy_prem = None

            print(
                f"[REAL {symbol} {venue}] sell={sell_prem} buy={buy_prem} "
                f"thr={tier1_thr:.2f} base_ratio={base_ratio:.2f}"
            )

            try:
                bal_k = e.fetch_balance()
            except AuthenticationError as ae:
                print(f"[ARB] {venue} balance auth ERR {ae}")
                continue
            except Exception as e3:
                print(f"[ARB] {venue} balance ERR {e3}")
                continue

            ex_krw = float(bal_k.get("KRW", {}).get("free", 0) or 0)
            ex_sym = float(bal_k.get(symbol, {}).get("free", 0) or 0)

            def net_edge_ok(prem: float) -> bool:
                """
                prem: 김프(+) 또는 역프(-) % 값
                """
                if prem is None:
                    return False
                gross = abs(prem)
                needed = EDGE_BUFFER_FEE_PCT + EDGE_BUFFER_SLIPPAGE_PCT + EDGE_MIN_NET_PCT
                return gross >= needed

            # ─ SELL SIDE (김프, KRW 거래소가 더 비쌀 때) ─
            if sell_prem is not None and can_trade_more(trade_times) and net_edge_ok(sell_prem):
                trade_tier = None
                trade_ratio = 0.0
                if sell_prem >= tier1_thr:
                    trade_tier = "TIER1"
                    trade_ratio = base_ratio
                elif sell_prem >= TIER2_THR:
                    trade_tier = "TIER2"
                    trade_ratio = base_ratio * TIER2_RATIO_FACTOR

                if trade_tier and ex_sym > 0 and free_usdt > 0:
                    max_from_k = ex_sym * trade_ratio
                    max_from_b = (free_usdt * trade_ratio) / base_usdt
                    amt = min(max_from_k, max_from_b)
                    if not vwap_sell_krw:
                        vwap_sell_krw = t_krw["bid"]
                    notional_krw_sell = amt * vwap_sell_krw
                    notional_krw_buy  = amt * ref_krw
                    notional_krw = min(notional_krw_sell, notional_krw_buy)
                    if notional_krw >= MIN_NOTIONAL_KRW:
                        fee_sell = estimate_fee_krw(venue, notional_krw_sell)
                        fee_buy  = estimate_fee_krw("binance", notional_krw_buy)
                        gross_pnl = (vwap_sell_krw - ref_krw) * amt
                        total_fee = fee_sell + fee_buy
                        net_pnl = gross_pnl - total_fee

                        print(
                            f"[ARB {symbol}] {venue} SELL {trade_tier} amt={amt} "
                            f"notional={int(notional_krw)} net_pnl={net_pnl:.0f}"
                        )
                        create_order(b, base_pair, "buy", amt)
                        create_order(e, f"{symbol}/KRW", "sell", amt)
                        trade_times.append(now_ts())
                        update_pnl(f"{symbol}-{venue}-SELL-{trade_tier}", net_pnl, total_fee)
                        send_telegram(
                            f"[{symbol}] {venue} SELL {trade_tier} prem={sell_prem:.2f}% "
                            f"amt={amt:.6f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}"
                        )

            # ─ BUY SIDE (역프, KRW 거래소가 더 쌀 때) ─
            if buy_prem is not None and can_trade_more(trade_times) and net_edge_ok(buy_prem):
                trade_tier = None
                trade_ratio = 0.0
                if buy_prem <= -tier1_thr:
                    trade_tier = "TIER1"
                    trade_ratio = base_ratio
                elif buy_prem <= -TIER2_THR:
                    trade_tier = "TIER2"
                    trade_ratio = base_ratio * TIER2_RATIO_FACTOR

                if trade_tier and ex_krw > 0 and free_sym > 0:
                    ref_buy_price_krw = vwap_buy_krw or t_krw["ask"]
                    max_from_krw = (ex_krw * trade_ratio) / ref_buy_price_krw
                    max_from_sym = free_sym * trade_ratio
                    amt = min(max_from_krw, max_from_sym)
                    notional_krw_buy = amt * ref_buy_price_krw
                    notional_krw_sell = amt * ref_krw
                    notional_krw = min(notional_krw_buy, notional_krw_sell)
                    if notional_krw >= MIN_NOTIONAL_KRW:
                        fee_buy  = estimate_fee_krw(venue, notional_krw_buy)
                        fee_sell = estimate_fee_krw("binance", notional_krw_sell)
                        gross_pnl = (ref_krw - ref_buy_price_krw) * amt
                        total_fee = fee_buy + fee_sell
                        net_pnl = gross_pnl - total_fee

                        print(
                            f"[ARB {symbol}] {venue} BUY {trade_tier} amt={amt} "
                            f"notional={int(notional_krw)} net_pnl={net_pnl:.0f}"
                        )
                        create_order(e, f"{symbol}/KRW", "buy", amt)
                        create_order(b, base_pair, "sell", amt)
                        trade_times.append(now_ts())
                        update_pnl(f"{symbol}-{venue}-BUY-{trade_tier}", net_pnl, total_fee)
                        send_telegram(
                            f"[{symbol}] {venue} BUY {trade_tier} prem={buy_prem:.2f}% "
                            f"amt={amt:.6f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}"
                        )

    except Exception as e:
        print(f"[ARB ERR] {symbol} {e}")
        send_telegram(f"[ARB ERR] {symbol}: {e}")

###############################################################################
# LAYER 2: 업비트 ↔ 빗썸 KRW 차익 레이어
###############################################################################

def run_krw_cross_arb(symbol: str):
    """
    업비트 vs 빗썸 BTC/ETH 가격 차이가 KRW_ARB_THR 이상이면
    양쪽 거래소에서 동시에 사고 파는 KRW 차익거래.
    순엣지(수수료+슬리피지 제외 후)가 남는 경우에만 진입.
    """
    if disable_trading or not ENABLE_LAYER_KRW_CROSS:
        print(f"[KRW-ARB] trading disabled or layer off, skip {symbol}")
        return

    try:
        u = ex["upbit"]
        b = ex["bithumb"]
        t_u = safe_ticker(u, f"{symbol}/KRW")
        t_b = safe_ticker(b, f"{symbol}/KRW")
        price_u = float(t_u["last"])
        price_b = float(t_b["last"])

        diff = price_u - price_b
        mid  = (price_u + price_b) / 2
        prem = (diff / mid) * 100

        print(f"[KRW-ARB {symbol}] up={price_u} bt={price_b} prem={prem:.3f}%")

        if abs(prem) < KRW_ARB_THR:
            return

        gross = abs(prem)
        needed = EDGE_BUFFER_FEE_PCT + EDGE_BUFFER_SLIPPAGE_PCT + EDGE_MIN_NET_PCT
        if gross < needed:
            print(f"[KRW-ARB {symbol}] prem={prem:.3f}% but net edge 부족(need {needed:.2f}%)")
            return

        bal_u = u.fetch_balance()
        bal_b = b.fetch_balance()
        free_u_sym = float(bal_u.get(symbol, {}).get("free", 0) or 0)
        free_b_sym = float(bal_b.get(symbol, {}).get("free", 0) or 0)
        free_u_krw = float(bal_u.get("KRW", {}).get("free", 0) or 0)
        free_b_krw = float(bal_b.get("KRW", {}).get("free", 0) or 0)

        max_notional = KRW_ARB_RATIO * min(
            free_u_sym * price_u + free_u_krw,
            free_b_sym * price_b + free_b_krw
        )
        if max_notional < MIN_NOTIONAL_KRW:
            return

        if prem > 0:
            # upbit 비쌈 → upbit SELL, bithumb BUY
            amt = max_notional / price_u
            amt = min(amt, free_u_sym * 0.9, (free_b_krw * 0.9) / price_b)
            if amt <= 0:
                return
            notional_sell = amt * price_u
            notional_buy  = amt * price_b
            fee_sell = estimate_fee_krw("upbit", notional_sell)
            fee_buy  = estimate_fee_krw("bithumb", notional_buy)
            gross_pnl = (price_u - price_b) * amt
            total_fee = fee_sell + fee_buy
            net_pnl = gross_pnl - total_fee

            print(f"[KRW-ARB {symbol}] upbit SELL, bithumb BUY amt={amt} net_pnl={net_pnl:.0f}")
            create_order(u, f"{symbol}/KRW", "sell", amt)
            create_order(b, f"{symbol}/KRW", "buy", amt)
            update_pnl(f"{symbol}-KRW-ARB-up-sell", net_pnl, total_fee)
            send_telegram(
                f"[KRW ARB {symbol}] upbit SELL / bithumb BUY prem={prem:.3f}% "
                f"amt={amt:.5f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}"
            )
        else:
            # prem < 0 → 빗썸 비쌈 → 빗썸 SELL, 업비트 BUY
            amt = max_notional / price_b
            amt = min(amt, free_b_sym * 0.9, (free_u_krw * 0.9) / price_u)
            if amt <= 0:
                return
            notional_sell = amt * price_b
            notional_buy  = amt * price_u
            fee_sell = estimate_fee_krw("bithumb", notional_sell)
            fee_buy  = estimate_fee_krw("upbit", notional_buy)
            gross_pnl = (price_b - price_u) * amt
            total_fee = fee_sell + fee_buy
            net_pnl = gross_pnl - total_fee

            print(f"[KRW-ARB {symbol}] bithumb SELL, upbit BUY amt={amt} net_pnl={net_pnl:.0f}")
            create_order(b, f"{symbol}/KRW", "sell", amt)
            create_order(u, f"{symbol}/KRW", "buy", amt)
            update_pnl(f"{symbol}-KRW-ARB-bt-sell", net_pnl, total_fee)
            send_telegram(
                f"[KRW ARB {symbol}] bithumb SELL / upbit BUY prem={prem:.3f}% "
                f"amt={amt:.5f} net_pnl={int(net_pnl)} DRY_RUN={DRY_RUN}"
            )

    except Exception as e:
        print(f"[KRW-ARB ERR {symbol}] {e}")

###############################################################################
# LAYER 3: Funding Arbitrage (실제 포지션 진입 + 자동 청산)
###############################################################################

def funding_arbitrage_signals():
    """
    Binance USDT 선물 / Bybit swap / OKX swap의 BTC/USDT 펀딩 레이트를 비교.

    - 새 포지션 진입 조건 (FUNDING_POS["active"] == False):
      1) funding spread >= FUNDING_SPREAD_THR_OPEN
      2) disable_trading == False
      3) 두 선물 계좌에 USDT가 충분함 (FUNDING_MIN_NOTIONAL_USDT 이상)

      → 고펀딩 거래소: 숏 (short)
        저펀딩 거래소: 롱 (long)

    - 기존 포지션 청산 조건 (FUNDING_POS["active"] == True):
      1) 보유 시간 >= FUNDING_MAX_HOURS_HOLD
         (FUNDING_TARGET_PAYMENTS * FUNDING_INTERVAL_HOURS)
         OR
      2) 현재 funding spread <= FUNDING_SPREAD_THR_CLOSE

      → 기존 포지션의:
        short_ex: buy (숏 청산)
        long_ex : sell (롱 청산)

    DRY_RUN=True일 때는 create_order 내부에서 실제 주문은 나가지 않고 로그만 찍는다.
    """
    if not ENABLE_LAYER_FUNDING_SIG:
        return

    global FUNDING_POS

    try:
        # 선물 인스턴스 체크
        if not ex_fut:
            print("[FUND] futures exchanges not initialized")
            return

        rates = {}

        # Binance USDT margined futures
        try:
            bin_fut = ex_fut.get("binance_fut")
            if bin_fut:
                fr = bin_fut.fetch_funding_rate("BTC/USDT")
                rates["binance_fut"] = fr["fundingRate"]
        except Exception as e:
            print(f"[FUND] binance_fut ERR {e}")

        # Bybit linear swap
        try:
            bybit_fut = ex_fut.get("bybit_fut")
            if bybit_fut:
                frs = bybit_fut.fetch_funding_rates()
                for r in frs:
                    if r.get("symbol") in ["BTC/USDT", "BTCUSDT"]:
                        rates["bybit_fut"] = r.get("fundingRate", 0)
                        break
        except Exception as e:
            print(f"[FUND] bybit_fut ERR {e}")

        # OKX swap
        try:
            okx_fut = ex_fut.get("okx_fut")
            if okx_fut:
                frs = okx_fut.fetch_funding_rates()
                for r in frs:
                    if r.get("symbol") in ["BTC-USDT-SWAP", "BTC/USDT:USDT"]:
                        rates["okx_fut"] = r.get("fundingRate", 0)
                        break
        except Exception as e:
            print(f"[FUND] okx_fut ERR {e}")

        print(f"[FUND RATES] {rates}")
        if len(rates) < 2:
            return

        # 가장 높은/낮은 funding 찾기
        max_ex = max(rates, key=rates.get)
        min_ex = min(rates, key=rates.get)
        spread = rates[max_ex] - rates[min_ex]

        print(
            f"[FUND SPREAD] max={max_ex}({rates[max_ex]:.5f}) "
            f"min={min_ex}({rates[min_ex]:.5f}) diff={spread:.5f}"
        )

        now = now_ts()

        # ──────────────────────────────────────────────
        # 1) 기존 포지션이 열려 있는 경우 → 청산 조건 체크
        # ──────────────────────────────────────────────
        if FUNDING_POS["active"]:
            hold_hours = (now - FUNDING_POS["open_time"]) / 3600.0
            close_reason = None

            # (A) 목표 펀딩 횟수만큼 보유했는지 (시간 기준)
            if hold_hours >= FUNDING_MAX_HOURS_HOLD:
                close_reason = (
                    f"TIME: hold_hours={hold_hours:.2f}h "
                    f">= {FUNDING_MAX_HOURS_HOLD:.2f}h"
                )

            # (B) 스프레드가 충분히 줄었는지 (되돌림)
            elif spread <= FUNDING_SPREAD_THR_CLOSE:
                close_reason = (
                    f"SPREAD: spread={spread:.5f} "
                    f"<= close_thr={FUNDING_SPREAD_THR_CLOSE}"
                )

            if close_reason:
                short_key = FUNDING_POS["short_ex"]
                long_key  = FUNDING_POS["long_ex"]
                symbol    = FUNDING_POS["symbol"]
                amount    = FUNDING_POS["amount"]

                short_ex = ex_fut.get(short_key)
                long_ex  = ex_fut.get(long_key)

                if not short_ex or not long_ex:
                    print("[FUND CLOSE] missing futures instance, skip close")
                    return

                print(
                    f"[FUND ARB CLOSE] reason={close_reason}, "
                    f"short={short_key}, long={long_key}, amt={amount:.4f}"
                )
                # 숏 청산: buy
                create_order(short_ex, symbol, "buy", amount)
                # 롱 청산: sell
                create_order(long_ex,  symbol, "sell", amount)

                msg = (
                    "[FUND ARB CLOSE]\n"
                    f"- short: {short_key}\n"
                    f"- long : {long_key}\n"
                    f"- amt  : {amount:.4f} BTC\n"
                    f"- reason: {close_reason}\n"
                    f"- open_spread={FUNDING_POS['open_spread']:.5f}\n"
                    f"- current_spread={spread:.5f}\n"
                    f"- hold_hours={hold_hours:.2f}\n"
                    f"- DRY_RUN={DRY_RUN}"
                )
                print(msg)
                send_telegram(msg)

                # 상태 초기화
                FUNDING_POS["active"] = False
                FUNDING_POS["short_ex"] = None
                FUNDING_POS["long_ex"]  = None
                FUNDING_POS["amount"]   = 0.0
                FUNDING_POS["open_spread"] = 0.0
                FUNDING_POS["open_time"]   = 0.0

            return  # 포지션이 있었으면 여기서 종료

        # ──────────────────────────────────────────────
        # 2) 포지션이 없고, 새로 진입할지 여부 판단
        # ──────────────────────────────────────────────
        if disable_trading:
            print("[FUND] trading disabled, skip open")
            return

        if spread < FUNDING_SPREAD_THR_OPEN:
            # 스프레드가 진입 기준보다 작으면 아무것도 안 함
            return

        # 진입 후보 거래소
        high_key = max_ex   # funding rate 높은 쪽 → 숏 후보
        low_key  = min_ex   # funding rate 낮은 쪽 → 롱 후보

        high_ex = ex_fut.get(high_key)
        low_ex  = ex_fut.get(low_key)
        if not high_ex or not low_ex:
            print("[FUND] missing futures instance for open")
            return

        symbol = "BTC/USDT"

        # 가격
        t_high = safe_ticker(high_ex, symbol)
        t_low  = safe_ticker(low_ex, symbol)
        price_high = float(t_high["last"] or t_high["bid"])
        price_low  = float(t_low["last"] or t_low["bid"])
        mid_price  = (price_high + price_low) / 2.0

        # USDT 잔고
        bal_high = high_ex.fetch_balance()
        bal_low  = low_ex.fetch_balance()
        free_high_usdt = float(bal_high.get("USDT", {}).get("free", 0) or 0)
        free_low_usdt  = float(bal_low.get("USDT", {}).get("free", 0) or 0)

        max_usable_usdt = min(free_high_usdt, free_low_usdt) * FUNDING_ARB_RATIO
        if max_usable_usdt < FUNDING_MIN_NOTIONAL_USDT:
            print(f"[FUND] not enough USDT for funding arb: {max_usable_usdt:.1f}")
            return

        amount = max_usable_usdt / mid_price  # BTC 수량
        if amount <= 0:
            return

        print(
            f"[FUND ARB OPEN] short on {high_key}, long on {low_key}, "
            f"amt={amount:.4f} BTC, notional≈{max_usable_usdt:.1f} USDT, spread={spread:.5f}"
        )

        # 고펀딩 거래소 숏, 저펀딩 거래소 롱
        create_order(high_ex, symbol, "sell", amount)  # short
        create_order(low_ex,  symbol, "buy",  amount)  # long

        FUNDING_POS["active"] = True
        FUNDING_POS["short_ex"] = high_key
        FUNDING_POS["long_ex"]  = low_key
        FUNDING_POS["symbol"]   = symbol
        FUNDING_POS["amount"]   = amount
        FUNDING_POS["open_spread"] = float(spread)
        FUNDING_POS["open_time"]   = now

        msg = (
            "[FUND ARB OPEN]\n"
            f"- short: {high_key} (funding={rates[high_key]:.5f})\n"
            f"- long : {low_key} (funding={rates[low_key]:.5f})\n"
            f"- spread={spread:.5f} >= open_thr={FUNDING_SPREAD_THR_OPEN}\n"
            f"- amt={amount:.4f} BTC, notional≈{max_usable_usdt:.1f} USDT\n"
            f"- hold_target={FUNDING_TARGET_PAYMENTS} payments "
            f"(≈ {FUNDING_MAX_HOURS_HOLD:.1f}h)\n"
            f"- DRY_RUN={DRY_RUN}"
        )
        print(msg)
        send_telegram(msg)

    except Exception as e:
        print(f"[FUND ARB ERR] {e}")
        send_telegram(f"[FUND ARB ERR] {e}")

###############################################################################
# TRIANGULAR MONITOR (Bybit/OKX)
###############################################################################

def triangular_monitor(name: str):
    if not ENABLE_LAYER_TRI_MONITOR:
        return
    try:
        inst = ex.get(name)
        if not inst:
            return
        t1 = safe_ticker(inst, "BTC/USDT")
        t2 = safe_ticker(inst, "ETH/USDT")
        t3 = safe_ticker(inst, "ETH/BTC")
        spread = (t2["bid"] / (t1["bid"] * t3["bid"]) - 1) * 100
        print(f"[TRI {name}] {spread:.3f}%")
    except Exception as e:
        print(f"[TRI ERR {name}] {e}")

###############################################################################
# MAIN LOOP
###############################################################################

def main():
    global disable_trading
    load_state()
    init_exchanges()
    send_telegram(f"공격형 김프봇 v2 시작 (DRY_RUN={DRY_RUN}) – Risk & PnL + Funding Arb")

    trade_times = []

    while True:
        loop_start = now_ts()
        try:
            rollover_daily_pnl()

            vol = get_daily_volatility()
            tier1_thr, base_ratio = auto_tier1_params(vol, trade_times)
            trades_1h = len([t for t in trade_times if now_ts() - t <= 3600])

            print(
                f"\n[LOOP] vol={vol:.2f}% tier1_thr={tier1_thr:.2f}% "
                f"base_ratio={base_ratio:.2f} trades_1h={trades_1h} "
                f"day_pnl={STATE['realized_pnl_krw_daily']:.0f}"
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
