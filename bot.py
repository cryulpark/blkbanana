import os
import time
import traceback
import requests
import csv
from datetime import datetime
import ccxt
from ccxt.base.errors import AuthenticationError

###############################################################################
# SETTINGS
###############################################################################

DRY_RUN = True                 # 실매매 전에는 반드시 True 유지
MAIN_LOOP_INTERVAL = 60        # 1분마다 루프
STATUS_INTERVAL = 3600         # 1시간마다 상태 리포트

# 김프 threshold 자동 조정 범위 (%)
THRESHOLD_MIN = 1.3            # 가장 공격적일 때
THRESHOLD_MAX = 1.8            # 가장 보수적일 때

# 잔고 사용 비율(auto-ratio) 범위
BASE_RATIO_MIN = 0.3           # 최소 30%
BASE_RATIO_MAX = 0.6           # 최대 60%

# per-trade 최소 노치널 (KRW)
MIN_NOTIONAL_KRW = 100000      # 10만 원 미만 거래는 제외

# 삼각차익 조건
MIN_TRIANGULAR_SPREAD = 0.5    # 0.5% 이상일 때만 관심

# 1시간 거래 횟수 제한 (과열 방지)
MAX_TRADES_1H = 30

# 일간 PnL 제한 (오늘 수익/손실)
DAILY_TARGET_KRW = 200000      # 하루 20만 이상 수익이면 정지
DAILY_STOP_KRW   = -80000      # 하루 -8만 손실이면 정지

# 1회 거래 최대 허용 손실(추정, KRW)
MAX_LOSS_PER_TRADE = -20000    # 1회 거래에서 -2만 이상 손해 가능성이면 강제 스킵

# 변동성 기준 (%)
VOL_THRESHOLD_BORDER = 10.0    # BTC/USDT 일간 변동성 10%를 기준으로 맵핑

# 김프 예측 엔진 가중치
PREMIUM_PRED_WEIGHTS = {
    "upbit_speed":          0.25,
    "bithumb_speed":        0.25,
    "volatility":           0.20,
    "orderbook_imbalance":  0.30,
}

# 잔고 리밸런싱 주기/기준
REBALANCE_INTERVAL = 1800      # 30분마다 리밸런싱
REBALANCE_DRIFT = 0.2          # 목표 비율에서 ±20% 이상 벗어나면 리밸런싱
REBALANCE_STEP = 0.3           # 초과분의 30%만 보정(한 번에 과도하게 조정 방지)

# CSV 파일명
TRADE_CSV = "trades.csv"
SNAPSHOT_CSV = "portfolio_snapshots.csv"

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
OKX_PASSWORD    = env("OKX_PASSWORD")     # OKX Passphrase

TELEGRAM_TOKEN  = env("TELEGRAM_TOKEN")
CHAT_ID         = env("CHAT_ID")

###############################################################################
# GLOBAL STATE
###############################################################################

exchanges = {}
TRADE_LOG = []                       # {ts,strategy,symbol,venue,direction,profit_krw}
TRADE_TIMES = []                     # 최근 트레이드 timestamp
last_status_time = time.time()
last_daily_report_date = ""          # "YYYY-MM-DD"
last_weekly_report_date = ""         # "YYYY-MM-DD" (월요일 9시 기준)
disable_trading = False
cumulative_profit_krw = 0.0
last_rebalance_ts = 0.0

last_trade_times = {}                # (strategy,symbol,venue)->ts
price_history = {"upbit": [], "bithumb": []}

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
# EXCHANGE INIT
###############################################################################

def init_exchanges():
    """
    모든 거래소 초기화. OKX는 password(=passphrase)까지 설정.
    """
    global exchanges

    config = [
        ("binance", ccxt.binance,   BINANCE_API,   BINANCE_SECRET, None),
        ("upbit",   ccxt.upbit,     UPBIT_API,     UPBIT_SECRET,   None),
        ("bithumb", ccxt.bithumb,   BITHUMB_API,   BITHUMB_SECRET, None),
        ("bybit",   ccxt.bybit,     BYBIT_API,     BYBIT_SECRET,   None),
        ("okx",     ccxt.okx,       OKX_API,       OKX_SECRET,     OKX_PASSWORD),
    ]

    for name, cls, key, sec, pwd in config:
        try:
            params = {
                "apiKey": key,
                "secret": sec,
                "enableRateLimit": True,
            }
            if name == "okx":
                params["password"] = pwd
            inst = cls(params)
            inst.load_markets()
            exchanges[name] = inst
            print(f"[INIT] {name} 연결 성공")
        except Exception as e:
            print(f"[INIT] {name} 연결 오류: {e}")

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
            raise Exception("empty orderbook")
        return ob
    except Exception as e2:
        print(f"[OB] {e.id} {symbol} ERR {e2}")
        return None

###############################################################################
# FX / VOL
###############################################################################

def get_usdt_krw() -> float:
    for name in ["upbit", "bithumb"]:
        inst = exchanges.get(name)
        if not inst:
            continue
        try:
            t = safe_ticker(inst, "USDT/KRW")
            return float(t["bid"])
        except Exception as e:
            print(f"[FX] {name} USDT/KRW 실패: {e}")
    print("[FX] 환율 실패 → 1350 사용")
    return 1350.0

def get_daily_volatility() -> float:
    try:
        b = exchanges["binance"]
        ohlcv = b.fetch_ohlcv("BTC/USDT", "1d", limit=2)
        if len(ohlcv) < 2:
            return 0.0
        p0 = ohlcv[0][4]
        p1 = ohlcv[1][4]
        return abs((p1 - p0) / p0 * 100)
    except Exception as e:
        print(f"[VOL] ERR {e}")
        return 0.0

###############################################################################
# ORDER HELPERS
###############################################################################

def create_market_order(inst, symbol: str, side: str, amount: float, params=None):
    params = params or {}
    print(f"[ORDER] {inst.id} {side.upper()} {symbol} {amount} DRY_RUN={DRY_RUN}")
    if DRY_RUN:
        return {"info": "dry_run", "symbol": symbol, "side": side, "amount": amount}
    if side.lower() == "buy":
        return inst.create_market_buy_order(symbol, amount, params)
    else:
        return inst.create_market_sell_order(symbol, amount, params)

###############################################################################
# ORDERBOOK 기반 평균 체결가(VWAP)
###############################################################################

def calc_vwap_from_orderbook(ob, amount: float, is_buy: bool):
    """
    amount 만큼 실제로 체결한다고 가정하고,
    호가창 상단부터 채워서 평균 체결가를 계산.
    """
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
# REALIZABLE PREMIUM (실현 가능한 김프)
###############################################################################

def realizable_premium(ex_k, symbol: str, base_usdt: float, test_amount: float, usdt_krw: float, is_sell: bool):
    """
    ex_k: 업비트 혹은 빗썸 인스턴스
    symbol: "BTC" 또는 "ETH"
    base_usdt: 바이낸스 기준 USDT 가격
    test_amount: 테스트 체결할 양
    usdt_krw: 환율
    is_sell: True면 KRW 거래소에서 매도, False면 매수
    """
    pair = f"{symbol}/KRW"
    ob = safe_orderbook(ex_k, pair, depth=10)
    if not ob:
        return None
    vwap_krw = calc_vwap_from_orderbook(ob, test_amount, is_buy=not is_sell)
    if not vwap_krw:
        return None
    vwap_usdt = vwap_krw / usdt_krw
    premium = (vwap_usdt / base_usdt - 1.0) * 100.0
    return premium, vwap_krw

###############################################################################
# PREMIUM PREDICTION ENGINE
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
    bid_vol = sum([v for p, v in ob["bids"]])
    ask_vol = sum([v for p, v in ob["asks"]])
    total = bid_vol + ask_vol
    if total == 0:
        return 0.0
    return (bid_vol - ask_vol) / total

def predict_premium_prob(vol: float) -> float:
    """
    김프 발생 가능성 점수(0~1) 추정
    """
    up_speed = price_speed("upbit")
    bt_speed = price_speed("bithumb")

    ob = safe_orderbook(exchanges["upbit"], "BTC/KRW", depth=5)
    imbal = orderbook_imbalance(ob)

    score = (
        PREMIUM_PRED_WEIGHTS["upbit_speed"] * up_speed +
        PREMIUM_PRED_WEIGHTS["bithumb_speed"] * bt_speed +
        PREMIUM_PRED_WEIGHTS["volatility"] * (vol / 15.0) +
        PREMIUM_PRED_WEIGHTS["orderbook_imbalance"] * imbal
    )
    return max(0.0, min(1.0, score))

###############################################################################
# AUTO PARAMS (threshold + ratio)
###############################################################################

def trades_last_hour(trade_times) -> int:
    now = now_ts()
    return len([t for t in trade_times if now - t <= 3600])

def auto_params(vol: float, trade_times) -> (float, float):
    """
    변동성 + 최근 1시간 거래 수 + 김프 예측 점수를 가지고
    threshold와 ratio를 자동으로 조정
    """
    tc = trades_last_hour(trade_times)
    prob = predict_premium_prob(vol)

    v = min(max(vol, 0.0), VOL_THRESHOLD_BORDER)
    if VOL_THRESHOLD_BORDER > 0:
        thr = THRESHOLD_MIN + (THRESHOLD_MAX - THRESHOLD_MIN) * (v / VOL_THRESHOLD_BORDER)
    else:
        thr = THRESHOLD_MAX

    thr -= prob * 0.4  # 김프 발생 가능성 높으면 threshold 낮춤
    if tc > MAX_TRADES_1H * 0.7:
        thr += 0.3
    elif tc > MAX_TRADES_1H * 0.4:
        thr += 0.1
    thr = max(1.0, min(2.2, thr))

    # ratio: 변동성↑ → 작게, 예측 prob↑ → 크게, 거래과열↑ → 작게
    base_ratio = 0.45
    vol_factor = v / VOL_THRESHOLD_BORDER if VOL_THRESHOLD_BORDER > 0 else 1.0
    base_ratio -= vol_factor * 0.15
    base_ratio += prob * 0.15
    if tc > MAX_TRADES_1H * 0.7:
        base_ratio -= 0.1
    elif tc > MAX_TRADES_1H * 0.4:
        base_ratio -= 0.05

    ratio = max(BASE_RATIO_MIN, min(BASE_RATIO_MAX, base_ratio))
    return thr, ratio

###############################################################################
# PnL / LOG
###############################################################################

def log_trade(strategy, symbol, venue, direction, profit_krw):
    TRADE_LOG.append({
        "ts": now_ts(),
        "strategy": strategy,
        "symbol": symbol,
        "venue": venue,
        "direction": direction,
        "profit_krw": float(profit_krw),
    })

def today_date_str(ts=None):
    if ts is None:
        ts = now_ts()
    lt = time.localtime(ts)
    return f"{lt.tm_year:04d}-{lt.tm_mon:02d}-{lt.tm_mday:02d}"

def compute_today_profit():
    today = today_date_str()
    s = 0.0
    for t in TRADE_LOG:
        if today_date_str(t["ts"]) == today:
            s += t["profit_krw"]
    return s

def format_krw(x: float) -> str:
    sign = "+" if x >= 0 else "-"
    return f"{sign}{abs(x):,.0f}원"

def in_cooldown(strategy, symbol, venue, cooldown):
    key = (strategy, symbol, venue)
    last = last_trade_times.get(key, 0.0)
    return now_ts() - last < cooldown

def touch_trade_time(strategy, symbol, venue):
    key = (strategy, symbol, venue)
    last_trade_times[key] = now_ts()

###############################################################################
# CSV LOG: trades & snapshots
###############################################################################

def append_trade_csv(strategy, symbol, venue, direction, profit_krw):
    try:
        exists = os.path.exists(TRADE_CSV)
        with open(TRADE_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not exists:
                w.writerow(["ts", "strategy", "symbol", "venue", "direction", "profit_krw"])
            w.writerow([
                datetime.fromtimestamp(now_ts()).isoformat(),
                strategy,
                symbol,
                venue,
                direction,
                f"{profit_krw:.0f}",
            ])
    except Exception as e:
        print(f"[CSV TRADE ERR] {e}")

def snapshot_portfolio():
    """
    포트폴리오 스냅샷을 CSV로 남김.
    Bybit/OKX는 USDT 또는 USD 잔고만 KRW로 환산해서 합산.
    """
    try:
        usdt_rate = get_usdt_krw()
        total_krw = 0.0
        rows = []
        ts = now_ts()
        ts_str = datetime.fromtimestamp(ts).isoformat()

        # Binance
        b = exchanges.get("binance")
        if b:
            bal = b.fetch_balance()
            usdt = float(bal.get("USDT", {}).get("free", 0.0) or 0.0)
            btc  = float(bal.get("BTC", {}).get("free", 0.0) or 0.0)
            eth  = float(bal.get("ETH", {}).get("free", 0.0) or 0.0)
            t_btc = safe_ticker(b, "BTC/USDT")
            t_eth = safe_ticker(b, "ETH/USDT")
            v_usdt = usdt
            v_btc  = btc * float(t_btc["last"])
            v_eth  = eth * float(t_eth["last"])
            v_krw  = (v_usdt + v_btc + v_eth) * usdt_rate
            total_krw += v_krw
            rows.append(["binance", v_krw])

        # Upbit
        u = exchanges.get("upbit")
        if u:
            bal = u.fetch_balance()
            krw = float(bal.get("KRW", {}).get("free", 0.0) or 0.0)
            btc = float(bal.get("BTC", {}).get("free", 0.0) or 0.0)
            eth = float(bal.get("ETH", {}).get("free", 0.0) or 0.0)
            t_btc = safe_ticker(u, "BTC/KRW")
            t_eth = safe_ticker(u, "ETH/KRW")
            v_krw = krw + btc * float(t_btc["last"]) + eth * float(t_eth["last"])
            total_krw += v_krw
            rows.append(["upbit", v_krw])

        # Bithumb
        bh = exchanges.get("bithumb")
        if bh:
            bal = bh.fetch_balance()
            krw = float(bal.get("KRW", {}).get("free", 0.0) or 0.0)
            btc = float(bal.get("BTC", {}).get("free", 0.0) or 0.0)
            eth = float(bal.get("ETH", {}).get("free", 0.0) or 0.0)
            t_btc = safe_ticker(bh, "BTC/KRW")
            t_eth = safe_ticker(bh, "ETH/KRW")
            v_krw = krw + btc * float(t_btc["last"]) + eth * float(t_eth["last"])
            total_krw += v_krw
            rows.append(["bithumb", v_krw])

        # Bybit / OKX: USDT 또는 USD 잔고만
        for name in ["bybit", "okx"]:
            inst = exchanges.get(name)
            if not inst:
                continue
            bal = inst.fetch_balance()
            usdt_info = bal.get("USDT") or bal.get("USD") or {}
            usdt_free = float(usdt_info.get("free", 0.0) or 0.0)
            if usdt_free > 0:
                v_krw = usdt_free * usdt_rate
                total_krw += v_krw
                rows.append([name, v_krw])

        # CSV 기록
        try:
            exists = os.path.exists(SNAPSHOT_CSV)
            with open(SNAPSHOT_CSV, "a", newline="", encoding="utf-8") as f:
                w = csv.writer(f)
                if not exists:
                    w.writerow(["ts", "exchange", "value_krw", "total_krw"])
                for name, v in rows:
                    w.writerow([ts_str, name, f"{v:.0f}", f"{total_krw:.0f}"])
        except Exception as e:
            print(f"[CSV SNAPSHOT ERR] {e}")

    except Exception as e:
        print(f"[SNAPSHOT ERR] {e}")

###############################################################################
# ARBITRAGE
###############################################################################

def arbitrage(symbol: str, threshold: float, ratio: float, trade_times):
    global cumulative_profit_krw, disable_trading

    if disable_trading:
        print(f"[ARB] trading disabled, skip {symbol}")
        return

    try:
        binance = exchanges["binance"]
        usdt_krw = get_usdt_krw()

        base_pair = f"{symbol}/USDT"
        bt = safe_ticker(binance, base_pair)
        base_usdt = float(bt["bid"])

        bin_bal = binance.fetch_balance()
        bin_usdt = float(bin_bal.get("USDT", {}).get("free", 0.0) or 0.0)
        bin_sym  = float(bin_bal.get(symbol, {}).get("free", 0.0) or 0.0)

        for venue in ["upbit", "bithumb"]:
            e = exchanges.get(venue)
            if not e:
                continue

            try:
                tkr = safe_ticker(e, f"{symbol}/KRW")
                last_price = tkr["last"]
                record_price(venue, last_price)
            except Exception as e2:
                print(f"[ARB] {venue} ticker fail: {e2}")
                continue

            test_amount = 0.01 if symbol == "BTC" else 0.05
            sp_data = realizable_premium(e, symbol, base_usdt, test_amount, usdt_krw, True)
            bp_data = realizable_premium(e, symbol, base_usdt, test_amount, usdt_krw, False)
            sp = sp_data[0] if sp_data else None
            bp = bp_data[0] if bp_data else None

            print(f"[REAL {symbol} {venue}] sell={sp} buy={bp} thr={threshold:.2f} ratio={ratio:.2f}")

            try:
                bal_k = e.fetch_balance()
            except AuthenticationError as ae:
                print(f"[ARB] {venue} balance auth err: {ae}")
                continue
            except Exception as e3:
                print(f"[ARB] {venue} balance err: {e3}")
                continue

            ex_krw = float(bal_k.get("KRW", {}).get("free", 0.0) or 0.0)
            ex_sym = float(bal_k.get(symbol, {}).get("free", 0.0) or 0.0)

            # SELL (KRW 거래소 비쌀 때)
            if sp is not None and sp > threshold:
                if ex_sym <= 0 or bin_usdt <= 0:
                    continue
                if trades_last_hour(trade_times) >= MAX_TRADES_1H:
                    print("[ARB] 1h trade limit reached")
                    continue
                if in_cooldown("spot_arb", symbol, venue+"_sell", 300):
                    print(f"[ARB] cooldown {symbol} {venue} sell")
                    continue

                max_from_ex = ex_sym * ratio
                max_from_bu = (bin_usdt * ratio) / base_usdt
                amt = min(max_from_ex, max_from_bu)

                vwap_krw = sp_data[1]
                notional_krw = amt * vwap_krw
                if notional_krw < MIN_NOTIONAL_KRW:
                    continue

                est_profit = notional_krw * (sp / 100.0)
                if est_profit < MAX_LOSS_PER_TRADE:
                    print(f"[ARB] est_profit {est_profit} < per-trade loss limit, skip")
                    continue

                create_market_order(binance, base_pair, "buy", amt)
                create_market_order(e, f"{symbol}/KRW", "sell", amt)

                cumulative_profit_krw += est_profit
                log_trade("spot_arb", symbol, venue, "KRW_sell", est_profit)
                append_trade_csv("spot_arb", symbol, venue, "KRW_sell", est_profit)
                touch_trade_time("spot_arb", symbol, venue+"_sell")
                trade_times.append(now_ts())

                msg = (
                    f"[{symbol}] SELL ARB @ {venue}\n"
                    f"- prem={sp:.2f}%\n"
                    f"- amt={amt:.6f}\n"
                    f"- est_profit={format_krw(est_profit)}\n"
                    f"- cum={format_krw(cumulative_profit_krw)}\n"
                    f"- ratio={ratio:.2f} DRY_RUN={DRY_RUN}"
                )
                print(msg)
                send_telegram(msg)

            # BUY (KRW 거래소 싸게 살 때)
            if bp is not None and bp < -threshold:
                if ex_krw <= 0 or bin_sym <= 0:
                    continue
                if trades_last_hour(trade_times) >= MAX_TRADES_1H:
                    print("[ARB] 1h trade limit reached")
                    continue
                if in_cooldown("spot_arb", symbol, venue+"_buy", 300):
                    print(f"[ARB] cooldown {symbol} {venue} buy")
                    continue

                vwap_krw = bp_data[1]
                max_from_krw  = (ex_krw * ratio) / vwap_krw
                max_from_bsym = bin_sym * ratio
                amt = min(max_from_krw, max_from_bsym)

                notional_krw = amt * vwap_krw
                if notional_krw < MIN_NOTIONAL_KRW:
                    continue

                est_profit = notional_krw * (-bp / 100.0)
                if est_profit < MAX_LOSS_PER_TRADE:
                    print(f"[ARB] est_profit {est_profit} < per-trade loss limit, skip")
                    continue

                create_market_order(e, f"{symbol}/KRW", "buy", amt)
                create_market_order(binance, base_pair, "sell", amt)

                cumulative_profit_krw += est_profit
                log_trade("spot_arb", symbol, venue, "KRW_buy", est_profit)
                append_trade_csv("spot_arb", symbol, venue, "KRW_buy", est_profit)
                touch_trade_time("spot_arb", symbol, venue+"_buy")
                trade_times.append(now_ts())

                msg = (
                    f"[{symbol}] BUY ARB @ {venue}\n"
                    f"- prem={bp:.2f}%\n"
                    f"- amt={amt:.6f}\n"
                    f"- est_profit={format_krw(est_profit)}\n"
                    f"- cum={format_krw(cumulative_profit_krw)}\n"
                    f"- ratio={ratio:.2f} DRY_RUN={DRY_RUN}"
                )
                print(msg)
                send_telegram(msg)

        # 일간 손익 제한 체크
        today_pnl = compute_today_profit()
        if today_pnl >= DAILY_TARGET_KRW and not disable_trading:
            disable_trading = True
            send_telegram(f"📈 일간 목표 수익 도달: {format_krw(today_pnl)} → 오늘 매매 정지")
        if today_pnl <= DAILY_STOP_KRW and not disable_trading:
            disable_trading = True
            send_telegram(f"⚠️ 일간 손실 한도 초과: {format_krw(today_pnl)} → 오늘 매매 정지")

    except Exception as e:
        print(f"[ARB ERR] {symbol} {e}")
        send_telegram(f"[ARB ERR] {symbol}: {e}")

###############################################################################
# TRIANGULAR ARB (Bybit/OKX 모니터링)
###############################################################################

def triangular(name: str):
    try:
        inst = exchanges.get(name)
        if not inst:
            return
        t1 = safe_ticker(inst, "BTC/USDT")
        t2 = safe_ticker(inst, "ETH/USDT")
        t3 = safe_ticker(inst, "ETH/BTC")
        spread = (t2["bid"] / (t1["bid"] * t3["bid"]) - 1.0) * 100.0
        print(f"[TRI {name}] {spread:.3f}%")
        if spread > MIN_TRIANGULAR_SPREAD:
            send_telegram(f"[TRI {name}] 삼각 차익 기회: {spread:.3f}% (DRY_RUN={DRY_RUN})")
    except Exception as e:
        print(f"[TRI ERR] {name} {e}")

###############################################################################
# REPORTS: DAILY & WEEKLY
###############################################################################

def send_daily_report_if_needed():
    global last_daily_report_date
    lt = time.localtime()
    cur = today_date_str()
    if lt.tm_hour != 9:
        return
    if last_daily_report_date == cur:
        return

    now = now_ts()
    cutoff = now - 86400
    recent = [t for t in TRADE_LOG if t["ts"] >= cutoff]

    if not recent:
        msg = f"[DAILY] {cur}\n- 최근 24h 거래 없음\n- DRY_RUN={DRY_RUN}"
        print(msg)
        send_telegram(msg)
        last_daily_report_date = cur
        return

    total = sum(t["profit_krw"] for t in recent)
    summary = {}
    for t in recent:
        k = (t["strategy"], t["symbol"])
        summary.setdefault(k, {"p": 0.0, "c": 0})
        summary[k]["p"] += t["profit_krw"]
        summary[k]["c"] += 1

    lines = [f"[DAILY] {cur}", f"- 총 수익: {format_krw(total)}", ""]
    for (st, sym), v in summary.items():
        lines.append(f"· {sym} {st}: {format_krw(v['p'])} (거래 {v['c']}회)")
    lines.append(f"\n- DRY_RUN={DRY_RUN}")
    msg = "\n".join(lines)
    print(msg)
    send_telegram(msg)
    last_daily_report_date = cur

def send_weekly_report_if_needed():
    global last_weekly_report_date
    lt = time.localtime()
    cur = today_date_str()
    # Monday(0) + 9시
    if lt.tm_wday != 0 or lt.tm_hour != 9:
        return
    if last_weekly_report_date == cur:
        return

    now = now_ts()
    cutoff = now - 7 * 86400
    recent = [t for t in TRADE_LOG if t["ts"] >= cutoff]
    if not recent:
        msg = (
            f"[WEEKLY] {cur}\n"
            f"- 최근 7일 거래 없음\n"
            f"- DRY_RUN={DRY_RUN}"
        )
        print(msg)
        send_telegram(msg)
        last_weekly_report_date = cur
        return

    total = sum(t["profit_krw"] for t in recent)
    summary = {}
    daily_map = {}
    for t in recent:
        day = today_date_str(t["ts"])
        daily_map.setdefault(day, 0.0)
        daily_map[day] += t["profit_krw"]

        k = (t["strategy"], t["symbol"])
        summary.setdefault(k, {"p": 0.0, "c": 0})
        summary[k]["p"] += t["profit_krw"]
        summary[k]["c"] += 1

    best_day = max(daily_map.items(), key=lambda x: x[1])
    worst_day = min(daily_map.items(), key=lambda x: x[1])

    lines = []
    lines.append(f"[WEEKLY] {cur} 기준 최근 7일 요약")
    lines.append(f"- 7일 총 수익: {format_krw(total)}")
    lines.append(f"- 일평균 수익: {format_krw(total / max(1, len(daily_map)))}")
    lines.append("")
    lines.append(f"- 최고 수익일: {best_day[0]} ({format_krw(best_day[1])})")
    lines.append(f"- 최저 수익일: {worst_day[0]} ({format_krw(worst_day[1])})")
    lines.append("")
    lines.append("[전략별 수익]")
    for (st, sym), v in summary.items():
        lines.append(f"· {sym} {st}: {format_krw(v['p'])} (거래 {v['c']}회)")
    lines.append("")
    lines.append(f"- DRY_RUN={DRY_RUN}")

    msg = "\n".join(lines)
    print(msg)
    send_telegram(msg)
    last_weekly_report_date = cur

###############################################################################
# REBALANCING
###############################################################################

def rebalance_binance():
    try:
        b = exchanges.get("binance")
        if not b:
            return
        bal = b.fetch_balance()
        usdt = float(bal.get("USDT", {}).get("free", 0.0) or 0.0)
        btc  = float(bal.get("BTC", {}).get("free", 0.0) or 0.0)
        eth  = float(bal.get("ETH", {}).get("free", 0.0) or 0.0)
        if usdt + btc + eth == 0:
            return

        t_btc = safe_ticker(b, "BTC/USDT")
        t_eth = safe_ticker(b, "ETH/USDT")

        v_usdt = usdt
        v_btc  = btc * float(t_btc["last"])
        v_eth  = eth * float(t_eth["last"])
        total  = v_usdt + v_btc + v_eth

        cur = {
            "BTC": v_btc / total,
            "ETH": v_eth / total,
            "USDT": v_usdt / total,
        }
        print(f"[REBAL BIN] cur={cur}")

        target = {"BTC": 0.33, "ETH": 0.33, "USDT": 0.34}
        for k in ["BTC", "ETH", "USDT"]:
            diff = cur[k] - target[k]
            if abs(diff) > REBALANCE_DRIFT:
                adj_value = -diff * total * REBALANCE_STEP
                if k == "BTC":
                    price = float(t_btc["last"])
                    amount = abs(adj_value) / price
                    side = "sell" if diff > 0 else "buy"
                    print(f"[REBAL BIN] {side.upper()} BTC {amount}")
                    create_market_order(b, "BTC/USDT", side, amount)
                elif k == "ETH":
                    price = float(t_eth["last"])
                    amount = abs(adj_value) / price
                    side = "sell" if diff > 0 else "buy"
                    print(f"[REBAL BIN] {side.upper()} ETH {amount}")
                    create_market_order(b, "ETH/USDT", side, amount)
    except Exception as e:
        print(f"[REBAL BIN ERR] {e}")

def rebalance_krw_exchange(name: str):
    try:
        inst = exchanges.get(name)
        if not inst:
            return
        bal = inst.fetch_balance()
        krw = float(bal.get("KRW", {}).get("free", 0.0) or 0.0)
        btc = float(bal.get("BTC", {}).get("free", 0.0) or 0.0)
        eth = float(bal.get("ETH", {}).get("free", 0.0) or 0.0)
        if krw + btc + eth == 0:
            return

        t_btc = safe_ticker(inst, "BTC/KRW")
        t_eth = safe_ticker(inst, "ETH/KRW")

        v_krw = krw
        v_btc = btc * float(t_btc["last"])
        v_eth = eth * float(t_eth["last"])
        total = v_krw + v_btc + v_eth

        cur = {
            "KRW": v_krw / total,
            "BTC": v_btc / total,
            "ETH": v_eth / total,
        }
        print(f"[REBAL {name}] cur={cur}")

        target = {"KRW": 0.4, "BTC": 0.3, "ETH": 0.3}
        for k in ["KRW", "BTC", "ETH"]:
            diff = cur[k] - target[k]
            if abs(diff) > REBALANCE_DRIFT:
                adj_value = -diff * total * REBALANCE_STEP
                if k == "KRW":
                    if adj_value > 0:
                        # KRW 늘려야 → BTC 매도
                        price = float(t_btc["last"])
                        amt = adj_value / price
                        print(f"[REBAL {name}] SELL BTC {amt}")
                        create_market_order(inst, "BTC/KRW", "sell", amt)
                elif k == "BTC":
                    price = float(t_btc["last"])
                    amt = abs(adj_value) / price
                    side = "sell" if diff > 0 else "buy"
                    print(f"[REBAL {name}] {side.upper()} BTC {amt}")
                    create_market_order(inst, "BTC/KRW", side, amt)
                elif k == "ETH":
                    price = float(t_eth["last"])
                    amt = abs(adj_value) / price
                    side = "sell" if diff > 0 else "buy"
                    print(f"[REBAL {name}] {side.upper()} ETH {amt}")
                    create_market_order(inst, "ETH/KRW", side, amt)
    except Exception as e:
        print(f"[REBAL {name} ERR] {e}")

def rebalance_all():
    print("[REBAL] 시작")
    rebalance_binance()
    rebalance_krw_exchange("upbit")
    rebalance_krw_exchange("bithumb")
    print("[REBAL] 종료")

###############################################################################
# MAIN LOOP
###############################################################################

def main():
    global last_status_time, last_rebalance_ts, disable_trading

    init_exchanges()
    send_telegram(f"김프봇 시작 (DRY_RUN={DRY_RUN}) – Realizable+Predict+Risk+Rebalance+CSV+Weekly")

    trade_times = []

    while True:
        loop_start = now_ts()
        try:
            vol = get_daily_volatility()
            threshold, ratio = auto_params(vol, trade_times)
            print(f"\n[LOOP] vol={vol:.2f}% thr={threshold:.2f}% ratio={ratio:.2f} trades_1h={trades_last_hour(trade_times)}")

            if not disable_trading:
                arbitrage("BTC", threshold, ratio, trade_times)
                arbitrage("ETH", threshold, ratio, trade_times)
                for n in ["bybit", "okx"]:
                    triangular(n)
            else:
                print("[LOOP] trading disabled – 매매 중단 상태")

            # 리밸런싱 (30분마다)
            now_ = now_ts()
            if now_ - last_rebalance_ts >= REBALANCE_INTERVAL and not disable_trading:
                rebalance_all()
                last_rebalance_ts = now_

            # 포트폴리오 스냅샷
            snapshot_portfolio()

            # 리포트
            send_daily_report_if_needed()
            send_weekly_report_if_needed()

            # 상태 리포트
            if now_ - last_status_time >= STATUS_INTERVAL:
                today_pnl = compute_today_profit()
                msg = (
                    f"[STATUS]\n"
                    f"- 오늘 손익: {format_krw(today_pnl)}\n"
                    f"- 누적 추정 이익: {format_krw(cumulative_profit_krw)}\n"
                    f"- 최근 1h 거래횟수: {trades_last_hour(trade_times)}회\n"
                    f"- DRY_RUN={DRY_RUN}"
                )
                print(msg)
                send_telegram(msg)
                last_status_time = now_

        except Exception as e:
            print(f"[MAIN ERR] {e}")
            send_telegram(f"[MAIN ERR] {e}")

        elapsed = now_ts() - loop_start
        sleep_time = max(5, MAIN_LOOP_INTERVAL - elapsed)
        print(f"[LOOP] sleep {sleep_time:.1f}s")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
