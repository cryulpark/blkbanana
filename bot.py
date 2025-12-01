import os, time, traceback, requests
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
MIN_NOTIONAL_KRW = 100000      # 10만 원 미만 거래는 무의미하므로 제외

# 삼각차익 조건
MIN_TRIANGULAR_SPREAD = 0.5    # 0.5% 이상일 때만 관심

# 1시간 거래 횟수 제한 (과열 방지)
MAX_TRADES_1H = 30

# 일간 PnL 제한 (오늘 수익/손실)
DAILY_TARGET_KRW = 200000      # 하루 20만 + 이상이면 그날 정지
DAILY_STOP_KRW   = -80000      # 하루 -8만 손실이면 그날 정지

# 1회 거래 최대 허용 손실(추정, KRW)
MAX_LOSS_PER_TRADE = -20000    # 1회 거래에서 -2만 이상 나오는 구조는 강제 스킵

# 변동성 기준 (%)
VOL_THRESHOLD_BORDER = 10.0    # BTC/USDT 일간 변동성 10% 기준

# 김프 예측 엔진 가중치
PREMIUM_PRED_WEIGHTS = {
    "upbit_speed":          0.25,
    "bithumb_speed":        0.25,
    "volatility":           0.20,
    "orderbook_imbalance":  0.30,
}

###############################################################################
# ENV
###############################################################################

def env(k: str) -> str:
    if k not in os.environ:
        raise Exception(f"[ENV] Missing: {k}")
    return os.environ[k]

BINANCE_API   = env("BINANCE_API_KEY")
BINANCE_SECRET= env("BINANCE_SECRET")

UPBIT_API     = env("UPBIT_API_KEY")
UPBIT_SECRET  = env("UPBIT_SECRET")

BITHUMB_API   = env("BITHUMB_API_KEY")
BITHUMB_SECRET= env("BITHUMB_SECRET")

BYBIT_API     = env("BYBIT_API_KEY")
BYBIT_SECRET  = env("BYBIT_SECRET")

OKX_API       = env("OKX_API_KEY")
OKX_SECRET    = env("OKX_SECRET")

TELEGRAM_TOKEN= env("TELEGRAM_TOKEN")
CHAT_ID       = env("CHAT_ID")

###############################################################################
# GLOBAL STATE
###############################################################################

exchanges = {}                       # 각 거래소 ccxt 인스턴스
TRADE_LOG = []                       # {ts, strategy, symbol, venue, direction, profit_krw}
TRADE_TIMES = []                     # 최근 트레이드 시간 리스트
last_status_time = time.time()
last_daily_report_date = ""          # "YYYY-MM-DD"
disable_trading = False
cumulative_profit_krw = 0.0

# 쿨다운용
last_trade_times = {}                # (strategy, symbol, venue) -> ts

# 가격 기록(김프 예측용)
price_history = {
    "upbit":   [],
    "bithumb": [],
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
# EXCHANGE INIT
###############################################################################

def init_exchanges():
    global exchanges
    config = [
        ("binance", ccxt.binance,   BINANCE_API,   BINANCE_SECRET),
        ("upbit",   ccxt.upbit,     UPBIT_API,     UPBIT_SECRET),
        ("bithumb", ccxt.bithumb,   BITHUMB_API,   BITHUMB_SECRET),
        ("bybit",   ccxt.bybit,     BYBIT_API,     BYBIT_SECRET),
        ("okx",     ccxt.okx,       OKX_API,       OKX_SECRET),
    ]
    for name, cls, key, sec in config:
        try:
            inst = cls({"apiKey": key, "secret": sec, "enableRateLimit": True})
            inst.load_markets()
            exchanges[name] = inst
            print(f"[INIT] {name} 연결 성공")
        except Exception as e:
            print(f"[INIT] {name} 연결 오류: {e}")

###############################################################################
# TIME / TICKER / ORDERBOOK HELPERS
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
# PRICE / FX
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
# ORDER EXEC
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
# ORDERBOOK 기반 평균 체결가 계산
###############################################################################

def calc_vwap_from_orderbook(ob, amount: float, is_buy: bool):
    """
    amount 만큼 실제로 체결한다고 가정했을 때,
    호가창 상단부터 채워서 평균 체결가격(VWAP)을 계산.
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
        # 깊이가 부족
        return None
    return cost / amount

###############################################################################
# 실현 가능한 김프 계산 (Realizable Premium)
###############################################################################

def realizable_premium(ex_k, symbol: str, base_usdt: float, test_amount: float, usdt_krw: float, is_sell: bool):
    """
    ex_k : 업비트 또는 빗썸
    symbol: "BTC" or "ETH"
    base_usdt: 바이낸스 기준 USDT 가격
    test_amount: 테스트할 수량 (예: 0.01 BTC)
    usdt_krw: USDT/KRW 환율
    is_sell: True면 KRW 거래소에서 매도, False면 매수.
    """
    pair = f"{symbol}/KRW"
    ob = safe_orderbook(ex_k, pair, depth=10)
    if not ob:
        return None

    # 매수: asks / 매도: bids 기준
    vwap_krw = calc_vwap_from_orderbook(ob, test_amount, is_buy=not is_sell)
    if not vwap_krw:
        return None

    vwap_usdt = vwap_krw / usdt_krw
    premium = (vwap_usdt / base_usdt - 1.0) * 100.0
    return premium, vwap_krw

###############################################################################
# 김프 예측 엔진 (간단 버전)
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
    # 단순 기울기
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
    score = max(0.0, min(1.0, score))
    return score

###############################################################################
# AUTO PARAMS (threshold + ratio 자동 조정)
###############################################################################

def trades_last_hour(trade_times) -> int:
    now = now_ts()
    recent = [t for t in trade_times if now - t <= 3600]
    return len(recent)

def auto_params(vol: float, trade_times) -> (float, float):
    """
    변동성 + 최근 1시간 거래 횟수 + 김프 예측 점수를 기반으로
    threshold와 ratio를 자동으로 설정.
    """
    tc = trades_last_hour(trade_times)
    prob = predict_premium_prob(vol)

    # threshold: 변동성↑ → threshold↑, 예측확률↑ → threshold↓
    v = min(max(vol, 0.0), VOL_THRESHOLD_BORDER)
    if VOL_THRESHOLD_BORDER > 0:
        thr = THRESHOLD_MIN + (THRESHOLD_MAX - THRESHOLD_MIN) * (v / VOL_THRESHOLD_BORDER)
    else:
        thr = THRESHOLD_MAX

    thr -= prob * 0.4  # 김프 올 확률 높으면 threshold 조금 낮춤
    if tc > MAX_TRADES_1H * 0.7:
        thr += 0.3
    elif tc > MAX_TRADES_1H * 0.4:
        thr += 0.1

    thr = max(1.0, min(2.2, thr))

    # ratio: 변동성↑ → ratio↓, 예측확률↑ → ratio↑, 거래횟수↑ → ratio↓
    base_ratio = 0.45
    if VOL_THRESHOLD_BORDER > 0:
        vol_factor = v / VOL_THRESHOLD_BORDER
    else:
        vol_factor = 1.0
    base_ratio -= vol_factor * 0.15
    base_ratio += prob * 0.15
    if tc > MAX_TRADES_1H * 0.7:
        base_ratio -= 0.1
    elif tc > MAX_TRADES_1H * 0.4:
        base_ratio -= 0.05

    ratio = max(BASE_RATIO_MIN, min(BASE_RATIO_MAX, base_ratio))
    return thr, ratio

###############################################################################
# PnL / LOG / RISK
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
    if ts is None: ts = now_ts()
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
# MAIN ARBITRAGE LOGIC
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
        bin_usdt = float(bin_bal["USDT"]["free"])
        bin_sym  = float(bin_bal[symbol]["free"])

        for venue in ["upbit", "bithumb"]:
            e = exchanges.get(venue)
            if not e: continue

            try:
                tkr = safe_ticker(e, f"{symbol}/KRW")
                last_price = tkr["last"]
                price_history[venue].append(last_price)
                if len(price_history[venue]) > 50: price_history[venue].pop(0)
            except Exception as e2:
                print(f"[ARB] {venue} ticker fail: {e2}")
                continue

            # 실현 가능한 김프 계산 (테스트 수량)
            test_amount = 0.01 if symbol=="BTC" else 0.05
            sell_prem = realizable_premium(e, symbol, base_usdt, test_amount, usdt_krw, is_sell=True)
            buy_prem  = realizable_premium(e, symbol, base_usdt, test_amount, usdt_krw, is_sell=False)

            sp = sell_prem[0] if sell_prem else None
            bp = buy_prem[0]  if buy_prem else None
            print(f"[REAL {symbol} {venue}] sell={sp} buy={bp} thr={threshold:.2f} ratio={ratio:.2f}")

            # SELL 김프 (KRW 비쌈)
            if sp is not None and sp > threshold:
                try:
                    bal = e.fetch_balance()
                except AuthenticationError as ae:
                    print(f"[ARB] {venue} balance auth err: {ae}")
                    continue
                except Exception as e3:
                    print(f"[ARB] {venue} balance err: {e3}")
                    continue

                ex_sym = float(bal[symbol]["free"])
                if ex_sym <= 0 or bin_usdt <= 0:
                    continue

                max_e = ex_sym * ratio
                max_b = (bin_usdt * ratio) / base_usdt
                amt   = min(max_e, max_b)

                # 실제 vwap 기준 notional
                vwap_krw = sell_prem[1]
                notional_krw = amt * vwap_krw
                if notional_krw < MIN_NOTIONAL_KRW:
                    continue

                if trades_last_hour(trade_times) >= MAX_TRADES_1H:
                    print("[ARB] 1h trade limit reached")
                    continue

                if in_cooldown("spot_arb", symbol, venue+"_sell", ARBITRAGE_COOLDOWN):
                    print(f"[ARB] cooldown {symbol} {venue} sell")
                    continue

                # 추정 수익 계산
                est_profit = notional_krw * (sp/100.0)  # 대략적인 net 기준
                if est_profit < MAX_LOSS_PER_TRADE:
                    print(f"[ARB] est_profit {est_profit} < per-trade loss limit, skip")
                    continue

                # 실행
                create_market_order(binance, base_pair, "buy", amt)
                create_market_order(e, f"{symbol}/KRW", "sell", amt)

                cumulative_profit_krw += est_profit
                log_trade("spot_arb", symbol, venue, "KRW_sell", est_profit)
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
                print(msg); send_telegram(msg)

            # BUY 역프 (KRW 싸게)
            if bp is not None and bp < -threshold:
                try:
                    bal = e.fetch_balance()
                except AuthenticationError as ae:
                    print(f"[ARB] {venue} balance auth err: {ae}")
                    continue
                except Exception as e3:
                    print(f"[ARB] {venue} balance err: {e3}")
                    continue

                ex_krw = float(bal["KRW"]["free"])
                if ex_krw <= 0 or bin_sym <= 0:
                    continue

                vwap_krw = buy_prem[1]
                max_from_krw = (ex_krw * ratio) / vwap_krw
                max_from_bin = bin_sym * ratio
                amt = min(max_from_krw, max_from_bin)

                notional_krw = amt * vwap_krw
                if notional_krw < MIN_NOTIONAL_KRW:
                    continue

                if trades_last_hour(trade_times) >= MAX_TRADES_1H:
                    print("[ARB] 1h trade limit reached")
                    continue

                if in_cooldown("spot_arb", symbol, venue+"_buy", ARBITRAGE_COOLDOWN):
                    print(f"[ARB] cooldown {symbol} {venue} buy")
                    continue

                est_profit = notional_krw * (-bp/100.0)
                if est_profit < MAX_LOSS_PER_TRADE:
                    print(f"[ARB] est_profit {est_profit} < per-trade loss limit, skip")
                    continue

                create_market_order(e, f"{symbol}/KRW", "buy", amt)
                create_market_order(binance, base_pair, "sell", amt)

                cumulative_profit_krw += est_profit
                log_trade("spot_arb", symbol, venue, "KRW_buy", est_profit)
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
                print(msg); send_telegram(msg)

        # 일간 손익 체크
        today_pnl = compute_today_profit()
        if today_pnl >= DAILY_TARGET_KRW:
            disable_trading = True
            send_telegram(f"📈 일간 목표 수익 도달: {format_krw(today_pnl)} → 오늘 매매 정지")
        if today_pnl <= DAILY_STOP_KRW:
            disable_trading = True
            send_telegram(f"⚠️ 일간 손실 한도 초과: {format_krw(today_pnl)} → 오늘 매매 정지")

    except Exception as e:
        print(f"[ARB ERR] {symbol} {e}")
        send_telegram(f"[ARB ERR] {symbol}: {e}")

###############################################################################
# TRIANGULAR (Bybit / OKX 모니터링 전용)
###############################################################################

def triangular(name: str):
    try:
        inst = exchanges.get(name)
        if not inst: return
        t1 = safe_ticker(inst,"BTC/USDT")
        t2 = safe_ticker(inst,"ETH/USDT")
        t3 = safe_ticker(inst,"ETH/BTC")
        spread = ( t2["bid"]/ (t1["bid"]*t3["bid"]) -1 )*100
        print(f"[TRI {name}] {spread:.3f}%")
        if spread > MIN_TRIANGULAR_SPREAD:
            send_telegram(f"[TRI {name}] 삼각차익 기회 감지: {spread:.3f}% (DRY_RUN={DRY_RUN})")
    except Exception as e:
        print(f"[TRI ERR] {name} {e}")

###############################################################################
# DAILY REPORT
###############################################################################

def send_daily_report_if_needed():
    global last_daily_report_date
    lt = time.localtime()
    cur_date = f"{lt.tm_year:04d}-{lt.tm_mon:02d}-{lt.tm_mday:02d}"
    if lt.tm_hour != 9: return
    if last_daily_report_date == cur_date: return

    now = now_ts()
    cutoff = now - 86400
    recent = [t for t in TRADE_LOG if t["ts"] >= cutoff]
    if not recent:
        msg = (
            f"[DAILY REPORT] {cur_date}\n"
            f"- 최근 24시간 재정거래 없음\n"
            f"- DRY_RUN={DRY_RUN}"
        )
        print(msg); send_telegram(msg)
        last_daily_report_date = cur_date
        return

    total = 0.0
    summary = {}
    for t in recent:
        k = (t["strategy"], t["symbol"])
        total += t["profit_krw"]
        if k not in summary: summary[k] = {"p":0.0,"c":0}
        summary[k]["p"] += t["profit_krw"]
        summary[k]["c"] += 1

    lines = [f"[DAILY REPORT] {cur_date}", f"- 총 수익: {format_krw(total)}", ""]
    for (st, sym), v in summary.items():
        label = f"{sym} {st}"
        lines.append(f"· {label}: {format_krw(v['p'])} (거래 {v['c']}회)")
    lines.append("")
    lines.append(f"- DRY_RUN={DRY_RUN}")

    msg = "\n".join(lines)
    print(msg); send_telegram(msg)
    last_daily_report_date = cur_date

###############################################################################
# MAIN LOOP
###############################################################################

def main():
    global last_status_time

    init_exchanges()
    send_telegram(f"김프봇 시작 (DRY_RUN={DRY_RUN}) – Realizable+Prediction+Risk 모드")

    trade_times = []

    while True:
        loop_start = now_ts()
        try:
            vol = get_daily_volatility()
            thr, ratio = auto_params(vol, trade_times)

            print(f"\n[LOOP] vol={vol:.2f}% thr={thr:.2f}% ratio={ratio:.2f} trades_1h={trades_last_hour(trade_times)}")

            if not disable_trading:
                arbitrage("BTC", thr, ratio, trade_times)
                arbitrage("ETH", thr, ratio, trade_times)

                for n in ["bybit","okx"]:
                    triangular(n)
            else:
                print("[LOOP] trading disabled – 매매 중단 상태")

            send_daily_report_if_needed()

            now = now_ts()
            if now - last_status_time >= STATUS_INTERVAL:
                today_pnl = compute_today_profit()
                msg = (
                    f"[STATUS]\n"
                    f"- 오늘 손익: {format_krw(today_pnl)}\n"
                    f"- 누적 추정 이익: {format_krw(cumulative_profit_krw)}\n"
                    f"- 최근 1h 거래횟수: {trades_last_hour(trade_times)}회\n"
                    f"- DRY_RUN={DRY_RUN}"
                )
                print(msg); send_telegram(msg)
                last_status_time = now

        except Exception as e:
            print(f"[MAIN ERR] {e}")
            send_telegram(f"[MAIN ERR] {e}")

        elapsed = now_ts() - loop_start
        sleep_time = max(5, MAIN_LOOP_INTERVAL - elapsed)
        print(f"[LOOP] sleep {sleep_time:.1f}s")
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
