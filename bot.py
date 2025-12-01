import os
import time
import requests
from datetime import datetime
import ccxt
from ccxt.base.errors import AuthenticationError

###############################################################################
# SETTINGS
###############################################################################

DRY_RUN = True                 # 실매매 전에는 반드시 True 유지
MAIN_LOOP_INTERVAL = 60        # 1분 루프

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
KRW_ARB_THR = 0.25             # 0.25% 이상이면 차익거래 시도
KRW_ARB_RATIO = 0.2            # 업빗/빗썸 사이 KRW 차익거래는 계좌의 20% 정도만

# ─ Funding 레이어(모니터링용) ─
FUNDING_SPREAD_THR = 0.02      # 0.02(=2%) 이상 차이날 때 시그널

# 변동성 기준 (%)
VOL_THRESHOLD_BORDER = 10.0    # BTC/USDT 일간 변동성 10% 기준

# 김프 예측 엔진 가중치 (단순화)
PREMIUM_PRED_WEIGHTS = {
    "upbit_speed":          0.3,
    "bithumb_speed":        0.3,
    "volatility":           0.2,
    "orderbook_imbalance":  0.2,
}

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

ex = {}
TRADE_TIMES = []                     # 최근 1시간 트레이드 타임스탬프
price_history = {"upbit": [], "bithumb": []}

disable_trading = False

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
    global ex
    config = [
        ("binance", ccxt.binance,   BINANCE_API,   BINANCE_SECRET, None),
        ("upbit",   ccxt.upbit,     UPBIT_API,     UPBIT_SECRET,   None),
        ("bithumb", ccxt.bithumb,   BITHUMB_API,   BITHUMB_SECRET, None),
        ("bybit",   ccxt.bybit,     BYBIT_API,     BYBIT_SECRET,   None),
        ("okx",     ccxt.okx,       OKX_API,       OKX_SECRET,     OKX_PASSWORD),
    ]
    for name, cls, key, sec, pwd in config:
        try:
            params = {"apiKey": key, "secret": sec, "enableRateLimit": True}
            if name == "okx":
                params["password"] = pwd
            inst = cls(params)
            inst.load_markets()
            ex[name] = inst
            print(f"[INIT] {name} 연결 성공")
        except Exception as e:
            print(f"[INIT] {name} ERR {e}")

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
        if not inst: continue
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
    if tot == 0: return 0.0
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
    if not ob: return None
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
        thr = TIER1_THR_MIN + (TIER1_THR_MAX - TIER1_THR_MIN)*(v / VOL_THRESHOLD_BORDER)
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
    base_ratio -= vol_factor*0.1
    base_ratio += prob*0.1
    base_ratio = max(BASE_RATIO_MIN, min(BASE_RATIO_MAX, base_ratio))
    return thr, base_ratio

###############################################################################
# CORE MARKET HELPERS
###############################################################################

def create_order(inst, symbol, side, amount):
    print(f"[ORDER] {inst.id} {side.upper()} {symbol} {amount} DRY_RUN={DRY_RUN}")
    if DRY_RUN:
        return
    if side.lower()=="buy":
        inst.create_market_buy_order(symbol, amount)
    else:
        inst.create_market_sell_order(symbol, amount)

###############################################################################
# LAYER 1: 김프/역프 재정거래 (TIER1 + TIER2)
###############################################################################

def run_spread_arbitrage(symbol: str, tier1_thr: float, base_ratio: float, trade_times):
    global disable_trading
    if disable_trading:
        print(f"[ARB] trading disabled, skip {symbol}")
        return

    try:
        b = ex["binance"]
        usdt_krw = get_usdt_krw()
        base_pair = f"{symbol}/USDT"
        t_base = safe_ticker(b, base_pair)
        base_usdt = float(t_base["bid"])

        bal_b = b.fetch_balance()
        free_usdt = float(bal_b.get("USDT",{}).get("free",0) or 0)
        free_sym  = float(bal_b.get(symbol,{}).get("free",0) or 0)

        for venue in ["upbit","bithumb"]:
            e = ex[venue]
            try:
                t_krw = safe_ticker(e,f"{symbol}/KRW")
                last_price = t_krw["last"]
                record_price(venue,last_price)
            except Exception as e2:
                print(f"[ARB] {venue} ticker ERR {e2}")
                continue

            # 테스트용 수량
            test_amount = 0.01 if symbol=="BTC" else 0.05

            # SELL 김프 (KRW 거래소가 비쌀 때)
            ob = safe_orderbook(e,f"{symbol}/KRW",depth=10)
            vwap_sell_krw = calc_vwap(ob,test_amount,is_buy=False)
            vwap_buy_krw  = calc_vwap(ob,test_amount,is_buy=True)

            if vwap_sell_krw:
                sell_usdt = vwap_sell_krw / usdt_krw
                sell_prem = (sell_usdt/base_usdt -1)*100
            else:
                sell_prem = None

            if vwap_buy_krw:
                buy_usdt = vwap_buy_krw / usdt_krw
                buy_prem = (buy_usdt/base_usdt -1)*100
            else:
                buy_prem = None

            print(f"[REAL {symbol} {venue}] sell={sell_prem} buy={buy_prem} thr={tier1_thr:.2f} base_ratio={base_ratio:.2f}")

            # 거래소 잔고
            try:
                bal_k = e.fetch_balance()
            except AuthenticationError as ae:
                print(f"[ARB] {venue} balance auth ERR {ae}")
                continue
            except Exception as e3:
                print(f"[ARB] {venue} balance ERR {e3}")
                continue

            ex_krw = float(bal_k.get("KRW",{}).get("free",0) or 0)
            ex_sym = float(bal_k.get(symbol,{}).get("free",0) or 0)

            def can_trade():
                now = now_ts()
                recent = [t for t in trade_times if now-t <= 3600]
                return len(recent) < MAX_TRADES_1H

            # ─ SELL SIDE ─
            if sell_prem is not None and can_trade():
                # Tier1
                trade_tier = None
                trade_ratio = 0.0
                if sell_prem >= tier1_thr:
                    trade_tier = "TIER1"
                    trade_ratio = base_ratio
                elif sell_prem >= TIER2_THR:
                    trade_tier = "TIER2"
                    trade_ratio = base_ratio*TIER2_RATIO_FACTOR

                if trade_tier and ex_sym>0 and free_usdt>0:
                    max_from_k = ex_sym * trade_ratio
                    max_from_b = (free_usdt*trade_ratio)/base_usdt
                    amt = min(max_from_k,max_from_b)
                    notional_krw = amt * (vwap_sell_krw or t_krw["bid"])
                    if notional_krw >= MIN_NOTIONAL_KRW:
                        print(f"[ARB {symbol}] {venue} SELL {trade_tier} amt={amt} notional={int(notional_krw)}")
                        create_order(b,base_pair,"buy",amt)
                        create_order(e,f"{symbol}/KRW","sell",amt)
                        trade_times.append(now_ts())
                        send_telegram(f"[{symbol}] {venue} SELL {trade_tier} prem={sell_prem:.2f}% amt={amt:.6f} DRY_RUN={DRY_RUN}")

            # ─ BUY SIDE (역프) ─
            if buy_prem is not None and can_trade():
                trade_tier = None
                trade_ratio = 0.0
                if buy_prem <= -tier1_thr:
                    trade_tier = "TIER1"
                    trade_ratio = base_ratio
                elif buy_prem <= -TIER2_THR:
                    trade_tier = "TIER2"
                    trade_ratio = base_ratio*TIER2_RATIO_FACTOR

                if trade_tier and ex_krw>0 and free_sym>0:
                    max_from_krw = (ex_krw*trade_ratio)/(vwap_buy_krw or t_krw["ask"])
                    max_from_sym = free_sym*trade_ratio
                    amt = min(max_from_krw,max_from_sym)
                    notional_krw = amt*(vwap_buy_krw or t_krw["ask"])
                    if notional_krw >= MIN_NOTIONAL_KRW:
                        print(f"[ARB {symbol}] {venue} BUY {trade_tier} amt={amt} notional={int(notional_krw)}")
                        create_order(e,f"{symbol}/KRW","buy",amt)
                        create_order(b,base_pair,"sell",amt)
                        trade_times.append(now_ts())
                        send_telegram(f"[{symbol}] {venue} BUY {trade_tier} prem={buy_prem:.2f}% amt={amt:.6f} DRY_RUN={DRY_RUN}")

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
    """
    try:
        u = ex["upbit"]; b = ex["bithumb"]
        t_u = safe_ticker(u,f"{symbol}/KRW")
        t_b = safe_ticker(b,f"{symbol}/KRW")
        price_u = float(t_u["last"])
        price_b = float(t_b["last"])

        # 업비트가 더 비쌀 때: 업비트 SELL, 빗썸 BUY
        diff = price_u - price_b
        mid  = (price_u + price_b)/2
        prem = (diff/mid)*100

        print(f"[KRW-ARB {symbol}] up={price_u} bt={price_b} prem={prem:.3f}%")

        if abs(prem) < KRW_ARB_THR:
            return

        # 잔고 가져오기
        bal_u = u.fetch_balance()
        bal_b = b.fetch_balance()
        free_u_sym = float(bal_u.get(symbol,{}).get("free",0) or 0)
        free_b_sym = float(bal_b.get(symbol,{}).get("free",0) or 0)
        free_u_krw = float(bal_u.get("KRW",{}).get("free",0) or 0)
        free_b_krw = float(bal_b.get("KRW",{}).get("free",0) or 0)

        # 한 번에 사용할 수 있는 최대 금액 (KRW 기준)
        # 업비트/빗썸 계좌 합산 20%까지 사용
        # (여기서는 수량은 소액으로 제한)
        max_notional = KRW_ARB_RATIO * min(
            free_u_sym*price_u + free_u_krw,
            free_b_sym*price_b + free_b_krw
        )
        if max_notional < MIN_NOTIONAL_KRW:
            return

        # 약간 보수적으로 수량 계산
        if prem > 0:
            # upbit 비쌈 → upbit SELL, bithumb BUY
            amt = max_notional / price_u
            amt = min(amt, free_u_sym*0.9, (free_b_krw*0.9)/price_b)
            if amt <= 0: return
            print(f"[KRW-ARB {symbol}] upbit SELL, bithumb BUY amt={amt}")
            create_order(u,f"{symbol}/KRW","sell",amt)
            create_order(b,f"{symbol}/KRW","buy",amt)
            send_telegram(f"[KRW ARB {symbol}] upbit SELL / bithumb BUY prem={prem:.3f}% amt={amt:.5f} DRY_RUN={DRY_RUN}")
        else:
            # prem < 0 → 빗썸 비쌈 → 빗썸 SELL, 업비트 BUY
            amt = max_notional / price_b
            amt = min(amt, free_b_sym*0.9, (free_u_krw*0.9)/price_u)
            if amt <= 0: return
            print(f"[KRW-ARB {symbol}] bithumb SELL, upbit BUY amt={amt}")
            create_order(b,f"{symbol}/KRW","sell",amt)
            create_order(u,f"{symbol}/KRW","buy",amt)
            send_telegram(f"[KRW ARB {symbol}] bithumb SELL / upbit BUY prem={prem:.3f}% amt={amt:.5f} DRY_RUN={DRY_RUN}")

    except Exception as e:
        print(f"[KRW-ARB ERR {symbol}] {e}")

###############################################################################
# LAYER 3: Funding Arbitrage (Signal Only)
###############################################################################

def funding_arbitrage_signals():
    """
    Binance / Bybit / OKX BTC/USDT funding rate 차이를 모니터링.
    현재는 '시그널+로그'만 남기고, 실제 포지션 진입은 하지 않는다.
    """
    try:
        symbols = ["BTC/USDT"]
        rates = {}

        # Binance futures (USDT-margined) – ccxt에서 binanceusdm 사용 권장
        try:
            bin_fut = ccxt.binanceusdm({
                "apiKey": BINANCE_API,
                "secret": BINANCE_SECRET,
                "enableRateLimit": True,
            })
            bin_fut.load_markets()
            fr = bin_fut.fetch_funding_rate("BTC/USDT")
            rates["binance_fut"] = fr["fundingRate"]
        except Exception as e:
            print(f"[FUND] binance_fut ERR {e}")

        # Bybit linear
        try:
            by = ex["bybit"]
            frs = by.fetch_funding_rates()
            for r in frs:
                if r.get("symbol") in ["BTC/USDT","BTCUSDT"]:
                    rates["bybit"] = r.get("fundingRate",0)
                    break
        except Exception as e:
            print(f"[FUND] bybit ERR {e}")

        # OKX swap
        try:
            ok = ex["okx"]
            frs = ok.fetch_funding_rates()
            for r in frs:
                if r.get("symbol") in ["BTC-USDT-SWAP","BTC/USDT:USDT"]:
                    rates["okx"] = r.get("fundingRate",0)
                    break
        except Exception as e:
            print(f"[FUND] okx ERR {e}")

        print(f"[FUND RATES] {rates}")
        if len(rates) < 2:
            return

        # 가장 높은/낮은 funding 찾기
        max_ex = max(rates, key=rates.get)
        min_ex = min(rates, key=rates.get)
        spread = rates[max_ex] - rates[min_ex]

        print(f"[FUND SPREAD] max={max_ex}({rates[max_ex]:.5f}) min={min_ex}({rates[min_ex]:.5f}) diff={spread:.5f}")

        if spread >= FUNDING_SPREAD_THR:
            msg = (
                f"[FUND ARB SIGNAL]\n"
                f"- {max_ex} funding={rates[max_ex]:.5f} (숏 후보)\n"
                f"- {min_ex} funding={rates[min_ex]:.5f} (롱 후보)\n"
                f"- spread={spread:.5f} >= {FUNDING_SPREAD_THR}\n"
                f"- 현재 코드는 시그널만 로그/알림. 실제 선물 포지션 진입은 구현 필요.\n"
            )
            print(msg)
            send_telegram(msg)

            # ⚠️ 실매매로 선물 롱/숏까지 자동 진입하려면:
            # - 각 거래소에 futures용 ccxt 인스턴스(binanceusdm, bybit with type='swap', okx with swap 설정)
            # - 포지션 관리 / 청산 로직
            # - 마진/레버리지 제어
            # 등을 추가 구현해야 하므로, 여기서는 안전을 위해 "시그널만" 남긴다.

    except Exception as e:
        print(f"[FUND ARB ERR] {e}")

###############################################################################
# TRIANGULAR MONITOR (Bybit/OKX)
###############################################################################

def triangular_monitor(name: str):
    try:
        inst = ex.get(name)
        if not inst: return
        t1 = safe_ticker(inst,"BTC/USDT")
        t2 = safe_ticker(inst,"ETH/USDT")
        t3 = safe_ticker(inst,"ETH/BTC")
        spread = (t2["bid"]/(t1["bid"]*t3["bid"]) -1)*100
        print(f"[TRI {name}] {spread:.3f}%")
        # 필요하면 여기에도 threshold 걸어서 경고/시그널만 보낼 수 있음
    except Exception as e:
        print(f"[TRI ERR {name}] {e}")

###############################################################################
# MAIN LOOP
###############################################################################

def main():
    global disable_trading
    init_exchanges()
    send_telegram(f"공격형 김프봇 시작 (DRY_RUN={DRY_RUN}) – Tier2+KRW+Funding 신호")

    trade_times = []

    while True:
        loop_start = now_ts()
        try:
            vol = get_daily_volatility()
            tier1_thr, base_ratio = auto_tier1_params(vol, trade_times)
            print(f"\n[LOOP] vol={vol:.2f}% tier1_thr={tier1_thr:.2f}% base_ratio={base_ratio:.2f} trades_1h={len([t for t in trade_times if now_ts()-t<=3600])}")

            if not disable_trading:
                # 1) 김프/역프 재정거래 (Tier1 + Tier2)
                run_spread_arbitrage("BTC", tier1_thr, base_ratio, trade_times)
                run_spread_arbitrage("ETH", tier1_thr, base_ratio, trade_times)

                # 2) 업비트 ↔ 빗썸 KRW 차익 레이어
                run_krw_cross_arb("BTC")
                run_krw_cross_arb("ETH")

                # 3) Funding arbitrage 신호
                funding_arbitrage_signals()

                # 4) 삼각차익 모니터링
                for name in ["bybit","okx"]:
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
