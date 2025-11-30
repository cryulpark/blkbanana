import os
import time
import traceback
from typing import Dict, Any, Tuple, List

import ccxt
from ccxt.base.errors import AuthenticationError
import requests


# ==============================
# 1. 설정값
# ==============================

# 실매매 여부 (True = 실제 주문, False = 시뮬레이션)
DRY_RUN = True

# 루프 주기(초)
MAIN_LOOP_INTERVAL = 60  # 1분

# 상태 보고 주기(초)
STATUS_INTERVAL = 3600  # 1시간

# 재정거래 진입 스프레드 기준 (%)
LOW_VOL_THRESHOLD = 2.5   # 일간 변동성 10% 미만
HIGH_VOL_THRESHOLD = 2.0  # 일간 변동성 10% 이상
VOL_THRESHOLD_BORDER = 10.0  # 변동성 10% 기준

# 삼각 차익, 펀딩비 기준
MIN_TRIANGULAR_SPREAD = 0.5   # %
MIN_FUNDING_RATE = 0.01       # 1%

# 거래 시 잔고에서 사용하는 비율 (예: 0.5면 잔고의 50%까지 사용)
USE_BALANCE_RATIO = 0.5

# 최소 체결 금액 (KRW 기준, 이보다 작으면 거래 안 함 – 수수료/미니멈 방지용)
MIN_NOTIONAL_KRW = 50000  # 5만원

# 쿨다운(초) – 동일 전략/심볼/거래소 기준
ARBITRAGE_COOLDOWN = 300  # 5분


# ==============================
# 2. 환경 변수
# ==============================

def load_env(key: str) -> str:
    try:
        return os.environ[key]
    except KeyError:
        raise ValueError(f"환경 변수 누락: {key} – Railway Variables에서 {key}를 설정하세요.")


BINANCE_API = load_env("BINANCE_API_KEY")
BINANCE_SECRET = load_env("BINANCE_SECRET")

# 선물(USDT-M)용 별도 키 (없으면 spot 키 재사용)
BINANCE_FUTURES_API = os.environ.get("BINANCE_FUTURES_API_KEY", BINANCE_API)
BINANCE_FUTURES_SECRET = os.environ.get("BINANCE_FUTURES_SECRET", BINANCE_SECRET)

UPBIT_API = load_env("UPBIT_API_KEY")
UPBIT_SECRET = load_env("UPBIT_SECRET")

BITHUMB_API = load_env("BITHUMB_API_KEY")
BITHUMB_SECRET = load_env("BITHUMB_SECRET")

BYBIT_API = load_env("BYBIT_API_KEY")
BYBIT_SECRET = load_env("BYBIT_SECRET")

OKX_API = load_env("OKX_API_KEY")
OKX_SECRET = load_env("OKX_SECRET")

TELEGRAM_TOKEN = load_env("TELEGRAM_TOKEN")
CHAT_ID = load_env("CHAT_ID")


# ==============================
# 3. 전역 객체
# ==============================

exchanges: Dict[str, ccxt.Exchange] = {}

cumulative_profit_krw: float = 0.0
last_status_time: float = time.time()

# (strategy, symbol, venue) -> 최근 실행 시각
last_trade_times: Dict[Tuple[str, str, str], float] = {}

# 트레이드 로그 (24시간 수익 리포트용)
TRADE_LOG: List[Dict[str, Any]] = []

# 일일 리포트 발송 여부 체크용 (날짜별 딱 한 번)
last_daily_report_date: str = ""  # "YYYY-MM-DD"


# ==============================
# 4. 유틸 함수
# ==============================

def init_exchanges() -> None:
    """
    ccxt 거래소 인스턴스 초기화.
    - binance: spot
    - binance_futures: USDT-M futures
    - upbit, bithumb, bybit, okx: spot
    """
    global exchanges

    config = [
        ("binance",         ccxt.binance,     BINANCE_API,         BINANCE_SECRET,         {"enableRateLimit": True}),
        ("binance_futures", ccxt.binanceusdm, BINANCE_FUTURES_API, BINANCE_FUTURES_SECRET, {"enableRateLimit": True}),
        ("upbit",           ccxt.upbit,       UPBIT_API,           UPBIT_SECRET,           {"enableRateLimit": True}),
        ("bithumb",         ccxt.bithumb,     BITHUMB_API,         BITHUMB_SECRET,         {"enableRateLimit": True}),
        ("bybit",           ccxt.bybit,       BYBIT_API,           BYBIT_SECRET,           {"enableRateLimit": True}),
        ("okx",             ccxt.okx,         OKX_API,             OKX_SECRET,             {"enableRateLimit": True}),
    ]

    for name, cls, api, secret, params in config:
        try:
            ex = cls({**params, "apiKey": api, "secret": secret})
            ex.load_markets()
            exchanges[name] = ex
            print(f"[INIT] {name} 연결 성공")
        except Exception as e:
            print(f"[INIT] {name} 연결 오류: {e}")


def send_telegram(message: str) -> None:
    """
    텔레그램 메시지를 HTTP API로 직접 전송 (동기).
    python-telegram-bot의 이벤트 루프 문제를 피하기 위해 requests 사용.
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    data = {"chat_id": CHAT_ID, "text": message}
    try:
        resp = requests.post(url, data=data, timeout=10)
        if not resp.ok:
            print(f"[TELEGRAM] 응답 오류: {resp.status_code} {resp.text}")
    except Exception as e:
        print(f"[TELEGRAM] 전송 예외: {e}")


def now_ts() -> float:
    return time.time()


def in_cooldown(strategy: str, symbol: str, venue: str, cooldown: float) -> bool:
    key = (strategy, symbol, venue)
    last_ts = last_trade_times.get(key, 0.0)
    return (now_ts() - last_ts) < cooldown


def touch_trade_time(strategy: str, symbol: str, venue: str) -> None:
    key = (strategy, symbol, venue)
    last_trade_times[key] = now_ts()


def safe_fetch_ticker(ex: ccxt.Exchange, symbol: str) -> Dict[str, Any]:
    """
    ticker를 안전하게 가져오는 래퍼.
    - bid/ask가 None이면 last 또는 info의 trade_price/closing_price 등으로 대체.
    """
    ticker = ex.fetch_ticker(symbol)
    bid = ticker.get("bid")
    ask = ticker.get("ask")
    last = ticker.get("last")

    # Upbit/Bithumb KRW 마켓은 bid/ask가 없는 경우가 많아서 last로 대체
    if bid is None:
        bid = last
        if bid is None:
            info = ticker.get("info", {})
            bid = float(
                info.get("trade_price")
                or info.get("closing_price")
                or info.get("opening_price")
                or 0
            )
        ticker["bid"] = bid

    if ask is None:
        ask = last
        if ask is None:
            info = ticker.get("info", {})
            ask = float(
                info.get("trade_price")
                or info.get("closing_price")
                or info.get("opening_price")
                or 0
            )
        ticker["ask"] = ask

    if bid is None or ask is None or bid == 0 or ask == 0:
        raise RuntimeError(f"ticker에 bid/ask 값이 유효하지 않음: {ex.id} {symbol} {ticker}")

    return ticker


def get_usdt_krw_rate() -> float:
    """
    USDT/KRW 환율.
    1순위: 업비트, 2순위: 빗썸, 실패 시 1350.
    """
    for name in ["upbit", "bithumb"]:
        ex = exchanges.get(name)
        if not ex:
            continue
        try:
            t = safe_fetch_ticker(ex, "USDT/KRW")
            return float(t["bid"])
        except Exception as e:
            print(f"[FX] {name} USDT/KRW 조회 실패: {e}")

    print("[FX] 환율 조회 실패 – 기본값 1350 사용")
    return 1350.0


def get_daily_volatility() -> float:
    """
    바이낸스 현물 BTC/USDT 일간 변동성(%).
    """
    try:
        ex = exchanges["binance"]
        ohlcv = ex.fetch_ohlcv("BTC/USDT", "1d", limit=2)
        if len(ohlcv) < 2:
            return 0.0
        prev_close = ohlcv[0][4]
        curr_close = ohlcv[1][4]
        return abs((curr_close - prev_close) / prev_close * 100.0)
    except Exception as e:
        print(f"[VOL] 변동성 계산 실패: {e}")
        return 0.0


def create_market_order(ex: ccxt.Exchange, symbol: str, side: str, amount: float, params: Dict[str, Any] = None) -> Any:
    """
    마켓 주문 헬퍼. DRY_RUN=True면 실제 주문 대신 로그만 출력.
    """
    params = params or {}
    print(f"[ORDER] {ex.id} {side.upper()} {symbol} {amount} (params={params}, dry_run={DRY_RUN})")
    if DRY_RUN:
        return {"info": "dry_run", "symbol": symbol, "side": side, "amount": amount, "params": params}

    if side.lower() == "buy":
        return ex.create_market_buy_order(symbol, amount, params)
    else:
        return ex.create_market_sell_order(symbol, amount, params)


def get_free(balance: Dict[str, Any], currency: str) -> float:
    """
    balance 딕셔너리에서 특정 통화의 free 잔고 가져오기.
    """
    info = balance.get(currency, {})
    return float(info.get("free", 0.0) or 0.0)


def log_trade(strategy: str, symbol: str, venue: str, direction: str, profit_krw: float) -> None:
    """
    트레이드 로그에 기록 (24시간 리포트용).
    strategy 예) "spot_arb", "triangular", ...
    symbol 예) "BTC", "ETH"
    venue  예) "upbit", "bithumb", ...
    direction 예) "KRW_sell", "KRW_buy"
    """
    TRADE_LOG.append(
        {
            "ts": now_ts(),
            "strategy": strategy,
            "symbol": symbol,
            "venue": venue,
            "direction": direction,
            "profit_krw": float(profit_krw),
        }
    )


def format_krw(x: float) -> str:
    sign = "+" if x >= 0 else "-"
    return f"{sign}{abs(x):,.0f}원"


# ==============================
# 5. 현물 재정거래 (동적 사이즈, 복리)
# ==============================

def run_spot_arbitrage(symbol: str, threshold: float) -> None:
    """
    Binance 현물 vs (Upbit/Bithumb) KRW 김치프 재정거래.
    symbol: 'BTC' / 'ETH'

    방향 1) KRW 거래소 비쌈 (프리미엄 > threshold):
        - KRW 거래소에서 symbol 매도
        - Binance에서 symbol 매수

    방향 2) KRW 거래소 쌈 (프리미엄 < -threshold):
        - KRW 거래소에서 symbol 매수
        - Binance에서 symbol 매도
    """
    global cumulative_profit_krw

    strategy_name = "spot_arb"

    try:
        binance = exchanges["binance"]
        usdt_krw = get_usdt_krw_rate()

        # 바이낸스 기준 가격 (USDT)
        base_pair = f"{symbol}/USDT"
        bin_ticker = safe_fetch_ticker(binance, base_pair)
        base_price_usdt = float(bin_ticker["bid"])  # 보수적으로 bid 사용

        # 바이낸스 잔고
        bin_balance = binance.fetch_balance()
        bin_free_usdt = get_free(bin_balance, "USDT")
        bin_free_symbol = get_free(bin_balance, symbol)

        # 각 KRW 거래소(upbit, bithumb)별로 기회 체크
        for venue in ["upbit", "bithumb"]:
            ex = exchanges.get(venue)
            if not ex:
                continue

            try:
                krw_pair = f"{symbol}/KRW"
                t = safe_fetch_ticker(ex, krw_pair)
                ask_krw = float(t["ask"])
                bid_krw = float(t["bid"])
            except Exception as e:
                print(f"[ARBITRAGE] {venue} {symbol}/KRW 티커 실패: {e}")
                continue

            # KRW 거래소 가격을 USDT로 변환
            ask_usdt = ask_krw / usdt_krw
            bid_usdt = bid_krw / usdt_krw

            # 프리미엄 계산
            premium_sell = (bid_usdt / base_price_usdt - 1.0) * 100.0  # KRW 비쌈 (우리는 KRW에서 sell)
            premium_buy = (ask_usdt / base_price_usdt - 1.0) * 100.0   # KRW 쌈 (우리는 KRW에서 buy)

            print(
                f"[ARBITRAGE] {symbol} @ {venue} premium_sell={premium_sell:.2f}%, premium_buy={premium_buy:.2f}% "
                f"(threshold={threshold:.2f}%)"
            )

            # KRW 거래소 잔고
            try:
                balance_krw_ex = ex.fetch_balance()
            except AuthenticationError as ae:
                # 업비트 키가 틀리거나 권한 문제일 때 여기서 잡힘
                print(f"[ARBITRAGE] {venue} 잔고 조회 인증 오류: {ae} – 이 거래소는 스킵합니다.")
                continue
            except Exception as e:
                print(f"[ARBITRAGE] {venue} 잔고 조회 실패: {e}")
                continue

            ex_free_krw = get_free(balance_krw_ex, "KRW")
            ex_free_symbol = get_free(balance_krw_ex, symbol)

            # ---------------------
            # 방향 1: KRW 거래소가 비쌈 (sell premium)
            # ---------------------
            if premium_sell > threshold:
                if bin_free_usdt <= 0 or ex_free_symbol <= 0:
                    print(f"[ARBITRAGE] {symbol} {venue} 방향1 불가 – 잔고 부족 (USDT or {symbol})")
                else:
                    max_from_usdt = (bin_free_usdt * USE_BALANCE_RATIO) / base_price_usdt
                    max_from_symbol = ex_free_symbol * USE_BALANCE_RATIO
                    trade_amount = min(max_from_usdt, max_from_symbol)

                    notional_krw = trade_amount * bid_krw
                    if trade_amount <= 0 or notional_krw < MIN_NOTIONAL_KRW:
                        print(f"[ARBITRAGE] {symbol} {venue} 방향1 – 금액 너무 작음, 스킵 (notional={notional_krw:.0f}원)")
                    else:
                        if in_cooldown(strategy_name, symbol, venue + "_sell", ARBITRAGE_COOLDOWN):
                            print(f"[ARBITRAGE] {symbol} {venue} 방향1 – 쿨다운 중, 스킵")
                        else:
                            try:
                                create_market_order(binance, base_pair, "buy", trade_amount)
                                create_market_order(ex, krw_pair, "sell", trade_amount)

                                est_profit = est_profit_krw(premium_sell, base_price_usdt, trade_amount, usdt_krw)
                                cumulative_profit_krw += est_profit
                                log_trade(strategy_name, symbol, venue, "KRW_sell", est_profit)

                                touch_trade_time(strategy_name, symbol, venue + "_sell")

                                msg = (
                                    f"[{symbol}] 재정거래 실행 (KRW 비쌈, {venue}에서 매도)\n"
                                    f"- venue: {venue}\n"
                                    f"- 방향: {venue} SELL, Binance BUY\n"
                                    f"- premium_sell: {premium_sell:.2f}%\n"
                                    f"- 수량: {trade_amount:.6f} {symbol}\n"
                                    f"- 추정 이익: {format_krw(est_profit)}\n"
                                    f"- 누적 추정 이익: {format_krw(cumulative_profit_krw)}\n"
                                    f"- DRY_RUN: {DRY_RUN}"
                                )
                                print(msg)
                                send_telegram(msg)
                            except Exception as e:
                                tb = traceback.format_exc()
                                print(f"[ARBITRAGE] {symbol} {venue} 방향1 주문 오류: {e}\n{tb}")
                                send_telegram(f"[ARBITRAGE] {symbol} {venue} 방향1 주문 오류: {e}")

            # ---------------------
            # 방향 2: KRW 거래소가 쌀 때 (buy discount)
            # ---------------------
            if premium_buy < -threshold:
                if ex_free_krw <= 0 or bin_free_symbol <= 0:
                    print(f"[ARBITRAGE] {symbol} {venue} 방향2 불가 – 잔고 부족 (KRW or {symbol})")
                else:
                    max_from_krw = (ex_free_krw * USE_BALANCE_RATIO) / ask_krw
                    max_from_bin_symbol = bin_free_symbol * USE_BALANCE_RATIO
                    trade_amount = min(max_from_krw, max_from_bin_symbol)

                    notional_krw = trade_amount * ask_krw
                    if trade_amount <= 0 or notional_krw < MIN_NOTIONAL_KRW:
                        print(f"[ARBITRAGE] {symbol} {venue} 방향2 – 금액 너무 작음, 스킵 (notional={notional_krw:.0f}원)")
                    else:
                        if in_cooldown(strategy_name, symbol, venue + "_buy", ARBITRAGE_COOLDOWN):
                            print(f"[ARBITRAGE] {symbol} {venue} 방향2 – 쿨다운 중, 스킵")
                        else:
                            try:
                                create_market_order(ex, krw_pair, "buy", trade_amount)
                                create_market_order(binance, base_pair, "sell", trade_amount)

                                est_profit = est_profit_krw(-premium_buy, base_price_usdt, trade_amount, usdt_krw)
                                cumulative_profit_krw += est_profit
                                log_trade(strategy_name, symbol, venue, "KRW_buy", est_profit)

                                touch_trade_time(strategy_name, symbol, venue + "_buy")

                                msg = (
                                    f"[{symbol}] 재정거래 실행 (KRW 쌈, {venue}에서 매수)\n"
                                    f"- venue: {venue}\n"
                                    f"- 방향: {venue} BUY, Binance SELL\n"
                                    f"- discount: {premium_buy:.2f}%\n"
                                    f"- 수량: {trade_amount:.6f} {symbol}\n"
                                    f"- 추정 이익: {format_krw(est_profit)}\n"
                                    f"- 누적 추정 이익: {format_krw(cumulative_profit_krw)}\n"
                                    f"- DRY_RUN: {DRY_RUN}"
                                )
                                print(msg)
                                send_telegram(msg)
                            except Exception as e:
                                tb = traceback.format_exc()
                                print(f"[ARBITRAGE] {symbol} {venue} 방향2 주문 오류: {e}\n{tb}")
                                send_telegram(f"[ARBITRAGE] {symbol} {venue} 방향2 주문 오류: {e}")

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ARBITRAGE] {symbol} 전체 오류: {e}\n{tb}")
        send_telegram(f"[ARBITRAGE] {symbol} 전체 오류: {e}")


def est_profit_krw(spread_pct: float, base_price_usdt: float, amount: float, usdt_krw: float) -> float:
    """
    단순 추정 수익(수수료/슬리피지 미반영).
    spread_pct: 절대값 스프레드 (%)
    """
    profit_usdt = (spread_pct / 100.0) * base_price_usdt * amount
    return profit_usdt * usdt_krw


# ==============================
# 6. 삼각 차익 / 펀딩 (알림 위주, DRY_RUN 모의)
# ==============================

def run_triangular_arb(ex_name: str) -> None:
    """
    Bybit/OKX 삼각 차익 모니터링 + (DRY_RUN 모의 주문).
    루프: USDT -> BTC -> ETH -> USDT
    """
    def pick_symbol(ex: ccxt.Exchange, candidates):
        """여러 후보 중 실제 존재하는 심볼 하나 선택."""
        for s in candidates:
            if s in ex.markets:
                return s
        raise RuntimeError(f"{ex.id}에서 사용 가능한 심볼이 없음: {candidates}")

    strategy_name = "triangular"
    symbol = "BTC-ETH"
    ex = exchanges.get(ex_name)
    if not ex:
        return

    try:
        # 심볼 자동 선택 (spot 또는 :USDT 선물 등)
        btc_usdt_sym = pick_symbol(ex, ["BTC/USDT", "BTC/USDT:USDT"])
        eth_usdt_sym = pick_symbol(ex, ["ETH/USDT", "ETH/USDT:USDT"])
        eth_btc_sym = pick_symbol(ex, ["ETH/BTC"])

        t_btc_usdt = safe_fetch_ticker(ex, btc_usdt_sym)
        t_eth_usdt = safe_fetch_ticker(ex, eth_usdt_sym)
        t_eth_btc = safe_fetch_ticker(ex, eth_btc_sym)

        p_btc_usdt = float(t_btc_usdt["bid"])  # 1 BTC = ? USDT
        p_eth_usdt = float(t_eth_usdt["bid"])  # 1 ETH = ? USDT
        p_eth_btc = float(t_eth_btc["bid"])    # 1 ETH = ? BTC

        # 루프: 1 USDT -> BTC -> ETH -> USDT
        loop_val = p_eth_usdt / (p_btc_usdt * p_eth_btc) - 1.0
        spread_pct = loop_val * 100.0

        print(f"[TRIANGULAR] {ex_name} spread={spread_pct:.4f}%")

        if spread_pct <= max(MIN_TRIANGULAR_SPREAD, 0.01):
            return

        # 쿨다운 체크
        if in_cooldown(strategy_name, symbol, ex_name, ARBITRAGE_COOLDOWN):
            print(f"[TRIANGULAR] {ex_name} 쿨다운 중 – 진입 건너뜀")
            return

        msg = (
            f"[TRIANGULAR] {ex_name.upper()} 삼각 차익 기회 감지\n"
            f"- 스프레드: {spread_pct:.4f}%\n"
            f"- {btc_usdt_sym} bid={p_btc_usdt}, {eth_usdt_sym} bid={p_eth_usdt}, {eth_btc_sym} bid={p_eth_btc}\n"
            f"- DRY_RUN: {DRY_RUN} (실매매는 충분한 테스트 후 권장)\n"
        )

        # DRY_RUN 모드에서 소량 루프 흉내
        amount_usdt = 10.0
        btc_amount = amount_usdt / p_btc_usdt
        eth_amount = btc_amount / p_eth_btc

        try:
            create_market_order(ex, btc_usdt_sym, "buy", btc_amount)   # USDT -> BTC
            create_market_order(ex, eth_btc_sym, "buy", eth_amount)    # BTC -> ETH (ETH/BTC 기준 buy)
            create_market_order(ex, eth_usdt_sym, "sell", eth_amount)  # ETH -> USDT
        except Exception as oe:
            msg += f"- 모의 주문 중 오류: {oe}\n"

        touch_trade_time(strategy_name, symbol, ex_name)

        print(msg)
        send_telegram(msg)

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[TRIANGULAR] {ex_name} 오류: {e}\n{tb}")
        send_telegram(f"[TRIANGULAR] {ex_name} 오류: {e}")


def monitor_funding() -> None:
    """
    바이낸스 USDT-M 선물 BTC/USDT 펀딩비 모니터링.
    현재는 알림만, 자동 포지션 진입 없음.
    """
    try:
        ex = exchanges["binance_futures"]

        # 선물 심볼 자동 선택
        symbol_candidates = ["BTC/USDT:USDT", "BTC/USDT"]
        symbol = None
        for s in symbol_candidates:
            if s in ex.markets:
                symbol = s
                break
        if symbol is None:
            raise RuntimeError("Binance USDM에서 BTC/USDT 선물 심볼을 찾을 수 없음")

        fr = ex.fetch_funding_rate(symbol)
        rate = float(fr.get("rate", 0.0))

        print(f"[FUNDING] {symbol} rate={rate:.6f}")

        if rate >= MIN_FUNDING_RATE:
            msg = (
                f"[FUNDING] 펀딩비 기회 감지\n"
                f"- {symbol} funding rate: {rate:.6f}\n"
                f"- 현재 코드는 알림만 보내고 자동 포지션은 진입하지 않습니다.\n"
            )
            print(msg)
            send_telegram(msg)

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[FUNDING] 오류: {e}\n{tb}")
        send_telegram(f"[FUNDING] 오류: {e}")


# ==============================
# 7. 일일 24시간 수익 리포트 (매일 9시)
# ==============================

def send_daily_report_if_needed() -> None:
    """
    매일 아침 9시에 지난 24시간 수익/손실을 분야별로 텔레그램 리포트.
    기준 시간: 컨테이너 로컬타임.
    """
    global last_daily_report_date

    lt = time.localtime()
    current_date = f"{lt.tm_year:04d}-{lt.tm_mon:02d}-{lt.tm_mday:02d}"

    # 아침 9시 & 오늘 아직 리포트를 안 보냈을 때만 실행
    if lt.tm_hour != 9:
        return
    if last_daily_report_date == current_date:
        return

    now = now_ts()
    cutoff = now - 86400  # 최근 24시간

    recent_trades = [t for t in TRADE_LOG if t["ts"] >= cutoff]

    if not recent_trades:
        msg = (
            f"[DAILY REPORT] {current_date} 기준 최근 24시간 수익 리포트\n"
            f"- 최근 24시간 동안 실행된 거래가 없습니다.\n"
            f"- DRY_RUN: {DRY_RUN}"
        )
        print(msg)
        send_telegram(msg)
        last_daily_report_date = current_date
        return

    # 분야별 집계: (strategy, symbol) 단위
    summary: Dict[Tuple[str, str], Dict[str, Any]] = {}
    total_profit = 0.0

    for t in recent_trades:
        key = (t["strategy"], t["symbol"])
        profit = t["profit_krw"]
        total_profit += profit
        if key not in summary:
            summary[key] = {"profit": 0.0, "count": 0}
        summary[key]["profit"] += profit
        summary[key]["count"] += 1

    # 리포트 문자열 구성
    lines = []
    lines.append(f"[DAILY REPORT] {current_date} 기준 최근 24시간 수익 리포트")
    lines.append(f"- 총 추정 수익: {format_krw(total_profit)}")
    lines.append("")

    for (strategy, symbol), data in summary.items():
        분야명 = ""
        if strategy == "spot_arb":
            분야명 = f"{symbol} 현물 재정거래"
        elif strategy == "triangular":
            분야명 = f"{symbol} 삼각 차익"
        else:
            분야명 = f"{strategy}/{symbol}"

        lines.append(
            f"· {분야명}: "
            f"{format_krw(data['profit'])} "
            f"(거래 {data['count']}회)"
        )

    lines.append("")
    lines.append(f"- DRY_RUN: {DRY_RUN}")

    msg = "\n".join(lines)
    print(msg)
    send_telegram(msg)

    last_daily_report_date = current_date


# ==============================
# 8. 메인 루프
# ==============================

def main_loop() -> None:
    global last_status_time

    send_telegram(f"까망빠나나 시작! DRY_RUN={DRY_RUN} / Railway에서 24/7 모니터링 중.")

    while True:
        loop_start = now_ts()
        try:
            # 1) 변동성 기반 threshold 설정
            vol = get_daily_volatility()
            threshold = HIGH_VOL_THRESHOLD if vol >= VOL_THRESHOLD_BORDER else LOW_VOL_THRESHOLD
            print(f"[LOOP] 시작 – 일간 변동성={vol:.2f}% / 스프레드 기준={threshold:.2f}%")

            # 2) BTC 재정거래 (동적 사이즈)
            run_spot_arbitrage("BTC", threshold)

            # 3) ETH 재정거래 (동적 사이즈)
            run_spot_arbitrage("ETH", threshold)

            # 4) Bybit/OKX 삼각 차익 모니터링
            for ex_name in ["bybit", "okx"]:
                if ex_name in exchanges:
                    run_triangular_arb(ex_name)

            # 5) 펀딩비 모니터링
            monitor_funding()

            # 6) 일일 24시간 수익 리포트 (매일 9시)
            send_daily_report_if_needed()

            # 7) 상태 보고 (1시간마다)
            now = now_ts()
            if now - last_status_time >= STATUS_INTERVAL:
                msg = (
                    f"[STATUS] 봇 정상 동작 중\n"
                    f"- 누적 추정 이익: {format_krw(cumulative_profit_krw)}\n"
                    f"- DRY_RUN: {DRY_RUN}\n"
                )
                print(msg)
                send_telegram(msg)
                last_status_time = now

        except Exception as e:
            tb = traceback.format_exc()
            print(f"[LOOP] 치명적 오류: {e}\n{tb}")
            send_telegram(f"[LOOP] 치명적 오류 발생, 10초 후 재시작: {e}")
            time.sleep(10)

        # 루프 간격 맞추기
        elapsed = now_ts() - loop_start
        sleep_time = max(5.0, MAIN_LOOP_INTERVAL - elapsed)
        print(f"[LOOP] 대기 {sleep_time:.1f}초 후 다음 루프")
        time.sleep(sleep_time)


if __name__ == "__main__":
    init_exchanges()
    main_loop()
