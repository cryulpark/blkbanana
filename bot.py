import os
import time
import traceback
from typing import Dict, Any, Tuple

import ccxt
from telegram import Bot  # python-telegram-bot==20.7


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
bot = Bot(token=TELEGRAM_TOKEN)

cumulative_profit_krw: float = 0.0
last_status_time: float = time.time()

# (strategy, symbol, venue) -> 최근 실행 시각
last_trade_times: Dict[Tuple[str, str, str], float] = {}


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
    python-telegram-bot 20.x -> Bot.send_message는 async.
    여기서는 매번 asyncio.run으로 한 번씩 실행.
    """
    import asyncio

    async def _send():
        try:
            await bot.send_message(chat_id=CHAT_ID, text=message)
        except Exception as e:
            print(f"[TELEGRAM] 전송 오류: {e}")

    try:
        asyncio.run(_send())
    except RuntimeError:
        # 이미 다른 이벤트 루프가 돌고 있는 특수 환경 대비
        print("[TELEGRAM] asyncio 루프 충돌 – 메시지 전송 스킵")


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
    ticker를 안전하게 가져오는 래퍼. bid/ask 없거나 None이면 예외 발생.
    """
    ticker = ex.fetch_ticker(symbol)
    bid = ticker.get("bid")
    ask = ticker.get("ask")
    if bid is None or ask is None:
        raise RuntimeError(f"ticker에 bid/ask 값이 없음: {ex.id} {symbol} {ticker}")
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
        필요 잔고:
        - KRW 거래소: symbol (BTC/ETH)
        - Binance: USDT

    방향 2) KRW 거래소 쌈 (프리미엄 < -threshold):
        - KRW 거래소에서 symbol 매수
        - Binance에서 symbol 매도
        필요 잔고:
        - KRW 거래소: KRW
        - Binance: symbol (BTC/ETH)
    """
    global cumulative_profit_krw

    strategy_name = "spot_arbitrage"

    try:
        binance = exchanges["binance"]
        usdt_krw = get_usdt_krw_rate()

        # 바이낸스 기준 가격 (USDT)
        base_pair = f"{symbol}/USDT"
        bin_ticker = safe_fetch_ticker(binance, base_pair)
        base_price_usdt = float(bin_ticker["bid"])  # 우리가 살 때는 bid 기준으로 보수적으로

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

            # 프리미엄 계산: KRW 거래소 기준 ask vs Binance bid
            # (우리가 KRW 거래소에서 팔면 bid, 살 때는 ask를 사용)
            # 여기서는 두 방향을 모두 본다.
            # 방향1: KRW 비쌈 → KRW bid 기준으로 프리미엄 계산
            premium_sell = (bid_usdt / base_price_usdt - 1.0) * 100.0
            # 방향2: KRW 쌈 → KRW ask 기준으로 디스카운트 계산
            premium_buy = (ask_usdt / base_price_usdt - 1.0) * 100.0

            print(
                f"[ARBITRAGE] {symbol} @ {venue} premium_sell={premium_sell:.2f}%, premium_buy={premium_buy:.2f}% "
                f"(threshold={threshold:.2f}%)"
            )

            balance_krw_ex = ex.fetch_balance()
            ex_free_krw = get_free(balance_krw_ex, "KRW")
            ex_free_symbol = get_free(balance_krw_ex, symbol)

            # ---------------------
            # 방향 1: KRW 거래소가 비쌀 때 (sell premium)
            # ---------------------
            if premium_sell > threshold:
                # 필요 잔고:
                # - Binance: USDT
                # - venue: symbol (BTC/ETH)
                if bin_free_usdt <= 0 or ex_free_symbol <= 0:
                    print(f"[ARBITRAGE] {symbol} {venue} 방향1 불가 – 잔고 부족 (USDT or {symbol})")
                else:
                    # Binance USDT에서 쓸 수 있는 BTC/ETH 수량
                    max_from_usdt = (bin_free_usdt * USE_BALANCE_RATIO) / base_price_usdt
                    # venue에서 팔 수 있는 BTC/ETH 수량
                    max_from_symbol = ex_free_symbol * USE_BALANCE_RATIO

                    trade_amount = min(max_from_usdt, max_from_symbol)

                    # KRW 기준 노치널 체크
                    notional_krw = trade_amount * bid_krw
                    if trade_amount <= 0 or notional_krw < MIN_NOTIONAL_KRW:
                        print(f"[ARBITRAGE] {symbol} {venue} 방향1 – 금액 너무 작음, 스킵 (notional={notional_krw:.0f}원)")
                    else:
                        # 쿨다운 체크
                        if in_cooldown(strategy_name, symbol, venue + "_sell", ARBITRAGE_COOLDOWN):
                            print(f"[ARBITRAGE] {symbol} {venue} 방향1 – 쿨다운 중, 스킵")
                        else:
                            # Binance에서 매수, venue에서 매도
                            try:
                                create_market_order(binance, base_pair, "buy", trade_amount)
                                create_market_order(ex, krw_pair, "sell", trade_amount)

                                est_profit = est_profit_krw(premium_sell, base_price_usdt, trade_amount, usdt_krw)
                                cumulative_profit_krw += est_profit

                                touch_trade_time(strategy_name, symbol, venue + "_sell")

                                msg = (
                                    f"[{symbol}] 재정거래 실행 (KRW 비쌈, {venue}에서 매도)\n"
                                    f"- venue: {venue}\n"
                                    f"- 방향: {venue} SELL, Binance BUY\n"
                                    f"- premium_sell: {premium_sell:.2f}%\n"
                                    f"- 수량: {trade_amount:.6f} {symbol}\n"
                                    f"- 추정 이익: +{est_profit:,.0f}원\n"
                                    f"- 누적 추정 이익: +{cumulative_profit_krw:,.0f}원\n"
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
                # 필요 잔고:
                # - venue: KRW
                # - Binance: symbol (BTC/ETH)
                if ex_free_krw <= 0 or bin_free_symbol <= 0:
                    print(f"[ARBITRAGE] {symbol} {venue} 방향2 불가 – 잔고 부족 (KRW or {symbol})")
                else:
                    # venue의 KRW에서 살 수 있는 BTC/ETH 수량
                    max_from_krw = (ex_free_krw * USE_BALANCE_RATIO) / ask_krw
                    # Binance에서 팔 수 있는 BTC/ETH 수량
                    max_from_bin_symbol = bin_free_symbol * USE_BALANCE_RATIO

                    trade_amount = min(max_from_krw, max_from_bin_symbol)

                    notional_krw = trade_amount * ask_krw
                    if trade_amount <= 0 or notional_krw < MIN_NOTIONAL_KRW:
                        print(f"[ARBITRAGE] {symbol} {venue} 방향2 – 금액 너무 작음, 스킵 (notional={notional_krw:.0f}원)")
                    else:
                        # 쿨다운 체크
                        if in_cooldown(strategy_name, symbol, venue + "_buy", ARBITRAGE_COOLDOWN):
                            print(f"[ARBITRAGE] {symbol} {venue} 방향2 – 쿨다운 중, 스킵")
                        else:
                            try:
                                # venue에서 매수, Binance에서 매도
                                create_market_order(ex, krw_pair, "buy", trade_amount)
                                create_market_order(binance, base_pair, "sell", trade_amount)

                                # premium_buy는 음수이므로 절대값으로 이익 추정
                                est_profit = est_profit_krw(-premium_buy, base_price_usdt, trade_amount, usdt_krw)
                                cumulative_profit_krw += est_profit

                                touch_trade_time(strategy_name, symbol, venue + "_buy")

                                msg = (
                                    f"[{symbol}] 재정거래 실행 (KRW 쌈, {venue}에서 매수)\n"
                                    f"- venue: {venue}\n"
                                    f"- 방향: {venue} BUY, Binance SELL\n"
                                    f"- discount: {premium_buy:.2f}%\n"
                                    f"- 수량: {trade_amount:.6f} {symbol}\n"
                                    f"- 추정 이익: +{est_profit:,.0f}원\n"
                                    f"- 누적 추정 이익: +{cumulative_profit_krw:,.0f}원\n"
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
        # USDT -> BTC      : BTC1 = 1 / p_btc_usdt
        # BTC -> ETH(ETH/BTC): ETH1 = BTC1 / p_eth_btc
        # ETH -> USDT      : USDT2 = ETH1 * p_eth_usdt
        # => USDT2 = p_eth_usdt / (p_btc_usdt * p_eth_btc)
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
# 7. 메인 루프
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

            # 6) 상태 보고 (1시간마다)
            now = now_ts()
            if now - last_status_time >= STATUS_INTERVAL:
                msg = (
                    f"[STATUS] 봇 정상 동작 중\n"
                    f"- 누적 추정 이익: +{cumulative_profit_krw:,.0f}원\n"
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
