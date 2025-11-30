import os
import time
import math
import traceback
from typing import Dict, Any, Tuple

import ccxt
from telegram import Bot  # python-telegram-bot==20.7


# ==============================
# 1. 설정값
# ==============================

# 실매매 여부 (True = 실제 주문, False = 드라이런 / 시뮬레이션)
DRY_RUN = True

# 기본 루프 주기(초)
MAIN_LOOP_INTERVAL = 60  # 1분마다 체크

# 상태 보고 주기(초)
STATUS_INTERVAL = 3600  # 1시간마다 텔레그램 보고

# 재정거래 진입 기본 스프레드 기준 (%)
LOW_VOL_THRESHOLD = 2.5
HIGH_VOL_THRESHOLD = 2.0
VOL_THRESHOLD_BORDER = 10.0  # 변동성 10% 기준으로 high/low 구분

# 각 전략별 최소 스프레드 / 펀딩 기준
MIN_TRIANGULAR_SPREAD = 0.5   # 삼각 차익 최소 스프레드 (%)
MIN_FUNDING_RATE = 0.01       # 펀딩비 1% 이상 시 알림

# 각 코인별 1회 주문 수량 (예시)
BTC_AMOUNT = 0.001
ETH_AMOUNT = 0.01

# 같은 전략으로 연속 진입을 막기 위한 쿨다운(초)
ARBITRAGE_COOLDOWN = 300  # 5분


# ==============================
# 2. 환경 변수 로딩
# ==============================

def load_env(key: str) -> str:
    try:
        return os.environ[key]
    except KeyError:
        raise ValueError(f"환경 변수 누락: {key} – Railway Variables에서 {key}를 설정하세요.")


BINANCE_API = load_env("BINANCE_API_KEY")
BINANCE_SECRET = load_env("BINANCE_SECRET")

# 바이낸스 선물(USDT-M)용 키 (없으면 spot 키 재사용)
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

# 최근 전략 실행 시간 기록용 (중복 진입 방지)
last_trade_times: Dict[Tuple[str, str, str], float] = {}  # (strategy, symbol, venue)


# ==============================
# 4. 유틸 함수
# ==============================

def init_exchanges() -> None:
    """
    ccxt 거래소 인스턴스 초기화.
    - binance: 현물
    - binance_futures: USDT-M 선물
    - upbit, bithumb, bybit, okx: 현물
    """
    global exchanges

    config = [
        ("binance",         ccxt.binance,     BINANCE_API,          BINANCE_SECRET,          {"enableRateLimit": True}),
        ("binance_futures", ccxt.binanceusdm, BINANCE_FUTURES_API,  BINANCE_FUTURES_SECRET, {"enableRateLimit": True}),
        ("upbit",           ccxt.upbit,       UPBIT_API,            UPBIT_SECRET,            {"enableRateLimit": True}),
        ("bithumb",         ccxt.bithumb,     BITHUMB_API,          BITHUMB_SECRET,          {"enableRateLimit": True}),
        ("bybit",           ccxt.bybit,       BYBIT_API,            BYBIT_SECRET,            {"enableRateLimit": True}),
        ("okx",             ccxt.okx,         OKX_API,              OKX_SECRET,              {"enableRateLimit": True}),
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
    python-telegram-bot 20.x 기준 Bot 메서드는 async이므로,
    asyncio.run으로 한 번씩 실행 (메인 루프는 동기).
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
        # 혹시 이미 이벤트 루프가 도는 환경이라면 (일반 Railway에서는 거의 없음)
        # 그냥 에러만 찍고 넘어간다.
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
    ticker를 안전하게 가져오는 래퍼. bid/ask 없으면 예외 발생.
    """
    ticker = ex.fetch_ticker(symbol)
    if "bid" not in ticker or "ask" not in ticker:
        raise RuntimeError(f"ticker 데이터에 bid/ask 없음: {ex.id} {symbol} {ticker}")
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
    바이낸스 현물 BTC/USDT 일간 변동성(%) 계산.
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


def get_binance_spot_price(symbol: str) -> float:
    """
    바이낸스 현물 기준가 (bid) USDT.
    symbol: 'BTC' / 'ETH'
    """
    ex = exchanges["binance"]
    base_pair = f"{symbol}/USDT"
    t = safe_fetch_ticker(ex, base_pair)
    return float(t["bid"])


def get_krw_spread(symbol: str, usdt_krw: float) -> Dict[str, Dict[str, float]]:
    """
    업비트 및 빗썸의 KRW 마켓을 기준으로
    바이낸스 현물(USDT) 대비 스프레드(%) 계산.

    반환 형식:
    {
      "upbit":  {"spread": float, "ask_krw": float},
      "bithumb":{"spread": float, "ask_krw": float},
    }
    """
    result: Dict[str, Dict[str, float]] = {}
    base_price_usdt = get_binance_spot_price(symbol)
    krw_pair = f"{symbol}/KRW"

    for name in ["upbit", "bithumb"]:
        ex = exchanges.get(name)
        if not ex:
            continue
        try:
            t = safe_fetch_ticker(ex, krw_pair)
            ask_krw = float(t["ask"])
            ask_usdt = ask_krw / usdt_krw
            spread = (ask_usdt / base_price_usdt - 1.0) * 100.0
            result[name] = {
                "spread": spread,
                "ask_krw": ask_krw,
                "base_price_usdt": base_price_usdt,
            }
        except Exception as e:
            print(f"[SPREAD] {name} {symbol}/KRW 조회 실패: {e}")

    return result


def est_profit_krw(spread_pct: float, base_price_usdt: float, amount: float, usdt_krw: float) -> float:
    """
    단순 추정 수익(수수료/슬리피지 미반영).
    spread_pct: 스프레드(%)
    """
    profit_usdt = (spread_pct / 100.0) * base_price_usdt * amount
    return profit_usdt * usdt_krw


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


# ==============================
# 5. 전략 구현
# ==============================

def run_spot_arbitrage(symbol: str, amount: float, threshold: float) -> None:
    """
    Binance 현물 vs (Upbit/Bithumb) KRW 김치프 재정거래.
    symbol: 'BTC' / 'ETH'
    """
    global cumulative_profit_krw

    strategy_name = "spot_arbitrage"

    try:
        usdt_krw = get_usdt_krw_rate()
        spreads = get_krw_spread(symbol, usdt_krw)
        if not spreads:
            return

        # 최대 스프레드 거래소 선택
        best_name = max(spreads.keys(), key=lambda k: spreads[k]["spread"])
        best_spread = spreads[best_name]["spread"]
        best_ask_krw = spreads[best_name]["ask_krw"]
        base_price_usdt = spreads[best_name]["base_price_usdt"]

        print(f"[ARBITRAGE] {symbol} best={best_name} spread={best_spread:.2f}% threshold={threshold:.2f}%")

        if best_spread <= threshold:
            return

        # 쿨다운 체크
        if in_cooldown(strategy_name, symbol, best_name, ARBITRAGE_COOLDOWN):
            print(f"[ARBITRAGE] {symbol} {best_name} 쿨다운 중 – 진입 건너뜀")
            return

        binance = exchanges["binance"]
        target_ex = exchanges[best_name]

        # 1) 바이낸스에서 매수
        base_pair = f"{symbol}/USDT"
        create_market_order(binance, base_pair, "buy", amount)

        # 2) 타 거래소에서 같은 수량 매도
        krw_pair = f"{symbol}/KRW"
        create_market_order(target_ex, krw_pair, "sell", amount)

        est_profit = est_profit_krw(best_spread, base_price_usdt, amount, usdt_krw)
        cumulative_profit_krw += est_profit

        touch_trade_time(strategy_name, symbol, best_name)

        msg = (
            f"[{symbol}] 현물 재정거래 실행\n"
            f"- 대상 거래소: {best_name}\n"
            f"- 스프레드: {best_spread:.2f}% (기준 {threshold:.2f}%)\n"
            f"- 수량: {amount} {symbol}\n"
            f"- 기준가(Binance): {base_price_usdt:.2f} USDT\n"
            f"- 매도가({best_name}): {best_ask_krw:,.0f} KRW\n"
            f"- 추정 이익: +{est_profit:,.0f}원\n"
            f"- 누적 추정 이익: +{cumulative_profit_krw:,.0f}원\n"
            f"- DRY_RUN: {DRY_RUN}"
        )
        print(msg)
        send_telegram(msg)

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[ARBITRAGE] {symbol} 오류: {e}\n{tb}")
        send_telegram(f"[ARBITRAGE] {symbol} 오류: {e}")


def run_triangular_arb(ex_name: str) -> None:
    """
    Bybit/OKX 삼각 차익 모니터링 및 (옵션) 소량 테스트 주문.
    DRY_RUN=True 상태에서 테스트 권장.
    """
    strategy_name = "triangular"
    symbol = "BTC-ETH"
    ex = exchanges.get(ex_name)
    if not ex:
        return

    try:
        # BTC/USDT, ETH/USDT, BTC/ETH 세 마켓이 있다고 가정
        t_btc_usdt = safe_fetch_ticker(ex, "BTC/USDT")
        t_eth_usdt = safe_fetch_ticker(ex, "ETH/USDT")
        t_btc_eth = safe_fetch_ticker(ex, "BTC/ETH")

        p_btc_usdt = float(t_btc_usdt["bid"])
        p_eth_usdt = float(t_eth_usdt["bid"])
        p_btc_eth = float(t_btc_eth["bid"])

        loop_val = p_btc_usdt / (p_eth_usdt * p_btc_eth) - 1.0
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
            f"- p(BTC/USDT)={p_btc_usdt}, p(ETH/USDT)={p_eth_usdt}, p(BTC/ETH)={p_btc_eth}\n"
            f"- DRY_RUN: {DRY_RUN} (실매매는 매우 위험, 충분한 테스트 필요)\n"
        )

        # 예시: DRY_RUN 모드에서 소량 루프 흉내
        amount_eth = 0.01
        try:
            create_market_order(ex, "ETH/USDT", "buy", amount_eth)
            # 단순 비율 – 실제로는 훨씬 정교한 루프가 필요
            btc_amount = amount_eth * (p_eth_usdt / p_btc_usdt)
            create_market_order(ex, "BTC/ETH", "sell", btc_amount)
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
    현재는 알림만, 자동 포지션 진입은 없음.
    """
    try:
        ex = exchanges["binance_futures"]
        fr = ex.fetch_funding_rate("BTC/USDT")
        rate = float(fr.get("rate", 0.0))

        print(f"[FUNDING] BTC/USDT rate={rate:.6f}")

        if rate >= MIN_FUNDING_RATE:
            msg = (
                f"[FUNDING] 펀딩비 기회 감지\n"
                f"- Binance Futures BTC/USDT funding rate: {rate:.6f}\n"
                f"- 현재 코드는 알림만 보내고 자동 포지션은 진입하지 않습니다.\n"
            )
            print(msg)
            send_telegram(msg)

    except Exception as e:
        tb = traceback.format_exc()
        print(f"[FUNDING] 오류: {e}\n{tb}")
        send_telegram(f"[FUNDING] 오류: {e}")


# ==============================
# 6. 메인 루프
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

            # 2) BTC 현물 재정거래
            run_spot_arbitrage("BTC", BTC_AMOUNT, threshold)

            # 3) ETH 현물 재정거래
            run_spot_arbitrage("ETH", ETH_AMOUNT, threshold)

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
