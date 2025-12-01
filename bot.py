import os
import time
import json
import requests
from datetime import datetime, timezone, date
import ccxt
from ccxt.base.errors import AuthenticationError

###############################################################################
# SETTINGS
###############################################################################

DRY_RUN = True                 # 실매매 전에는 반드시 True 유지
MAIN_LOOP_INTERVAL = 60        # 1분 루프

# ─ 리스크 관리 (동적 2% 손실 한도) ─
MAX_DAILY_LOSS_RATIO = 0.02    # "현재 자본의 2%"까지 일일 손실 허용 (복리 반영)
STATE_FILE = "kimchi_bot_state.json"

# ─ Layer ON/OFF ─
ENABLE_LAYER_SPREAD_ARB   = True   # Binance vs KRW 김프/역프
ENABLE_LAYER_KRW_CROSS    = True   # 업비트 vs 빗썸 KRW 차익
ENABLE_LAYER_FUNDING_SIG  = True   # 펀딩 아비트 (실제 포지션 + 자동 청산)
ENABLE_LAYER_TRI_MONITOR  = True   # 삼각차익 모니터

# ─ 기본 김프/역프 레이어 (성장형) ─
# Tier1: 굵은 김프 (강하게 진입)
TIER1_THR_MIN = 1.0            # 변동성 낮을 때 최소 1.0%
TIER1_THR_MAX = 1.4            # 변동성 높을 때 1.4%까지

# Tier2: 얕은 김프 (소액 비중으로 진입)
TIER2_THR = 0.5                # 0.5% 이상이면 Tier2 후보

# 자본 비율 (성장형: 한 번 진입에 40~75%)
BASE_RATIO_MIN = 0.4           # 최소 40%
BASE_RATIO_MAX = 0.75          # 최대 75%
TIER2_RATIO_FACTOR = 0.3       # Tier2는 기본 ratio의 30%만 사용

# per-trade 최소 노치널 (KRW)
MIN_NOTIONAL_KRW = 50000       # 5만 미만은 거래 안 함

# 1시간 거래 횟수 제한 (성장형: 40회까지)
MAX_TRADES_1H = 40

# ─ 업비트 ↔ 빗썸 KRW 차익 레이어 (성장형) ─
KRW_ARB_THR = 0.18             # 명목 스프레드 0.18% 이상이면 후보
KRW_ARB_RATIO = 0.2            # 업빗/빗썸 차익거래에 계좌의 20%까지 사용

# ─ Funding 레이어(실제 포지션 진입 + 자동 청산, 성장형) ─
FUTURES_SYMBOL = "BTC/USDT:USDT"   # ccxt 통일 심볼 (Binance/Bybit/OKX USDT 선물)

FUNDING_SPREAD_THR_OPEN  = 0.015   # 진입 기준: 스프레드 1.5% 이상
FUNDING_SPREAD_THR_CLOSE = 0.004   # 청산 기준: 스프레드 0.4% 이하

FUNDING_ARB_RATIO            = 0.10    # 각 선물 계좌 USDT의 10% 사용
FUNDING_MIN_NOTIONAL_USDT    = 100.0   # 100 USDT 미만이면 진입 안 함
FUNDING_TARGET_PAYMENTS      = 3       # 목표 펀딩 횟수 (3번 펀딩 받으면 청산)
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
# 기본 티어 가정 (필요시 조정)
FEE_RATES = {
    "binance": 0.0004,     # 0.04%
    "upbit":   0.0005,     # 0.05%
    "bithumb": 0.0005,     # 0.05%
    "bybit":   0.0006,
    "okx":     0.0005,
}
DEFAULT_FEE_RATE = 0.0005

# 수수료 + 슬리피지 + 최소 순이익 여유 (단위: %)
EDGE_BUFFER_FEE_PCT       = 0.20   # 왕복 수수료 추정치 (0.20%)
EDGE_BUFFER_SLIPPAGE_PCT  = 0.10   # 슬리피지 여유 (0.10%)
EDGE_MIN_NET_PCT          = 0.10   # 순이익 최소 요구 0.10% (성장형: 기존 0.15% → 완화)

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

ex = {}      # 현물/스팟 계정
ex_fut = {}  # 선물/스왑 계정

TRADE_TIMES = []                     # 최근 1시간 트레이드 타임스탬프
price_history = {"upbit": [], "bithumb": []}

disable_trading = False

# 자본 추정 (동적 손실 한도용) – 초기 대략치 (네 자본 기준)
LAST_EQUITY_KRW = 21500000.0   # ≈ 2,150만 원

# STATE: PnL/수수료/트레이드 + 일/주간 집계
STATE = {
    "date": None,                   # "YYYY-MM-DD" (UTC 기준, 일간 구분용)

    "realized_pnl_krw": 0.0,        # 누적 실현손익
    "realized_pnl_krw_daily": 0.0,  # 당일 실현손익
    "realized_pnl_krw_weekly": 0.0, # 주간 실현손익

    "fees_krw": 0.0,                # 누적 수수료
    "fees_krw_daily": 0.0,          # 당일 수수료
    "fees_krw_weekly": 0.0,         # 주간 수수료

    "num_trades": 0,                # 누적 트레이드 수
    "num_trades_daily": 0,          # 당일 트레이드 수
    "num_trades_weekly": 0,         # 주간 트레이드 수

    "weekly_start_date": None,      # 주간 집계 시작일 (YYYY-MM-DD)
}

# 현재 열린 펀딩 아비트 포지션(1쌍만 운용)
FUNDING_POS = {
    "active": False,
    "short_ex": None,         # "binance_fut" / "bybit_fut" / "okx_fut"
    "long_ex":  None,
    "symbol":   FUTURES_SYMBOL,
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

DEFAULT_STATE = STATE.copy()

def load_state():
    global STATE
    try:
        if os.path.exists(STATE_FILE):
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            STATE = DEFAULT_STATE.copy()
            STATE.update(data)
            print(f"[STATE] Loaded from {STATE_FILE}: {STATE}")
        else:
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            STATE["date"] = today
            STATE["weekly_start_date"] = today
            save_state()
    except Exception as e:
        print(f"[STATE] load ERR {e}")

def save_state():
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(STATE, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"[STATE] save ERR {e}")

###############################################################################
# FX / EQUITY / REPORTING
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

def estimate_total_equity_krw() -> float:
    """
    현재 보유 자산(업비트/빗썸 KRW + BTC/ETH, 바이낸스/OKX/바이비트 USDT 등)을
    대략 KRW로 환산. 손실 한도 계산용으로 쓰기 때문에,
    약간 보수적으로 추정해도 괜찮다.
    """
    global LAST_EQUITY_KRW

    try:
        usdt_krw = get_usdt_krw()

        # 기준 가격: 바이낸스 BTC/USDT, ETH/USDT
        b = ex.get("binance")
        if not b:
            return LAST_EQUITY_KRW

        t_btc = safe_ticker(b, "BTC/USDT")
        t_eth = safe_ticker(b, "ETH/USDT")
        btc_usdt = float(t_btc["last"])
        eth_usdt = float(t_eth["last"])

        total_krw = 0.0

        # 업비트 / 빗썸: KRW + BTC + ETH
        for name in ["upbit", "bithumb"]:
            inst = ex.get(name)
            if not inst:
                continue
            try:
                bal = inst.fetch_balance()
            except Exception as e:
                print(f"[EQ] {name} balance ERR {e}")
                continue

            krw = float(bal.get("KRW", {}).get("total", 0) or 0)
            btc = float(bal.get("BTC", {}).get("total", 0) or 0)
            eth = float(bal.get("ETH", {}).get("total", 0) or 0)
            total_krw += krw
            total_krw += btc * btc_usdt * usdt_krw
            total_krw += eth * eth_usdt * usdt_krw

        # 바이낸스 spot: USDT + BTC + ETH
        inst = ex.get("binance")
        if inst:
            try:
                bal = inst.fetch_balance()
                usdt = float(bal.get("USDT", {}).get("total", 0) or 0)
                btc  = float(bal.get("BTC", {}).get("total", 0) or 0)
                eth  = float(bal.get("ETH", {}).get("total", 0) or 0)
                total_krw += usdt * usdt_krw
                total_krw += btc * btc_usdt * ust
