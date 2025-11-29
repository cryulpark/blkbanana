import ccxt
import time
import telegram  # pip install python-telegram-bot
from telegram.ext import Application

# API 키 입력 (Railway 환경 변수로)
BINANCE_API = 'YOUR_BINANCE_KEY'
BINANCE_SECRET = 'YOUR_BINANCE_SECRET'
UPBIT_API = 'YOUR_UPBIT_KEY'
UPBIT_SECRET = 'YOUR_UPBIT_SECRET'
TELEGRAM_TOKEN = 'YOUR_TELEGRAM_TOKEN'
CHAT_ID = 'YOUR_CHAT_ID'

binance = ccxt.binance({'apiKey': BINANCE_API, 'secret': BINANCE_SECRET})
upbit = ccxt.upbit({'apiKey': UPBIT_API, 'secret': UPBIT_SECRET})

app = Application.builder().token(TELEGRAM_TOKEN).build()

def send_telegram(message):
    app.bot.send_message(chat_id=CHAT_ID, text=message)

def get_spread():
    try:
        btc_bin = binance.fetch_ticker('BTC/USDT')['bid']
        btc_up = upbit.fetch_ticker('BTC/KRW')['ask'] / 1350
        spread = (btc_up / btc_bin - 1) * 100
        return spread
    except Exception as e:
        send_telegram(f"오류: {e}")
        return 0  # 에러 시 재시작 트리거

def execute_arbitrage(spread):
    if spread > 2.5:
        # 간단 arbitrage: 바이낸스 매수 + 업비트 매도 (실제 금액 조정)
        binance.create_market_buy_order('BTC/USDT', 0.001)
        upbit.create_market_sell_order('BTC/KRW', 1350 * 0.001 * (btc_bin + 0.01 * btc_bin))  # 1% 이익 가정
        profit = spread * 0.001 * 20000  # 2천만 원 기준 추정
        send_telegram(f"Arbitrage 실행! Spread {spread:.2f}% - 예상 수익 +{profit:.0f}원")
    # Grid/Sentiment 추가 로직 (이전 샘플처럼 확장 가능)

# 메인 루프 (에러 시 자동 재시작)
while True:
    try:
        spread = get_spread()
        execute_arbitrage(spread)
        time.sleep(60)  # 1분 스캔
    except Exception as e:
        send_telegram(f"봇 재시작: {e}")
        time.sleep(10)  # 10초 후 재시작

if __name__ == '__main__':
    send_telegram("봇 시작! Railway 도쿄에서 24/7 실행 중.")
    # 루프 실행
