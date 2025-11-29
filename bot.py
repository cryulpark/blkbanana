import ccxt
import time
import telegram  # pip install python-telegram-bot
from telegram.ext import Application

# API 키 입력 (Railway 환경 변수로)
BINANCE_API = 'YOUR_BINANCE_KEY'
BINANCE_SECRET = 'YOUR_BINANCE_SECRET'
UPBIT_API = 'YOUR_UPBIT_KEY'
UPBIT_SECRET = 'YOUR_UPBIT_SECRET'
BITHUMB_API = 'YOUR_BITHUMB_KEY'
BITHUMB_SECRET = 'YOUR_BITHUMB_SECRET'
BYBIT_API = 'YOUR_BYBIT_KEY'
BYBIT_SECRET = 'YOUR_BYBIT_SECRET'
OKX_API = 'YOUR_OKX_KEY'
OKX_SECRET = 'YOUR_OKX_SECRET'
TELEGRAM_TOKEN = 'YOUR_TELEGRAM_TOKEN'
CHAT_ID = 'YOUR_CHAT_ID'

# 거래소 연결
exchanges = {
    'binance': ccxt.binance({'apiKey': BINANCE_API, 'secret': BINANCE_SECRET}),
    'upbit': ccxt.upbit({'apiKey': P5Z7vxjkM4XNlvrb7VAdfMmgo5usBz30cxAMikDH, 'secret': FcDJdwrNLL4HFg5ZI5Sx3ZAIyOKxvJYe7xXOGCh4}),
    'bithumb': ccxt.bithumb({'apiKey': BITHUMB_API, 'secret': BITHUMB_SECRET}),
    'bybit': ccxt.bybit({'apiKey': BYBIT_API, 'secret': BYBIT_SECRET}),
    'okx': ccxt.okx({'apiKey': OKX_API, 'secret': OKX_SECRET}),
}

app = Application.builder().token(TELEGRAM_TOKEN).build()

def send_telegram(message):
    app.bot.send_message(chat_id=CHAT_ID, text=message)

def get_spread(base_ex = 'binance', target_ex = 'upbit', pair = 'BTC/USDT', krw_pair = 'BTC/KRW'):
    try:
        btc_base = exchanges[base_ex].fetch_ticker(pair)['bid']
        btc_target = exchanges[target_ex].fetch_ticker(krw_pair)['ask'] / 1350
        spread = (btc_target / btc_base - 1) * 100
        return spread
    except Exception as e:
        send_telegram(f"오류: {e}")
        return 0

# 메인 루프 (풀세트 자동)
while True:
    try:
        spreads = {
            'upbit': get_spread('binance', 'upbit'),
            'bithumb': get_spread('binance', 'bithumb'),
            'bybit': get_spread('binance', 'bybit', 'BTC/USDT', 'BTC/USDT'),
            'okx': get_spread('binance', 'okx', 'BTC/USDT', 'BTC/USDT'),
        }
        max_spread_ex = max(spreads, key=spreads.get)
        spread = spreads[max_spread_ex]
        if spread > 2.5:
            # 자동 arbitrage 실행 (예시, 실제 금액 조정)
            amount = 0.001
            exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
            exchanges[max_spread_ex].create_market_sell_order('BTC/KRW', 1350 * amount * (btc_base + 0.01 * btc_base))
            profit = spread * amount * 20000
            send_telegram(f"Arbitrage 실행! {max_spread_ex} Spread {spread:.2f}% - 예상 수익 +{profit:.0f}원")
        time.sleep(60)
    except Exception as e:
        send_telegram(f"봇 재시작: {e}")
        time.sleep(10)

if __name__ == '__main__':
    send_telegram("까망빠나나 시작! Railway 도쿄에서 24/7 실행 중.")
    # 루프 실행
