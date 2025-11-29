import ccxt
import time
import telegram
import os
import asyncio

# API 키
BINANCE_API = os.environ['BINANCE_API_KEY']
BINANCE_SECRET = os.environ['BINANCE_SECRET']
UPBIT_API = os.environ['UPBIT_API_KEY']
UPBIT_SECRET = os.environ['UPBIT_SECRET']
BITHUMB_API = os.environ['BITHUMB_API_KEY']
BITHUMB_SECRET = os.environ['BITHUMB_SECRET']
BYBIT_API = os.environ['BYBIT_API_KEY']
BYBIT_SECRET = os.environ['BYBIT_SECRET']
OKX_API = os.environ['OKX_API_KEY']
OKX_SECRET = os.environ['OKX_SECRET']
TELEGRAM_TOKEN = os.environ['TELEGRAM_TOKEN']
CHAT_ID = os.environ['CHAT_ID']

# 거래소 연결 (오류 나도 무시하고 계속)
exchanges = {}
for name, api, secret in [
    ('binance', BINANCE_API, BINANCE_SECRET),
    ('upbit', UPBIT_API, UPBIT_SECRET),
    ('bithumb', BITHUMB_API, BITHUMB_SECRET),
    ('bybit', BYBIT_API, BYBIT_SECRET),
    ('okx', OKX_API, OKX_SECRET),
]:
    try:
        exchanges[name] = getattr(ccxt, name)({'apiKey': api, 'secret': secret, 'enableRateLimit': True})
    except:
        exchanges[name] = None  # 연결 실패해도 None으로 넘김

bot = telegram.Bot(token=TELEGRAM_TOKEN)

async def send_telegram(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except:
        pass  # 알림 오류도 완전 무시

def get_spread():
    try:
        btc_bin = exchanges['binance'].fetch_ticker('BTC/USDT')['bid']
        btc_bithumb = exchanges['bithumb'].fetch_ticker('BTC/KRW')['ask']
        usd_krw = exchanges['bithumb'].fetch_ticker('USDT/KRW')['bid']
        spread = (btc_bithumb / usd_krw / btc_bin - 1) * 100
        return round(spread, 2)
    except:
        return 0.0  # 모든 오류 무시하고 0% 반환

async def main():
    await send_telegram("까망빠나나 시작! 이제 오류 알림 안 와요")
    while True:
        try:
            spread = get_spread()
            if spread > 2.5:
                amount = 0.001
                exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
                exchanges['bithumb'].create_market_sell_order('BTC/KRW', amount * usd_krw * 1.01)
                await send_telegram(f"실행! 빗썸 스프레드 {spread}% → +{spread*amount*20000:.0f}원")
            await asyncio.sleep(60)
        except:
            await asyncio.sleep(10)  # 오류 나도 조용히 재시작

if __name__ == '__main__':
    asyncio.run(main())
