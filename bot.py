import ccxt
import time
import telegram
import os
import asyncio

# API 키 환경 변수
BINANCE_API = os.environ.get('BINANCE_API_KEY')
BINANCE_SECRET = os.environ.get('BINANCE_SECRET')
UPBIT_API = os.environ.get('UPBIT_API_KEY')
UPBIT_SECRET = os.environ.get('UPBIT_SECRET')
BITHUMB_API = os.environ.get('BITHUMB_API_KEY')
BITHUMB_SECRET = os.environ.get('BITHUMB_SECRET')
BYBIT_API = os.environ.get('BYBIT_API_KEY')
BYBIT_SECRET = os.environ.get('BYBIT_SECRET')
OKX_API = os.environ.get('OKX_API_KEY')
OKX_SECRET = os.environ.get('OKX_SECRET')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN')
CHAT_ID = os.environ.get('CHAT_ID')

exchanges = {
    'binance': ccxt.binance({'apiKey': BINANCE_API, 'secret': BINANCE_SECRET, 'enableRateLimit': True}),
    'upbit': ccxt.upbit({'apiKey': UPBIT_API, 'secret': UPBIT_SECRET}),
    'bithumb': ccxt.bithumb({'apiKey': BITHUMB_API, 'secret': BITHUMB_SECRET}),
    'bybit': ccxt.bybit({'apiKey': BYBIT_API, 'secret': BYBIT_SECRET}),
    'okx': ccxt.okx({'apiKey': OKX_API, 'secret': OKX_SECRET}),
}

bot = telegram.Bot(token=TELEGRAM_TOKEN)

async def send_telegram(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except:
        pass

async def get_exchange_rate():
    try:
        return exchanges['upbit'].fetch_ticker('USDT/KRW')['bid']
    except:
        return 1350

async def get_spread(target_ex, pair='BTC/USDT', krw_pair='BTC/KRW'):
    try:
        base_price = exchanges['binance'].fetch_ticker(pair)['bid']
        target_price = exchanges[target_ex].fetch_ticker(krw_pair)['ask'] / await get_exchange_rate()
        spread = (target_price / base_price - 1) * 100
        return spread
    except:
        return 0

async def main():
    await send_telegram("까망빠나나 시작! Rate limit 안전 모드로 실행 중.")
    while True:
        try:
            spreads = {}
            for target in ['upbit', 'bithumb', 'bybit', 'okx']:
                spreads[target] = await get_spread(target)
                await asyncio.sleep(2)  # 요청 간 딜레이 (rate limit 방지)
            
            max_spread_ex = max(spreads, key=spreads.get)
            spread = spreads[max_spread_ex]
            if spread > 2.5:
                amount = 0.001
                exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
                exchanges[max_spread_ex].create_market_sell_order('BTC/KRW', await get_exchange_rate() * amount * 1.01)
                await send_telegram(f"실행! {max_spread_ex} Spread {spread:.2f}% - 수익 +{spread*amount*20000:.0f}원")
            await asyncio.sleep(300)  # 루프 5분으로 변경 (rate limit 안전)
        except Exception as e:
            await send_telegram(f"재시작: {e}")
            await asyncio.sleep(10)

asyncio.run(main())
