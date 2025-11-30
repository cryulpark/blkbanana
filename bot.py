import ccxt
import time
import telegram  # pip install python-telegram-bot
import os
import asyncio

# 환경 변수 로드
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

# 거래소 연결
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
        pass

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
        try:
            return exchanges['bithumb'].fetch_ticker('USDT/KRW')['bid']
        except:
            return 1350

async def get_spread(target_ex, pair, krw_pair):
    try:
        base_price = exchanges['binance'].fetch_ticker(pair)['bid']
        target_price = exchanges[target_ex].fetch_ticker(krw_pair)['ask'] / await get_exchange_rate()
        return (target_price / base_price - 1) * 100
    except:
        return 0

# 메인 루프
async def main():
    await send_telegram("까망빠나나 시작! BTC + ETH arbitrage 24/7 실행 중.")
    last_status = time.time()
    while True:
        try:
            # BTC arbitrage
            spreads_btc = {}
            for target in ['upbit', 'bithumb', 'bybit', 'okx']:
                spreads_btc[target] = await get_spread(target, 'BTC/USDT', 'BTC/KRW')
            
            max_ex_btc = max(spreads_btc, key=spreads_btc.get)
            spread_btc = spreads_btc[max_ex_btc]
            if spread_btc > 2.5:
                amount = 0.001
                exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
                exchanges[max_ex_btc].create_market_sell_order('BTC/KRW', (await get_exchange_rate()) * amount * 1.01)
                await send_telegram(f"BTC 실행! {max_ex_btc} Spread {spread_btc:.2f}% - 수익 +{spread_btc * amount * 20000:.0f}원")

            # ETH arbitrage
            spreads_eth = {}
            for target in ['upbit', 'bithumb', 'bybit', 'okx']:
                spreads_eth[target] = await get_spread(target, 'ETH/USDT', 'ETH/KRW')
            
            max_ex_eth = max(spreads_eth, key=spreads_eth.get)
            spread_eth = spreads_eth[max_ex_eth]
            if spread_eth > 2.5:
                amount = 0.001
                exchanges['binance'].create_market_buy_order('ETH/USDT', amount)
                exchanges[max_ex_eth].create_market_sell_order('ETH/KRW', (await get_exchange_rate()) * amount * 1.01)
                await send_telegram(f"ETH 실행! {max_ex_eth} Spread {spread_eth:.2f}% - 수익 +{spread_eth * amount * 20000:.0f}원")

            # 한 시간마다 상태 알림
            if time.time() - last_status >= 3600:
                await send_telegram("상태: 정상 가동 중.")
                last_status = time.time()

            await asyncio.sleep(300)  # 5분 루프
        except Exception as e:
            await asyncio.sleep(10)
