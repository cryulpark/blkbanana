import ccxt
import time
import telegram
import os
import asyncio

# 환경 변수 로드
required_env = ['BINANCE_API_KEY', 'BINANCE_SECRET', 'UPBIT_API_KEY', 'UPBIT_SECRET', 'BITHUMB_API_KEY', 'BITHUMB_SECRET', 'BYBIT_API_KEY', 'BYBIT_SECRET', 'OKX_API_KEY', 'OKX_SECRET', 'TELEGRAM_TOKEN', 'CHAT_ID']
env = {k: os.environ.get(k) for k in required_env}
if None in env.values():
    raise ValueError("환경 변수 누락")

# 거래소 연결
exchanges = {}
for name, api, secret in [
    ('binance', env['BINANCE_API_KEY'], env['BINANCE_SECRET']),
    ('upbit', env['UPBIT_API_KEY'], env['UPBIT_SECRET']),
    ('bithumb', env['BITHUMB_API_KEY'], env['BITHUMB_SECRET']),
    ('bybit', env['BYBIT_API_KEY'], env['BYBIT_SECRET']),
    ('okx', env['OKX_API_KEY'], env['OKX_SECRET'])
]:
    try:
        exchanges[name] = getattr(ccxt, name)({'apiKey': api, 'secret': secret, 'enableRateLimit': True})
    except:
        pass

bot = telegram.Bot(token=env['TELEGRAM_TOKEN'])

async def send_telegram(message):
    try:
        await bot.send_message(chat_id=env['CHAT_ID'], text=message)
    except:
        pass

async def get_exchange_rate():
    for ex in ['upbit', 'bithumb']:
        try:
            return exchanges[ex].fetch_ticker('USDT/KRW')['bid']
        except:
            pass
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
    await send_telegram("까망빠나나 시작! 24/7 가동 중.")
    last_status = time.time()
    while True:
        try:
            spreads = {target: await get_spread(target, 'BTC/USDT', 'BTC/KRW') for target in ['upbit', 'bithumb', 'bybit', 'okx']}
            max_ex = max(spreads, key=spreads.get)
            spread = spreads[max_ex]
            if spread > 2.5:
                amount = 0.001
                exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
                exchanges[max_ex].create_market_sell_order('BTC/KRW', (await get_exchange_rate()) * amount * 1.01)
                await send_telegram(f"실행! {max_ex} Spread {spread:.2f}% - 수익 +{spread * amount * 20000:.0f}원")

            if time.time() - last_status >= 3600:
                await send_telegram("상태: 정상 가동 중.")
                last_status = time.time()

            await asyncio.sleep(300)  # 5분 루프
        except Exception as e:
            await asyncio.sleep(10)

asyncio.run(main())
