import ccxt
import time
import telegram
import os
import asyncio

# API 키 환경 변수로만 읽기 (누락 시 기본값으로 fallback, 안전 강화)
BINANCE_API = os.environ.get('BINANCE_API_KEY', '')
BINANCE_SECRET = os.environ.get('BINANCE_SECRET', '')
UPBIT_API = os.environ.get('UPBIT_API_KEY', '')
UPBIT_SECRET = os.environ.get('UPBIT_SECRET', '')
BITHUMB_API = os.environ.get('BITHUMB_API_KEY', '')
BITHUMB_SECRET = os.environ.get('BITHUMB_SECRET', '')
BYBIT_API = os.environ.get('BYBIT_API_KEY', '')
BYBIT_SECRET = os.environ.get('BYBIT_SECRET', '')
OKX_API = os.environ.get('OKX_API_KEY', '')
OKX_SECRET = os.environ.get('OKX_SECRET', '')
TELEGRAM_TOKEN = os.environ.get('TELEGRAM_TOKEN', '')
CHAT_ID = os.environ.get('CHAT_ID', '')

# 거래소 연결
exchanges = {}
try:
    exchanges['binance'] = ccxt.binance({'apiKey': BINANCE_API, 'secret': BINANCE_SECRET})
except Exception as e:
    print(f"Binance 연결 오류: {e}")

try:
    exchanges['upbit'] = ccxt.upbit({'apiKey': UPBIT_API, 'secret': UPBIT_SECRET})
except Exception as e:
    print(f"Upbit 연결 오류: {e}")

try:
    exchanges['bithumb'] = ccxt.bithumb({'apiKey': BITHUMB_API, 'secret': BITHUMB_SECRET})
except Exception as e:
    print(f"Bithumb 연결 오류: {e}")

try:
    exchanges['bybit'] = ccxt.bybit({'apiKey': BYBIT_API, 'secret': BYBIT_SECRET})
except Exception as e:
    print(f"Bybit 연결 오류: {e}")

try:
    exchanges['okx'] = ccxt.okx({'apiKey': OKX_API, 'secret': OKX_SECRET})
except Exception as e:
    print(f"OKX 연결 오류: {e}")

bot = telegram.Bot(token=TELEGRAM_TOKEN)

async def send_telegram(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        print(f"텔레그램 오류: {e}")

async def get_exchange_rate(base_ex='binance'):
    try:
        usd_krw = exchanges[base_ex].fetch_ticker('USDT/KRW')['bid']
        return usd_krw
    except Exception as e:
        await send_telegram(f"환율 오류: {e}")
        return 1350

async def get_spread(base_ex = 'binance', target_ex = 'upbit', pair = 'BTC/USDT', krw_pair = 'BTC/KRW'):
    try:
        btc_base = exchanges[base_ex].fetch_ticker(pair)['bid']
        btc_target = exchanges[target_ex].fetch_ticker(krw_pair)['ask'] / await get_exchange_rate()
        spread = (btc_target / btc_base - 1) * 100
        return spread
    except Exception as e:
        await send_telegram(f"{target_ex} Spread 오류: {e}")
        return 0

# 메인 루프 (풀세트 자동, 안전 모드)
async def main():
    await send_telegram("까망빠나나 시작! Railway 도쿄에서 24/7 실행 중.")
    while True:
        try:
            spreads = {}
            for target in ['upbit', 'bithumb', 'bybit', 'okx']:
                spreads[target] = await get_spread('binance', target)
            
            max_spread_ex = max(spreads, key=spreads.get)
            spread = spreads[max_spread_ex]
            if spread > 2.5:
                amount = 0.001  # 최대 0.001 BTC (약 200만 원, 안전 제한)
                exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
                exchanges[max_spread_ex].create_market_sell_order('BTC/KRW', (await get_exchange_rate()) * amount * (btc_base + 0.01 * btc_base))
                profit = spread * amount * 20000
                await send_telegram(f"Arbitrage 실행! {max_spread_ex} Spread {spread:.2f}% - 예상 수익 +{profit:.0f}원")
            await asyncio.sleep(60)
        except Exception as e:
            await send_telegram(f"봇 재시작: {e}")
            await asyncio.sleep(10)  # 안전 재시작 딜레이

asyncio.run(main())
