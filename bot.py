import ccxt
import time
import telegram  # pip install python-telegram-bot
import os
import asyncio

# API 키 환경 변수로만 읽기 (하드코딩 금지, 보안 강화)
try:
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
except KeyError as e:
    raise ValueError(f"환경 변수 누락: {e} – Railway Variables 확인하세요")

# 거래소 연결 (enableRateLimit으로 정지 방지, 각 연결 try로 안정)
exchanges = {}
for ex_name, api, secret in [
    ('binance', BINANCE_API, BINANCE_SECRET),
    ('upbit', UPBIT_API, UPBIT_SECRET),
    ('bithumb', BITHUMB_API, BITHUMB_SECRET),
    ('bybit', BYBIT_API, BYBIT_SECRET),
    ('okx', OKX_API, OKX_SECRET)
]:
    try:
        exchanges[ex_name] = getattr(ccxt, ex_name)({'apiKey': api, 'secret': secret, 'enableRateLimit': True})
    except Exception as e:
        print(f"{ex_name.capitalize()} 연결 오류: {e}")

bot = telegram.Bot(token=TELEGRAM_TOKEN)

async def send_telegram(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        print(f"텔레그램 오류: {e}")

async def get_exchange_rate():
    try:
        # 업비트 우선 (KRW 정확), 오류 시 빗썸 fallback
        return exchanges['upbit'].fetch_ticker('USDT/KRW')['bid']
    except:
        try:
            return exchanges['bithumb'].fetch_ticker('USDT/KRW')['bid']
        except Exception as e:
            await send_telegram(f"환율 오류: {e} – 기본 1350 사용")
            return 1350

async def get_spread(target_ex, pair, krw_pair):
    try:
        base_price = exchanges['binance'].fetch_ticker(pair)['bid']
        target_price = exchanges[target_ex].fetch_ticker(krw_pair)['ask'] / await get_exchange_rate()
        spread = (target_price / base_price - 1) * 100
        return spread
    except Exception as e:
        await send_telegram(f"{target_ex} Spread 오류: {e}")
        return 0

async def get_volatility(pair='BTC/USDT'):
    try:
        ohlcv = exchanges['binance'].fetch_ohlcv(pair, '1d', limit=2)
        return abs((ohlcv[1][4] - ohlcv[0][4]) / ohlcv[0][4] * 100)
    except:
        return 0

async def get_funding_rate(pair='BTC/USDT'):
    try:
        return exchanges['binance'].fetch_funding_rate(pair)['rate']
    except:
        return 0

# 메인 루프 (풀세트 자동, 안전 모드, BTC + ETH 추가)
async def main():
    await send_telegram("까망빠나나 시작! Railway 도쿄에서 24/7 실행 중.")
    last_status_time = time.time()
    while True:
        try:
            volatility = await get_volatility()
            threshold = 2.5 if volatility < 10 else 2.0  # 변동성 높을 때 문턱 낮춰 기회 최적화
            # BTC arbitrage
            spreads_btc = {}
            for target in ['upbit', 'bithumb', 'bybit', 'okx']:
                spreads_btc[target] = await get_spread(target, 'BTC/USDT', 'BTC/KRW')
                await asyncio.sleep(2)  # 요청 간 딜레이 (rate limit 방지)
            
            max_spread_ex_btc = max(spreads_btc, key=spreads_btc.get)
            spread_btc = spreads_btc[max_spread_ex_btc]
            if spread_btc > threshold:
                amount = 0.001  # 최대 0.001 BTC (약 200만 원, 안전 제한)
                leverage = 1 if volatility > 20 else 3  # 변동성 20% 초과 시 레버리지 1배로 안전
                exchanges['binance'].set_leverage(leverage, 'BTC/USDT')
                exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
                exchanges[max_spread_ex_btc].create_market_sell_order('BTC/KRW', (await get_exchange_rate()) * amount * (btc_base + 0.01 * btc_base))
                profit = spread_btc * amount * 20000 * leverage
                await send_telegram(f"BTC 실행! {max_spread_ex_btc} Spread {spread_btc:.2f}% - 레버리지 {leverage}배 - 수익 +{profit:.0f}원")

            # ETH arbitrage (추가)
            spreads_eth = {}
            for target in ['upbit', 'bithumb', 'bybit', 'okx']:
                spreads_eth[target] = await get_spread(target, 'ETH/USDT', 'ETH/KRW')
                await asyncio.sleep(2)  # rate limit

            max_spread_ex_eth = max(spreads_eth, key=spreads_eth.get)
            spread_eth = spreads_eth[max_spread_ex_eth]
            if spread_eth > threshold:
                amount = 0.001  # 최대 0.001 ETH (약 200만 원)
                leverage = 1 if volatility > 20 else 3
                exchanges['binance'].set_leverage(leverage, 'ETH/USDT')
                exchanges['binance'].create_market_buy_order('ETH/USDT', amount)
                exchanges[max_spread_ex_eth].create_market_sell_order('ETH/KRW', (await get_exchange_rate()) * amount * (eth_base + 0.01 * eth_base))
                profit = spread_eth * amount * 20000 * leverage
                await send_telegram(f"ETH 실행! {max_spread_ex_eth} Spread {spread_eth:.2f}% - 레버리지 {leverage}배 - 수익 +{profit:.0f}원")

            funding = await get_funding_rate()
            if funding > 0.01:
                amount = 0.001
                exchanges['binance'].create_market_sell_order('BTC/USDT', amount, {'type': 'future'})
                profit = funding * amount * 20000 * 3  # 8시간 3회 가정
                await send_telegram(f"Funding 실행! Rate {funding:.4f}% - 이자 +{profit:.0f}원")

            # 한 시간마다 상태 알림 (꾸준함 확인)
            if time.time() - last_status_time >= 3600:
                await send_telegram(f"상태 확인: 정상. 누적 수익 +{last_profit:.0f}원")
                last_status_time = time.time()

            await asyncio.sleep(300)  # 5분 루프 (rate limit 최적화, 꾸준함 강화)
        except Exception as e:
            await send_telegram(f"재시작: {e}")
            await asyncio.sleep(10)
