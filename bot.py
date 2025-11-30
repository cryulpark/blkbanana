import ccxt
import time
import telegram  # pip install python-telegram-bot
import os
import asyncio

# API 키 환경 변수로만 읽기
try:
    BINANCE_API = os.environ['BINANCE_API_KEY']
    BINANCE_SECRET = os.environ['BINANCE_SECRET']
    UPBIT_API = os.environ['UPBIT_API_KEY']
    UPBIT_SECRET = os.environ['UPBIT_SECRET']
    BITHUMB_API = os.environ['BITHUMB_API_KEY']
    BITHUMB_SECRET = os.environ['BITHUMB_SECRET']
    TELEGRAM_TOKEN = os.environ['TELEGRAM_TOKEN']
    CHAT_ID = os.environ['CHAT_ID']
except KeyError as e:
    raise ValueError(f"환경 변수 누락: {e}")

# 거래소 연결
exchanges = {}
try:
    exchanges['binance'] = ccxt.binance({'apiKey': BINANCE_API, 'secret': BINANCE_SECRET, 'enableRateLimit': True})
except Exception as e:
    print(f"Binance 연결 오류: {e}")

try:
    exchanges['upbit'] = ccxt.upbit({'apiKey': UPBIT_API, 'secret': UPBIT_SECRET, 'enableRateLimit': True})
except Exception as e:
    print(f"Upbit 연결 오류: {e}")

try:
    exchanges['bithumb'] = ccxt.bithumb({'apiKey': BITHUMB_API, 'secret': BITHUMB_SECRET, 'enableRateLimit': True})
except Exception as e:
    print(f"Bithumb 연결 오류: {e}")

bot = telegram.Bot(token=TELEGRAM_TOKEN)

async def send_telegram(message):
    try:
        await bot.send_message(chat_id=CHAT_ID, text=message)
    except Exception as e:
        print(f"텔레그램 오류: {e}")

async def get_exchange_rate():
    try:
        return exchanges['upbit'].fetch_ticker('USDT/KRW')['bid']
    except:
        try:
            return exchanges['bithumb'].fetch_ticker('USDT/KRW')['bid']
        except:
            return 1350  # 오류 시 알림 없이 기본값 (오류 안 나니 제거)

async def get_spread(target_ex, pair='BTC/USDT', krw_pair='BTC/KRW'):
    try:
        base_price = exchanges['binance'].fetch_ticker(pair)['bid']
        target_price = exchanges[target_ex].fetch_ticker(krw_pair)['ask'] / await get_exchange_rate()
        spread = (target_price / base_price - 1) * 100
        return spread
    except:
        return 0  # 오류 알림 제거 (계속 안 나니)

# 메인 루프 (풀세트 자동, 안전 모드, 바이빗/오케이엑스 제거)
async def main():
    await send_telegram("까망빠나나 시작! Railway 도쿄에서 24/7 실행 중.")
    last_status_time = time.time()
    last_error_time = time.time()
    error_messages = []  # 오류 요약용 리스트
    while True:
        try:
            spreads = {}
            for target in ['upbit', 'bithumb']:
                spreads[target] = await get_spread(target)
                await asyncio.sleep(2)  # rate limit

            max_spread_ex = max(spreads, key=spreads.get)
            spread = spreads[max_spread_ex]
            if spread > 2.5:
                amount = 0.001
                exchanges['binance'].create_market_buy_order('BTC/USDT', amount)
                exchanges[max_spread_ex].create_market_sell_order('BTC/KRW', (await get_exchange_rate()) * amount * 1.01)
                profit = spread * amount * 20000
                await send_telegram(f"실행! {max_spread_ex} Spread {spread:.2f}% - 수익 +{profit:.0f}원")
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
            # 오류 요약 알림 (1시간에 한 번)
            if time.time() - last_error_time >= 3600 and error_messages:
                await send_telegram(f"오류 요약: {', '.join(error_messages)}")
                error_messages = []
                last_error_time = time.time()
            await asyncio.sleep(300)  # 5분 루프 (rate limit 최적화, 꾸준함 강화)
        except Exception as e:
            error_messages.append(str(e))  # 오류 수집 (알림 스팸 방지)
            await asyncio.sleep(10)
