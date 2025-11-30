import ccxt
import os

def main():
    api_key = os.environ.get("BITHUMB_API_KEY", "")
    secret = os.environ.get("BITHUMB_SECRET", "")

    print("=== Bithumb API Railway 테스트 ===")
    print("API_KEY prefix:", api_key[:4] + "..." if api_key else "(empty)")

    bithumb = ccxt.bithumb({
        "apiKey": api_key,
        "secret": secret,
        "enableRateLimit": True,
    })

    # 1) 공개 API 테스트 (시세 조회)
    try:
        ticker = bithumb.fetch_ticker("BTC/KRW")
        print("[PUBLIC] BTC/KRW ticker OK, last price:", ticker.get("last"))
    except Exception as e:
        print("[PUBLIC] ticker 오류:", type(e), e)

    # 2) 비공개 API 테스트 (잔고 조회)
    try:
        balance = bithumb.fetch_balance()
        print("[PRIVATE] 잔고 조회 성공!")
        krw = balance.get("KRW", {})
        btc = balance.get("BTC", {})
        print("  KRW free:", krw.get("free"))
        print("  BTC free:", btc.get("free"))
    except Exception as e:
        print("[PRIVATE] 잔고 조회 오류:", type(e), e)

if __name__ == "__main__":
    main()
