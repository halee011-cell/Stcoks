import os
from data.fetch_data import fetch_ohlcv

def test_yahoo_fetch():
    df = fetch_ohlcv("AAPL", provider="yahoo")
    assert not df.empty
    print("✅ Yahoo data fetch OK — rows:", len(df))

def test_polygon_fetch():
    if not os.getenv("POLYGON_API_KEY"):
        print("⚠️ Skipping Polygon test — no POLYGON_API_KEY set.")
        return
    df = fetch_ohlcv("AAPL", provider="polygon")
    assert not df.empty
    print("✅ Polygon data fetch OK — rows:", len(df))

if __name__ == "__main__":
    test_yahoo_fetch()
    test_polygon_fetch()
