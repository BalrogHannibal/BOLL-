import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep="|")
nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep="|")

nasdaq_tickers = nasdaq['Symbol'].dropna()
nyse_tickers = nyse['ACT Symbol'].dropna()
tickers = pd.concat([nasdaq_tickers, nyse_tickers])
tickers = [t for t in tickers if t.isalpha()]
tickers = tickers[:200]  # 可删掉限制变全市场

def check_boll_oversold(ticker):
    try:
        data = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if len(data) < 20:
            return None
        close = data["Close"]
        basis = close.rolling(20).mean()
        std = close.rolling(20).std()
        lower = basis - 2 * std
        if close.iloc[-1] < lower.iloc[-1]:
            return {
                "Ticker": ticker,
                "Close": round(close.iloc[-1], 2),
                "LowerBand": round(lower.iloc[-1], 2),
                "Date": datetime.now().strftime("%Y-%m-%d")
            }
    except:
        return None

results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(check_boll_oversold, t) for t in tickers]
    for f in as_completed(futures):
        r = f.result()
        if r:
            results.append(r)

df = pd.DataFrame(results)
df.to_csv("boll_oversold_results.csv", index=False)
df.to_csv("results/boll_oversold_results.csv", index=False)
print(f"🎯 完成：共 {len(df)} 支跌破布林下轨")

