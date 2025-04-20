import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# 加载全美股（NASDAQ + NYSE）
nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep="|")
nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep="|")

tickers = pd.concat([
    nasdaq['Symbol'].dropna(),
    nyse['ACT Symbol'].dropna()
])
tickers = [t for t in tickers if t.isalpha()]  # 排除 ETF/权证
# tickers = tickers[:500]  # 如需调试可先限制数量

# 检查是否超卖（布林下轨）
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

# 多线程执行
results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(check_boll_oversold, t) for t in tickers]
    for f in as_completed(futures):
        r = f.result()
        if r:
            results.append(r)

# 保存或打印
df = pd.DataFrame(results)
from datetime import datetime
import os

os.makedirs("results", exist_ok=True)  # 自动创建目录

# 用日期命名文件，例如 boll_2025-04-20.csv
date_str = datetime.now().strftime("%Y-%m-%d")
filename = f"results/boll_{date_str}.csv"
df.to_csv(filename, index=False)

print(f"✅ 筛选结果已保存为 {filename}，共 {len(df)} 支股票")

print(df)
print(f"✅ 共检测 {len(tickers)} 支股票，符合条件的有 {len(df)} 支")
