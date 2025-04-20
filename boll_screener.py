import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import os

# Step 1: 获取全美股 ticker 列表（来自 NASDAQ 官方 FTP）
nasdaq_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt"
nyse_url = "ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt"

nasdaq = pd.read_csv(nasdaq_url, sep="|")
nyse = pd.read_csv(nyse_url, sep="|")

nasdaq_tickers = nasdaq['Symbol'].dropna()
nyse_tickers = nyse['ACT Symbol'].dropna()
tickers = pd.concat([nasdaq_tickers, nyse_tickers])
tickers = [t for t in tickers if t.isalpha()]  # 过滤 ETF/权证等
tickers = tickers[:200]  # 初次测试建议限制数量，可注释掉

# Step 2: 定义筛选函数
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

# Step 3: 多线程并发执行
results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(check_boll_oversold, t) for t in tickers]
    for f in as_completed(futures):
        r = f.result()
        if r:
            results.append(r)

# Step 4: 结果保存到 GitHub 仓库
df = pd.DataFrame(results)

# 创建文件夹（自动创建）
os.makedirs("results", exist_ok=True)

# 保存到结果文件夹
df.to_csv("results/boll_oversold_results.csv", index=False)

# 控制台输出
print(f"🎯 完成：共 {len(df)} 支股票跌破布林下轨")

