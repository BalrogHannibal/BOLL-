import pandas as pd
import yfinance as yf
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import numpy as np
import os

# === 获取全美股代码 ===
nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep="|")
nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep="|")

tickers = pd.concat([nasdaq['Symbol'].dropna(), nyse['ACT Symbol'].dropna()])
tickers = [t for t in tickers if t.isalpha()]

# === 指标函数 ===
def compute_rsi(series, period=14):
    delta = series.diff()
    gain = delta.where(delta > 0, 0).rolling(period).mean()
    loss = -delta.where(delta < 0, 0).rolling(period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def compute_indicators(data):
    data["CCI"] = (data["Close"] - data["Close"].rolling(20).mean()) / (0.015 * data["Close"].rolling(20).std())
    ema12 = data["Close"].ewm(span=12).mean()
    ema26 = data["Close"].ewm(span=26).mean()
    data["MACD"] = ema12 - ema26
    data["MACD_signal"] = data["MACD"].ewm(span=9).mean()
    data["RSI"] = compute_rsi(data["Close"])
    data["OBV"] = (np.where(data["Close"].diff() > 0, data["Volume"], -data["Volume"])).cumsum()
    data["OBV_MA"] = data["OBV"].rolling(10).mean()
    data["Volume_MA"] = data["Volume"].rolling(20).mean()
    data["Breakout"] = data["Close"] > data["Close"].rolling(20).max().shift(1)
    return data

# === 卦象策略筛选函数 ===
def check_guaxiang_strategy(ticker):
    try:
        data = yf.download(ticker, period="3mo", interval="1d", progress=False)
        if len(data) < 25:
            return None
        data = compute_indicators(data)
        last = data.iloc[-1]
        prev = data.iloc[-2]

        macd_cross = last["MACD"] > last["MACD_signal"] and prev["MACD"] <= prev["MACD_signal"]
        macd_deadcross = last["MACD"] < last["MACD_signal"] and prev["MACD"] >= prev["MACD_signal"]

        zhen = last["CCI"] > 100 and macd_cross
        li = macd_cross and last["Volume"] > last["Volume_MA"] and last["Breakout"]
        obv_buy = last["OBV"] > last["OBV_MA"] and macd_cross

        buy_signal = zhen or li or obv_buy
        sell_signal = macd_deadcross or last["RSI"] > 80 or last["OBV"] < last["OBV_MA"]

        if buy_signal:
            return {
                "Ticker": ticker,
                "Signal": "卦象买入",
                "Close": round(last["Close"], 2),
                "Date": datetime.now().strftime("%Y-%m-%d")
            }
    except:
        return None

# === 多线程执行全市场扫描 ===
results = []
with ThreadPoolExecutor(max_workers=10) as executor:
    futures = [executor.submit(check_guaxiang_strategy, t) for t in tickers]
    for f in as_completed(futures):
        r = f.result()
        if r:
            results.append(r)

# === 保存每日结果文件 ===
df = pd.DataFrame(results)
os.makedirs("results", exist_ok=True)
filename = f"results/guaxiang_{datetime.now().strftime('%Y-%m-%d')}.csv"
df.to_csv(filename, index=False)

print(f"✅ 筛选完成，共 {len(df)} 支符合卦象买入条件")
print(df.head())


print(f"✅ 筛选结果已保存为 {filename}，共 {len(df)} 支股票")

print(df)
print(f"✅ 共检测 {len(tickers)} 支股票，符合条件的有 {len(df)} 支")
