import pandas as pd
import yfinance as yf
import numpy as np
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta

# === 設置目標交易日（周一檢查上周五，其它天檢查昨天） ===
def get_target_date():
    today = datetime.now()
    weekday = today.weekday()  # Monday=0, Sunday=6
    if weekday == 0:
        delta = 3
    elif weekday == 6:
        delta = 2
    elif weekday == 5:
        delta = 1
    else:
        delta = 1
    return (today - timedelta(days=delta)).date()

TARGET_DATE = get_target_date()

# === 獲取股票列表 ===
nasdaq = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/nasdaqlisted.txt", sep="|")
nyse = pd.read_csv("ftp://ftp.nasdaqtrader.com/SymbolDirectory/otherlisted.txt", sep="|")
tickers = pd.concat([nasdaq['Symbol'].dropna(), nyse['ACT Symbol'].dropna()])
tickers = [t for t in tickers if t.isalpha()]

# === 指標計算 ===
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

# === 策略檢查 ===
def safe_download(ticker, retries=3, delay=3):
    for _ in range(retries):
        try:
            data = yf.download(ticker, period="3mo", interval="1d", progress=False)
            if not data.empty:
                return data
        except Exception as e:
            print(f"[{ticker}] 下載失敗，重試：{e}")
            time.sleep(delay)
    return None

def check_guaxiang_strategy(ticker):
    data = safe_download(ticker)
    if data is None or len(data) < 25:
        return None
    data = compute_indicators(data)
    last = data.iloc[-1]
    last_date = data.index[-1].date()
    if last_date != TARGET_DATE:
        return None

    fallback = (
        pd.isna(last["Volume"]) or last["Volume"] == 0 or
        pd.isna(last["OBV"]) or pd.isna(last["MACD"])
    )
    if fallback:
        close = data["Close"]
        basis = close.rolling(20).mean()
        std = close.rolling(20).std()
        lower = basis - 2 * std
        if close.iloc[-1] < lower.iloc[-1]:
            return {"Ticker": ticker, "Signal": "BOLL超賣Fallback", "Close": round(close.iloc[-1], 2), "Date": str(last_date)}
        return None

    prev = data.iloc[-2]
    macd_cross = last["MACD"] > last["MACD_signal"] and prev["MACD"] <= prev["MACD_signal"]
    zhen = last["CCI"] > 100 and macd_cross
    li = macd_cross and last["Volume"] > last["Volume_MA"] and last["Breakout"]
    obv_buy = last["OBV"] > last["OBV_MA"] and macd_cross
    buy_signal = zhen or li or obv_buy

    if buy_signal:
        return {"Ticker": ticker, "Signal": "六卦策略買入", "Close": round(last["Close"], 2), "Date": str(last_date)}
    return None

# === 主程序 ===
results = []
with ThreadPoolExecutor(max_workers=5) as executor:
    futures = [executor.submit(check_guaxiang_strategy, t) for t in tickers]
    for f in as_completed(futures):
        r = f.result()
        if r:
            results.append(r)
        time.sleep(0.05)  # 防止過快觸發限速

# === 保存結果 ===
os.makedirs("results", exist_ok=True)
filename = f"results/guaxiang_{TARGET_DATE}.csv"
pd.DataFrame(results).to_csv(filename, index=False)

print(f"✅ 完成！共找到 {len(results)} 支符合條件的股票。結果保存於 {filename}")

