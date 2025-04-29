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
def compute_indicators(data):
    data["Close_MA20"] = data["Close"].rolling(20).mean()
    data["Close_STD20"] = data["Close"].rolling(20).std()
    data["BOLL_lower"] = data["Close_MA20"] - 2 * data["Close_STD20"]
    return data

# === 安全下載 ===
def safe_download(ticker, retries=3, delay=3):
    for _ in range(retries):
        try:
            time.sleep(0.2)  # 每次請求前小延遲
            data = yf.download(ticker, period="3mo", interval="1d", progress=False)
            if not data.empty:
                return data
        except Exception as e:
            print(f"[{ticker}] 下載失敗，重試中：{e}")
            time.sleep(delay * 2)
    return None

# === 檢查純BOLL超賣策略 ===
def check_boll_oversold(ticker):
    print(f"開始檢查 {ticker}")
    data = safe_download(ticker)
    if data is None or len(data) < 25:
        print(f"[{ticker}] 無法獲取數據，跳過")
        return None
    data = compute_indicators(data)
    last = data.iloc[-1]
    last_date = data.index[-1].date()
    if last_date != TARGET_DATE:
        print(f"[{ticker}] 非目標日數據，跳過")
        return None

    if pd.isna(last["BOLL_lower"]):
        print(f"[{ticker}] 無BOLL數據，跳過")
        return None

    if last["Close"] < last["BOLL_lower"]:
        print(f"[{ticker}] 符合BOLL超賣條件")
        return {
            "Ticker": ticker,
            "Signal": "BOLL超賣",
            "Close": round(last["Close"], 2),
            "Date": str(last_date)
        }

    print(f"[{ticker}] 不符合BOLL超賣")
    return None

# === 主程序 ===
results = []
with ThreadPoolExecutor(max_workers=3) as executor:
    futures = [executor.submit(check_boll_oversold, t) for t in tickers]
    for f in as_completed(futures):
        r = f.result()
        if r:
            results.append(r)
        time.sleep(0.5)  # 每支ticker間隔

# === 保存結果 ===
os.makedirs("results", exist_ok=True)
filename = f"results/boll_oversold_{TARGET_DATE}.csv"
pd.DataFrame(results).to_csv(filename, index=False)

print(f"\n✅ 完成！共找到 {len(results)} 支符合BOLL超賣的股票。結果保存於 {filename}")


