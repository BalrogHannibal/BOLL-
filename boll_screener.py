import pandas as pd
import os
import time
import numpy as np
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from alpha_vantage.timeseries import TimeSeries

# === 配置 ===
ALPHA_VANTAGE_KEY = os.getenv('ALPHA_VANTAGE_KEY')  # 從環境變量讀取API KEY
if not ALPHA_VANTAGE_KEY:
    raise ValueError("缺少 ALPHA_VANTAGE_KEY，請設置到環境變量！")

ts = TimeSeries(key=ALPHA_VANTAGE_KEY, output_format='pandas')

# === 設置目標交易日（周一檢查上周五，其它天檢查昨天） ===
def get_target_date():
    today = datetime.now()
    weekday = today.weekday()
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

# === 安全下載 ===
def safe_download(ticker, retries=3, delay=20):
    for _ in range(retries):
        try:
            time.sleep(15)  # Alpha免費版，必須每15秒拉1支
            data, _ = ts.get_daily(symbol=ticker, outputsize='compact')
            data = data.rename(columns={
                '1. open': 'Open',
                '2. high': 'High',
                '3. low': 'Low',
                '4. close': 'Close',
                '5. volume': 'Volume'
            })
            return data
        except Exception as e:
            print(f"[{ticker}] 下載失敗，重試中：{e}")
            time.sleep(delay)
    return None

# === 指標計算 ===
def compute_indicators(data):
    data["Close_MA20"] = data["Close"].rolling(20).mean()
    data["Close_STD20"] = data["Close"].rolling(20).std()
    data["BOLL_lower"] = data["Close_MA20"] - 2 * data["Close_STD20"]
    return data

# === 檢查純BOLL超賣策略 ===
def check_boll_oversold(ticker):
    print(f"開始檢查 {ticker}")
    data = safe_download(ticker)
    if data is None or len(data) < 25:
        print(f"[{ticker}] 無法獲取數據，跳過")
        return None
    data = compute_indicators(data)
    data = data.sort_index()
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
with ThreadPoolExecutor(max_workers=1) as executor:  # 免費版只允許一個線程
    futures = [executor.submit(check_boll_oversold, t) for t in tickers]
    for f in as_completed(futures):
        r = f.result()
        if r:
            results.append(r)

# === 保存結果 ===
os.makedirs("results", exist_ok=True)
filename = f"results/boll_oversold_{TARGET_DATE}.csv"
pd.DataFrame(results).to_csv(filename, index=False)

print(f"\n✅ 完成！共找到 {len(results)} 支符合BOLL超賣的股票。結果保存於 {filename}")

