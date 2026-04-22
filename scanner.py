import os
from datetime import datetime, timedelta

import pandas as pd
import FinanceDataReader as fdr
import yfinance as yf


# 미국 레버리지 종목 예시 리스트
# 필요하면 자유롭게 추가/삭제하세요.
US_LEVERAGED_SYMBOLS = [
    "TQQQ", "SQQQ",
    "SOXL", "SOXS",
    "UPRO", "SPXU",
    "TNA", "TZA",
    "FNGU",
    "TECL", "TECS",
    "LABU", "LABD",
    "NVDL", "TSLL",
    "BULZ", "BERZ",
    "UDOW", "SDOW",
    "FAS", "FAZ",
    "DPST",
    "GUSH", "DRIP"
]


def get_date_range():
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=7)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_kr_leveraged_etf_weekly_returns():
    start_date, end_date = get_date_range()

    etfs = fdr.StockListing("ETF/KR")
    levered = etfs[etfs["Name"].str.contains("레버리지", na=False)].copy()

    results = []

    for _, row in levered.iterrows():
        symbol = str(row["Symbol"]).zfill(6)
        name = row["Name"]

        try:
            df = fdr.DataReader(symbol, start_date, end_date)

            if df is None or df.empty or "Close" not in df.columns:
                continue

            start_close = df["Close"].iloc[0]
            end_close = df["Close"].iloc[-1]

            if pd.isna(start_close) or pd.isna(end_close) or start_close == 0:
                continue

            ret = (end_close / start_close - 1) * 100

            results.append({
                "Market": "KR",
                "Symbol": symbol,
                "Name": name,
                "StartDate": start_date,
                "EndDate": end_date,
                "StartClose": round(float(start_close), 2),
                "EndClose": round(float(end_close), 2),
                "WeeklyReturnPct": round(float(ret), 2),
            })

        except Exception as e:
            print(f"[WARN] KR {symbol} {name} 조회 실패: {e}", flush=True)

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values("WeeklyReturnPct", ascending=False).reset_index(drop=True)

    return result_df


def get_us_leveraged_weekly_returns():
    start_date, end_date = get_date_range()
    results = []

    for symbol in US_LEVERAGED_SYMBOLS:
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=(datetime.today().date() + timedelta(days=1)).strftime("%Y-%m-%d"), auto_adjust=False)

            if df is None or df.empty or "Close" not in df.columns:
                continue

            df = df.dropna(subset=["Close"])
            if df.empty:
                continue

            start_close = df["Close"].iloc[0]
            end_close = df["Close"].iloc[-1]

            if pd.isna(start_close) or pd.isna(end_close) or start_close == 0:
                continue

            long_name = ticker.info.get("longName", symbol)
            ret = (end_close / start_close - 1) * 100

            results.append({
                "Market": "US",
                "Symbol": symbol,
                "Name": long_name,
                "StartDate": start_date,
                "EndDate": end_date,
                "StartClose": round(float(start_close), 2),
                "EndClose": round(float(end_close), 2),
                "WeeklyReturnPct": round(float(ret), 2),
            })

        except Exception as e:
            print(f"[WARN] US {symbol} 조회 실패: {e}", flush=True)

    result_df = pd.DataFrame(results)
    if not result_df.empty:
        result_df = result_df.sort_values("WeeklyReturnPct", ascending=False).reset_index(drop=True)

    return result_df


def save_outputs(kr_df: pd.DataFrame, us_df: pd.DataFrame):
    os.makedirs("output", exist_ok=True)

    if kr_df.empty:
        pd.DataFrame(columns=[
            "Market", "Symbol", "Name", "StartDate", "EndDate",
            "StartClose", "EndClose", "WeeklyReturnPct"
        ]).to_csv("output/kr_leveraged_etf_weekly_returns.csv", index=False, encoding="utf-8-sig")
    else:
        kr_df.to_csv("output/kr_leveraged_etf_weekly_returns.csv", index=False, encoding="utf-8-sig")

    if us_df.empty:
        pd.DataFrame(columns=[
            "Market", "Symbol", "Name", "StartDate", "EndDate",
            "StartClose", "EndClose", "WeeklyReturnPct"
        ]).to_csv("output/us_leveraged_weekly_returns.csv", index=False, encoding="utf-8-sig")
    else:
        us_df.to_csv("output/us_leveraged_weekly_returns.csv", index=False, encoding="utf-8-sig")

    combined = pd.concat([kr_df, us_df], ignore_index=True) if (not kr_df.empty or not us_df.empty) else pd.DataFrame(columns=[
        "Market", "Symbol", "Name", "StartDate", "EndDate",
        "StartClose", "EndClose", "WeeklyReturnPct"
    ])
    combined.to_csv("output/all_leveraged_weekly_returns.csv", index=False, encoding="utf-8-sig")

    with open("output/summary.txt", "w", encoding="utf-8") as f:
        f.write("레버리지 종목 1주일 수익률 요약\n\n")

        f.write("[한국 레버리지 ETF]\n")
        if kr_df.empty:
            f.write("결과 없음\n\n")
        else:
            f.write(kr_df.head(10).to_string(index=False))
            f.write("\n\n")

        f.write("[미국 레버리지 종목]\n")
        if us_df.empty:
            f.write("결과 없음\n")
        else:
            f.write(us_df.head(10).to_string(index=False))
            f.write("\n")


def main():
    print("===== 한국 레버리지 ETF 1주일 수익률 =====", flush=True)
    kr_df = get_kr_leveraged_etf_weekly_returns()

    if kr_df.empty:
        print("[INFO] 한국 결과 없음", flush=True)
    else:
        print(kr_df.to_string(index=False), flush=True)

    print("\n===== 미국 레버리지 종목 1주일 수익률 =====", flush=True)
    us_df = get_us_leveraged_weekly_returns()

    if us_df.empty:
        print("[INFO] 미국 결과 없음", flush=True)
    else:
        print(us_df.to_string(index=False), flush=True)

    save_outputs(kr_df, us_df)

    print("\n===== 저장 완료 =====", flush=True)
    print("output/kr_leveraged_etf_weekly_returns.csv", flush=True)
    print("output/us_leveraged_weekly_returns.csv", flush=True)
    print("output/all_leveraged_weekly_returns.csv", flush=True)
    print("output/summary.txt", flush=True)


if __name__ == "__main__":
    main()
