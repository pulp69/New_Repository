import os
from datetime import datetime, timedelta

import pandas as pd
import FinanceDataReader as fdr


def get_date_range():
    end_date = datetime.today().date()
    start_date = end_date - timedelta(days=7)
    return start_date.strftime("%Y-%m-%d"), end_date.strftime("%Y-%m-%d")


def get_leveraged_etf_weekly_returns():
    start_date, end_date = get_date_range()

    # 한국 ETF 전체 목록
    etfs = fdr.StockListing("ETF/KR")

    # 이름에 '레버리지'가 포함된 ETF만 선택
    levered = etfs[etfs["Name"].str.contains("레버리지", na=False)].copy()

    results = []

    for _, row in levered.iterrows():
        symbol = str(row["Symbol"]).zfill(6)
        name = row["Name"]

        try:
            # 한국 종목은 기본적으로 NAVER 소스를 사용할 수 있음
            df = fdr.DataReader(symbol, start_date, end_date)

            if df is None or df.empty or "Close" not in df.columns:
                continue

            start_close = df["Close"].iloc[0]
            end_close = df["Close"].iloc[-1]

            if pd.isna(start_close) or pd.isna(end_close) or start_close == 0:
                continue

            ret = (end_close / start_close - 1) * 100

            results.append({
                "Symbol": symbol,
                "Name": name,
                "StartDate": start_date,
                "EndDate": end_date,
                "StartClose": round(float(start_close), 2),
                "EndClose": round(float(end_close), 2),
                "WeeklyReturnPct": round(float(ret), 2),
            })

        except Exception as e:
            print(f"[WARN] {symbol} {name} 조회 실패: {e}", flush=True)

    result_df = pd.DataFrame(results)

    if result_df.empty:
        return result_df, start_date, end_date

    result_df = result_df.sort_values("WeeklyReturnPct", ascending=False).reset_index(drop=True)
    return result_df, start_date, end_date


def save_outputs(df: pd.DataFrame, start_date: str, end_date: str):
    os.makedirs("output", exist_ok=True)

    if df.empty:
        pd.DataFrame(columns=[
            "Symbol", "Name", "StartDate", "EndDate",
            "StartClose", "EndClose", "WeeklyReturnPct"
        ]).to_csv("output/leveraged_etf_weekly_returns.csv", index=False, encoding="utf-8-sig")

        with open("output/summary.txt", "w", encoding="utf-8") as f:
            f.write(f"{start_date} ~ {end_date}\n")
            f.write("레버리지 ETF 수익률 데이터를 가져오지 못했습니다.\n")
        return

    df.to_csv("output/leveraged_etf_weekly_returns.csv", index=False, encoding="utf-8-sig")

    with open("output/summary.txt", "w", encoding="utf-8") as f:
        f.write(f"{start_date} ~ {end_date}\n")
        f.write(f"총 {len(df)}개 레버리지 ETF\n")
        f.write("\n상위 10개\n")
        f.write(df.head(10).to_string(index=False))


def main():
    print("===== 레버리지 ETF 1주일 수익률 조회 시작 =====", flush=True)

    df, start_date, end_date = get_leveraged_etf_weekly_returns()

    if df.empty:
        print("[INFO] 결과 없음", flush=True)
    else:
        print(df.to_string(index=False), flush=True)

    save_outputs(df, start_date, end_date)

    print("===== 종료 =====", flush=True)


if __name__ == "__main__":
    main()
