import os
import sys
import time
import exchange_calendars as xcals
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
from pykrx import stock

KST = ZoneInfo("Asia/Seoul")

pd.set_option("display.max_rows", 200)
pd.set_option("display.max_columns", 50)
pd.set_option("display.width", 200)


def yyyymmdd(dt: datetime) -> str:
    return dt.strftime("%Y%m%d")


def log(msg: str):
    print(msg, flush=True)


def retry_krx(func, *args, retries=4, delay=2, **kwargs):
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            last_err = e
            log(f"[WARN] KRX 호출 실패 {attempt}/{retries}: {func.__name__}{args} -> {e}")
            if attempt < retries:
                time.sleep(delay * attempt)
    raise last_err

def get_xkrx_calendar():
    return xcals.get_calendar("XKRX")


def nearest_prev_business_day_local(date_str: str) -> str:
    cal = get_xkrx_calendar()
    dt = pd.Timestamp(datetime.strptime(date_str, "%Y%m%d").date())

    # "직전 영업일"이어야 하므로 하루 먼저 빼고 previous 사용
    dt = dt - pd.Timedelta(days=1)

    session = cal.date_to_session(dt, direction="previous")
    return pd.Timestamp(session).strftime("%Y%m%d")


def nearest_same_or_prev_business_day_local(date_str: str) -> str:
    cal = get_xkrx_calendar()
    dt = pd.Timestamp(datetime.strptime(date_str, "%Y%m%d").date())

    if cal.is_session(dt):
        return dt.strftime("%Y%m%d")

    session = cal.date_to_session(dt, direction="previous")
    return pd.Timestamp(session).strftime("%Y%m%d")


def decide_target_date_kst():
    now_kst = datetime.now()
    cutoff_hour = 18

    if now_kst.hour < cutoff_hour:
        base_dt = now_kst - timedelta(days=1)
        mode = "PRE_CLOSE_USE_PREV"
        target_date = nearest_prev_business_day_local(base_dt.strftime("%Y%m%d"))
    else:
        mode = "POST_CLOSE_USE_SAME_OR_PREV"
        target_date = nearest_same_or_prev_business_day_local(now_kst.strftime("%Y%m%d"))

    return target_date, mode, now_kst


def get_market_data_safe(date_str: str, market: str = "KOSPI") -> pd.DataFrame:
    try:
        df = retry_krx(stock.get_market_ohlcv_by_ticker, date_str, market=market)
        if df is None or df.empty:
            log(f"[WARN] 종목 데이터가 비어 있습니다: {date_str}, market={market}")
            return pd.DataFrame()
        return df
    except Exception as e:
        log(f"[WARN] 종목 데이터 조회 실패: {date_str}, market={market}, err={e}")
        return pd.DataFrame()


def run_conditions(df: pd.DataFrame, target_date: str) -> pd.DataFrame:
    """
    예시 조건식입니다.
    원하시면 여기를 기존 본인 조건식으로 바꾸면 됩니다.
    """
    if df.empty:
        return pd.DataFrame()

    work = df.copy()

    required_cols = ["종가", "거래량"]
    for col in required_cols:
        if col not in work.columns:
            log(f"[WARN] 필수 컬럼 없음: {col}")
            return pd.DataFrame()

    result = work[
        (work["종가"] >= 5000) &
        (work["거래량"] >= 100000)
    ].copy()

    result["날짜"] = target_date
    result = result.sort_values(["거래량", "종가"], ascending=[False, False])

    return result


def print_result(result_df: pd.DataFrame, target_date: str):
    print("\n" + "=" * 100)
    print(f"조건검색 결과 - {target_date}")
    print("=" * 100)

    if result_df is None or result_df.empty:
        print("조건 충족 종목 없음")
        print("=" * 100)
        return

    result_df = result_df.copy()
    result_df.index.name = "티커"

    show_cols = [c for c in ["날짜", "종가", "거래량", "거래대금", "시가", "고가", "저가"] if c in result_df.columns]
    out_df = result_df[show_cols] if show_cols else result_df

    print(f"총 {len(out_df)}개 종목")
    print("-" * 100)
    print(out_df.to_string())
    print("=" * 100)


def save_result(result_df: pd.DataFrame, target_date: str):
    os.makedirs("output", exist_ok=True)

    if result_df is None or result_df.empty:
        empty_df = pd.DataFrame(columns=["날짜", "티커", "종가", "거래량", "거래대금", "시가", "고가", "저가"])
        empty_df.to_csv("output/result.csv", index=False, encoding="utf-8-sig")
        with open("output/summary.txt", "w", encoding="utf-8") as f:
            f.write(f"{target_date}: 조건 충족 종목 없음\n")
        return

    out = result_df.copy().reset_index().rename(columns={"index": "티커"})
    out.to_csv("output/result.csv", index=False, encoding="utf-8-sig")

    with open("output/summary.txt", "w", encoding="utf-8") as f:
        f.write(f"{target_date}: 총 {len(out)}개 종목\n")


def main():
    log("===== 조건검색 시작 =====")

    try:
        target_date, mode, now_kst = decide_target_date_kst()
        log(f"[INFO] now_kst={now_kst}")
        log(f"[INFO] mode={mode}")
        log(f"[INFO] target_date={target_date}")
    except Exception as e:
        log(f"[ERROR] 대상일 계산 실패: {e}")
        sys.exit(1)

    market_df = get_market_data_safe(target_date, market="KOSPI")
    if market_df.empty:
        log(f"[ERROR] {target_date} 시장 데이터가 비어 있어 스캐너를 종료합니다.")
        sys.exit(1)

    result_df = run_conditions(market_df, target_date)
    print_result(result_df, target_date)
    save_result(result_df, target_date)

    log("===== 조건검색 종료 =====")


if __name__ == "__main__":
    main()
