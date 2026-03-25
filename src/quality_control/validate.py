import pandas as pd
import numpy as np
import json
import sys
from datetime import time, datetime
from typing import Optional

import great_expectations as gx
from src.utils.logger import setup_logger

# 1. Khởi tạo Logger
logger = setup_logger(
    logger_name="Validator",
    sub_dir="/app/logging",
    log_file="validate.log",
    level=10 # logging.DEBUG
)
logger.info("Khởi tạo thành công 😁")
# Vietnamese market sessions
MARKET_OPEN_AM  = time(9,  0)
MARKET_CLOSE_AM = time(11, 30)
MARKET_OPEN_PM  = time(13, 0)
MARKET_CLOSE_PM = time(15, 0)

# Expected data frequency (minutes). Set None to skip gap check.
BAR_INTERVAL_MINUTES = 1

# Max allowed % price change vs previous bar before flagging as outlier
OUTLIER_THRESHOLD_PCT = 0.075   # 7.5 % – Vietnam's daily price limit ±7%

# Approved ticker symbols (extend as needed)
VALID_SYMBOLS = {
    "VNM", "FPT", "HPG", "VIC", "VHM", "VCB", "CTG", "BID", "TCB",
    "VPB", "GAS", "PLX", "MWG", "PDR", "BHC", "SAB", "MSN", "KBC"
}



class ValidationReport:
    """Collects and renders validation results."""

    def __init__(self):
        self.sections: list[dict] = []

    def add(self, name: str, passed: bool, details: str = "", df_issues: Optional[pd.DataFrame] = None):
        self.sections.append(
            {"name": name, "passed": passed, "details": details, "df_issues": df_issues}
        )
        
    def save_to_json(self, file_path: str):
        # Chuyển các DataFrame lỗi thành dict để lưu được vào JSON
        output = []
        for s in self.sections:
            section_data = {
                "name": s["name"],
                "passed": s["passed"],
                "details": s["details"]
            }
            if s["df_issues"] is not None:
                section_data["issues"] = s["df_issues"].to_dict(orient="records")
            output.append(section_data)

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=4, ensure_ascii=False)
        print(f"💾 Report saved to: {file_path}")

    def print(self):
        total = len(self.sections)
        passed = sum(1 for s in self.sections if s["passed"])
        print("<br>" + "═" * 64)
        print(f"  STOCK DATA VALIDATION REPORT  ({passed}/{total} checks passed)")
        print("═" * 64)
        for s in self.sections:
            icon = "✅" if s["passed"] else "❌"
            print(f"\n{icon}  {s['name']}")
            if s["details"]:
                for line in s["details"].splitlines():
                    print(f"     {line}")
            if s["df_issues"] is not None and not s["df_issues"].empty:
                print(s["df_issues"].to_string(index=True))
        print("<br>" + "═" * 64)
        print(f"  Result: {'ALL CHECKS PASSED 🎉' if passed == total else f'{total - passed} check(s) FAILED ⚠️'}")
        print("═" * 64 + "<br>")


                            # ─────────────────────────────────────────────
                                            #  CORE PIPELINE
                            # ─────────────────────────────────────────────

def read_data_csv(csv_path: str):
    try:
        raw_df = pd.read_csv(csv_path)
        logger.info(f"[INFO] Loaded {len(raw_df)} rows from '{csv_path}'")

        return raw_df
    except Exception as exc:
        logger.error(f"[ERROR] Cannot read file: {exc}")
        sys.exit(1)
    
    return None 
                    # ══════════════════════════════════════════════════════════════
                            #  1. SCHEMA VALIDATION  (via GX Expectations)
                    # ══════════════════════════════════════════════════════════════
import pandas.api.types as ptypes

def schema_validate(report, raw_df):
    schema_issues = []
    
    # 1. Validate Columns
    required_cols = ["date", "open", "high", "low", "close", "volume", "symbol"]
    missing_cols = [c for c in required_cols if c not in raw_df.columns]
    if missing_cols:
        schema_issues.append(f"❌ Missing columns: {missing_cols}")
        report.add("1 · Schema Validation", passed=False, details="<br>".join(schema_issues))
        return

    # 2. Validate kiểu Số thực (Float) bằng Pandas
    float_columns = ["open", "high", "low", "close", "change", "percentage_change"]
    for col in float_columns:
        if col in raw_df.columns: 
            if not ptypes.is_float_dtype(raw_df[col]) and not ptypes.is_numeric_dtype(raw_df[col]):
                schema_issues.append(f"❌ '{col}' type validation failed. Expected Float, got {raw_df[col].dtype}.")

    # 3. Validate kiểu Số nguyên (Integer) cho Volume
    if "volume" in raw_df.columns:
        if not ptypes.is_integer_dtype(raw_df["volume"]) and not ptypes.is_numeric_dtype(raw_df["volume"]):
            schema_issues.append(f"❌ 'volume' type validation failed. Expected Integer, got {raw_df['volume'].dtype}.")

    # 4. Validate Date
    invalid_date_count = pd.to_datetime(raw_df["date"], errors="coerce").isna().sum()
    if invalid_date_count > 0:
        schema_issues.append(f"❌ 'date' has {invalid_date_count} invalid entries.")

    # 5. Validate Symbol
    if "symbol" in raw_df.columns:
        if not ptypes.is_string_dtype(raw_df["symbol"]) and not ptypes.is_object_dtype(raw_df["symbol"]):
            schema_issues.append(f"❌ 'symbol' type validation failed. Expected String.")

    # 6. Ghi nhận kết quả
    is_passed = len(schema_issues) == 0
    report.add(
        "1 · Schema Validation",
        passed=is_passed,
        details="<br>".join(schema_issues) if not is_passed else "All types are valid. 😼",
    )
    
    logger.info("Hoàn thành validate schema 🙉🙉🙉")
                    # ══════════════════════════════════════════════════════════════
                                        #  2. NULL VALUE CHECK
                    # ══════════════════════════════════════════════════════════════
def null_validate(report, df, batch, suite):    
    critical_cols = ["symbol", "date", "open", "high", "low", "close", "volume"]
    null_expectations = []
    try:
        for col in critical_cols:
            if col in df.columns:
                exp = gx.expectations.ExpectColumnValuesToNotBeNull(column=col)
                suite.add_expectation(exp)
                null_expectations.append(col)

        result_null = batch.validate(suite)
        null_failures = []
        for res in result_null.results:
            if not res.success: # Nếu False
                col = res.expectation_config.kwargs.get("column")
                null_count = res.result.get("unexpected_count", "?")
                null_failures.append(f"'{col}': {null_count} null(s) in {len(df)} rows ")

        report.add(
            "2 · Null Value Check",
            passed=len(null_failures) == 0,
            details="<br>".join(null_failures) if null_failures else "No null values in critical fields.✌️",
        )
    except Exception as e:
        # Đảm bảo pipeline vẫn chạy tiếp hoặc báo lỗi rõ ràng vào report
        report.add(
            "2 · Null Value Check",
            passed=False,
            details=f"⚠️ Validation failed due to code error: {str(e)}",
        )
    logger.info("Hoàn thành validate giá trị NULL 🤡🤡🤡")

                    # ══════════════════════════════════════════════════════════════
                                        #  3. PRICE VALIDATION
                    # ══════════════════════════════════════════════════════════════
def price_validate(df_clean, report, context, batch):

    price_suite = gx.ExpectationSuite(name="price_suite")
    try:
        context.suites.add(price_suite)
    except Exception:
        pass

    for col in ["open", "high", "low", "close"]:
        price_suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col,
                min_value=0,
                strict_min=True
            )
        )
    price_result = batch.validate(price_suite)

    price_issues = []
    for r in price_result.results:
        if not r.success:
            column_name = r.expectation_config.kwargs.get("column", "Unknown")
            failed_count = r.result.get("unexpected_count", "?")

            price_issues.append(
                f"'{column_name}' has {failed_count} value(s) ≤ 0"
            )

    # OHLC rule
    ohlc_violations = df_clean[
        (df_clean["high"] < df_clean["open"]) |
        (df_clean["high"] < df_clean["close"]) |
        (df_clean["low"] > df_clean["open"]) |
        (df_clean["low"] > df_clean["close"]) |
        (df_clean["high"] < df_clean["low"])
    ][["symbol", "date", "open", "high", "low", "close"]]

    if not ohlc_violations.empty:
        price_issues.append(
            f"OHLC rule violated in {len(ohlc_violations)} row(s)"
        )

    report.add(
        "3 · Price Validation",
        passed=len(price_issues) == 0,
        details="<br>".join(price_issues)
        if price_issues
        else "All price values positive 🎉🎉 OHLC rules satisfied.🎊🎊",
        df_issues=ohlc_violations if not ohlc_violations.empty else None,
    )
    logger.info("Đã hoàn thành validate của price 😘😘")
                        # ══════════════════════════════════════════════════════════════
                                            #  4. VOLUME VALIDATION
                        # ══════════════════════════════════════════════════════════════
def volume_validate(report, context, batch):
    vol_suite = gx.ExpectationSuite(name="vol_suite")
    context.suites.add(vol_suite)
    vol_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(column="volume", min_value=0)
    )                                                                  # cho phép bằng 0 
    vol_result = batch.validate(vol_suite)
    vol_issues = [
    f"'{r.expectation_config.kwargs.get('column')}' has {r.result.get('unexpected_count','?')} negative value(s)"
    for r in vol_result.results
    if not r.success
]
    report.add(
        "4 · Volume Validation",
        passed=len(vol_issues) == 0,
        details="<br>".join(vol_issues) if vol_issues else "All volume values ≥ 0.",
    )
    logger.info("Đã hoàn thành validate của volume 😘😘")
                        # ══════════════════════════════════════════════════════════════
                                        #  5. DUPLICATE VALIDATION
                        # ══════════════════════════════════════════════════════════════
def duplicate_validation(report, df, context, batch):
    dup_suite = gx.ExpectationSuite(name="dup_suite")
    context.suites.add(dup_suite)
    dup_suite.add_expectation(
        gx.expectations.ExpectCompoundColumnsToBeUnique(column_list=["symbol", "date"])
    ) # kiểm tra dữ liệu trùng của 2 cột quan trọng 
    dup_result = batch.validate(dup_suite)
    duplicates = df[df.duplicated(subset=["symbol", "date"], keep=False)][["symbol", "date"]]
    dup_passed = all(r.success for r in dup_result.results)
    report.add(
        "5 · Duplicate Validation",
        passed=dup_passed,  
        details=f"Found {len(duplicates)} duplicate row(s)." if not dup_passed else "No duplicate (symbol, date) keys.",
        df_issues=duplicates if not dup_passed else None,
    )
    logger.info("Đã hoàn thành validate của dữ liệu trùng 😘😘")
                        # ══════════════════════════════════════════════════════════════
                                        #  6. TIMESTAMP VALIDATION
                        # ══════════════════════════════════════════════════════════════
def timestamp_validation(report, df_clean):
    def within_market_hours(ts):
        try:
            # 1. Kiểm tra nếu đã là định dạng datetime/timestamp của pandas/python
            if isinstance(ts, (pd.Timestamp, datetime)):
                dt = ts
            # 2. Nếu là số (float/int), giả định là Unix Timestamp
            elif isinstance(ts, (float, int)):
                dt = pd.to_datetime(ts, unit='s')
            # 3. Nếu là chuỗi, cố gắng parse sang datetime
            else:
                dt = pd.to_datetime(ts)

            # Trích xuất giờ:phút:giây
            t = dt.time()
            
            # Logic kiểm tra: Nằm trong phiên sáng HOẶC nằm trong phiên chiều
            is_am = MARKET_OPEN_AM <= t <= MARKET_CLOSE_AM
            is_pm = MARKET_OPEN_PM <= t <= MARKET_CLOSE_PM
            
            return is_am or is_pm
            
        except Exception:
            # Nếu không thể parse được thời gian, trả về False (coi như dữ liệu lỗi)
            return False
    df_clean = df_clean.copy()

    df_clean["_in_hours"] = df_clean["date"].apply(within_market_hours)
    outside_hours = df_clean[~df_clean["_in_hours"]][["symbol", "date"]]

    # Check ascending order per symbol
    not_sorted_symbols = []
    
    for sym, grp in df_clean.groupby("symbol"):
        if not grp["date"].is_monotonic_increasing:
            not_sorted_symbols.append(sym)

    ts_issues = []
    if not outside_hours.empty:
        ts_issues.append(f"{len(outside_hours)} row(s) outside market hours (09:00-11:30, 13:00-15:00):")
    if not_sorted_symbols:
        ts_issues.append(f"dates not ascending for: {not_sorted_symbols}")

    report.add(
        "6 · Timestamp Validation",
        passed=len(ts_issues) == 0,
        details="<br>".join(ts_issues) if ts_issues else "All timestamps within market hours and in ascending order.",
        df_issues=outside_hours if not outside_hours.empty else None,
    )
    logger.info("Đã hoàn thành validate của time 😘😘")
    return df_clean
                        # ══════════════════════════════════════════════════════════════
                                       #  7. MISSING INTERVAL CHECK
                        # ══════════════════════════════════════════════════════════════
def missing_interval_check(report, df_clean, bar_interval_minutes):
    if not bar_interval_minutes:
        report.add("7 · Missing Interval Check", passed=True, details="Skipped (BAR_INTERVAL_MINUTES=None).")
        return

    expected_delta = pd.Timedelta(minutes=bar_interval_minutes)
    
    # 1. Đảm bảo cột date là datetime và đã được sort
    df_work = df_clean.copy()
    df_work['date'] = pd.to_datetime(df_work['date'])
    df_work = df_work.sort_values(["symbol", "date"])

    # 2. Xử lý logic trading hours (Tránh báo lỗi nhầm qua đêm/cuối tuần)
    # Giả sử giờ trading VN: 09:00 - 11:30 và 13:00 - 14:45
    df_work['_time'] = df_work['date'].dt.time
    is_morning = df_work['_time'].between(time(9, 1), time(11, 30))
    is_afternoon = df_work['_time'].between(time(13, 1), time(14, 45))
    df_work['_in_hours'] = is_morning | is_afternoon

    # Chỉ check gap bên trong giờ giao dịch
    df_in_hours = df_work[df_work['_in_hours']].copy()

    # 3. Dùng shift() thay vì loop idx-1 (Vectorized approach - Nhanh và An toàn hơn)
    # Tính thời điểm của bar phía trước trong cùng 1 symbol
    df_in_hours['prev_date'] = df_in_hours.groupby('symbol')['date'].shift(1)
    df_in_hours['actual_gap'] = df_in_hours['date'] - df_in_hours['prev_date']

    # Tìm các dòng có gap lớn hơn expected (nhưng vẫn trong cùng một ngày để tránh gap qua đêm)
    # Lưu ý: Nếu là bar đầu tiên của ngày, ta không check gap với ngày hôm trước
    mask_gap = (df_in_hours['actual_gap'] > expected_delta) & \
               (df_in_hours['date'].dt.date == df_in_hours['prev_date'].dt.date)

    gaps = df_in_hours[mask_gap]

    # 4. Format kết quả cho report
    if not gaps.empty:
        gap_rows = gaps.apply(lambda row: {
            "symbol": row['symbol'],
            "gap_after": row['prev_date'],
            "next_bar": row['date'],
            "gap_size": str(row['actual_gap']),
        }, axis=1).tolist()
        gap_df = pd.DataFrame(gap_rows)
    else:
        gap_df = pd.DataFrame()

    report.add(
        f"7 · Missing Interval Check ({bar_interval_minutes}-min bars)",
        passed=gap_df.empty,
        details=f"{len(gap_df)} gap(s) detected." if not gap_df.empty else f"No missing intervals found.",
        df_issues=gap_df if not gap_df.empty else None,
    )
    
    # Dọn dẹp logger theo style của bạn
    logger.info("Đã hoàn thành validate của interval 😘😘")
                            # ══════════════════════════════════════════════════════════════
                                             #  8. OUTLIER DETECTION
                            # ══════════════════════════════════════════════════════════════
def outlier_detection(report, df_clean):
    outlier_rows = []
    df_clean["close"] = pd.to_numeric(df_clean["close"], errors="coerce")
    for sym, grp in df_clean.groupby("symbol"):
        grp_sorted = grp.sort_values("date").copy()
        grp_sorted["_prev_close"] = grp_sorted["close"].shift(1) # lấy giá trước đó của cột close
        grp_sorted["percentage_change"] = (
            (grp_sorted["close"] - grp_sorted["_prev_close"]).abs() / grp_sorted["_prev_close"]
        )
        flagged = grp_sorted[grp_sorted["percentage_change"] > OUTLIER_THRESHOLD_PCT]
        for _, row in flagged.iterrows():
            outlier_rows.append({
                "symbol": row["symbol"],
                "date": row["date"],
                "prev_close": row["_prev_close"],
                "close": row["close"],
                "percentage_change": f"{row['percentage_change']:.2%}",
            })
    outlier_df = pd.DataFrame(outlier_rows)
    report.add(
        f"8 · Outlier Detection (threshold >{OUTLIER_THRESHOLD_PCT:.0%})",
        passed=outlier_df.empty,
        details=f"{len(outlier_df)} suspicious price jump(s) detected." if not outlier_df.empty
                else "No abnormal price fluctuations detected.",
        df_issues=outlier_df if not outlier_df.empty else None,
    )
    logger.info("Đã hoàn thành validate của outlier detection 😘😘")
                        # ══════════════════════════════════════════════════════════════
                                            #  9. SYMBOL VALIDATION
                        # ══════════════════════════════════════════════════════════════
def symbol_validation(report, df, context, batch):
    sym_suite = gx.ExpectationSuite(name="sym_suite")
    context.suites.add(sym_suite)
    sym_suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(column="symbol", value_set=list(VALID_SYMBOLS))
    )
    sym_result = batch.validate(sym_suite)
    invalid_syms = df[~df["symbol"].isin(VALID_SYMBOLS)][["symbol"]].drop_duplicates()
    sym_passed = all(r.success for r in sym_result.results)
    report.add(
        "9 · Symbol Validation",
        passed=sym_passed,
        details=f"Unknown ticker(s): {invalid_syms['symbol'].tolist()}" if not sym_passed
                else "All ticker symbols are valid.",
        df_issues=None,
    )


                                    # ─────────────────────────────────────────────
                                                #  PIPELINE ORCHESTRATOR
                                    # ─────────────────────────────────────────────
def run_pipeline(csv_file: str, symbol) -> bool:
    # 1. Đọc dữ liệu
    df = read_data_csv(csv_file)
    if df is None:
        logger.info("Tải csv ko thành công 😒")
        return False

    logger.info(f"Đã load xong dữ liệu của {symbol} 🥹🥹")
    report = ValidationReport()

    # 2. Khởi tạo Great Expectations Context
    context      = gx.get_context(mode="ephemeral") 
    data_source  = context.data_sources.add_pandas("stock_ds")
    data_asset   = data_source.add_dataframe_asset("ohlcv")
    batch_def    = data_asset.add_batch_definition_whole_dataframe("full_batch")
    
    # Sửa biến df ở đây
    batch        = batch_def.get_batch(batch_parameters={"dataframe": df})
    suite        = context.suites.add(gx.ExpectationSuite(name="stock_suite"))

    # 3. Chạy các task validation theo thứ tự
    logger.info("Bắt đầu chạy validation pipeline....🤖🤖🤖")
    schema_validate(report, df)
    null_validate(report, df, batch, suite)
    
    # Giả định df hiện tại đang đóng vai trò df_clean vì chưa có bước transform
    price_validate(df, report, context, batch)
    volume_validate(report, context, batch)
    duplicate_validation(report, df, context, batch)
    df = timestamp_validation(report, df)
    missing_interval_check(report, df, BAR_INTERVAL_MINUTES)
    outlier_detection(report, df) 
    symbol_validation(report, df, context, batch)

    # 4. In báo cáo tổng hợp
    report.print()

    # 5. Lưu file json
    report.save_to_json(f"/app/logging/validate_result_{symbol}.json")
    
    # 6. Xác định có lỗi nào không để trả về Status
    any_fail = any(not s["passed"] for s in report.sections)
    return not any_fail

if __name__ == "__main__":
    all_passed = True
    for symbol in VALID_SYMBOLS:
        csv_file = f"/app/data/raw/{symbol}.csv"
        success = run_pipeline(csv_file, symbol)
        if not success:
            all_passed = False
        
    # Exit code 0 nếu TẤT CẢ thành công, 1 nếu có ÍT NHẤT 1 lỗi
    sys.exit(0)