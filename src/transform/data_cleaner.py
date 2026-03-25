import polars as pl
import sys
from src.utils.logger import setup_logger

# 1. Khởi tạo Logger
logger = setup_logger(
    logger_name="cleaner",
    sub_dir="/app/logging",
    log_file="data_cleaner.log",
    level=10 # logging.DEBUG
)

logger.info("🚀 Đã khởi tạo Logger thành công cho module Extract!")

def build_cleaning_pipeline(lazy_df: pl.LazyFrame) -> pl.LazyFrame:
    float_cols = ["open", "high", "low", "close", "change"]

    cleaned_df = (
        lazy_df
        .with_columns([
            pl.col("date")  
                .str.strip_chars()             
                .str.replace_all(r"[./]", "-")  
                .str.strptime(pl.Date, "%d-%m-%Y", strict=False) 
                .alias("date"),

            pl.col("percentage_change")
                .str.replace_all(r"[%+,]", "") 
                .cast(pl.Float64) / 100,

            pl.col(float_cols)
                .str.replace_all(r"[+,]", "") 
                .cast(pl.Float64),
                
            pl.col("volume")
                .str.replace_all(r",", "")
                .cast(pl.Int64)
        ])
        .filter(pl.col("date").is_not_null())
        .unique(subset=["date"], keep="last")
        .sort("date")
        .with_columns(pl.col(float_cols + ["percentage_change"]).forward_fill())
    )
    
    return cleaned_df


def run_pipeline(csv_file: str, symbol: str) -> bool:
    try:
        # 1. Khởi tạo LazyFrame
        df = pl.scan_csv(csv_file, null_values=["", "-"])
        logger.info(f"Đang xử lý dữ liệu của {symbol} 🧥🧥")

        # raw_df = df.collect() 
        # test_parse = raw_df.with_columns(
        #     pl.col("date").str.strptime(pl.Datetime, "%d-%m-%Y", strict=False).alias("parsed_date")
        # )
        # bad_dates = test_parse.filter(pl.col("parsed_date").is_null() & pl.col("date").is_not_null())
        
        # if bad_dates.height > 0:
        #     sample_errors = bad_dates["date"].head(5).to_list()
        #     logger.error(f"❌ Sai format ngày cho {symbol}! Dữ liệu gốc thực tế là: {sample_errors}")
        #     return False

        # 2. Áp dụng transformation pipeline
        processed_lazy_df = build_cleaning_pipeline(df)

        # 3. Collect dữ liệu
        final_df = processed_lazy_df.collect()

        if final_df.filter(pl.col("date").is_null()).height > 0:
            logger.error(f"Schema Validation Failed cho {symbol}: Có dòng không parse được ngày.")
            return False

        # 4. Ghi ra file
        output_path = f"/app/data/processed/{symbol}.csv"
        final_df.write_csv(output_path)
        
        logger.info(f"Hoàn thành clean data cho {symbol} 🎉🎉🎉")
        return True

    except Exception as e:
        logger.error(f"Xử lý {symbol} thất bại do: {e} (っ °Д °;)っ")
        return False


if __name__ == "__main__":
    VALID_SYMBOLS = {
        "VNM", "FPT", "HPG", "VIC", "VHM", "VCB", "CTG", "BID", "TCB",
        "VPB", "GAS", "PLX", "MWG", "PDR", "BHC", "SAB", "MSN", "KBC"
    }
    
    all_success = True
    for symbol in VALID_SYMBOLS:
        csv_file = f"/app/data/raw/{symbol}.csv"
        # Dùng toán tử & để nếu có 1 file fail thì tổng thể batch sẽ fail
        is_success = run_pipeline(csv_file, symbol)
        all_success = all_success and is_success
        
    logger.info("Hoàn tất pipeline cho toàn bộ danh mục! (●ˇ∀ˇ●)")
    
    # Exit code 0 nếu TẤT CẢ thành công, 1 nếu có ÍT NHẤT 1 lỗi
    sys.exit(0 if all_success else 1)