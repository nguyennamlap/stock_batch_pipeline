from src.utils.logger import setup_logger
import glob
from src.utils.db_connector import db
import os

logger = setup_logger(
    logger_name="Loader",
    sub_dir="/app/logging",
    log_file="database_load.log",
    level=10
)

def create_main_table(cursor):
    
    query = """
    CREATE TABLE IF NOT EXISTS stock_prices (
        date DATE,
        open NUMERIC(15, 4),
        high NUMERIC(15, 4),
        low NUMERIC(15, 4),
        close NUMERIC(15, 4),
        change NUMERIC(10,4),
        percentage_change NUMERIC(7,4),  
        volume BIGINT,
        symbol VARCHAR(10),
        PRIMARY KEY (symbol, date)
    );
    """
    cursor.execute(query)

def load_all_csvs(folder_path):
    csv_files = glob.glob(os.path.join(folder_path, "*.csv"))
    
    if not csv_files:
        logger.warning(f"Không tìm thấy file CSV nào tại: {folder_path}")
        return

    logger.info(f"Bắt đầu xử lý {len(csv_files)} file CSV...")

    try:
        with db.get_connection() as connection:
            raw_conn = connection.connection
            cursor = raw_conn.cursor()

            # 1. Khởi tạo bảng chính (chỉ chạy 1 lần)
            create_main_table(cursor)
            raw_conn.commit()
            
            success_count = 0

            for file_path in csv_files:
                file_name = os.path.basename(file_path)
                logger.info(f"Đang xử lý file: {file_name}")
                
                try:
                    # Pattern ETL: Load vào Temp Table -> Upsert vào Main Table -> Xóa Temp
                    cursor.execute("CREATE TEMP TABLE temp_stock (LIKE stock_prices) ON COMMIT DROP;")
                    
                    with open(file_path, 'r', encoding='utf-8') as f:
                        # Copy vào bảng tạm
                        sql_copy = """
                            COPY temp_stock (date, open, high, low, close, change, percentage_change, volume, symbol) 
                            FROM STDIN WITH CSV HEADER DELIMITER ','
                        """
                        cursor.copy_expert(sql=sql_copy, file=f)
                    # date,open,high,low,close,change,percentage_change,volume,symbol
                    # Upsert (Insert on conflict) từ bảng tạm sang bảng chính
                    sql_upsert = """
                        INSERT INTO stock_prices (date, open, high, low, close, change, percentage_change, volume, symbol)
                        SELECT date, open, high, low, close, change, percentage_change, volume, symbol FROM temp_stock
                        ON CONFLICT (symbol, date) DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            change = EXCLUDED.change,
                            percentage_change = EXCLUDED.percentage_change,
                            volume = EXCLUDED.volume;
                    """
                    cursor.execute(sql_upsert)
                    
                    # Cứ xong 1 file thì commit để đảm bảo dữ liệu
                    raw_conn.commit()
                    success_count += 1
                    logger.info(f"Đã tải thành công file {file_name} vào hệ thống.")

                except Exception as e:
                    logger.error(f"Lỗi khi xử lý file {file_name}: {e}")
                    raw_conn.rollback() # Rollback lại trạng thái của file bị lỗi để không hỏng các file sau
                    continue 

            logger.info(f"Hoàn thành nạp thành công {success_count}/{len(csv_files)} file!")

    except Exception as e:
        logger.error(f"Lỗi hệ thống nghiêm trọng trong quá trình ETL: {e}")

if __name__ == "__main__":
    DATA_FOLDER = "/app/data/processed" 
    load_all_csvs(DATA_FOLDER)