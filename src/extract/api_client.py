from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.utils.logger import setup_logger

# 1. Khởi tạo Logger
logger = setup_logger(
    logger_name="Crawler",
    sub_dir="/app/logging",
    log_file="crawler.log",
    level=10 # logging.DEBUG
)

logger.info("🚀 Đã khởi tạo Logger thành công cho module Extract!")


def scrape_symbol(symbol, total_pages, max_retries=5):
    # Vòng lặp Retry
    for attempt in range(1, max_retries + 1):
        driver = None 
        
        try:
            logger.info(f"[{symbol}] --- Bắt đầu crawl (Lần thử {attempt}/{max_retries}) ---")
            
            # Khởi tạo lại Options và Driver cho mỗi lần thử để đảm bảo môi trường "sạch"
            opts = Options()
            opts.add_argument("--headless=new")
            opts.add_argument("--no-sandbox")
            opts.add_argument("--disable-dev-shm-usage")
            opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
            
            driver = webdriver.Chrome(options=opts)
            
            all_symbol_data = []
            headers = []

            driver.get(f"https://simplize.vn/co-phieu/{symbol}/lich-su-gia")

            for page in range(1, total_pages + 1):
                try:
                    if page > 1:
                        page_btn = WebDriverWait(driver, 7).until(
                            EC.element_to_be_clickable((By.XPATH, f"//li[contains(@class,'simplize-pagination-item')]//a[text()='{page}']"))
                        )
                        driver.execute_script("arguments[0].click();", page_btn)
                        time.sleep(1.5)

                    if not headers:
                        thead = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "thead")))
                        headers = [th.text for th in thead.find_elements(By.TAG_NAME, "th")]
                        headers.append("symbol")
                    
                    tbody = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "tbody")))
                    rows = tbody.find_elements(By.TAG_NAME, "tr")
                    
                    for row in rows:
                        cells = row.find_elements(By.TAG_NAME, "td")
                        row_data = [c.text.replace('\n', ' ') for c in cells]
                        row_data.append(symbol)
                        all_symbol_data.append(row_data)
                    
                    logger.debug(f"[{symbol}] Đã lấy xong dữ liệu trang {page}")

                except Exception as e:
                    logger.warning(f"[{symbol}] Dừng lại ở trang {page}. Lỗi/Hết trang: {e}")
                    break 

            # Lưu file CSV ngay trong luồng
            if all_symbol_data:
                df = pd.DataFrame(all_symbol_data, columns=headers)
                df = df.rename(columns={
                    "Ngày":"date",
                    "Giá mở cửa":"open",
                    "Giá cao nhất":"high",
                    "Giá thấp nhất":"low",
                    "Giá đóng cửa":"close",
                    "Thay đổi giá":"change",
                    "% thay đổi":"percentage_change",
                    "Khối lượng":"volume"
                })
                df['date'] = pd.to_datetime(df["date"], format='%d/%m/%Y')
                file_name = f"{symbol}.csv"
                df.to_csv(f"/app/data/raw/{file_name}", index=False, encoding='utf-8-sig')
                logger.info(f"✅ [{symbol}] Đã lưu {len(all_symbol_data)} dòng vào file {file_name}")

         
            return symbol 

        except Exception as e:
            logger.error(f"❌ [{symbol}] Lỗi ở lần thử {attempt}: {e}")
            if attempt < max_retries:
                logger.info(f"⏳ [{symbol}] Nghỉ 5 giây trước khi thử lại...")
                time.sleep(5) # Nghỉ một nhịp để hệ thống/website ổn định lại
            else:
                logger.error(f"💀 [{symbol}] THẤT BẠI HOÀN TOÀN sau {max_retries} lần thử.")
                # Ném lỗi ra ngoài để luồng chính (Main) ghi nhận luồng này đã chết
                raise Exception(f"Failed after {max_retries} retries for {symbol}") from e
                
        finally:
            if driver is not None:
                driver.quit()
                logger.debug(f"[{symbol}] Đã dọn dẹp và đóng trình duyệt.")

# 3. Hàm Main điều phối đa luồng
def main():
    total_number_page = 50
    MAX_WORKERS = 6 
    symbol_list = ["VNM", "FPT", "HPG", "VIC", "VHM", "VCB", "CTG", "BID", "TCB",
                   "VPB", "GAS", "PLX", "MWG", "PDR", "BHC", "SAB", "MSN", "KBC"]
    
    logger.info(f"Khởi động ThreadPoolExecutor với {MAX_WORKERS} workers...")

    # Sử dụng ThreadPoolExecutor
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Gọi hàm scrape_symbol, ngầm định max_retries=5 như đã set ở parameter
        futures = {executor.submit(scrape_symbol, sym, total_number_page): sym for sym in symbol_list}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                result = future.result()
                logger.info(f"🎉 Tiến trình cho {result} đã hoàn tất hoàn toàn.")
            except Exception as e:
                logger.error(f"❌ Tiến trình cho {sym} văng lỗi ở cấp độ Thread Pool: {e}")

    logger.info("🏁 Hoàn thành toàn bộ batch crawl! 🥳🎉")

if __name__ == "__main__":
    main()