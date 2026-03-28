# 📊 Stock Data Pipeline (Airflow + Docker)

![alt text](image-1.png)
## 🚀 Giới thiệu

Đây là một mini project Data Engineering xây dựng pipeline xử lý dữ liệu chứng khoán theo mô hình **ETL + Data Quality**.

Pipeline thực hiện:

* Extract dữ liệu từ API
* Transform (làm sạch, xử lý)
* Validate (kiểm tra chất lượng dữ liệu)
* Load vào PostgreSQL
* Orchestrate bằng Airflow

Nói ngắn gọn: *dữ liệu đi vào như sinh viên năm nhất, đi ra thành senior sạch sẽ gọn gàng 😎*

---

## 🏗️ Kiến trúc tổng thể

```
API → Extract → Transform → Validate → Load → PostgreSQL
                      ↑
                   Airflow (DAG)
```

* **Airflow**: điều phối pipeline
* **Docker**: đóng gói từng service
* **PostgreSQL**: lưu trữ dữ liệu
* **Python**: xử lý chính

---

## 📁 Cấu trúc thư mục

```
.
├── dags/                  # Airflow DAG
├── scripts/               # Script setup môi trường
├── src/
│   ├── extract/           # Lấy dữ liệu từ API
│   ├── transform/         # Làm sạch dữ liệu
│   ├── quality_control/   # Validate dữ liệu
│   ├── load/              # Load vào DB
│   └── utils/             # Utils (DB, logger,...)
├── docker-compose.yml     # Khởi chạy toàn bộ hệ thống
└── README.md
```

---

## ⚙️ Cài đặt & Chạy dự án

### 1. Clone project

```bash
git clone <repo_url>
cd <project_name>
```

---

### 2. Tạo Fernet Key (Airflow cần)

```bash
python scripts/generate_fernet_key.py
```

➡️ Copy key và set vào `.env` hoặc `docker-compose.yml`

---

### 3. Setup Airflow & Database

```bash
bash scripts/setup_airflow.sh
```

Hoặc chạy từng bước:

```bash
bash scripts/init-airflow.sh
bash scripts/init-db.sh
```

---

### 4. Chạy hệ thống

```bash
docker-compose up -d
```

---

### 5. Truy cập Airflow

* URL: http://localhost:8080
* User/Pass: airflow / airflow (hoặc theo config)

---

## 🔄 DAG: `stock_daily_dag.py`

Pipeline gồm các task chính:

1. **Extract**

   * Gọi API lấy dữ liệu stock
2. **Transform**

   * Làm sạch dữ liệu (missing, format,...)
3. **Validate**

   * Kiểm tra:

     * Giá > 0
     * Volume hợp lệ
4. **Load**

   * Insert vào PostgreSQL

---

## 🧠 Chi tiết các module

### 📥 Extract (`src/extract`)

* `api_client.py`: gọi API
* Trả về dữ liệu dạng JSON / DataFrame

---

### 🔄 Transform (`src/transform`)

* `data_cleaner.py`:

  * Xử lý null
  * Chuẩn hóa schema
  * Fix lỗi data

---

### ✅ Quality Control (`src/quality_control`)

* `validate.py`:

  * Dùng rule validate
  * Có thể tích hợp Great Expectations

---

### 📤 Load (`src/load`)

* `db_loader.py`:

  * Kết nối PostgreSQL
  * Insert dữ liệu

---

### 🧰 Utils (`src/utils`)

* `db_connector.py`: connection pool
* `logger.py`: logging chuẩn

---

## 🐳 Docker

Mỗi module có Dockerfile riêng:

* extract
* transform
* validate
* load

➡️ Giúp scale từng bước pipeline độc lập

---

## 📌 Yêu cầu

* Docker
* Docker Compose
* Python 3.9+


