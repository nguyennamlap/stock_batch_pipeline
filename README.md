# 📊 Stock Data Pipeline (Airflow + Docker)

![alt text](image-1.png)

## 🚀 Introduction

This is a mini Data Engineering project building a pipeline to process stock data using the **ETL + Data Quality** model.

Pipeline performs:

* Extracts data from API
* Transforms (cleans, processes)
* Validates (checks data quality)
* Loads into PostgreSQL
* Orchestrates using Airflow

In short: *Data goes in like a freshman, comes out as a clean and tidy senior 😎*

---

## 🏗️ Overall Architecture

```
API → Extract → Transform → Validate → Load → PostgreSQL

↑

Airflow (DAG)
```

* **Airflow**: orchestrates the pipeline
* ​​**Docker**: encapsulates each service
* **PostgreSQL**: stores data
* **Python**: handles the main processing

---
## 📁 Directory Structure

```
.
├── dags/ # Airflow DAG
├── scripts/ # Environment setup script
├── src/
│ ├── extract/ # Get data from API
│ ├── transform/ # Clean data
│ ├── quality_control/ # Validate data
│ ├── load/ # Load into DB
│ └── utils/ # Utils (DB, logger,...)
├── docker-compose.yml # Start the entire system
└── README.md
```

---
## ⚙️ Installing & Running the Project

### 1. Clone project. project

```bash
git clone <repo_url>
cd <project_name>
```

---

### 2. Create Fernet Key (Airflow needed)

```bash
python scripts/generate_fernet_key.py
```

➡️ Copy the key and put it into `.env` or `docker-compose.yml`

---

### 3. Setup Airflow & Database

```bash
bash scripts/setup_airflow.sh
```

Or run step by step:

```bash
bash scripts/init-airflow.sh
bash scripts/init-db.sh
```

---

### 4. Run the system

```bash
docker-compose up -d
```

---

### 5. Access Airflow

* URL: http://localhost:8080
* User/Pass: airflow / airflow (or according to configuration)

---

## 🔄 DAG: `stock_daily_dag.py`

The pipeline includes the following main tasks:

1. **Extract**

* Calls API to retrieve stock data
2. **Transform**

* Cleans data (missing, formatting, etc.)

3. **Validate**

* Checks:

* Price > 0

* Valid volume
4. **Load**

* Inserts into PostgreSQL

---

## 🧠 Module Details

### 📥 Extract (`src/extract`)

* `api_client.py`: Calls API

* Returns data in JSON / DataFrame format

---

### 🔄 Transform (`src/transform`)

* `data_cleaner.py`:

* Handles nulls

* Schema normalization

* Data error fixing

---

### ✅ Quality Control (`src/quality_control`)

* `validate.py`:

* Use rule validation

* Great Expectations can be integrated

---

### 📤 Load (`src/load`)

* `db_loader.py`:

* Connect to PostgreSQL

* Insert data

---

### 🧰 Utils (`src/utils`)

* `db_connector.py`: connection pool
* `logger.py`: standard logging

---

## 🐳 Docker

Each module has its own Dockerfile:

* extract
* transform
* validate
* load

➡️ Allows scaling each pipeline step independently

---

## 📌 Requirements

* Docker
* Docker Compose
* Python 3.9+