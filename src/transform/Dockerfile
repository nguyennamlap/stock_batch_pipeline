# Sử dụng base image Python gọn nhẹ
FROM python:3.11-slim

# Thiết lập environment variables giúp Python chạy trong Docker tốt hơn
ENV PYTHONDONTWRITEBYTECODE=1 
ENV PYTHONUNBUFFERED=1 
ENV PYTHONPATH=/app

# Thiết lập thư mục làm việc mặc định trong container
WORKDIR /app

# Copy file requirements và cài đặt thư viện
COPY /src/transform/requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code vào container
COPY src/utils/ /app/src/utils
COPY src/transform/data_cleaner.py /app/

# Lệnh chạy mặc định khi container khởi động
