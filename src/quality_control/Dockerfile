# Sử dụng bản slim để giảm dung lượng image
FROM python:3.11-slim

# Thiết lập các biến môi trường để Python không tạo file .pyc và log được xuất trực tiếp
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Thiết lập thư mục làm việc trong container
WORKDIR /app

# Cài đặt các thư viện hệ thống cần thiết (nếu cần build một số package đặc thù)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy file requirements trước để tận dụng Docker cache
COPY /src/quality_control/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN mkdir -p logging && chmod 777 /app/logging
# Copy toàn bộ code vào thư mục làm việc
COPY /src/quality_control/validate.py /app/

# Lệnh chạy mặc định (thay main.py bằng file của bạn)
