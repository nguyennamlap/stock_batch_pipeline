# Sử dụng Python image nhẹ
FROM python:3.9-slim

ENV PYTHONDONTWRITEBYTECODE=1 
ENV PYTHONUNBUFFERED=1 
ENV PYTHONPATH=/app

# Đặt thư mục làm việc
WORKDIR /app

# Copy file requirements trước để tận dụng cache Docker
COPY src/load/requirements.txt .

# Cài đặt các thư viện Python
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ mã nguồn vào container
COPY src/utils/ /app/src/utils
COPY src/load/db_loader.py /app/

# Lệnh mặc định khi chạy container
# CMD ["python", "main.py"]