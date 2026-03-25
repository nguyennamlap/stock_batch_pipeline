# Sử dụng Python bản slim để giảm kích thước image
FROM python:3.10-slim

# Thiết lập biến môi trường để tránh tạo file .pyc và buff output log
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

# Cài đặt các thư viện hệ thống cần thiết cho Chrome và Selenium
RUN apt-get update && apt-get install -y \
    wget \
    gnupg \
    unzip \
    curl \
    libnss3 \
    libxss1 \
    libasound2 \
    libatk-bridge2.0-0 \
    libgtk-3-0 \
    && rm -rf /var/lib/apt/lists/*

# Cài đặt Google Chrome (Bản ổn định)
RUN wget -q -O /usr/share/keyrings/google-chrome.gpg https://dl.google.com/linux/linux_signing_key.pub \
    && echo "deb [arch=amd64 signed-by=/usr/share/keyrings/google-chrome.gpg] http://dl.google.com/linux/chrome/deb/ stable main" \
    > /etc/apt/sources.list.d/google-chrome.list \
    && apt-get update \
    && apt-get install -y google-chrome-stable
# Tạo thư mục làm việc
WORKDIR /app

# Copy file requirements và cài đặt thư viện Python
COPY /src/extract/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy toàn bộ code vào container
COPY /src/extract/api_client.py /app/

# Tạo thư mục log nếu chưa có (để khớp với src.utils.logger của bạn)
RUN mkdir -p logging && chmod 777 /app/logging

CMD [ "python","api_client.py" ]