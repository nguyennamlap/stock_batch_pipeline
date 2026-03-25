#!/bin/bash

# Hiển thị màu sắc
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'


SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo -e "${BLUE}>>> [1/3] Đang load cấu hình từ file .env...${NC}"

# 2. Load file .env từ thư mục gốc
if [ -f "$PROJECT_ROOT/.env" ]; then
    # Cách load .env an toàn trong Bash
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
    echo -e "${GREEN}Đã load HOST_PROJECT_DIR = $HOST_PROJECT_DIR${NC}"
else
    echo -e "${RED}Lỗi: Không tìm thấy file .env tại $PROJECT_ROOT!${NC}"
    exit 1
fi

echo -e "đang cấp quyền"
sudo chmod 666 /var/run/docker.sock

echo -e "${BLUE}>>> [2/3] Tạo các thư mục cần thiết tại $PROJECT_ROOT...${NC}"
cd "$PROJECT_ROOT" 

echo -e "${BLUE}>>> [3/3] Build các Image cho Pipeline...${NC}"

BUILD_CONTEXT=${HOST_PROJECT_DIR:-$PROJECT_ROOT}
BUILD_CONTEXT=$(echo $BUILD_CONTEXT | tr -d '\r')

echo "Context build: $BUILD_CONTEXT"

docker build -t stock-crawler:latest   -f "$BUILD_CONTEXT/src/extract/Dockerfile" "$BUILD_CONTEXT"
docker build -t stock-validator:latest -f "$BUILD_CONTEXT/src/quality_control/Dockerfile" "$BUILD_CONTEXT"
docker build -t stock-cleaner:latest   -f "$BUILD_CONTEXT/src/transform/Dockerfile" "$BUILD_CONTEXT"
docker build -t stock-loader:latest    -f "$BUILD_CONTEXT/src/load/Dockerfile" "$BUILD_CONTEXT"

echo -e "${GREEN}==============================================${NC}"
echo -e "${GREEN}    SETUP MÔI TRƯỜNG XONG!                    ${NC}"
echo -e "${GREEN}    BÂY GIỜ CHỈ CẦN CHẠY: docker-compose up -d ${NC}"
echo -e "${GREEN}==============================================${NC}"