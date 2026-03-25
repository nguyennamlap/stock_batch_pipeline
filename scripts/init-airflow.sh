#!/bin/bash
set -e

echo ">>> Đang kiểm tra kết nối tới Database..."

# Khởi tạo Metadata Database (tạo các bảng cần thiết cho Airflow)
# 'db init' hoặc 'db upgrade' đều được, upgrade an toàn hơn khi chạy lại nhiều lần
airflow db upgrade

echo ">>> Đang kiểm tra và tạo User Admin..."

# Tạo user admin. 
# Dùng '|| true' để nếu user đã tồn tại thì script không bị dừng (lỗi)
airflow users create \
    --username "${AIRFLOW_ADMIN_USER}" \
    --password "${AIRFLOW_ADMIN_PASSWORD}" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email "${AIRFLOW_ADMIN_EMAIL}" || echo "Admin user already exists."

echo ">>> Khởi tạo Airflow hoàn tất!"