from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator
from docker.types import Mount
from datetime import datetime, timedelta
import os


default_args = {
    'owner': 'data_engineer_team',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# Lấy đường dẫn tuyệt đối của project từ biến môi trường (do docker-compose truyền vào)
HOST_PROJECT_DIR = os.getenv('HOST_PROJECT_DIR', '/path/to/your/project')

# Các Mount dùng chung
data_mount = Mount(
    source=f"{HOST_PROJECT_DIR}/data",
    target="/app/data",
    type="bind"
)

utils_mount = Mount(
    source=f"{HOST_PROJECT_DIR}/src/utils",
    target="/app/src/utils",
    type="bind"
)

# =====================================================
# ĐỊNH NGHĨA DAG
# =====================================================
with DAG(
    dag_id='stock_daily_docker_pipeline',
    default_args=default_args,
    description='Stock ETL pipeline sử dụng DockerOperator',
    schedule_interval="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['stock', 'docker', 'etl'],
    doc_md="""
    # Stock Daily Pipeline
    Pipeline xử lý dữ liệu chứng khoán hàng ngày.
    * **crawl_data**: Lấy dữ liệu từ API (image: stock-crawler)
    * **validate_data**: Kiểm tra chất lượng dữ liệu (image: stock-validator)
    * **clean_data**: Làm sạch và transform (image: stock-cleaner)
    * **load_data**: Ghi vào PostgreSQL (image: stock-loader)
    """
) as dag:

    start_task = EmptyOperator(task_id='start')

    # ==================== TASK 1: CRAWLER ====================
    crawl_task = DockerOperator(
        task_id='crawl_data',
        image='stock-crawler:latest',
        api_version='auto',
        auto_remove='success',
        command='python api_client.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='stock_network',
        mounts=[
            data_mount,
            utils_mount,
            Mount(
                source=f"{HOST_PROJECT_DIR}/logging/crawl_log",
                target="/app/logging",
                type="bind"
            )
        ],
        environment={
            'PYTHONPATH': '/app',
        }
    )

    # ==================== TASK 2: VALIDATOR ====================
    validate_task = DockerOperator(
        task_id='validate_data',
        image='stock-validator:latest',
        api_version='auto',
        auto_remove='success',
        command='python validate.py',          
        docker_url='unix://var/run/docker.sock',
        mount_tmp_dir=False, #
        network_mode='stock_network',
        mounts=[
            data_mount,
            utils_mount,
            Mount(
                source=f"{HOST_PROJECT_DIR}/logging/validate_log",
                target="/app/logging",
                type="bind"
            )
        ],
        environment={
            'PYTHONPATH': '/app',
        }
    )

    # ==================== TASK 3: CLEANER ====================
    clean_task = DockerOperator(
        task_id='clean_data',
        image='stock-cleaner:latest',
        api_version='auto',
        auto_remove='success',
        command='python data_cleaner.py',      # Tên file trong transform
        docker_url='unix://var/run/docker.sock',
        network_mode='stock_network',
        mounts=[
            data_mount,
            utils_mount,
            Mount(
                source=f"{HOST_PROJECT_DIR}/logging/transform_log",
                target="/app/logging",
                type="bind"
            )
        ],
        environment={
            'PYTHONPATH': '/app',
            'LOG_LEVEL': 'INFO'
        }
    )

    # ==================== TASK 4: LOADER ====================
    load_task = DockerOperator(
        task_id='load_data',
        image='stock-loader:latest',
        api_version='auto',
        auto_remove='success',
        command='python db_loader.py',          # Tên file trong load
        docker_url='unix://var/run/docker.sock',
        network_mode='stock_network',
        mounts=[    
            data_mount,
            utils_mount,
            Mount(
                source=f"{HOST_PROJECT_DIR}/logging/runtime_log",
                target="/app/logging",
                type="bind"
            )
        ],
        environment={
            'PYTHONPATH': '/app',
            # Lấy thông tin PostgreSQL từ Airflow Connection (đã tạo)
            'POSTGRES_HOST': '{{ conn.postgres_stock.host }}',
            'POSTGRES_PORT': '{{ conn.postgres_stock.port }}',
            'POSTGRES_DB': '{{ conn.postgres_stock.schema }}',
            'POSTGRES_USER': '{{ conn.postgres_stock.login }}',
            'POSTGRES_PASSWORD': '{{ conn.postgres_stock.password }}',
        }
    )

    end_task = EmptyOperator(task_id='end')


    start_task >> crawl_task >> validate_task >> clean_task >> load_task >> end_task