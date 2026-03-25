import os
from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv
from src.utils.logger import setup_logger
# Thiết lập logging cơ bản để theo dõi lỗi trong quá trình ETL

# Tải các biến môi trường từ file .env (Bảo mật thông tin đăng nhập)
load_dotenv()
logger = setup_logger(
    logger_name="Set_up",
    sub_dir="/app/logging",
    log_file="database_load.log",
    level=10 # logging.DEBUG
)
class DatabaseConnector:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(DatabaseConnector, cls).__new__(cls)
            cls._instance._initialize_engine()
        return cls._instance

    def _initialize_engine(self):
        try:
            # Lấy thông tin từ biến môi trường
            db_user = os.getenv("DB_USER")
            db_password = os.getenv("DB_PASSWORD")
            db_host = os.getenv("DB_HOST", "localhost")
            db_port = os.getenv("DB_PORT", "5432")
            db_name = os.getenv("DB_NAME")
            
            # Ví dụ URL cho PostgreSQL. Đổi 'postgresql://' thành 'mysql+pymysql://' nếu dùng MySQL
            db_url = f"postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}"
            
            # Khởi tạo engine với pool_size để tái sử dụng kết nối (rất quan trọng cho DE)
            # Giúp kiểm soát bao nhiêu query có thể chạy song song
            self.engine = create_engine(
                db_url,
                pool_size=5,          # Số lượng kết nối thường trực
                max_overflow=10,      # Số lượng kết nối tối đa có thể mở thêm khi quá tải
                pool_pre_ping=True    # Kiểm tra kết nối còn sống trước khi dùng
            )
            # Tạo session
            self.SessionLocal = sessionmaker(
                autocommit=False, # session sẽ KHÔNG tự commit
                autoflush=False,
                bind=self.engine
                )
            
            
            # Python code -> Session -> engine -> connection pool -> database 
            logger.info("Database engine initialized successfully.")
            
        except Exception as e:
            logger.error(f"Failed to initialize database engine: {e}")
            raise

    @contextmanager
    def get_connection(self):
        connection = self.engine.connect()
        try:
            yield connection
        except SQLAlchemyError as e:
            logger.error(f"Database connection error: {e}")
            raise
        finally:
            connection.close()

    @contextmanager
    def get_session(self):

        session = self.SessionLocal()
        try:
            yield session
            session.commit()
        except SQLAlchemyError as e:
            session.rollback()
            logger.error(f"Database session error. Rolled back. Details: {e}")
            raise
        finally:
            session.close()

# Khởi tạo sẵn một instance để import trực tiếp ở các file khác
db = DatabaseConnector()