import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime

# Lấy đường dẫn gốc của project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__))) 
LOG_ROOT_DIR = os.path.join(BASE_DIR, 'logging')

def setup_logger(logger_name, sub_dir, log_file, level=logging.INFO):
    logger = logging.getLogger(logger_name)
    logger.setLevel(level)

    if not logger.handlers:
        formatter = logging.Formatter('%(asctime)s | %(name)-15s | %(levelname)-8s | %(message)s')

        # Handler 1: Console
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Handler 2: File
        target_dir = os.path.join(LOG_ROOT_DIR, sub_dir)
        os.makedirs(target_dir, exist_ok=True)
        
        file_path = os.path.join(target_dir, log_file)
        
        # Encoding='utf-8' rất quan trọng để tránh lỗi khi log có tiếng Việt hoặc emoji
        file_handler = RotatingFileHandler(file_path, maxBytes=10*1024*1024, backupCount=3, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

