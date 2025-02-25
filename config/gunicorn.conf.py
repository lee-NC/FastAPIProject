import os
import multiprocessing
import logging

logger = logging.getLogger("Lakehouse")
bind = "127.0.0.1:8000"
workers = multiprocessing.cpu_count() * 2

# Đổi thư mục log về thư mục của user
log_dir = os.path.expanduser("/usr/local/log_lakehouse")
os.makedirs(log_dir, exist_ok=True)  # Tự động tạo thư mục nếu chưa có

loglevel = "info"
errorlog = os.path.join(log_dir, "error.log")
accesslog = os.path.join(log_dir, "app.log")

capture_output = True
enable_stdio_inheritance = True

logger.info("Gunicorn config loaded!")  # Log test xem có chạy không
