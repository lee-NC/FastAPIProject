import logging
import os
from logging.handlers import TimedRotatingFileHandler


def setup_logging(log_dir):
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    # Cấu hình định dạng log
    log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Cấu hình logging cho file (TimedRotatingFileHandler)
    log_file = os.path.join(log_dir, "lakehouse.log")
    log_handler = TimedRotatingFileHandler(
        log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8"
    )
    log_handler.setFormatter(log_formatter)
    log_handler.suffix = "%Y-%m-%d"  # Chia log theo ngày

    # Cấu hình logging cơ bản
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.StreamHandler(),  # Ghi log ra console
            logging.FileHandler(f"{log_dir}/app.log", mode='a', encoding='utf-8'),  # Ghi log vào app.log
            log_handler  # Ghi log vào lakehouse.log (với việc chia log theo ngày)
        ]
    )

    # Lấy logger chung
    logger = logging.getLogger("Lakehouse")
    return logger
