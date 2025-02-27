import os
import multiprocessing
import logging

logger = logging.getLogger("Lakehouse")
bind = "0.0.0.0:8000"
workers = multiprocessing.cpu_count() * 2
worker_class = "uvicorn.workers.UvicornWorker"

log_dir = os.path.expanduser("/usr/local/log_lakehouse")
os.makedirs(log_dir, exist_ok=True)

loglevel = "info"
errorlog = os.path.join(log_dir, "error.log")
accesslog = os.path.join(log_dir, "app.log")

capture_output = True
enable_stdio_inheritance = True
