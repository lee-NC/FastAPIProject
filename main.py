import asyncio
import os
import sys
import time
from contextlib import asynccontextmanager
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from fastapi import FastAPI, Request

from controllers import data_controller, report_controller, dashboard_controller
from helper.config import Config
from helper.custom_logging import setup_logging
from processing.fetch_datas import process_fetch_tables

sys.path.append(os.path.abspath("config"))
sys.path.append(os.path.abspath("controllers"))
sys.path.append(os.path.abspath("model"))
sys.path.append(os.path.abspath("processing"))
sys.path.append(os.path.abspath("helper"))
sys.path.append(os.path.abspath("repositories"))

# Khởi tạo ứng dụng FastAPI
config = Config().get_config()
LOG_DIR = config["log"]["path"]
logger = setup_logging(LOG_DIR)


def run_etl_fact_tables():
    # Chạy coroutine trong vòng lặp sự kiện
    asyncio.run(process_fetch_tables())


async def schedule_job():
    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_etl_fact_tables, 'cron', hour=0, minute=5)
    print("Scheduler started. Jobs will run sequentially.")
    scheduler.start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    await schedule_job()  # Khởi chạy task khi ứng dụng bắt đầu
    yield  # Đợi cho tới khi ứng dụng tắt


app = FastAPI(title="Data Lakehouse Server", version="1.0", lifespan=lifespan)

# Đăng ký các router từ controllers
app.include_router(data_controller.router)
app.include_router(report_controller.router)
app.include_router(dashboard_controller.router)


@app.get("/", summary="Check Health", description="Check Health")
async def read_root():
    return {"message": "Hello, World!"}


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    logger.info(f"Request: {request.method} {request.url}")

    response = await call_next(request)

    duration = time.time() - start_time
    logger.info(f"Response: {response.status_code} - Time taken: {duration:.4f}s")

    return response


@app.middleware("https")
async def log_requests(request: Request, call_next):
    start_time = time.time()

    logger.info(f"Request: {request.method} {request.url}")

    response = await call_next(request)

    duration = time.time() - start_time
    logger.info(f"Response: {response.status_code} - Time taken: {duration:.4f}s")

    return response


if __name__ == "__main__":
    # import uvicorn
    #
    logger.info("Server is starting...")
    # uvicorn.run(app, host="127.0.0.1", port=8000)
    logger.info("Gunicorn đang khởi động FastAPI...")
