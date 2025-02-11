import os
import sys
import time
from fastapi import FastAPI, HTTPException, Request

sys.path.append(os.path.abspath("Model"))
sys.path.append(os.path.abspath("Processing"))
sys.path.append(os.path.abspath("Helper"))

from Processing.load_fact_tables import process_fetch_tables
from Processing.process_report_tele_bot import *

from Helper.config import Config
from Helper.custom_logging import setup_logging

config = Config().get_config()

LOG_DIR = config["log"]["path"]
logger = setup_logging(LOG_DIR)
app = FastAPI()


@app.get("/", summary="Check Health", description="Check Health")
async def read_root():
    return {"message": "Hello, World!"}


@app.get("/fetch_data", summary="fetch data", description="fetch data")
async def fetch_data():
    try:
        mess = await process_fetch_tables()
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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
    import uvicorn

    logger.info("Server is starting...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
