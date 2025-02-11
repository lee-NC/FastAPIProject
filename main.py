import os
import sys
import time
from fastapi import FastAPI, HTTPException, Request

sys.path.append(os.path.abspath("Model"))
sys.path.append(os.path.abspath("Processing"))
sys.path.append(os.path.abspath("Helper"))

from Processing.load_fact_tables import process_fetch_tables
from Processing.process_report_tele_bot import *
from Processing.transfer_data import cut_off_data
from Model.Request import *

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


@app.get("/report_cumulative_credential", summary="Báo cáo lũy kế", description="Báo cáo lũy kế")
async def report_cumulative_credential(request: ReportRequest):
    date = request.date
    logger.info(f"report_cumulative_credential at {datetime.datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_accumulate_credential(date)
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/report_accumulate_signature_transaction", summary="Thống kê giao dịch theo tháng", description="Thống kê giao dịch theo tháng")
async def signature_transaction(request: ReportRequest):
    date = request.date
    logger.info(f"report_accumulate_signature_transaction at {datetime.datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_signature_transaction(date)
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/report_accumulate_cert_order_register", summary="Thống kê đơn hàng và đăng ký", description="Thống kê đơn hàng và đăng ký")
async def cert_order_register(request: ReportRequest):
    date = request.date
    logger.info(f"report_accumulate_cert_order_register at {datetime.datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_cert_order_register(date)
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cut_off_data", summary="cut_off_data", description="cut_off_data")
async def cut_off_data(request: CutOffRequest):
    table_name = request.table_name
    logger.info(f"cut_off_data at {datetime.datetime.now()} with param {table_name}")
    try:
        if table_name is None:
            raise HTTPException(status_code=400, detail="table_name is required")
        check = await cut_off_data(config, table_name)
        mess = "Transfer data successfully!"
        if not check:
            mess = "Transfer data failed!"
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
