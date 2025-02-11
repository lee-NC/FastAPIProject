import os
import sys
from pathlib import Path
import logging
from logging.handlers import TimedRotatingFileHandler

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException

from Model.Request import *

sys.path.append(os.path.abspath("Processing"))
from Processing.load_fact_tables import process_fetch_tables
from Processing.process_report_tele_bot import *
from Processing.transfer_data import cut_off_data

sys.path.append(os.path.abspath("Model"))

load_dotenv("config/config.env", override=True)
ENV = os.getenv('ENV', 'development')


def get_config():
    directory = Path('config')
    task_files = [f.name for f in directory.iterdir() if f.is_file()]
    if ENV == 'production':
        return [file for file in task_files if "production" in file]
    else:
        return [file for file in task_files if "development" in file]


name_files = get_config()
if not name_files:
    print("No configuration file found!")
    sys.exit(1)
task_file = f"config/{name_files[0]}"

log_dir = task_file["log"]["path"]
os.makedirs(log_dir, exist_ok=True)

# Định dạng log
log_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

# Tạo handler ghi log theo ngày
log_file = os.path.join(log_dir, "lakehouse.log")
log_handler = TimedRotatingFileHandler(log_file, when="midnight", interval=1, backupCount=30, encoding="utf-8")
log_handler.setFormatter(log_formatter)
log_handler.suffix = "%Y-%m-%d"  # Chia log theo ngày
logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(log_handler)
app = FastAPI()


@app.get("/", summary="Check Health", description="Check Health")
async def read_root():
    return {"message": "Hello, World!"}


@app.get("/fetch_data", summary="fetch data", description="fetch data")
async def fetch_data():
    try:
        mess = await process_fetch_tables(task_file)
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/report_cumulative_credential", summary="Báo cáo lũy kế", description="Báo cáo lũy kế")
async def report_cumulative_credential(request: ReportRequest):
    date = request.date
    logger.info(f"report_cumulative_credential at {datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_accumulate_credential(date, task_file)
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/signature_transaction", summary="signature_transaction", description="signature_transaction")
async def signature_transaction(request: ReportRequest):
    date = request.date
    logger.info(f"get_trang_thai_giao_dich at {datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_signature_transaction(date, task_file)
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cert_order_register", summary="cert_order_register", description="cert_order_register")
async def cert_order_register(request: ReportRequest):
    date = request.date
    logger.info(f"get_cert_order_register at {datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_cert_order_register(date, task_file)
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/cut_off_data", summary="cut_off_data", description="cut_off_data")
async def cut_off_data(request: CutOffRequest):
    table_name = request.table_name
    logger.info(f"cut_off_data at {datetime.now()} with param {table_name}")
    try:
        if table_name is None:
            raise HTTPException(status_code=400, detail="table_name is required")
        check = await cut_off_data(task_file, table_name)
        mess = "Transfer data successfully!"
        if not check:
            mess = "Transfer data failed!"
        return HTTPException(status_code=200, detail=mess)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn

    logger.info("Server is starting...")
    uvicorn.run(app, host="127.0.0.1", port=8000)
