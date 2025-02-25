import traceback
from fastapi import HTTPException, APIRouter
from processing.load_fact_tables import process_fetch_tables_hbase
from processing.fetch_datas import process_fetch_tables
from processing.transfer_data import cut_off_data
from model.request_dto import *
from model.response_dto import ResponseModel
import logging

logger = logging.getLogger("Lakehouse")
router = APIRouter(prefix="/data", tags=["Data"])


@router.get("/fetch_data_hbase")
async def fetch_data_hbase():
    try:
        logger.info(f"fetch_data_hbase at {datetime.datetime.now()}")
        mess = await process_fetch_tables_hbase()
        logger.info(f"fetch_data_hbase success at {datetime.datetime.now()}")
        return ResponseModel.success(mess)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


@router.get("/fetch_data")
async def fetch_data():
    try:
        logger.info(f"fetch_data at {datetime.datetime.now()}")
        mess = await process_fetch_tables()
        logger.info(f"fetch_data success at {datetime.datetime.now()}")
        return ResponseModel.success(mess)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


@router.post("/cut_off_data")
async def cut_off_data(request: CutOffRequest):
    table_name = request.table_name
    logger.info(f"cut_off_data at {datetime.datetime.now()} with param {table_name}")
    try:
        if table_name is None:
            raise HTTPException(status_code=400, detail="table_name is required")
        check = await cut_off_data(table_name)
        if not check:
            mess = "Transfer data failed!"
            logger.info(f"cut_off_data failed at {datetime.datetime.now()} with param {table_name}")
        else:
            mess = "Transfer data successfully!"
            logger.info(f"cut_off_data success at {datetime.datetime.now()} with param {table_name}")
        return ResponseModel.success(mess=mess)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))
