from fastapi import APIRouter, HTTPException

from model.request_dto import ReportRequest
from model.response_dto import ResponseModel
from processing.process_report_tele_bot import *
import logging

router = APIRouter(prefix="/report", tags=["Report"])
logger = logging.getLogger("Lakehouse")


@router.post("/cumulative_credential")
async def cumulative_credential(request: ReportRequest):
    date = request.date
    logger.info(f"cumulative_credential at {datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_accumulate_credential(date)
        logger.info(f"cumulative_credential success at {datetime.now()} with param {date}")
        return ResponseModel.success(mess=mess)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


@router.post("/accumulate_signature_transaction")
async def signature_transaction(request: ReportRequest):
    date = request.date
    logger.info(f"accumulate_signature_transaction at {datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_signature_transaction(date)
        logger.info(f"accumulate_signature_transaction success at {datetime.now()} with param {date}")
        return ResponseModel.success(mess=mess)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))


@router.post("/accumulate_cert_order_register")
async def cert_order_register(request: ReportRequest):
    date = request.date
    logger.info(f"accumulate_cert_order_register at {datetime.now()} with param {date}")
    try:
        if date is None:
            raise HTTPException(status_code=400, detail="Date is required")
        mess = await processing_cert_order_register(date)
        logger.info(f"accumulate_cert_order_register success at {datetime.now()} with param {date}")
        return ResponseModel.success(mess=mess)
    except Exception as e:
        logger.error(traceback.format_exc())
        return ResponseModel.error(str(e))
