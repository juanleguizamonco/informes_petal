from fastapi import APIRouter
from .process import router as process_router

router = APIRouter()
router.include_router(process_router, prefix="/api", tags=["Process"])