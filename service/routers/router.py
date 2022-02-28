from fastapi import APIRouter
router = APIRouter()

import Service

@router.get("/", tags=["routers"])
async def root():
    s = Service.Service()
    data = s.invoke()
    return data