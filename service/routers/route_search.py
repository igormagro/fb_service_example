
from Service import Service
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()

@router.get("/", tags=["search"])
async def search(feature_type: str = None, feature_value: str = None, lat: float = None, lon: float = None, radius: int = None):
    s = Service()
    [status_code, data] = s.invoke(feature_type, feature_value, lat, lon, radius)
    
    return JSONResponse(
        status_code=status_code,
        content=data
    )
