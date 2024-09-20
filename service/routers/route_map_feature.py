
from Service import Service
from fastapi import APIRouter
from fastapi.responses import JSONResponse

router = APIRouter()


@router.get("/", tags=["map_features"])
async def list_map_features(feature: str = "all"):
    
    s = Service()
    (status_code, data) = s.get_map_features(feature)
    
    return JSONResponse(
        status_code=status_code,
        content=data
    )




