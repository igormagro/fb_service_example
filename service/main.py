# Load environment file
from middlewares import router_logger
from routers import route_map_feature, route_search
from Service import Service
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
import logging.config
import os
from dotenv import load_dotenv
load_dotenv()


# Setup FastAPI
# app = FastAPI(
#     title=os.getenv("SERVICE_TITLE"),
#     description=os.getenv("SERVICE_DESCRIPTION"),
#     version=os.getenv("SERVICE_VERSION"),
#     terms_of_service=os.getenv("SERVICE_TOS_LINK"),
#     contact={
#         "name": os.getenv("SERVICE_CONTACT_NAME"),
#         "url": os.getenv("SERVICE_CONTACT_URL"),
#         "email": os.getenv("SERVICE_CONTACT_EMAIL"),
#     },
#     license_info={
#         "name": os.getenv("SERVICE_LICENSE_NAME"),
#         "url": os.getenv("SERVICE_LICENSE_URL"),
#     },
app = FastAPI()

# DEFAULT LOGGING MIDDLEWARE
# logging.config.fileConfig(f"{os.path.dirname(__file__)}/logging.conf")
# app.add_middleware(router_logger.RouteLoggerMiddleware)

# Shared secret auth | Just an additional layer of security, calls shouldn't be possible due to firewall anyhow
# The secrets must be generated on Fusionbase side and then shared with the service
# All secrets are unique per service


# @app.middleware("http")
# async def shared_secret_auth(request: Request, call_next):
#     secrets = os.getenv("FB_SHARED_SECRET")
#     # print(secrets)
#     # if '|' in secrets:
#     #     secrets = [x.strip() for x in secrets.split('|')]
#     # else:
#     #     secrets = [str(secrets).strip()]
#     # if secrets == "NONE":

#     #     response = await call_next(request)
#     #     return response
#     # if request.headers.get("fb-shared-secret") is None or request.headers.get("fb-shared-secret") not in secrets:
#     #     return Response('NOT_AUTHORIZED', status_code=403, media_type='text/plain')
#     response = await call_next(request)
#     return response

## DEFAULT PREFIX
DEFAULT_PREFIX = f'/api/v1'

## YOUR ROUTERS HERE

app.include_router(
    route_search.router,
    prefix=f"{DEFAULT_PREFIX}/search",
    responses={418: {"description": "I'm a teapot"}} # Change this
)

app.include_router(
    route_map_feature.router,
    prefix=f"{DEFAULT_PREFIX}/map_features",
    responses={418: {"description": "I'm a teapot"}} # Change this
)


## MAIN ROUTES HERE
@app.get("/")
def read_root():
    return {"Hello": "World::ROOT"}