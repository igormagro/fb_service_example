# Load environment file
import logging.config
import os
from pathlib import Path
import pickle
import pandas as pd
import json

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from fusionbase.DataStream import DataStream

from Service import Service
from middlewares import router_logger

load_dotenv()
# Setup FastAPI
app = FastAPI(
    title=os.getenv("SERVICE_TITLE"),
    description=os.getenv("SERVICE_DESCRIPTION"),
    version=os.getenv("SERVICE_VERSION"),
    terms_of_service=os.getenv("SERVICE_TOS_LINK"),
    contact={
        "name": os.getenv("SERVICE_CONTACT_NAME"),
        "url": os.getenv("SERVICE_CONTACT_URL"),
        "email": os.getenv("SERVICE_CONTACT_EMAIL"),
    },
    license_info={
        "name": os.getenv("SERVICE_LICENSE_NAME"),
        "url": os.getenv("SERVICE_LICENSE_URL"),
    },
)

# DEFAULT LOGGING MIDDLEWARE
#logging.config.fileConfig(f"{os.path.dirname(__file__)}/logging.conf")
#app.add_middleware(router_logger.RouteLoggerMiddleware)


# ALWAYS KEEP THE SOURCE DATA UP2DATE
def __download_from_fusionbase():
    data_stream = DataStream(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                             connection={"base_uri": "https://api.fusionbase.com/api/v1"}, log=True)

    crime_data_key = 2246748

    if not os.path.exists('./data/source/crime_data.parquet'):
        last_update = data_stream.get_meta_data(crime_data_key).get('data_updated_at')
        df = data_stream.get_dataframe(key=crime_data_key)
        df.to_parquet('./data/source/crime_data.parquet')
        with open('./data/source/local_last_update.pickle', 'wb') as f:
            pickle.dump(last_update, f)

    else:
        last_update = data_stream.get_meta_data(crime_data_key).get('data_updated_at')
        with open('./data/source/local_last_update.pickle', 'rb') as f:
            local_last_update = pickle.load(f)

        if local_last_update != last_update:
            df = data_stream.get_dataframe(key=crime_data_key)
            df.to_parquet('./data/source/crime_data.parquet')
            with open('./data/source/local_last_update.pickle', 'wb') as f:
                pickle.dump(last_update, f)


def __build_ags_lookup():
    data_stream = DataStream(auth={"api_key": os.getenv('FUSIONBASE_API_KEY')},
                             connection={"base_uri": "https://api.fusionbase.com/api/v1"}, log=True)
    german_geo_lookup_key = 4994292

    if not Path("./data/source/german_geo_lookup.parquet").exists():
        last_update = data_stream.get_meta_data(german_geo_lookup_key).get('data_updated_at')
        df = data_stream.get_dataframe(key=german_geo_lookup_key)
        df.to_parquet('./data/source/german_geo_lookup.parquet')
        with open('./data/source/german_geo_lookup_last_update.pickle', 'wb') as f:
            pickle.dump(last_update, f)
    
    else:
        last_update = data_stream.get_meta_data(german_geo_lookup_key).get('data_updated_at')
        with open('./data/source/german_geo_lookup_last_update.pickle', 'rb') as f:
            local_last_update = pickle.load(f)

        if local_last_update != last_update:
            df = data_stream.get_dataframe(key=german_geo_lookup_key)
            df.to_parquet('./data/source/german_geo_lookup.parquet')
            with open('./data/source/german_geo_lookup_last_update.pickle', 'wb') as f:
                pickle.dump(last_update, f)
        else:
            df = pd.read_parquet("./data/source/german_geo_lookup.parquet")

    df.drop_duplicates(subset=["zip_code"], inplace=True)
    lookup_base = df.to_dict(orient="records")
    lookup = dict()
    for l in lookup_base:
        lookup[l["zip_code"]] = l["administrative_district_key"]
    

    Path("./data/lookup/").mkdir(exist_ok=True, parents=True)
    with open("./data/lookup/de_geo_lookup.json", "w") as fp:
        json.dump(lookup, fp)
        fp.close()

    return None


@app.on_event("startup")
async def startup_event():
    __download_from_fusionbase()
    __build_ags_lookup()
    print("STARTUP DONE")
    

# # Shared secret auth | Just an additional layer of security, calls shouldn't be possible due to firewall anyhow
# # The secrets must be generated on Fusionbase side and then shared with the service
# # All secrets are unique per service
# @app.middleware("http")
# async def shared_secret_auth(request: Request, call_next):
#     secrets = os.getenv("FB_SHARED_SECRET")
#     if '|' in secrets:
#         secrets = [x.strip() for x in secrets.split('|')]
#     else:
#         secrets = [str(secrets).strip()]
#
#     if request.headers.get("fb-shared-secret") is None or request.headers.get("fb-shared-secret") not in secrets:
#         return Response('NOT_AUTHORIZED', status_code=403, media_type='text/plain')
#     response = await call_next(request)
#     return response

@app.get("/")
async def read_root():
    return {"Hello": "World::ROOT"}

service = Service()
@app.get("/get-crimes")
async def get_crimes(address_string: str):
    if not isinstance(address_string, str) or address_string == '' or address_string is None:
        return JSONResponse(status_code=422, content={'msg': 'Your input string was not processable by the API'})
    
    result = service.invoke(address_string=address_string)

    return JSONResponse(status_code=200, content=result)