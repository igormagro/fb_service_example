FROM tiangolo/uvicorn-gunicorn:python3.9
#FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

ARG GIT_TOKEN

RUN pip install --upgrade pip
RUN pip install fastapi
RUN pip install pyarrow
RUN pip install requests
RUN pip install pandas==1.3.0
RUN pip install pydantic

# Install Fusionbase packages
RUN pip install -e git+https://$GIT_TOKEN:x-oauth-basic@github.com/FusionbaseHQ/fb_user__fusionbase_py.git#egg=fusionbase

# Install requirements
COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

RUN apt-get update
RUN apt-get install poppler-utils -y


COPY ./service /app