FROM tiangolo/uvicorn-gunicorn:python3.8
#FROM tiangolo/uvicorn-gunicorn-fastapi:python3.7

ARG GIT_TOKEN

RUN pip install --upgrade pip
RUN pip install fastapi
RUN pip install pyarrow
RUN pip install requests
RUN pip install pandas==1.3.0
RUN pip install pydantic

RUN apt-get update
RUN apt-get install poppler-utils -y

RUN rm -rf /service/src

COPY ./service /app

RUN rm -rf /app/src