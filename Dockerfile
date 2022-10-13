# syntax=docker/dockerfile:1
FROM python:3.10-slim-bullseye
COPY . /updater
WORKDIR /updater
RUN pip install -r requirements.txt 

CMD ["python3", "-u", "downloader.py"]
