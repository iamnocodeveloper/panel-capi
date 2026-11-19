FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    DATA_DIR=/data \
    PORT=8080

WORKDIR /app
COPY server.py index.html ./

RUN mkdir -p /data
VOLUME ["/data"]

EXPOSE 8080
CMD ["python", "server.py"]
