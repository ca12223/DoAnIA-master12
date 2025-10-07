FROM python:3.11-slim

WORKDIR /app

# Copy all Python scripts
COPY *.py /app/
COPY datasets/ /app/datasets/
COPY requirements.txt /app/requirements.txt

RUN pip install --no-cache-dir -r requirements.txt

ENV BROKER_HOST=emqx
ENV BROKER_PORT=1883
ENV INFLUXDB_URL=http://influxdb:8086
ENV INFLUXDB_TOKEN=iot-admin-token-123
ENV INFLUXDB_ORG=iot-org
ENV INFLUXDB_BUCKET=iot-data
ENV PYTHONUNBUFFERED=1

CMD ["python", "replay_storage.py", "--broker", "emqx", "--port", "1883"]
