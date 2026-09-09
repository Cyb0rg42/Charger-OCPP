FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /opt/charger/data /var/log/charger \
    && useradd -m -u 1000 charger \
    && chown -R charger:charger /opt/charger/data /var/log/charger
USER charger

ENV PROXY_DATA_DIR=/opt/charger/data \
    PROXY_LOG_DIR=/var/log/charger \
    PROXY_LISTEN_HOST=0.0.0.0 \
    PROXY_LISTEN_PORT=9310

# No EXPOSE: the web dashboard port comes from the mounted config file's "port"
# field, and the OCPP-facing port lives in the database (PROXY_LISTEN_PORT above
# only seeds it). docker-compose.yml publishes the real ports.

ENTRYPOINT ["python", "ocppproxy.py"]
CMD ["--config", "/config/config.yaml"]
