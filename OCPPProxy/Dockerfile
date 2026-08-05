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
    PROXY_LISTEN_PORT=9100

# Web dashboard port comes from the mounted config file's "port" field.
# PROXY_LISTEN_PORT (env, above) is the OCPP-facing port charge points connect to.
EXPOSE 4000 9100

ENTRYPOINT ["python", "ocppproxy.py"]
CMD ["--config", "/config/config.yaml"]
