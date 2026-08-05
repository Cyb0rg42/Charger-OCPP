FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /opt/charger/data /var/log/charger \
    && useradd -m -u 1000 charger \
    && chown -R charger:charger /opt/charger/data /var/log/charger
USER charger

# web_port / ocpp_port come from the mounted config file (see docker-compose.yml).
# Defaults below only apply if no config is provided.
EXPOSE 9000 9001

ENTRYPOINT ["python", "csms.py"]
CMD ["--config", "/config/config.yaml"]
