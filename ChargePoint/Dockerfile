FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /opt/charger/data /var/log/charger \
    && useradd -m -u 1000 charger \
    && chown -R charger:charger /opt/charger/data /var/log/charger
USER charger

# No EXPOSE: the GUI port comes from the mounted config file's "port" field
# and differs per instance, so any fixed value here would be wrong for at
# least one of them. docker-compose.yml publishes the real ports.

ENTRYPOINT ["python", "chargepoint.py"]
CMD ["--config", "/config/config.json"]
