FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

RUN mkdir -p /opt/charger/data /var/log/charger \
    && useradd -m -u 1000 charger \
    && chown -R charger:charger /opt/charger/data /var/log/charger
USER charger

# GUI port comes from the mounted config file (see docker-compose.yml).
EXPOSE 5000

ENTRYPOINT ["python", "chargepoint.py"]
CMD ["--config", "/config/config.json"]
