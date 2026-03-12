FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY turbina_mqtt_publisher.py .
CMD ["python", "-u", "turbina_mqtt_publisher.py"]
