FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY exporter.py .

ENV PYTHONPATH=/app

EXPOSE 8090

CMD ["python", "exporter.py"]
