FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir requests flask

COPY exporter.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "exporter.py"]
