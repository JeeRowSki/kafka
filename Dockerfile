FROM python:3.12-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .
COPY producer.py .
COPY single_consumer.py .
COPY batch_consumer.py .
COPY message.py .

RUN useradd --create-home --uid 10001 appuser

USER appuser

CMD ["python", "main.py"]
