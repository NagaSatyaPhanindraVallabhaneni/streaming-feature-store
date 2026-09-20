FROM python:3.11-slim

WORKDIR /app

# Install dependencies first for better layer caching.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/
COPY scripts/ ./scripts/

ENV PYTHONPATH=/app/src

EXPOSE 8000

CMD ["uvicorn", "streaming_feature_store.service:app", "--host", "0.0.0.0", "--port", "8000"]
