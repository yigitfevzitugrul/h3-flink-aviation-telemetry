# Build context: repo kökü (web/ klasörü de imaja girer)
FROM python:3.12-slim

WORKDIR /app
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY api/*.py ./
COPY web ./web

ENV WEB_DIR=/app/web
EXPOSE 8080
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
