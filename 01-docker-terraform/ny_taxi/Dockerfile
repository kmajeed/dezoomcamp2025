FROM python:3.12-slim-bullseye

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY ingest.py .

CMD ["python", "ingest.py"]