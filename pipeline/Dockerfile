FROM python:3.11-slim

WORKDIR /app

# Install snowflake-cli system dependency
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install Snowflake CLI (snow)
RUN pip install --no-cache-dir snowflake-cli-labs==3.2.0

COPY runner.py .

EXPOSE 8080

CMD ["uvicorn", "runner:app", "--host", "0.0.0.0", "--port", "8080"]
