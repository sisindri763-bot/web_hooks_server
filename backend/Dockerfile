# Use an official lightweight Python runtime
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PORT=5000

# Set working directory
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /app/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copy project codebase
COPY . /app/

# Ensure runtime directories exist
RUN mkdir -p /app/results /app/config

# Expose server port
EXPOSE 5000

# Start server with Gunicorn WSGI
CMD ["gunicorn", "--workers", "3", "--bind", "0.0.0.0:5000", "wsgi:app"]
