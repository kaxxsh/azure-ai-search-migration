# Use official Python runtime as base image
FROM python:3.11-slim

# Set working directory in container
WORKDIR /app

# Install system dependencies (if needed for Azure SDK)
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/* \
    && pip install uv

# Copy application code
COPY backup_service/ ./backup_service/
COPY main.py .
COPY pyproject.toml .

# Install Python dependencies
RUN uv sync

# Create directory for logs
RUN mkdir -p /app/logs

# Set environment variables (these can be overridden at runtime)
ENV PYTHONUNBUFFERED=1

# Set the entrypoint to the main script
ENTRYPOINT ["uv", "run", "main.py"]
