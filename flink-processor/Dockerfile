# Use Python 3.10 for better Flink compatibility
FROM --platform=linux/arm64 python:3.10-slim

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    default-jdk-headless \
    curl \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Download Flink connector JARs
RUN mkdir -p /app/jars && \
    curl -L https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.1.0-1.18/flink-connector-kafka-3.1.0-1.18.jar \
         -o /app/jars/flink-connector-kafka.jar && \
    curl -L https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.1.1/kafka-clients-3.1.1.jar \
         -o /app/jars/kafka-clients.jar

# Copy source code
COPY src/ ./src/

# Set environment variables
ENV PYTHONPATH=/app/src
ENV FLINK_CLASSPATH=/app/jars/flink-connector-kafka.jar:/app/jars/kafka-clients.jar

# Create non-root user
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Default command (use news processing job with alerting)
CMD ["python", "src/news_processing_job.py"]