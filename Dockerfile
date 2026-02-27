# CLOUD MIGRATION NOTE: This Dockerfile works as-is for cloud deployment.
# Push the image to ECR / Artifact Registry and reference it from your
# Lambda container image, ECS task definition, or Cloud Run service.

FROM python:3.11-slim

WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY fda_pipeline/ fda_pipeline/

# Create data directory
RUN mkdir -p /app/fda_data

# Default: run immediately then exit (useful for cron / cloud triggers)
# Override with --run-now=false to enter scheduled mode
CMD ["python", "-m", "fda_pipeline.pipeline", "--run-now"]
