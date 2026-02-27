#!/usr/bin/env bash
# =============================================================================
# FDA Pipeline — GCP Deployment Script
# =============================================================================
# One-shot script that provisions all GCP resources and deploys the app.
#
# Prerequisites:
#   - gcloud CLI installed and authenticated (gcloud auth login)
#   - Docker installed and running
#   - A GCP project created
#
# Usage:
#   export GCP_PROJECT=your-project-id
#   export GCP_REGION=us-central1          # optional, defaults below
#   export BUCKET_NAME=fda-pipeline-data   # optional, defaults below
#   bash deploy/setup.sh
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Config — override via env vars before running
# ---------------------------------------------------------------------------
PROJECT="${GCP_PROJECT:?Set GCP_PROJECT to your GCP project ID}"
REGION="${GCP_REGION:-us-central1}"
BUCKET="${BUCKET_NAME:-fda-pipeline-data}"
SERVICE_NAME="fda-monitor"
SA_NAME="fda-pipeline-sa"
SA_EMAIL="${SA_NAME}@${PROJECT}.iam.gserviceaccount.com"
REPO_NAME="fda-pipeline"
IMAGE="$REGION-docker.pkg.dev/$PROJECT/$REPO_NAME/$SERVICE_NAME"
SCHEDULER_JOB="fda-nightly"
SCHEDULE="0 2 * * *"   # 2:00 AM UTC daily

echo ""
echo "========================================"
echo " FDA Pipeline — GCP Setup"
echo "========================================"
echo " Project : $PROJECT"
echo " Region  : $REGION"
echo " Bucket  : $BUCKET"
echo " Image   : $IMAGE"
echo "========================================"
echo ""

# ---------------------------------------------------------------------------
# 1. Enable required APIs
# ---------------------------------------------------------------------------
echo "[1/8] Enabling GCP APIs..."
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com \
  --project="$PROJECT"

# ---------------------------------------------------------------------------
# 2. Create service account
# ---------------------------------------------------------------------------
echo "[2/8] Creating service account: $SA_NAME"
gcloud iam service-accounts create "$SA_NAME" \
  --display-name="FDA Pipeline Service Account" \
  --project="$PROJECT" 2>/dev/null || echo "  (already exists)"

# Grant Storage admin on the bucket (object-level write access)
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/storage.objectAdmin" \
  --quiet

# Grant Cloud Run invoker (so Cloud Scheduler can call the service)
gcloud projects add-iam-policy-binding "$PROJECT" \
  --member="serviceAccount:$SA_EMAIL" \
  --role="roles/run.invoker" \
  --quiet

# ---------------------------------------------------------------------------
# 3. Create GCS bucket
# ---------------------------------------------------------------------------
echo "[3/8] Creating GCS bucket: gs://$BUCKET"
gcloud storage buckets create "gs://$BUCKET" \
  --project="$PROJECT" \
  --location="$REGION" \
  --uniform-bucket-level-access 2>/dev/null || echo "  (already exists)"

# ---------------------------------------------------------------------------
# 4. Create Artifact Registry repository
# ---------------------------------------------------------------------------
echo "[4/8] Creating Artifact Registry repo: $REPO_NAME"
gcloud artifacts repositories create "$REPO_NAME" \
  --repository-format=docker \
  --location="$REGION" \
  --project="$PROJECT" 2>/dev/null || echo "  (already exists)"

# ---------------------------------------------------------------------------
# 5. Build and push Docker image
# ---------------------------------------------------------------------------
echo "[5/8] Building and pushing Docker image..."
gcloud auth configure-docker "$REGION-docker.pkg.dev" --quiet

# Build from project root
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

docker build -t "$IMAGE:latest" "$PROJECT_ROOT"
docker push "$IMAGE:latest"

# ---------------------------------------------------------------------------
# 6. Deploy Cloud Run service (monitor dashboard)
# ---------------------------------------------------------------------------
echo "[6/8] Deploying Cloud Run service: $SERVICE_NAME"
gcloud run deploy "$SERVICE_NAME" \
  --image="$IMAGE:latest" \
  --platform=managed \
  --region="$REGION" \
  --project="$PROJECT" \
  --service-account="$SA_EMAIL" \
  --min-instances=1 \
  --max-instances=2 \
  --port=5050 \
  --memory=512Mi \
  --cpu=1 \
  --timeout=3600 \
  --set-env-vars="STORAGE_BACKEND=cloud,CLOUD_STORAGE_PROVIDER=gcs,CLOUD_STORAGE_BUCKET=$BUCKET,CLOUD_STORAGE_PREFIX=fda_data/,SCHEDULER_BACKEND=cloud,LOG_LEVEL=INFO" \
  --command="python" \
  --args="-m,fda_pipeline.monitor" \
  --no-allow-unauthenticated

# Get the service URL
SERVICE_URL=$(gcloud run services describe "$SERVICE_NAME" \
  --region="$REGION" \
  --project="$PROJECT" \
  --format="value(status.url)")
echo "  Service URL: $SERVICE_URL"

# ---------------------------------------------------------------------------
# 7. Create Cloud Scheduler job
# ---------------------------------------------------------------------------
echo "[7/8] Creating Cloud Scheduler job: $SCHEDULER_JOB"
gcloud scheduler jobs create http "$SCHEDULER_JOB" \
  --schedule="$SCHEDULE" \
  --uri="${SERVICE_URL}/api/run" \
  --http-method=POST \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  --location="$REGION" \
  --project="$PROJECT" \
  --time-zone="UTC" \
  --message-body='{}' 2>/dev/null || \
gcloud scheduler jobs update http "$SCHEDULER_JOB" \
  --schedule="$SCHEDULE" \
  --uri="${SERVICE_URL}/api/run" \
  --http-method=POST \
  --oidc-service-account-email="$SA_EMAIL" \
  --oidc-token-audience="$SERVICE_URL" \
  --location="$REGION" \
  --project="$PROJECT" \
  --time-zone="UTC" \
  --message-body='{}'

# ---------------------------------------------------------------------------
# 8. Summary
# ---------------------------------------------------------------------------
echo ""
echo "[8/8] Done!"
echo ""
echo "========================================"
echo " Deployment Complete"
echo "========================================"
echo " Dashboard : $SERVICE_URL"
echo " Bucket    : gs://$BUCKET/fda_data/"
echo " Schedule  : $SCHEDULE UTC (Cloud Scheduler: $SCHEDULER_JOB)"
echo ""
echo " Next steps:"
echo "   1. Run a full refresh to populate initial data:"
echo "      curl -X POST '$SERVICE_URL/api/run?full_refresh=true' \\"
echo "        -H 'Authorization: Bearer \$(gcloud auth print-identity-token)'"
echo ""
echo "   2. Trigger the scheduler manually to test:"
echo "      gcloud scheduler jobs run $SCHEDULER_JOB --location=$REGION"
echo ""
echo "   3. View logs:"
echo "      gcloud run services logs read $SERVICE_NAME --region=$REGION"
echo "========================================"
