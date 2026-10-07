#!/usr/bin/env bash
# ==============================================================================
# AirDoctor Google Cloud Run One-Click Deployment Script
# ==============================================================================
set -euo pipefail

SERVICE_NAME="${SERVICE_NAME:-airdoctor}"
REGION="${REGION:-us-central1}"
PROJECT_ID="${GOOGLE_CLOUD_PROJECT:-$(gcloud config get-value project 2>/dev/null || echo '')}"

if [[ -z "${PROJECT_ID}" ]]; then
    echo "❌ Error: GOOGLE_CLOUD_PROJECT is not set and no active gcloud project was found."
    echo "Run: gcloud config set project <your-project-id>"
    exit 1
fi

echo "🚀 Deploying AirDoctor to Google Cloud Run..."
echo "  • Project: ${PROJECT_ID}"
echo "  • Service: ${SERVICE_NAME}"
echo "  • Region:  ${REGION}"

gcloud run deploy "${SERVICE_NAME}" \
    --project="${PROJECT_ID}" \
    --region="${REGION}" \
    --source="." \
    --platform="managed" \
    --allow-unauthenticated \
    --set-env-vars="LLM_PROVIDER=${LLM_PROVIDER:-vertexai},VERTEX_MODEL_NAME=${VERTEX_MODEL_NAME:-gemini-3-pro},GOOGLE_CLOUD_PROJECT=${PROJECT_ID},GOOGLE_CLOUD_REGION=${REGION}"

echo "✔ AirDoctor successfully deployed to Cloud Run!"
