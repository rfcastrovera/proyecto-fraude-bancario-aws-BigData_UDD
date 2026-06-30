#!/bin/bash
set -euo pipefail

# ============================================================
# Deploy — Pipeline Fraude: Glue Jobs + Scripts + Datalake
# ============================================================
# Uso:  bash scripts/deploy_glue.sh
# Pre:  aws CLI configurado, bucket origen con datos subidos
# ============================================================

# ---------- Config ----------
BUCKET_ORIGEN="bucket-origen-adrianespinoza"
BUCKET_DATALAKE="datalake-adrianespinoza"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
SCRIPT_S3="s3://${BUCKET_ORIGEN}/scripts"
GLUE_VERSION="5.0"
WORKERS=5
WORKER_TYPE="G.1X"

# ---------- Get account ID ----------
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text --region "${REGION}")
echo "Account: ${ACCOUNT_ID} | Region: ${REGION}"

# ---------- 1. Create datalake bucket ----------
echo ">>> Creating datalake bucket: ${BUCKET_DATALAKE}"
aws s3 mb "s3://${BUCKET_DATALAKE}" --region "${REGION}" 2>/dev/null || echo "  (bucket already exists)"

# ---------- 2. Upload scripts ----------
echo ">>> Uploading scripts to ${SCRIPT_S3}"
aws s3 cp src/ "${SCRIPT_S3}/" --recursive --exclude "__pycache__/*" --exclude ".*"

# ---------- 3. Create IAM role ----------
ROLE_NAME="GlueFraudeRole"
ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"

echo ">>> Creating IAM role: ${ROLE_NAME}"
aws iam create-role \
  --role-name "${ROLE_NAME}" \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "glue.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }' 2>/dev/null || echo "  (role already exists)"

aws iam attach-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-arn "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole" 2>/dev/null || true

S3_POLICY=$(cat <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${BUCKET_ORIGEN}",
        "arn:aws:s3:::${BUCKET_ORIGEN}/raw-data/*",
        "arn:aws:s3:::${BUCKET_ORIGEN}/scripts/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject","s3:PutObject","s3:DeleteObject","s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::${BUCKET_DATALAKE}",
        "arn:aws:s3:::${BUCKET_DATALAKE}/bronze/*",
        "arn:aws:s3:::${BUCKET_DATALAKE}/silver/*",
        "arn:aws:s3:::${BUCKET_DATALAKE}/gold/*"
      ]
    }
  ]
}
EOF
)

aws iam put-role-policy \
  --role-name "${ROLE_NAME}" \
  --policy-name "S3AccessFraude" \
  --policy-document "${S3_POLICY}" 2>/dev/null || true

# ---------- 4. Create Glue jobs ----------
echo ">>> Creating Glue jobs"

aws glue create-job \
  --region "${REGION}" \
  --name "fraude-ingestion-bronze" \
  --role "${ROLE_ARN}" \
  --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"${SCRIPT_S3}/ingestion.py\", \"PythonVersion\": \"3\"}" \
  --default-arguments '{"--datalake-formats": "delta", "--job-language": "python"}' \
  --glue-version "${GLUE_VERSION}" \
  --number-of-workers "${WORKERS}" \
  --worker-type "${WORKER_TYPE}" 2>/dev/null || echo "  (job fraude-ingestion-bronze already exists)"

aws glue create-job \
  --region "${REGION}" \
  --name "fraude-transform-silver-gold" \
  --role "${ROLE_ARN}" \
  --command "{\"Name\": \"glueetl\", \"ScriptLocation\": \"${SCRIPT_S3}/transformations.py\", \"PythonVersion\": \"3\"}" \
  --default-arguments '{"--datalake-formats": "delta", "--job-language": "python"}' \
  --glue-version "${GLUE_VERSION}" \
  --number-of-workers "${WORKERS}" \
  --worker-type "${WORKER_TYPE}" 2>/dev/null || echo "  (job fraude-transform-silver-gold already exists)"

# ---------- Helper: wait for job ----------
wait_for_job() {
  local job_name="$1" run_id="$2"
  echo "  Waiting for ${job_name} (${run_id})..."
  while true; do
    status=$(aws glue get-job-run \
      --region "${REGION}" \
      --job-name "${job_name}" \
      --run-id "${run_id}" \
      --query "JobRun.JobRunState" --output text)
    echo "    Status: ${status}"
    if [ "${status}" = "SUCCEEDED" ]; then
      break
    elif [ "${status}" = "FAILED" ] || [ "${status}" = "STOPPED" ] || [ "${status}" = "TIMEOUT" ]; then
      echo "    ERROR: Job ${job_name} ended with status ${status}"
      exit 1
    fi
    sleep 30
  done
}

# ---------- Helper: wait for pending runs before start ----------
wait_for_available_slot() {
  local job_name="$1"
  while true; do
    running=$(aws glue get-job-runs \
      --region "${REGION}" \
      --job-name "${job_name}" \
      --query 'JobRuns[?JobRunState==`RUNNING` || JobRunState==`STARTING` || JobRunState==`STOPPING`].JobRunId' \
      --output text)
    if [ -z "${running}" ]; then
      break
    fi
    echo "  Waiting for previous run of ${job_name} to finish..."
    sleep 30
  done
}

# ---------- 5. Run Bronze ----------
echo ">>> Running: fraude-ingestion-bronze"
wait_for_available_slot "fraude-ingestion-bronze"
RUN_ID_1=$(aws glue start-job-run \
  --region "${REGION}" \
  --job-name "fraude-ingestion-bronze" \
  --query JobRunId --output text)
echo "  RunId: ${RUN_ID_1}"

wait_for_job "fraude-ingestion-bronze" "${RUN_ID_1}"

# ---------- 6. Run Silver+Gold ----------
echo ">>> Running: fraude-transform-silver-gold"
wait_for_available_slot "fraude-transform-silver-gold"
RUN_ID_2=$(aws glue start-job-run \
  --region "${REGION}" \
  --job-name "fraude-transform-silver-gold" \
  --query JobRunId --output text)
echo "  RunId: ${RUN_ID_2}"

wait_for_job "fraude-transform-silver-gold" "${RUN_ID_2}"

echo ""
echo "============================================"
echo " Pipeline ejecutado exitosamente"
echo "============================================"
echo " Bronze: s3://${BUCKET_DATALAKE}/bronze/transactions/"
echo " Silver: s3://${BUCKET_DATALAKE}/silver/transactions/"
echo " Gold:   s3://${BUCKET_DATALAKE}/gold/risk_profile/"
