#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# deploy_scheduler.sh — Orquestación via EventBridge Scheduler
# ============================================================
# Reemplaza MWAA con:
#   1. Lambda orquestador (inicia Glue jobs en secuencia)
#   2. EventBridge Scheduler (disparo diario @ 03:00 UTC)
#
# Pre-requisitos: aws CLI, zip, Glue jobs existentes
# ============================================================

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGION="${AWS_DEFAULT_REGION:-us-east-1}"
LAMBDA_ROLE="arn:aws:iam::${ACCOUNT_ID}:role/LambdaGlueOrchestratorRole"
LAMBDA_NAME="glue-fraude-orchestrator"
SCHEDULE_NAME="fraude-diario-0300"
SCHEDULE_GROUP="fraude-pipeline"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LAMBDA_ZIP="/tmp/${LAMBDA_NAME}.zip"

echo "=== 1. IAM Role para Lambda ==="
cat > /tmp/lambda-trust-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Principal": { "Service": "lambda.amazonaws.com" },
    "Action": "sts:AssumeRole"
  }]
}
EOF

aws iam create-role --role-name LambdaGlueOrchestratorRole \
  --assume-role-policy-document file:///tmp/lambda-trust-policy.json 2>/dev/null \
  && echo "  Role created" || echo "  Role already exists"

cat > /tmp/lambda-execution-policy.json << EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:StartJobRun",
        "glue:GetJobRun",
        "glue:GetJobRuns"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "logs:CreateLogGroup",
        "logs:CreateLogStream",
        "logs:PutLogEvents"
      ],
      "Resource": "arn:aws:logs:${REGION}:${ACCOUNT_ID}:log-group:/aws/lambda/${LAMBDA_NAME}:*"
    }
  ]
}
EOF

aws iam put-role-policy --role-name LambdaGlueOrchestratorRole \
  --policy-name LambdaGlueExecutionPolicy \
  --policy-document file:///tmp/lambda-execution-policy.json

echo "=== 2. Empaquetar Lambda ==="
cd "$SCRIPT_DIR"
pip install -q boto3 -t /tmp/lambda-pkg 2>/dev/null || true
cp glue_orchestrator_lambda.py /tmp/lambda-pkg/
cd /tmp/lambda-pkg
zip -rq "$LAMBDA_ZIP" .
cd "$SCRIPT_DIR"

echo "=== 3. Crear/Actualizar Lambda ==="
if aws lambda get-function --function-name "$LAMBDA_NAME" &>/dev/null; then
  aws lambda update-function-code --function-name "$LAMBDA_NAME" --zip-file fileb://"$LAMBDA_ZIP" --quiet
  echo "  Lambda updated"
else
  aws lambda create-function \
    --function-name "$LAMBDA_NAME" \
    --runtime python3.12 \
    --role "$LAMBDA_ROLE" \
    --handler glue_orchestrator_lambda.lambda_handler \
    --zip-file fileb://"$LAMBDA_ZIP" \
    --timeout 600 \
    --environment Variables="{JOB_INGESTION=fraude-ingestion-bronze,JOB_TRANSFORM=fraude-transform-silver-gold,POLL_INTERVAL=10,TIMEOUT=600}"
  echo "  Lambda created"
fi

echo "=== 4. EventBridge Scheduler ==="
# Crear grupo de schedules
aws scheduler create-schedule-group --name "$SCHEDULE_GROUP" 2>/dev/null && \
  echo "  Schedule group created" || echo "  Schedule group already exists"

# Obtener ARN de la Lambda
LAMBDA_ARN=$(aws lambda get-function --function-name "$LAMBDA_NAME" --query Configuration.FunctionArn --output text)

# Dar permisos a EventBridge para invocar Lambda
aws lambda add-permission \
  --function-name "$LAMBDA_NAME" \
  --statement-id AllowEventBridgeScheduler \
  --action lambda:InvokeFunction \
  --principal scheduler.amazonaws.com \
  --source-arn "arn:aws:scheduler:${REGION}:${ACCOUNT_ID}:schedule/${SCHEDULE_GROUP}/${SCHEDULE_NAME}" \
  2>/dev/null || true

# Crear schedule diario a las 03:00 UTC
aws scheduler create-schedule \
  --name "$SCHEDULE_NAME" \
  --group-name "$SCHEDULE_GROUP" \
  --flexible-time-window '{ "Mode": "OFF" }' \
  --schedule-expression "cron(0 3 * * ? *)" \
  --target "{
    \"Arn\": \"${LAMBDA_ARN}\",
    \"RoleArn\": \"arn:aws:iam::${ACCOUNT_ID}:role/LambdaGlueOrchestratorRole\",
    \"Input\": \"{\\\"source\\\":\\\"scheduler\\\",\\\"schedule\\\":\\\"diario-0300\\\"}\"
  }" \
  2>&1 && echo "  Schedule created" || echo "  Schedule already exists, updating…"

echo ""
echo "=== Deploy completo ==="
echo "Lambda:     $LAMBDA_NAME"
echo "Schedule:   ${SCHEDULE_GROUP}/${SCHEDULE_NAME} (daily @ 03:00 UTC)"
echo ""
echo "Para probar manualmente la Lambda:"
echo "  aws lambda invoke --function-name $LAMBDA_NAME /tmp/test-output.json"
echo "  cat /tmp/test-output.json"
