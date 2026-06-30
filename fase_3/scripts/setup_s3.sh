#!/bin/bash
set -euo pipefail

# ============================================================
# Setup S3 — Crear buckets y subir datos/scripts
# ============================================================
# Pre:  aws CLI instalado y configurado
# Post: datos en bucket-origen, scripts en ambos buckets
# ============================================================

BUCKET_ORIGEN="bucket-origen-adrianespinoza"
BUCKET_DATALAKE="datalake-adrianespinoza"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

echo "1. Creando buckets..."
aws s3 mb "s3://${BUCKET_ORIGEN}"  --region "${REGION}" 2>/dev/null && echo "   ${BUCKET_ORIGEN} creado" || echo "   ${BUCKET_ORIGEN} ya existe"
aws s3 mb "s3://${BUCKET_DATALAKE}" --region "${REGION}" 2>/dev/null && echo "   ${BUCKET_DATALAKE} creado" || echo "   ${BUCKET_DATALAKE} ya existe"

echo ""
echo "2. Subiendo datos sinteticos (10M filas, 2.3 GB)..."
aws s3 cp data/raw/ "s3://${BUCKET_ORIGEN}/raw-data/" --recursive

echo ""
echo "3. Subiendo scripts PySpark..."
aws s3 cp src/ "s3://${BUCKET_ORIGEN}/scripts/" --recursive --exclude "__pycache__/*"

echo ""
echo "============================================"
echo " Buckets listos:"
echo "   Origen:  s3://${BUCKET_ORIGEN}/raw-data/"
echo "   Scripts: s3://${BUCKET_ORIGEN}/scripts/"
echo "   Datalake: s3://${BUCKET_DATALAKE}/"
echo "============================================"
