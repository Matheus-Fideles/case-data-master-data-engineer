#!/bin/sh
set -e

until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  echo "Aguardando MinIO..."
  sleep 2
done

# ── Buckets ──────────────────────────────────────────────────────────────────
for bucket in landing bronze silver gold; do
  mc mb --ignore-existing "local/$bucket"
  echo "Bucket '$bucket' pronto."
done

# ── Policies IAM ──────────────────────────────────────────────────────────────
# svc-pipeline: leitura/escrita em todos os buckets (Spark + Airflow)
cat > /tmp/policy-pipeline.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
        "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::landing",  "arn:aws:s3:::landing/*",
        "arn:aws:s3:::bronze",   "arn:aws:s3:::bronze/*",
        "arn:aws:s3:::silver",   "arn:aws:s3:::silver/*",
        "arn:aws:s3:::gold",     "arn:aws:s3:::gold/*"
      ]
    }
  ]
}
EOF

# svc-serving: somente leitura em gold (Trino + Metabase)
cat > /tmp/policy-serving.json << 'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject", "s3:ListBucket", "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::gold", "arn:aws:s3:::gold/*"
      ]
    }
  ]
}
EOF

mc admin policy create local pipeline-rw  /tmp/policy-pipeline.json
mc admin policy create local serving-ro   /tmp/policy-serving.json
echo "Policies criadas: pipeline-rw, serving-ro"

# ── Service accounts ──────────────────────────────────────────────────────────
mc admin user add local "$MINIO_SVC_PIPELINE_USER" "$MINIO_SVC_PIPELINE_PASSWORD"
mc admin policy attach local pipeline-rw --user "$MINIO_SVC_PIPELINE_USER"
echo "Usuário '$MINIO_SVC_PIPELINE_USER' criado com policy pipeline-rw"

mc admin user add local "$MINIO_SVC_SERVING_USER" "$MINIO_SVC_SERVING_PASSWORD"
mc admin policy attach local serving-ro --user "$MINIO_SVC_SERVING_USER"
echo "Usuário '$MINIO_SVC_SERVING_USER' criado com policy serving-ro"

echo "MinIO init concluído."
