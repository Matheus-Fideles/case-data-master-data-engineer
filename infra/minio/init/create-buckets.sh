#!/bin/sh
set -e

# Aguarda MinIO estar pronto
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" 2>/dev/null; do
  echo "Aguardando MinIO..."
  sleep 2
done

# Cria buckets se não existirem
for bucket in landing bronze silver gold; do
  mc mb --ignore-existing "local/$bucket"
  echo "Bucket '$bucket' pronto."
done

echo "MinIO init concluído."
