#!/bin/sh
set -e

MINIO_HOST="minio:9000"
BUCKET="sample-bucket"
ACCESS_KEY="minioadmin"
SECRET_KEY="minioadmin"
CSV_LOCAL_PATH="/data/sample.csv"
CSV_REMOTE_NAME="sample.csv"

# Extract host and port for nc
HOST=$(echo $MINIO_HOST | cut -d: -f1)
PORT=$(echo $MINIO_HOST | cut -d: -f2)

# Wait for MinIO to be up by trying to set the alias
until mc alias set myminio http://$MINIO_HOST $ACCESS_KEY $SECRET_KEY 2>/dev/null; do
  echo "Waiting for MinIO at $MINIO_HOST..."
  sleep 2
done

echo "MinIO is up. Configuring client."

mc alias set myminio http://$MINIO_HOST $ACCESS_KEY $SECRET_KEY

# Create bucket if it doesn't exist
if ! mc ls myminio/$BUCKET >/dev/null 2>&1; then
  echo "Creating bucket: $BUCKET"
  mc mb myminio/$BUCKET
else
  echo "Bucket $BUCKET already exists."
fi

# Upload CSV file
if mc ls myminio/$BUCKET/$CSV_REMOTE_NAME >/dev/null 2>&1; then
  echo "File $CSV_REMOTE_NAME already exists in $BUCKET. Overwriting."
fi
mc cp $CSV_LOCAL_PATH myminio/$BUCKET/$CSV_REMOTE_NAME

echo "MinIO bucket and CSV upload complete." 