#!/bin/bash

# Exit on any error
set -e

echo "Setting up test environment..."

# Ensure MinIO is running
if ! docker ps | grep -q minio; then
    echo "Starting MinIO container..."
    docker compose up -d minio
    
    # Wait for MinIO to be ready
    echo "Waiting for MinIO to be ready..."
    sleep 5
fi

# Initialize MinIO bucket and upload test data
echo "Initializing MinIO..."
docker compose up -d minio-init

# Install required Python packages if not already installed
echo "Installing required Python packages..."
pip install pyspark boto3 minio

# Run the test pipeline
echo "Running pipeline test..."
python3 scripts/test_pipeline_locally.py

echo "Test completed!" 