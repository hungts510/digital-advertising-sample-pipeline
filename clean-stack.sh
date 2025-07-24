#!/bin/bash
set -e

# Name of the compose file (adjust if needed)
COMPOSE_FILE="docker-compose.yml"

# Print header
echo "🧹 Aggressively cleaning all Docker Compose resources for this project..."

# Stop and remove all containers, networks, and volumes defined in docker-compose.yml
if [ -f "$COMPOSE_FILE" ]; then
    echo "🛑 Stopping and removing containers, networks, and volumes..."
    docker compose -f "$COMPOSE_FILE" down --volumes --remove-orphans --rmi all
else
    echo "❌ $COMPOSE_FILE not found in current directory. Aborting."
    exit 1
fi

# Remove named volumes (if defined in docker-compose)
echo "💾 Removing named volumes if they exist..."
docker volume rm minio_data postgres_data 2>/dev/null || true

# Remove networks (if defined in docker-compose)
echo "🌐 Removing custom networks if they exist..."
docker network rm p39-sde-exercise_spark-net spark-net 2>/dev/null || true

# Remove containers by name (if any are left)
echo "🗑️  Removing specific containers if they exist..."
for c in minio minio-init spark-master spark-worker postgres airflow-init airflow-webserver airflow-scheduler; do
    docker rm -f $c 2>/dev/null || true
done

# Remove images used by this compose file (if any remain)
echo "🖼️  Removing images used by this project if they exist..."
for img in minio/minio:latest minio/mc:latest bitnami/spark:latest postgres:15 apache/airflow:2.10.5; do
    docker rmi $img 2>/dev/null || true
done

# Remove dangling images (built by compose, not tagged)
echo "🖼️  Removing dangling images built by compose..."
docker images -f "dangling=true" -q | xargs -r docker rmi 2>/dev/null || true

# Print final status
echo ""
echo "✅ Aggressive Docker Compose environment cleanup complete."
echo "You can now run: ./create-stack.sh or docker compose up -d"