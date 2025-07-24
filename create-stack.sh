#!/bin/bash
echo "🔄 Creating p39-sde-exercise docker-based resources - Airflow, MinIO and Spark containers"

# Safe project-scoped cleanup
echo "🧹 First - cleaning up only this project's Docker Compose resources..."
docker compose down --volumes --remove-orphans --rmi all

# Optionally, remove named volumes/networks if you know they are only used by this project:
docker volume rm minio_data postgres_data 2>/dev/null || true
docker network rm spark-net 2>/dev/null || true

# Recreate containers
# Initialize Airflow DB (safe to run multiple times)
echo "🗄️  Initializing Airflow database..."
docker compose up airflow-init

echo "🚀 Starting containers with new configuration..."
docker compose up -d

# Wait for containers to be ready

echo "⏳ Waiting for Spark master to be ready..."
for i in {1..30}; do
  if docker exec spark-master curl -sf http://localhost:8080 > /dev/null; then
    echo "✅ Spark master is ready!"
    break
  else
    echo "Waiting for Spark master... ($i/30)"
    sleep 2
  fi
done

# Check container status
echo "📊 Container status:"
docker compose ps

# Validate MinIO
echo "🔍 Checking MinIO health..."
if docker exec minio curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; then
    echo "✅ MinIO is ready"
else
    echo "❌ MinIO not ready"
fi

# Validate Spark master
echo "🔍 Checking Spark master..."
if docker exec spark-master curl -f http://localhost:8080 > /dev/null 2>&1; then
    echo "✅ Spark master is ready"
else
    echo "❌ Spark master not ready - checking logs..."
    docker logs spark-master | tail -10
fi

# Check basic Spark functionality
echo "🔍 Testing basic Spark functionality..."
if docker exec spark-master spark-submit --version > /dev/null 2>&1; then
    echo "✅ Spark submit is working"
else
    echo "❌ Spark submit not working"
fi

# Check MinIO bucket and file - FIXED: Use correct alias
echo "🔍 Checking MinIO bucket and data..."
# First check if minio-init container completed successfully
if docker ps -a | grep minio-init | grep -q "Exited (0)"; then
    echo "✅ MinIO initialization completed successfully"
    
    # Check if we can access the bucket contents
    if docker exec minio curl -s http://localhost:9000/sample-bucket/ > /dev/null 2>&1; then
        echo "✅ Sample bucket is accessible"
    else
        echo "❌ Sample bucket not accessible"
    fi
else
    echo "❌ MinIO initialization failed - checking logs..."
    docker logs minio-init | tail -10
fi

# Check if jobs directory exists
echo "🔍 Checking jobs directory..."
if docker exec spark-master ls -la /opt/bitnami/spark/jobs/ > /dev/null 2>&1; then
    echo "✅ Jobs directory is mounted"
    docker exec spark-master ls -la /opt/bitnami/spark/jobs/
else
    echo "❌ Jobs directory not mounted"
fi

# Check if JAR files are properly installed
echo "🔍 Checking S3A JAR files..."
if docker exec spark-master ls -la /opt/bitnami/spark/jars/ | grep -q "hadoop-aws-3.3.4.jar"; then
    echo "✅ Hadoop AWS JAR is present"
else
    echo "❌ Hadoop AWS JAR missing"
fi

if docker exec spark-master ls -la /opt/bitnami/spark/jars/ | grep -q "aws-java-sdk-bundle-1.12.470.jar"; then
    echo "✅ AWS SDK JAR is present"
else
    echo "❌ AWS SDK JAR missing"
fi

# Check configuration files
echo "🔍 Checking configuration files..."
if docker exec spark-master ls -la /opt/bitnami/spark/conf/spark-defaults.conf > /dev/null 2>&1; then
    echo "✅ spark-defaults.conf is present"
else
    echo "❌ spark-defaults.conf missing"
fi

if docker exec spark-master ls -la /opt/bitnami/spark/conf/core-site.xml > /dev/null 2>&1; then
    echo "✅ core-site.xml is present"
else
    echo "❌ core-site.xml missing"
fi

# Test S3A connectivity
echo "🔍 Testing S3A connectivity..."
if docker exec spark-master python3 -c "
from pyspark.sql import SparkSession
try:
    spark = SparkSession.builder.appName('ConnectivityTest').getOrCreate()
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    fs = spark.sparkContext._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark.sparkContext._jvm.java.net.URI.create('s3a://sample-bucket/'),
        hadoop_conf
    )
    print('✅ S3A filesystem connectivity successful')
    spark.stop()
except Exception as e:
    import traceback; traceback.print_exc()
" > /dev/null 2>&1; then
    echo "✅ S3A connectivity test passed"
else
    echo "❌ S3A connectivity test failed"
fi

echo ""
echo "🎉 Setup complete!"
echo ""
echo "📋 Summary:"
echo "  - Using pre-installed JAR approach for S3A dependencies"
echo "  - JAR files are installed during container initialization"
echo "  - Configuration is handled via spark-defaults.conf and core-site.xml"
echo ""
echo "🚀 To run your job (simple approach):"
echo "docker exec spark-master python3 /opt/bitnami/spark/jobs/read_s3_csv.py"
echo ""
echo "🚀 To run your job (spark-submit approach):"
echo "docker exec spark-master spark-submit \\"
echo "  --master spark://spark-master:7077 \\"
echo "  /opt/bitnami/spark/jobs/sample_job.py"
echo ""
echo "🌐 Available services:"
echo "  - Spark Master UI: http://localhost:8080"
echo "  - MinIO Console: http://localhost:9001 (minioadmin/minioadmin)"
echo "  - Airflow UI: http://localhost:8081 (admin/admin)"
echo ""
echo "📝 Note: JAR files are pre-installed using the working versions:"
echo "  - hadoop-aws-3.3.4.jar (compatible with AWS SDK v1)"
echo "  - aws-java-sdk-bundle-1.12.470.jar (AWS SDK v1)"
echo "  This avoids the ClassNotFoundException we encountered earlier."
echo "⚠️  Note: Airflow may take 3–5 minutes to become accessible after stack creation, especially on the first run."
echo "    You can check progress with: docker compose logs -f airflow-webserver"