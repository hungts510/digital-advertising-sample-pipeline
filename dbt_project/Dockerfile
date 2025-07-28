FROM python:3.9-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    curl \
    openjdk-11-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Install DBT and dependencies
RUN pip install --no-cache-dir \
    dbt-core==1.8.7 \
    dbt-spark==1.8.0 \
    pyspark==3.5.0

# Create app directory
WORKDIR /usr/app/dbt

# Copy DBT project files
COPY . .

# Install DBT packages
RUN dbt deps

# Set DBT profiles directory
ENV DBT_PROFILES_DIR=/usr/app/dbt

# Create entrypoint script
RUN echo '#!/bin/bash\n\
# Wait for Spark to be ready\n\
echo "Waiting for Spark master to be ready..."\n\
until curl -f http://spark-master:8080 > /dev/null 2>&1; do\n\
  echo "Spark not ready, waiting..."\n\
  sleep 5\n\
done\n\
echo "Spark is ready!"\n\
\n\
# Execute DBT command\n\
exec "$@"' > /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["dbt", "run"] 