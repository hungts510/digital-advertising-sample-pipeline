FROM python:3.9-slim

# Set working directory
WORKDIR /usr/app/dbt

# Install system dependencies including Java for Spark connectivity
RUN apt-get update && apt-get install -y \
    curl \
    git \
    gcc \
    g++ \
    netcat-traditional \
    default-jdk \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Install DBT core and Spark adapter
RUN pip install --no-cache-dir \
    dbt-core==1.6.0 \
    dbt-spark==1.6.0

# Copy DBT project files
COPY . .

# Install DBT dependencies
RUN dbt deps --profiles-dir . || echo "No packages.yml found or deps failed"

# Create entrypoint script
RUN echo '#!/bin/bash\n\
echo "🔧 DBT container is ready"\n\
echo "📁 Working directory: $(pwd)"\n\
echo "🔍 Available files: $(ls -la)"\n\
exec "$@"' > /entrypoint.sh && chmod +x /entrypoint.sh

# Set the entrypoint
ENTRYPOINT ["/entrypoint.sh"]

# Default command - keep container alive
CMD ["tail", "-f", "/dev/null"] 