# Use base image with Oracle JDK 8 (matches laptop)
FROM openjdk:8-jdk

# Install Python and pip
RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

# Install PySpark 3.5.0 (compatible with Java 8)
RUN pip3 install pyspark==3.5.0

# Install other Python dependencies
RUN pip3 install flask flask-cors psycopg2-binary pandas sqlalchemy

# Set working directory
WORKDIR /app

# Copy application files
COPY app.py .
COPY requirements.txt .
COPY postgresql-42.7.2.jar .
COPY templates/ templates/

# Create uploads and downloads directories
RUN mkdir -p uploads downloads

# Set Java environment variables to match laptop path structure
ENV JAVA_HOME=/usr/local/openjdk-8
ENV PATH=$JAVA_HOME/bin:$PATH

# Create symbolic link to match laptop path structure
RUN mkdir -p /opt/java && \
    ln -s /usr/local/openjdk-8 /opt/java/jdk-1.8

# Expose port
EXPOSE 5001

# Run the application
CMD ["python3", "app.py"] 