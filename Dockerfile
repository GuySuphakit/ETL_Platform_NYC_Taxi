FROM apache/airflow:2.8.0-python3.11

USER root

# Update and install JDK 17
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk && \
    rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME
ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64/
RUN export JAVA_HOME

RUN mkdir -p /opt/airflow/jars && \
    curl -L https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -o /opt/airflow/jars/postgresql-42.7.4.jar

USER airflow

# Copy the requirements file
COPY requirements.txt .

# Install Python dependencies from requirements.txt
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt
