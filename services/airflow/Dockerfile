# Dockerfile-airflow
FROM apache/airflow:2.10.5
USER root
# Java 설치 + 폰트 설치 같이
RUN apt update && apt install -y \
    curl \
    wget \
    unzip \
    fontconfig \
    fonts-nanum \
    libfreetype6-dev \
    libpng-dev \
    libjpeg-dev \
    python3-dev \
    gcc \
    g++ \
    make \
    pkg-config \
    sudo

# Java 설치 - default-jdk 사용
RUN apt-get update && apt-get install -y default-jdk && \
  echo 'export JAVA_HOME=/usr/lib/jvm/default-java' >> ~/.bashrc && \
  echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc

# Spark 설치
ENV SPARK_VERSION=3.5.4
ENV SPARK_HOME=/opt/spark

RUN wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
  tar -xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ && \
  mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
  rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

# 폰트 캐시 갱신
RUN fc-cache -fv

# 환경변수 추가
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="$JAVA_HOME/bin:/opt/spark/bin:${PATH}"
ENV PYTHONPATH="${PYTHONPATH}:/opt/airflow"

USER airflow
# Python 패키지 설치 - Claude API 및 PDF 관련 패키지 추가
RUN pip install --no-cache-dir \
    apache-airflow-providers-apache-spark \
    pyspark==3.5.4 \
    matplotlib \
    anthropic \
    python-dotenv \
    reportlab \
    pillow \
    tiktoken