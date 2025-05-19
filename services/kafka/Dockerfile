FROM confluentinc/cp-kafka:7.4.0

# root 권한으로 필요한 패키지 설치
USER root

# 1. Python 및 필수 도구 설치
RUN yum update -y && \
    yum install -y \
        python3 \
        python3-pip \
        curl \
        wget \
        net-tools \
        iputils \
        bind-utils \
        vim \
        lsof \
        nc \
        unzip \
        tar \
        git \
        postgresql-devel \
        gcc \
        python3-devel && \
    yum clean all && \
    rm -rf /var/cache/yum

# 2. pip 업그레이드 및 필요한 패키지만 설치
RUN pip3 install --upgrade pip setuptools wheel && \
    pip3 install \
        kafka-python==2.0.2 \
        psycopg2-binary==2.9.8

# 3. 환경 변수 설정
ENV PYTHONPATH="/opt/workspace:$PYTHONPATH"

# 4. 작업 디렉토리 설정
WORKDIR /opt/workspace

# 5. 다시 appuser로 전환 (보안상)
USER appuser

# 6. Kafka 서버용 포트 개방
EXPOSE 9092

# 7. Kafka 브로커 실행 (Confluent의 기본 실행 명령 사용)
CMD ["/etc/confluent/docker/run"]