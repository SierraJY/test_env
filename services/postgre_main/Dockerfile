FROM postgres:16

# 초기화 SQL 스크립트와 인증 설정 파일 복사
COPY init.sql /docker-entrypoint-initdb.d/
COPY pg_hba.conf /etc/postgresql/pg_hba.conf

# 커스텀 pg_hba.conf 파일 사용
CMD ["postgres", "-c", "hba_file=/etc/postgresql/pg_hba.conf"]