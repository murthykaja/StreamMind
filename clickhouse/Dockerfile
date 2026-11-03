FROM clickhouse/clickhouse-server:24.8-alpine
ENV CLICKHOUSE_SKIP_USER_SETUP=1
COPY init.sql /docker-entrypoint-initdb.d/init.sql
