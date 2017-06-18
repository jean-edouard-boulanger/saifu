FROM postgres:9.6.3-alpine

ENV POSTGRES_DB saifudb
COPY saifudb.sql /docker-entrypoint-initdb.d/
COPY test_data.sql /docker-entrypoint-initdb.d/
