FROM postgres
ENV POSTGRES_DB saifudb
COPY saifudb.sql /docker-entrypoint-initdb.d/
COPY test_data.sql /docker-entrypoint-initdb.d/
