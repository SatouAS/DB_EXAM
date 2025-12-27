FROM mongo:7.0

COPY mongo_data/data.reviews.json /docker-entrypoint-initdb.d/01_reviews.json

COPY mongo_data/data.reviews.csv /tmp/data.reviews.csv

COPY import_csv.sh /docker-entrypoint-initdb.d/02_import_csv.sh

RUN chmod +x /docker-entrypoint-initdb.d/02_import_csv.sh