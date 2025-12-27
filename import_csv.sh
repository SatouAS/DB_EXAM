#!/usr/bin/env bash

mongoimport \
  --db mongo \
  --collection reviews \
  --type csv \
  --file /tmp/data.reviews.csv \
  --headerline \
  --mode upsert \
  --upsertFields review_id