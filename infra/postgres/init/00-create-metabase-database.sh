#!/usr/bin/env sh
set -eu

psql \
  --username "$POSTGRES_USER" \
  --dbname "$POSTGRES_DB" \
  --set ON_ERROR_STOP=on \
  --command "CREATE DATABASE \"$METABASE_DB_NAME\";"
