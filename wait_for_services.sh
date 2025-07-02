#!/bin/bash

# Ожидание PostgreSQL
while ! pg_isready -h postgres -p 5432 -U ${POSTGRES_USER}; do
  echo "Waiting for PostgreSQL..."
  sleep 1
done

# Ожидание Backend (если нужно)
while ! curl -s "http://backend:8000" >/dev/null; do
  echo "Waiting for Backend..."
  sleep 1
done

echo "All services are ready! Running tests..."
pytest tests/ -v > tests/tests.log 2>&1
tail -f tests/tests.log