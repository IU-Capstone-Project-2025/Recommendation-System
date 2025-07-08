FROM python:3.13-slim

ENV LISTEN_PORT=8000
EXPOSE 8000


RUN apt-get update && apt-get install -y libpq-dev gcc python3-dev

RUN apt-get update && \
    apt-get install -y \
    libpq-dev \
    gcc \
    python3-dev \
    g++ \  
    build-essential 

RUN mkdir -p /app 
COPY . /app
RUN g++ /app/src/scripts/searching_mechanism/levenshtein_length.cpp -o /app/src/scripts/searching_mechanism/levenshtein_length
RUN pip install poetry

WORKDIR /app

RUN poetry lock
RUN poetry install --no-root
RUN poetry add clickhouse_driver
RUN poetry add python-multipart

CMD ["poetry", "run", "python3", "-m", "src.microservices.recommendation_system_project"]
