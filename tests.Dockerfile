FROM python:3.13-slim

RUN apt-get update && apt-get install -y libpq-dev gcc python3-dev
RUN pip install pytest
WORKDIR /app
COPY . .
RUN pip install pytest requests
CMD ["pytest", "tests/", "-v"]