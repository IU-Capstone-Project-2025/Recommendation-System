FROM python:3.13-slim

ENV LISTEN_PORT=8000
EXPOSE 8000

RUN pip install poetry  
RUN mkdir -p /app  
COPY . /app

WORKDIR /app

RUN poetry install --no-root
RUN poetry add python-multipart

CMD ["poetry", "run", "python3", "-m", "src.microservices.recommendation_system_project"]