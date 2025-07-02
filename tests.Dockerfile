FROM python:3.13-slim

RUN apt-get update && apt-get install -y libpq-dev gcc python3-dev postgresql-client curl
RUN pip install pytest
WORKDIR /app
COPY . .
RUN pip install pytest requests dotenv psycopg2-binary

COPY wait_for_services.sh /wait_for_services.sh
RUN chmod +x /wait_for_services.sh

CMD ["/wait_for_services.sh"]