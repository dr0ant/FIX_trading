FROM python:3.11-slim

WORKDIR /app

COPY . .

RUN pip install quickfix pandas boto3 psycopg2-binary

CMD ["python", "parser.py"]
