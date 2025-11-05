FROM python:latest

WORKDIR /app

COPY . .

RUN pip install Flask
RUN pip install Flask psycopg2-binary
EXPOSE 3000

CMD ["python", "app.py"]
