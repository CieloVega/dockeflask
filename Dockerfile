FROM python:latest

WORKDIR /app

COPY . .

RUN pip install Flask

EXPOSE 3000

CMD ["python", "app.py"]
