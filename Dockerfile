FROM python:3.13

WORKDIR /code

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

ENV http_proxy=http://localhost:8080
ENV https_proxy=https://localhost:8080
ENV no_proxy=localhost,127.0.0.1

COPY . .

CMD ["python", "-m", "celery", "-A", "main", "worker", "--beat", "--pool=threads", "--loglevel=INFO"]
