FROM python:3.11-slim

WORKDIR /usr/app/dbt

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential libpq-dev git \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir dbt-core==1.7.6 dbt-postgres==1.7.6

COPY . .

ENTRYPOINT ["dbt"]
CMD ["run", "--profiles-dir", "/root/.dbt"]
