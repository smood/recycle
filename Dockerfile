FROM python:3.9.8-slim-bullseye

RUN apt update && \
    apt install -y wget gnupg && \
    wget -qO - https://www.mongodb.org/static/pgp/server-4.4.asc | apt-key add - && \
    echo "deb http://repo.mongodb.org/apt/debian buster/mongodb-org/4.4 main" | tee /etc/apt/sources.list.d/mongodb-org-4.4.list && \
    echo "deb http://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list && \
    wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - && \
    wget -qO - https://deb.nodesource.com/setup_16.x | bash - && \
    apt update && \
    apt install -y \ 
    postgresql-client-13 \
    mongodb-org-tools \
    mongodb-org-shell \
    nodejs && \
    npm install elasticdump -g && \
    pip install --no-cache-dir jsonlines && \
    pip install --no-cache-dir gsutil && \
    pip install --no-cache-dir google-cloud-bigquery && \
    pip install --no-cache-dir pymongo[srv] && \
    pip install --no-cache-dir dnspython && \
    npm cache clean --force && \
    apt remove -y wget gnupg && \
    apt autoremove -y && \
    rm -rf /var/lib/apt/lists/* 

WORKDIR /app
COPY . ./recycle

RUN  pip install --no-cache-dir ./recycle

ENTRYPOINT [ "recycle" ]
