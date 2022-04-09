FROM python:3.10-slim-bullseye as base

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

RUN apt update \
    && apt install -y --no-install-recommends wget gnupg lsb-release \
    && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" | tee /etc/apt/sources.list.d/pgdg.list \
    && wget -qO - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
    && wget -qO - https://deb.nodesource.com/setup_16.x | bash - \
    && wget -qO mongodb-database-tools.deb https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian10-x86_64-100.5.2.deb \
    && wget -qO - https://raw.githubusercontent.com/python-poetry/poetry/master/get-poetry.py | python \
    && apt update \
    && apt install -y --no-install-recommends \
        postgresql-client-14 \
        ./mongodb-database-tools.deb \
        nodejs \
    && npm install elasticdump -g \
    && npm cache clean --force \
    && apt remove -y wget gnupg lsb-release \
    && apt autoremove -y \
    && rm -rf /var/lib/apt/lists/* mongodb-database-tools.deb

COPY . .

RUN pip install --no-cache-dir .

ENTRYPOINT [ "datacycle" ]
