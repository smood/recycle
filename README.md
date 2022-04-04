# Recycle

## Getting started

```
cp .env.example .env
vim .env
source .env

poetry install
poetry run recycle
```

```
docker build -f Dockerfile -t recycle .
docker run -it --rm --env-file .env recycle
```

### Mac requirements

```
brew tap mongodb/brew && brew install mongodb-database-tools
brew install libpq && brew link --force libpq
npm install elasticdump -g
pip3 install gsutil
```

### Linux requirements

```
apt install -y mongo-tools
apt install -y postgresql-client
pip3 install gsutil
npm install elasticdump -g
```

## How to

```
recycle --help
recycle doctor

recycle mongo "mongodb://user:password@localhost:27017/test1?authSource=admin" "mongodb://user:password@localhost:27017/test2?authSource=admin" --transform "
    transforms {
        test1 {
            before-transform {}
        }
    }
"

recycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin gs://smood-recycle-test/test1/snapshot --transform ops.hocon

recycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin mongodb://user:password@localhost:27017/test2?authSource=admin
recycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin gs://smood-recycle-test/test1/snapshot
recycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin test1/snapshot

recycle mongo gs://smood-recycle-test/test1/snapshot mongodb://user:password@localhost:27017/test2?authSource=admin
recycle mongo gs://smood-recycle-test/test1/snapshot gs://smood-recycle-test/test2/snapshot
recycle mongo gs://smood-recycle-test/test1/snapshot test2/snapshot

recycle mongo test1/snapshot mongodb://user:password@localhost:27017/test2?authSource=admin
recycle mongo test1/snapshot gs://smood-recycle-test/test2/snapshot
recycle mongo test1/snapshot test2/snapshot
```

## Providers

### Postgres

https://www.postgresql.org/docs/9.1/backup.html

- SQL dump
- file system snapshot
- continuous archiving

```
pg_dump --clean "postgres://user:password@localhost:5432/test" | gzip > dump.gz
gunzip -c dump.gz | psql "postgres://user:password@localhost:5432/test"
```

### Mongo

https://docs.mongodb.com/manual/core/backups/

- BSON dump
- file system snapshot
- CDC

```
mongodump --uri="mongodb://user:password@localhost:27017/test?authSource=admin" --out=dump --numParallelCollections=10 -v --gzip
mongorestore --uri="mongodb://user:password@localhost:27017/test?authSource=admin" dump/test --numParallelCollections=10 -v --gzip
```

### Elasticsearch

https://github.com/elasticsearch-dump/elasticsearch-dump

- dump

```
elasticdump --input=https://localhost:9200 --output=$ --limit 2000 | gzip > dump.gz
```
