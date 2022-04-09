# Datacycle

## Getting started

```
cp .env.example .env
vim .env
source .env

poetry install -E "postgres mongo google"
poetry run datacycle
```

```
docker build -f Dockerfile -t datacycle .
docker run -it --rm --env-file .env datacycle
```

### Mac requirements

```
brew install mongodb/brew/mongodb-database-tools
brew install libpq
brew link --force libpq
npm install elasticdump -g
```

### Linux requirements

```
apt install -y mongo-tools
apt install -y postgresql-client
npm install elasticdump -g
```

## How to

```
datacycle --help
datacycle doctor

datacycle mongo "mongodb://user:password@localhost:27017/test1?authSource=admin" "mongodb://user:password@localhost:27017/test2?authSource=admin" --transform "
    transforms {
        test1 {
            before-transform {}
        }
    }
"

datacycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin gs://datacycle-test/test1/snapshot --transform ops.hocon

datacycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin mongodb://user:password@localhost:27017/test2?authSource=admin
datacycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin gs://datacycle-test/test1/snapshot
datacycle mongo mongodb://user:password@localhost:27017/test1?authSource=admin test1/snapshot

datacycle mongo gs://datacycle-test/test1/snapshot mongodb://user:password@localhost:27017/test2?authSource=admin
datacycle mongo gs://datacycle-test/test1/snapshot gs://datacycle-test/test2/snapshot
datacycle mongo gs://datacycle-test/test1/snapshot test2/snapshot

datacycle mongo test1/snapshot mongodb://user:password@localhost:27017/test2?authSource=admin
datacycle mongo test1/snapshot gs://datacycle-test/test2/snapshot
datacycle mongo test1/snapshot test2/snapshot
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
