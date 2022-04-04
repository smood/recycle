from datetime import datetime
from datetime import timezone
import gzip
import itertools
import json
import logging
import os
from pathlib import Path
import shutil
from time import sleep

import bson
from furl import furl
import pymongo
from recycle import config
from recycle.utils import fake_like_gen
from recycle.utils import shell
from tqdm import tqdm


def get_database(uri):
    return pymongo.MongoClient(uri).get_default_database()


def log_formatter(log):
    return " ".join(log.split()[1:])


def dump_cli(uri, collection=None, folder="dump", n_parallel=10, jsonarray=False):
    db_name = get_database(uri).name
    assert db_name is not None

    subfolder = str(furl(uri).path).split("/")[1].split("&")[0]
    shell(f"mkdir -p {folder}/{subfolder}")
    cmd = f"mongodump --uri={uri} --out={folder} --numParallelCollections={n_parallel} --gzip"
    if jsonarray:
        # collections = "db.getCollectionNames().forEach(element => print(element))"
        collections = shell(
            f"mongo --quiet {uri} --eval db.getCollectionNames()", json=True
        )

        for collection in json.loads(collections):
            shell(
                f"mongoexport --uri={uri} -c {collection} --jsonArray --out {folder}/{subfolder}/{collection}.json"
            )
    else:
        if collection is not None:
            cmd += f" --collection={collection}"
        shell(cmd, log_formatter=log_formatter)


def restore_cli(
    uri, collection=None, folder="dump", n_parallel=10, keep_previous=False
):
    db_name = get_database(uri).name
    assert db_name is not None

    uri = uri.replace(f"/{db_name}", "/")
    ns_from = "$database$.$collection$"
    ns_to = (
        f"{db_name}.$collection$" if collection is None else f"{db_name}.{collection}"
    )

    cmd = f"mongorestore --uri={uri} {folder} --nsFrom={ns_from} --nsTo={ns_to} --numParallelCollections={n_parallel} --gzip --convertLegacyIndexes"
    if not keep_previous:
        cmd += " --drop"

    shell(cmd, log_formatter=log_formatter)


def bson_file_name(bson_file):
    return str(
        Path(bson_file)
        .name.replace(".gz", "")
        .replace(".bson", "")
        .replace(".slim", "")
    )


def json_file_name(json_file):
    return str(Path(json_file).name.replace(".json", ""))


def transform(ops: config.Ops, folder="dump", jsonarray=False, **kwargs):

    for database, transform in ops.transforms.items():
        files = [str(p) for p in Path(folder).glob(f"{database}/*.bson.gz")]
        if jsonarray:
            files = [str(p) for p in Path(folder).glob(f"{database}/*.json")]
        for file in files:
            collection_name = (
                json_file_name(file) if jsonarray else bson_file_name(file)
            )
            rules = transform.get(collection_name)
            if rules is None:
                logging.info(f"No rules for {collection_name} collection")
            else:
                if rules.drop is not None:
                    logging.info(f"Dropping collection {collection_name} collection")
                    os.remove(file)
                if rules.keep is not None:
                    limit = datetime.now(tz=timezone.utc) - rules.keep
                    logging.info(
                        f"Rules keep from {limit} for {collection_name} collection"
                    )
                    filtr(
                        file,
                        lambda doc: limit <= doc["_id"].generation_time,
                        jsonarray=jsonarray,
                        **kwargs,
                    )
                if rules.anonymize is not {}:
                    logging.info(f"Rules anonymize for {collection_name} collection")
                    if len(rules.anonymize) > 0:
                        override_gen = fake_like_gen(rules.anonymize)
                        filtr(
                            file,
                            lambda doc: doc.update(override_gen()) or True,
                            jsonarray=jsonarray,
                            **kwargs,
                        )


def filtr(file, inplace_transformer, verbose=False, jsonarray=False):
    file = str(file)
    output_file = (
        str(file).replace(".json", ".slim.json")
        if jsonarray
        else str(file).replace(".bson", ".slim.bson")
    )
    reader = gzip.open if file.endswith(".gz") else open
    progress = tqdm if verbose else lambda x: x
    binary = "b" if not jsonarray else ""
    with reader(file, f"r{binary}") as inp:
        with reader(output_file, f"w{binary}") as out:
            if not jsonarray:
                for doc in progress(bson.decode_file_iter(inp)):
                    if "email" in doc:
                        if "smood." in doc["email"] or "jamtech." in doc["email"]:
                            out.write(bson.encode(doc))
                            continue
                    if inplace_transformer(doc):
                        out.write(bson.encode(doc))
            else:
                json_output = []
                for doc in progress(json.load(inp)):
                    if "email" in doc:
                        if "smood." in doc["email"] or "jamtech." in doc["email"]:
                            out.write(bson.encode(doc))
                            continue
                    if inplace_transformer(doc):
                        json_output.append(doc)
                out.write(json.dumps(json_output))

    os.replace(output_file, file)


def pretty_log_write_error(e):
    if len(e.details["writeErrors"]):
        for we in e.details["writeErrors"]:
            logging.error(we["errmsg"])
    elif len(e.details["writeConcernErrors"]):
        for we in e.details["writeConcernErrors"]:
            logging.error(we["errmsg"])
    else:
        logging.error(f"unexpected bulk error: {e.details}")


def backoff_reconnect(fun_ops, *args, max_attempts=5, **kwargs):
    for attempt in range(max_attempts):
        try:
            return fun_ops(*args, **kwargs)
        except pymongo.errors.AutoReconnect as e:
            wait = 0.5 * pow(2, attempt)
            logging.warning(f"Reconnecting... {e}. Waiting {wait} seconds.")
            sleep(wait)
        except pymongo.errors.BulkWriteError as e:
            pretty_log_write_error(e)
            raise e


def batch_itr(itr, batch_size):
    while True:
        head = list(itertools.islice(itr, batch_size))
        if len(head):
            yield head
        else:
            break


def restore_py(
    uri, collection=None, folder="dump", n_parallel=10, keep_previous=False, **kwargs
):
    db_name = get_database(uri).name

    if not keep_previous:
        if collection is None:
            metadata_files = [
                str(p) for p in Path(folder).glob(f"{db_name}/*.metadata.json.gz")
            ]
        else:
            metadata_files = [f"{db_name}/{collection}.metadata.json.gz"]

        backbone_folder = Path(folder) / "backbone"

        for file in metadata_files:
            copy_file = file.replace(str(folder), str(backbone_folder))
            Path(copy_file).parents[0].mkdir(parents=True, exist_ok=True)
            shutil.copy(file, copy_file)

        restore_cli(
            uri,
            collection=collection,
            folder=backbone_folder,
            n_parallel=n_parallel,
            keep_previous=False,
        )

        shutil.rmtree(backbone_folder)

    if collection is None:
        bson_files = [str(p) for p in Path(folder).glob(f"{db_name}/*.bson.gz")]
    else:
        bson_files = [f"{db_name}/{collection}.bson.gz"]

    for bson_file in bson_files:
        collection_name = bson_file_name(bson_file)
        restore_py_collection(uri, collection_name, bson_file, **kwargs)


def restore_py_collection(uri, collection, bson_file, upsert=False, verbose=False):
    db = get_database(uri)

    reader = gzip.open if bson_file.endswith(".gz") else open
    progress = tqdm if verbose else lambda x: x

    with reader(bson_file, "rb") as inp:
        for docs in progress(batch_itr(bson.decode_file_iter(inp), 100)):
            if upsert:
                ops = [
                    pymongo.ReplaceOne({"_id": doc["_id"]}, doc, upsert=True)
                    for doc in docs
                ]
            else:
                ops = [pymongo.InsertOne(doc) for doc in docs]

            res = backoff_reconnect(
                db[collection].bulk_write, ops, ordered=False
            ).bulk_api_result
            logging.info(
                f'{res["nInserted"]} inserts, {res["nModified"]} modified, {res["nUpserted"]} upserts'
            )
