"""Module contains Couchbase ELT target class."""
import logging as log
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
import threading
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
import cbelt.lib.utils as utl
import sqlalchemy as sa
import time
import uuid

lock = threading.Lock()
"""Semaphor """
processed_records = 0
"""Numer of written records"""
cluster = None
"""Couchbase cluster"""
bucket = None
"""Couchbase bucket"""
collection = None
"""Couchbase collection"""
subjob = None
"""Subjob Configuration"""
key_expression: str = ""
"""Subjob Configuration"""


def init(config):
    """Initialise object."""
    global cfg, cluster, bucket
    cfg = config

    url = sa.engine.make_url(utl.get_dict_env(cfg, "writer_url"))

    log.info(f"Connecting <{config['writer_type']}> writer...")
    cluster = connect(url)
    bucket = cluster.bucket(url.database)


def init_subjob(sub_job):
    """Init subjob."""
    global total_records, collection, bucket, subjob, key_expression

    subjob = sub_job
    scope = bucket.scope(subjob["writer_scope"])
    collection = scope.collection(subjob["writer_collection"])
    total_records = 0
    key_expression = f"f\"{subjob['writer_key']}\""


def connect(url):
    """Return datasource engine."""
    auth = PasswordAuthenticator(url.username, url.password)
    options = ClusterOptions(auth)
    cluster = Cluster(f"{url.drivername}://{url.host}", options)
    cluster.wait_until_ready(timedelta(seconds=5))
    return cluster


def write(subjob, docs):
    """Write data in multithread mode."""
    """Split document list into the list of lists for threading"""
    docs_map = []
    map_size = round(len(docs) / subjob["writer_threads"] / 3)
    docs_map = [docs[i : i + map_size] for i in range(0, len(docs), map_size)]

    # start multi-threaded transfer
    with ThreadPoolExecutor(max_workers=subjob["writer_threads"]) as executor:
        executor.map(upsert_document, docs_map)


def upsert_document(docs):
    """Save documents into Couchbase."""
    global lock, processed_records, key_expression
    # doc_id = eval(key_expression)
    # collection.upsert(doc_id,doc)

    # log.info(doc_id)
    # time.sleep(10)
    try:
        # Transform list of documents to list of ids+docs
        multi_doc = {}
        for doc in docs:
            # calculate document UUID
            multi_doc_id = eval(key_expression)
            multi_doc[multi_doc_id] = doc

        # upsert transformed list
        collection.upsert_multi(multi_doc)
        with lock:
            processed_records += len(docs)
    except Exception as e:
        print(f"Failed to upsert batch: {str(e)}")
