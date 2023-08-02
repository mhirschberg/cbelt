"""Module contains Couchbase ELT target class."""
import logging as log
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
import threading
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import (
    BucketNotFoundException,
    CollectionNotFoundException,
)
import cbelt.lib.utils as utl
import sqlalchemy as sa

cfg = {}
"""Configuration dictionary"""
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
url = None
"""Couchbase SQLAlchemy like URL"""


def init(config):
    """Initialise object."""
    global cfg, cluster, url, bucket
    cfg = config

    url = sa.engine.make_url(utl.get_dict_env(cfg, "writer_url"))

    log.info(f"Connecting <{config['writer_type']}> writer...")
    cluster = connect()
    bucket = get_bucket(url.database)


def init_subjob(subjob):
    """Init subjob."""
    global total_records, cluster, collection, lock, bucket

    scope = bucket.scope(subjob["writer_scope"])
    collection = scope.collection(subjob["writer_collection"])


def connect():
    """Return datasource engine."""
    auth = PasswordAuthenticator(url.username, url.password)
    options = ClusterOptions(auth)
    cluster = Cluster(f"{url.drivername}://{url.host}", options)
    cluster.wait_until_ready(timedelta(seconds=5))
    return cluster


def write(subjob, docs):
    """Write data in multithread mode."""
    with ThreadPoolExecutor(max_workers=subjob["writer_threads"]) as executor:
        executor.map(upsert_documents, docs)


def upsert_documents(docs):
    """Save documents into Couchbase."""
    global lock, processed_records
    try:
        collection.upsert_multi(docs)
        with lock:
            processed_records += len(docs)
    except Exception as e:
        print(f"Failed to upsert batch: {str(e)}")


def get_bucket(bucket_name):
    """Return or create bucket if needed."""
    global cluster, bucket
    try:
        bucket = cluster.bucket(bucket_name)
        log.info(f"Bucket '{bucket_name}' already exists.")
        return bucket
    except BucketNotFoundException:
        log.info(f"Bucket '{bucket_name}' does not exist. Creating...")
        try:
            bucket = cluster.buckets().create_bucket(url.database, None)
            log.info(f"Bucket '{bucket_name}' created successfully.")
            return bucket
        except Exception as e:
            log.info(f"Error creating bucket '{bucket_name}': {e}")
            raise


def get_collection(scope, collection_name, collection_settings=None):
    """Return or create collection if needed."""
    try:
        collection = scope.collection_create(
            collection_name, collection_settings
        )
        print(f"Collection '{collection_name}' already exists.")
        return collection
    except CollectionNotFoundException:
        log.info(f"Collection '{collection_name}' does not exist. Creating...")
        try:
            scope.collection_create(collection_name, collection_settings)
            log.info(f"Collection '{collection_name}' created successfully.")
            return scope.collection(collection_name)
        except Exception as e:
            log.info(f"Error creating collection '{collection_name}': {e}")
            raise
