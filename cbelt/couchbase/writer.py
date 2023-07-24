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


cfg = {}
"""Configuration dictionary"""
lock = threading.Lock()
"""Semaphor """
processed_records = 0
"""Numer of written records"""
cluster = None
"""Couchbase cluster"""
collection = None
"""Couchbase collection"""
url = None
"""Couchbase SQLAlchemy like URL"""


def init(config):
    """Initialise object."""
    global cfg, cluster, url
    cfg = config

    url = sa.make_url(utl.get_dict_env(cfg, "writer_url"))

    log.info(f"Connecting <{config['writer_type']}> writer...")
    cluster = connect()


def init_subjob(subjob):
    """Init subjob."""
    global total_records, cluster, collection
    bucket = cluster.bucket(url.database)
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
        return


def upsert_documents(docs):
    """Save documents into Couchbase."""
    global lock, processed_records
    try:
        collection.upsert_multi(docs)
        lock.acquire()
        processed_records += len(docs)
        lock.release()
    except Exception as e:
        print(f"Failed to upsert batch: {str(e)}")
