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
import pprint

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
subjob_cfg = None
"""Subjob Configuration"""
key_expression: str = ""
"""Subjob Configuration"""
defaut_writer_threads: int = 5
"""Number of threads by default if not specified in config"""


def init(config):
    """Initialise object."""
    global cfg, cluster, bucket
    cfg = config

    url = sa.engine.make_url(utl.get_dict_env(cfg, "writer_url"))

    log.info(f"Connecting <{config['writer_type']}> writer...")
    cluster = connect(url)
    bucket = cluster.bucket(url.database)


def init_subjob(subjob_config):
    """Init subjob."""
    global total_records, collection, bucket, subjob_cfg, key_expression, cfg

    subjob_cfg = subjob_config

    writer_scope = utl.nvl_dict(
        subjob_cfg, 'writer_scope',
        utl.nvl_dict(cfg, 'default_writer_scope', None)
    )

    scope = bucket.scope(writer_scope)

    writer_collection = utl.nvl_dict(
        subjob_cfg, 'writer_collection',
        utl.nvl_dict(cfg, 'default_writer_collection', None)
    )

    collection = scope.collection(writer_collection)

    total_records = 0
    writer_key = utl.nvl_dict(
        subjob_cfg, 'writer_key', 
        utl.nvl_dict(
            cfg, 'default_writer_key', None)
    )
    
    key_expression = f"f\"{writer_key}\""


def connect(url):
    """Return datasource engine."""
    auth = PasswordAuthenticator(url.username, url.password)
    options = ClusterOptions(auth)
    cluster = Cluster(f"{url.drivername}://{url.host}", options)
    cluster.wait_until_ready(timedelta(seconds=5))
    return cluster


def write(subjob, docs):
    """Write data in multithread mode."""
    # get number of threads from config
    writer_threads = utl.nvl_dict(
        subjob, 'writer_threads', 
        utl.nvl_dict(cfg, 'default_writer_threads', 
                     defaut_writer_threads)         
    )

    """Split document list into the list of lists for threading"""
    docs_map = []
    map_size = round(len(docs) / writer_threads / 3)
    if map_size == 0:
        map_size=1
    docs_map = [docs[i : i + map_size] for i in range(0, len(docs), map_size)]
    # start multi-threaded transfer
    with ThreadPoolExecutor(max_workers = writer_threads) as executor:
        executor.map(upsert_document, docs_map)


def upsert_document(docs):
    """Save documents into Couchbase."""
    global lock, processed_records, key_expression

    try:
        # Transform list of documents to list of ids+docs
        multi_doc = {}
        for doc in docs:
            # calculate document UUID
            multi_doc_id = eval(key_expression)
            multi_doc[multi_doc_id] = doc
        # upsert transformed list
        #print(multi_doc)
        collection.upsert_multi(multi_doc)
        with lock:
            processed_records += len(docs)
    except Exception as e:
        pprint.pprint (f"Exeption {str(e)} with following documents: {multi_doc}")
        exit (0)
        print(f"Failed to upsert batch: {str(e)}")
