"""Module contains common Couchbase reader. The idea is to migrate the data from older Couchbase versions to Capella"""
from couchbase.cluster import Cluster
from couchbase.bucket import Bucket
from couchbase.auth import PasswordAuthenticator
from couchbase.options import ClusterOptions
import logging as log
import cbelt.lib.utils as utl
import pprint

cfg = {}
"""cfguration dictionary"""
total_records = 0
"""Total number of documents in resultset"""
engine = None
"""Couchbase datareader engine"""


def init(config):
    """Initialise module."""
    global cfg, total_records, engine
    cfg = config

    log.info(f"Connecting <{cfg['reader_type']}> reader...")
    engine = connect()


def init_subjob(batch):
    """Init batch."""
    global total_records
    total_records = get_total_records(batch, engine)
    log.info(f"Total records in batch to read: {total_records}")


def connect():
    """Return datareader engine."""
    # Configure the connection
    auth = PasswordAuthenticator(cfg['reader_user'], cfg['reader_password']) 
    options = ClusterOptions(auth)
    global cluster
    cluster = Cluster(cfg['reader_url'], options)  
    
    # Open the bucket
    bucket = cluster.bucket(cfg['reader_bucket'])
    
    return bucket

def get_total_records(subjob, engine):
    """Return total dataset record count."""
    query = "SELECT COUNT(*) AS total_items FROM `" + cfg['reader_bucket'] + "`"
    results = cluster.query(query)
    for row in results.rows():
        num_keys= row['total_items']
    print (num_keys)
    exit (0)
    return num_keys

def read(subjob, engine):
    """Read and yelds reader data as documents."""
    """Prepare key expression for a dynamic execution"""

    chunk_size = subjob['reader_chunksize']
    bucket = engine
    result = {}
    keys = []

    while True:
        for key in bucket.keys():
            keys.append(key)

            if len(keys) == chunk_size:
                # Fetch documents for the current chunk of keys
                for key, value in bucket.get_multi(keys).items():
                    result[key.decode()] = value.value

                # Yield the current chunk of documents
                yield result
                result = {}
                keys = []

        # Yield any remaining documents
        if keys:
            for key, value in bucket.get_multi(keys).items():
                result[key.decode()] = value.value
            yield result

        
        if not not keys:
            break
