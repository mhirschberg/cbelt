#!/usr/bin/env python3

import sys
import configparser
from datetime import timedelta
from concurrent.futures import ThreadPoolExecutor
from couchbase.cluster import Cluster
from couchbase.options import ClusterOptions
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException, ScopeNotFoundException, CollectionNotFoundException
import pandas as pd
import sqlalchemy as sa

# Read the config from file, should be any given
# This also gives you an idea which variables are required, but might be provided in a different way
if len(sys.argv) > 1:
    try:
        config = configparser.ConfigParser()
        config.read(sys.argv[1])
        db = config.get('source_db_config', 'db')
        table = config.get('source_db_config', 'table')
        endpoint = config.get('cb_config', 'endpoint')
        username = config.get('cb_config', 'username')
        password = config.get('cb_config', 'password')
        bucket_name = config.get('cb_config', 'bucket_name')
        scope_name = config.get('cb_config', 'scope_name')
        collection_name = config.get('cb_config', 'collection_name')
        BATCH_SIZE = config.getint('threads_config', 'BATCH_SIZE')
        THREADS = config.getint('threads_config', 'THREADS')
    except:
        print (f"Not all variables found in {sys.argv[1]}")

# CB Connect options - authentication
auth = PasswordAuthenticator(username, password)
#Get a reference to our cluster
options = ClusterOptions(auth)
cluster = Cluster(endpoint, options)
# Wait until the cluster is ready for use.
cluster.wait_until_ready(timedelta(seconds=5))

# Open the bucket
bucket = cluster.bucket(bucket_name)

# Get the scope / scope
scope = bucket.scope(scope_name)
collection = scope.collection(collection_name)

def stream_data(db, table):
    # This creates a DB connection, results will be streamed in chunks
    engine = sa.create_engine(db, execution_options=dict(stream_results=True))

    j=1
    # Read data in chunks, send the chunks to the function upserting to Couchbase
    for chunk_dataframe in pd.read_sql(f"SELECT * FROM {table}", engine, chunksize=10000):
        print(f"Working with the chunk {j}")
        load_data(chunk_dataframe)
        j+=1
    

def upsert_documents(docs):
    try:
        collection.upsert_multi(docs)
    except Exception as e:
        print(f"Failed to upsert batch: {str(e)}")

def load_data(my_chunk):
    data=[]
    for row in my_chunk.itertuples(index=False, name='Row'):
        data.append(row._asdict())                       

    with ThreadPoolExecutor(max_workers=THREADS) as executor:
            # Create tasks
            tasks = list(process_data(data))
            # Execute tasks
            executor.map(upsert_documents, tasks)
            
    data.clear()
def process_data(data):
    docs = {}
    for item in data:
        # Generate a composite document ID - change as you need
        try:
            doc_id = f"{item['storeno']}::{item['businessdate']}::{item['workstationid']}::{item['cashierno']}::{item['transactionsequencenumber']}"
        except KeyError as e:
            print(f"Warning: Key not found in document: {str(e)}")
            continue

        # Add the document to the batch
        docs[doc_id] = item

        # If we've reached the batch size, add it to the tasks
        if len(docs) >= BATCH_SIZE:
            yield docs
            docs = {}

    # Yield any remaining documents
    if docs:
        yield docs
        
# Load data
stream_data(db, table)
