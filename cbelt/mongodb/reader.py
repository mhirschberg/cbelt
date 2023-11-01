"""Module contains common Couchbase ELT MongoDB reader."""
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import logging as log
import cbelt.lib.utils as utl

cfg = {}
"""cfguration dictionary"""
total_records = 0
"""Total number of documents in resultset"""
engine = None
"""MongoDB datareader engine"""


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
    # Create a new client and connect to the server
    client = MongoClient(cfg['reader_url'], server_api=ServerApi('1'))

    # Select your database
    my_db = client[cfg['mongo_db']]

    # Select your collection
    my_collection = my_db[cfg['mongo_collection']]

    return my_collection


def get_total_records(subjob, engine):
    """Return total dataset record count."""
    total_documents = engine.count_documents({})
    return total_documents


def read(subjob):
    """Read and yelds reader data as documents."""
    """Prepare key expression for a dynamic execution"""
    # Initialize the last_id to 0

    last_id = None
    
    chunk_size = subjob['reader_chunksize']
    
    key = f"f\"{subjob['reader_key']}\""
    
    while True:
        # Create an empty list to store the documents for this chunk
        docs = []

        if last_id:
            cursor = engine.find({'_id': {'$gt': last_id}}).limit(chunk_size)
        else:
            cursor = engine.find().limit(chunk_size)

        for item in cursor:
            # Store each document in a dictionary
            doc = {}
            last_id = item['_id']
            
            # Generate a composite row ID
            doc_id = eval(key)

            # Add row document to the dictionary
            doc[doc_id] = utl.dict_timestamp_to_string(dict(item))
            doc[doc_id].pop('_id')
            # Add the document to the batch

            docs.append(doc)
            print (docs)
            exit (0)
        
        if not docs:
            break
        
        yield docs            


        
