"""Module contains common Couchbase ELT MongoDB reader."""
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import bson
import json
import logging as log
import cbelt.lib.utils as utl


def decode_value(value):
    if isinstance(value, dict):
        # Check if the value is a dictionary with a single key starting with '$'
        if len(value) == 1 and list(value.keys())[0].startswith('$'):
            return list(value.values())[0]
        else:
            # If the value is another dictionary, recursively decode it
            return {key: decode_value(val) for key, val in value.items()}
    elif isinstance(value, list):
        # If the value is a list, recursively decode each element
        return [decode_value(item) for item in value]
    elif isinstance(value, (int, float)):
        # If the value is a numerical type, keep it as is
        return value
    else:
        # Otherwise, convert the value to string
        return str(value)

def decode_dictionary(encoded_dict):
    return {key: decode_value(value) for key, value in encoded_dict.items()}

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
        
    while True:
        # Create an empty list to store the documents for this chunk
        docs = []

        
        if last_id:
            my_cursor = engine.find({'_id': {'$gt': last_id}}).limit(chunk_size)
        else:
            my_cursor = engine.find().limit(chunk_size)


        ''' 
        # Right now we're keeping the extended JSON as-is. 
        # But we could transform to a legacy JSON using this encoder (right now works with datetime and decimal128)
        class myJSONencoder(json.JSONEncoder):
            import datetime
            from bson.decimal128 import Decimal128
            def default(self, z):
                if isinstance(z, datetime.datetime):
                    return (str(z.isoformat()))
                elif isinstance(z, Decimal128):
                    return round((float(str(z))), 2)
                
                else:
                    return super().default(z)
        '''

        cursor = json.loads(bson.json_util.dumps(my_cursor))

        for nr, item in enumerate(cursor):
            # Store each document in a dictionary
            doc = {}
            last_id = item['_id']

            doc[nr] = item
            doc[nr].pop('_id')
            # Add the document to the batch
            docs.append(doc)
        
        if not docs:
            break

        yield docs
