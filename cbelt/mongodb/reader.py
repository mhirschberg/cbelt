"""Module contains common Couchbase ELT MongoDB reader."""
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
import bson.json_util
import json
import logging as log
import cbelt.lib.utils as utl

'''
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
'''

cfg = {}
"""cfguration dictionary"""
total_records = 0
"""Total number of documents in resultset"""
engine = None
"""MongoDB datareader engine"""
my_collection = None
"""Working current MongoDB collection"""

def init(config):
    """Initialise module."""
    global cfg, total_records, engine
    cfg = config

    log.info(f"Connecting <{cfg['reader_type']}> reader...")
    engine = connect()


def init_subjob(batch):
    """Init batch."""
   # Select your collection
    global my_collection
    my_collection = engine[batch['mongo_collection']]

    global total_records
    
    total_records = get_total_records(batch, my_collection)
    log.info(f"Total records in batch to read: {total_records}")


def connect():
    """Return datareader engine."""
    # Create a new client and connect to the server
    client = MongoClient(cfg['reader_url'], server_api=ServerApi('1'))

    # Select your database
    my_db = client[cfg['mongo_db']]

    return my_db


def get_total_records(subjob, my_collection):
    """Return total dataset record count."""
    total_documents = my_collection.count_documents({})
    return total_documents

def read(subjob):
    """Read and yelds reader data as documents."""
    """Prepare key expression for a dynamic execution"""
    # Initialize the last_id to 0
    default_reader_chunksize = 1000
    global my_collection
    last_id = None
    
    try: 
        chunk_size = subjob['reader_chunksize']
    except:
        chunk_size = default_reader_chunksize
    
        
    while True:
        # Create an empty list to store the documents for this chunk
        docs = []
        
        if last_id:
            my_cursor = my_collection.find({'_id': {'$gt': last_id}}).limit(chunk_size)
        else:
            my_cursor = my_collection.find().limit(chunk_size)

        cursor = json.loads(bson.json_util.dumps(my_cursor))

        for item in cursor:
            # Store each document in a dictionary
            # And replace ObjectId by its value in the field "_id"            
            last_id = item['_id']
            id_str = dict(last_id)
            item['_id'] = id_str['$oid']

            docs.append(item)
            
        if not docs:
            break

        yield docs
