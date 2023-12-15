# This module is a sample reader, please change accordingly.

# Add your data store module import here
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from bson.objectid import ObjectId
# The above is a MongoDB example
import logging as log
import cbelt.lib.utils as utl

"""cfguration dictionary"""
cfg = {}

"""Total number of documents in resultset"""
total_records = 0

"""Reader engine - this is used to read data from"""
engine = None



def init(config):
    """Initialise module."""
    global cfg, total_records, engine
    cfg = config

    log.info(f"Connecting <{cfg['reader_type']}> reader...")
    engine = connect()


def init_subjob(batch):
    """Init batch."""
    global total_records
    
    # This should get a total number of the documents
    total_records = get_total_records(batch, engine)
    log.info(f"Total records in batch to read: {total_records}")


def connect():
    """Connect to your data store here"""
    """Return the object you're going to read from"""

    # Create a new client and connect to the server
    client = MongoClient(cfg['reader_url'], server_api=ServerApi('1'))

    # Select your database
    my_db = client[cfg['mongo_db']]

    # Select your collection
    my_collection = my_db[cfg['mongo_collection']]

    return my_collection


def get_total_records(subjob, engine):
    """Return total dataset record count."""
    # This is a MongoDB example:
    total_documents = engine.count_documents({})
    return total_documents


def read(subjob):
    """Read and yields reader data as documents."""
    """Prepare key expression for a dynamic execution"""

    """This function should yield a list of dictionaries, holding the configured amount of documents.
       The key is an unique string up to 250 bytes,
       The value is the actual JSON document
       For example:
       [
          {
            "foo": {"name": "Alice"}
          },
          {
            "bar": {"name": "Bob"}
          }
       ]
    
    """

    # Get the chunk size from the config
    chunk_size = subjob['reader_chunksize']
    
    # Generate the key
    key = f"f\"{subjob['reader_key']}\""
    
    # This is a MongoDB example
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
            
            # Generate the key
            doc_id = eval(key)

            # Add document to the dictionary
            doc[doc_id] = dict(item)

            # This is MongoDB specific
            doc[doc_id].pop('_id')

            # Add the document to the batch
            docs.append(doc)
        
        if not docs:
            break

        # Send back the list holding the configured amount of documents 
        yield docs            


        
