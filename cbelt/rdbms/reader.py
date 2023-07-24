"""Module contains common Couchbase ELT RDBMS reader."""
import sqlalchemy as sa
import logging as log
import pandas as pd
import cbelt.lib.utils as utl

cfg = {}
"""cfguration dictionary"""
total_records = 0
"""Total number of rows in resultset"""
engine = None
"""SQLAlchemy datareader engine"""


def init(config):
    """Initialise module."""
    global cfg, total_records, engine
    cfg = config

    log.info(f"Connecting <{cfg['reader_type']}> reader...")
    engine = connect()


def init_subjob(batch):
    """Init batch."""
    global total_records
    total_records = get_total_records(batch)
    log.info(f"Total records in batch to read: {total_records}")


def connect():
    """Return datareader engine."""
    return sa.create_engine(
        utl.get_dict_env(cfg, "reader_url"),
        execution_options=dict(stream_results=True),
    )


def get_total_records(subjob):
    """Return total dataset record count."""
    count_query = f"select count(1) cnt from ({subjob['reader_query']})"
    df = pd.read_sql(count_query, engine)
    return df["cnt"].iloc[0]


def read(subjob):
    """Read and yelds reader data as documents."""
    """Prepare key expression for a dynamic execution"""
    key = f"f\"{subjob['reader_key']}\""

    for chunk in pd.read_sql(
        subjob["reader_query"], engine, chunksize=subjob["reader_chunksize"]
    ):
        # Initialise resulting record batch
        docs = []
        for item in chunk.itertuples(index=False, name="Row"):
            # Initialise dictionary to save dataframe row
            doc = {}
            row = item._asdict()

            # Generate a composite row ID
            doc_id = eval(key)

            # Add row document to the dictionary
            doc[doc_id] = row

            # Add the document to the batch
            docs.append(doc)
        yield docs
    return
