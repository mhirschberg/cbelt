#!/usr/bin/env python3
"""Couchbase ELT entry point"""
import json
import logging as log
import tqdm
import click
from importlib import import_module
import sys

print(sys.path)

log.basicConfig(format="%(levelname)s:%(message)s", level=log.INFO)


@click.command()
@click.version_option()
@click.argument(
    "config_filename",
    type=click.Path(exists=True, resolve_path=True),
)
def cli(config_filename):
    """CONFIG_FILENAME is a name of the configuration file."""
    config_name = click.format_filename(config_filename)
    click.echo(config_name)

    # Read configuration from provided JSON file
    config = json.load(open(config_name))

    # Iterate through the job definitions
    for job in config["jobs"]:

        # Define and import writer module
        writer_module_name = f"cbelt.{job['writer_type']}.writer"

        log.info(f"Using writer module '{writer_module_name}'...")
        writer = import_module(writer_module_name)
        writer.init(job)

        # Define and import reader reading module
        reader_module_name = f"cbelt.{job['reader_type']}.reader"
        log.info(f"Using reader module '{reader_module_name}'...")
        reader = import_module(reader_module_name)
        reader.init(job)    

        for subjob in job["subjobs"]:
            # Init progress bar
            reader.init_subjob(subjob)
            writer.init_subjob(subjob)
            pbar = tqdm.tqdm(total=reader.total_records)

            # Iterate throgh reader batches
            for docs in reader.read(subjob):
                writer.write(subjob, docs)
                pbar.update(n=writer.processed_records - pbar.n)
                #pbar.update(n= 10000)

    log.info("Completed.")


if __name__ == "__main__":
    cli()
