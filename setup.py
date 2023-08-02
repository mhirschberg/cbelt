from setuptools import setup, find_packages

setup(
    name="cbelt",
    version="0.1.2",
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        "Click",
        "couchbase",
        "pandas",
        "SQLAlchemy",
        "pymongo",
        "tqdm",
        "google-cloud-bigquery",
        "google-cloud-bigquery-storage",
        "pyarrow",
    ],
    entry_points={
        "console_scripts": [
            "cbelt = cbelt.app:cli",
        ],
    },
)
