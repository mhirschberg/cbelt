# Couchbase ELT Tool (cbelt)

This repository contains Couchbase ELT Tool (aka `cbelt`) allowing to import data from various source systems into Couchbase with ease.

# Installation

Installation is pretty easy and consists of following steps(we assuming you have Python 3.9 or above in place):

1. clone the repository
2. switch into the cloned repo directory
3. install `setuptols` if needed by issuing 

```bash
pip install --upgrade pip setuptools wheel
```

4. install cbelt as a utility with 

```bash
pip install .
```

After the installation you have to be able to make a test runby issuing  `cbelt` in your command line

# Configuration

`cbelt` is configured via json configuration files placed under *config* directory in the root of the project. Several configuration examples for different kinds of sources are already included into the repository. Please check comments provided in example configuration files for a further configuration details

# Execution 

To start a `cbelt` with a particular configuration you have to issue *cbelt* command providing relative or absolute path to a desired configuration file. For example:

```bash
cbelt ./config/rdbms.json
```

# Principle of work 

Upon the start `cbelt` follows followinf steps:

1. opens a configuration file 
2. read `source` and `targets` sections
3. establishes connection to the source and target systems
4. reads the data from the source as configured in form of pandas dataframes
5. writes the data into the target Couchbase database using *bulk* methods


# License
Copyright 2021 Couchbase Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
