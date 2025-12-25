# database-manager

- Centralised repository for managing database services
- Single source for database logic across internal pipelines and services

---

## Pre-requisites

- Ensure `.env` and `ef_aliyun_pem` exist in `config/` folder 
- Due to deprecated DSS key support in newer versions of `paramiko` you must patch `sshtunnel`:
  - Locate the file `site-packages/sshtunnel.py` (path may vary)
  - Comment out all references to `DSSKey`
  - Failure to do so will result in: `AttributeError: module 'paramiko' has no attribute 'DSSKey'`

---

## Project Structure

```
database-manager/
│
├── src/
│   └── database_manager/
│       ├── __init__.py                 # Package entry point
│       └── database/
│           ├── __init__.py
│           ├── mongo.py                # MongoDB connector
│           ├── mysql.py                # MySQL connector
│           └── queries.py              # Centralised SQL queries
│
├── config/
│   ├── configs.py                      # Loads .env and exposes configs
│   ├── ef_aliyun_pem                   # SSH private key
│   └── .env                            # Environment variables
│
├── mongo_manager.py                    # MongoDB utility
├── RAW_RECORDS.js                      # MongoDB JSON schema (RAW_RECORDS)
├── requirements.txt                    # Runtime dependencies
├── pyproject.toml                      # Package metadata & build config
└── README.md
```

---

## `src/database_manager`

- src/database_manager is the core Python package of this repository
- All database logic live here so they can be imported cleanly by pipelines, services, notebooks, and APIs

### 📥 Installation

##### Development

- Editable install allows changes to immediately take effect
- Ideal for local development

```python
# RUN FROM OTHER REPO
pip install -e /path/to/src/database_manager
```

##### Stable Usage

- Build wheel from this repository (a `.whl` file will be produced)

```python
# RUN FROM THIS REPO
python -m build
```

- Place the file into the other repository and run

```python
# RUN FROM OTHER REPO
pip install dist/database_manager-<version>-py3-none-any.whl
```

### 🔄 Versioning & Updates

1. Update the version number in `pyproject.toml`
2. Commit the change
3. Build the new package and install the new version where needed

### 🔌 MongoDBConnector Usage

- Use this when you need to read or write MongoDB data 
- Full collection reads (`get_all_documents`)
- Stream large collections (`stream_all_documents`) 
- Writes data in batches (`upsert_documents_hashed`)


```python
from database_manager.database.mongo import MongoDBConnector
from config.configs import TEST_MONGO_CONFIG # or REMOTE_MONGO_CONFIG

import asyncio

mongo = MongoDBConnector(TEST_MONGO_CONFIG)
docs = asyncio.run(mongo.get_all_documents("{collection_name}"))
```

### 🔌 SQLDBConnector Usage

- Use this when you need to query MySQL databases
- Opens a secure SSH tunnel automatically
- Connects to MySQL safely
- Returns query results as Pandas DataFrames

```python
from database_manager.database.sql import SQLDBConnector
from config.configs import SQL_CONFIG

sql = SQLDBConnector(SQL_CONFIG)
df = sql.query_to_dataframe(query="SELECT ...")
```

---

##  MongoDB Collection Schemas

- Safe against accidental field loss or type corruption 
  - Prevent malformed or partial writes 
  - Prevent silent schema drift (MongoDB is schemaless by default)
- `RAW_RECORDS.js` defines the  MongoDB JSON Schema for the `RAW_RECORDS` collection


### 💻 CLI Usage

```bash
mongosh --nodb \
  --eval "var MONGO_URI='{INSERT_MONGO_URI}'" \
  schemas/RAW_RECORDS.js # or schemas/FILT_RECORDS.js
```

### 🚨 Expected Failure Behaviour

- Invalid writes will fail with `MongoServerError: Document failed validation`
- This indicates a pipeline or ingestion bug, not a database issue

---

## `mongo_manager.py`

- Utility for organising MongoDB collections 
  - Archive collections 
  - Move collections across databases

### 💻 CLI Usage

```
python mongo_manager.py
```

### 🌀 Internal Flow

- `mongodump` reads collection from the source DB and streams to `mongorestore`
- `--nsFrom / --nsTo` cleanly remap namespaces
- `--drop` ensures no residual data 
- Source collection is deleted only after success

```python
# Dump source collection

[
    "mongodump",
    "--archive",
    f"--uri={URI}",
    f"--db={db_from}",
    f"--collection={collection}"
]
```

```python
# Stream directly into restore
[
    "mongorestore",
    "--archive",
    f"--uri={URI}",
    f"--nsFrom={db_from}.{collection}",
    f"--nsTo={db_to}.{collection}",
    "--drop"
]
```

---