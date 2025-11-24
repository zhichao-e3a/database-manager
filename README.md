# database-manager

Centralised repository for managing **MongoDB** and **MySQL** database connectors in a consistent, reusable way.

* Loading DB credentials from a central config (`.env`)
* Async MongoDB operations (Motor)
* Efficient batched reads, upserts, hashed-change detection
* MySQL access via SSH tunnel (SQLAlchemy + SSHTunnelForwarder)

This repository centralises all DB interaction logic for internal pipelines and services.

---

## **📁 Project Structure**

```
database-manager/
│
├── config/
│   ├── configs.py        # Loads .env, exposes configs
│   ├── ef_aliyun_pem     # SSH private key
│   └── .env              # Environment variables
│
├── database/
│   ├── MongoDBConnector.py   # Async MongoDB client + helpers
│   ├── SQLDBConnector.py     # MySQL connector through SSH tunnel
│   └── queries.py            # Predefined SQL queries
│
```

---

## 🔧 Usage

### Pre-requisites

```bash
pip install -r requirements.txt
```
- Ensure `.env` and `ef_aliyun_pem` are correctly set up in `config/` directory
- After installing dependencies, navigate to `site-packages/sshtunnel.py` (or equivalent path) and **comment out all 'DSSKey' attributes**
  - Failure to do so will result in `AttributeError: module 'paramiko' has no attribute 'DSSKey'` errors

### MongoDBConnector

An async wrapper around Motor:

- **Batched, hashed upserts (`upsert_documents_hashed`)**

- **Batched streaming (`stream_all_documents`)**

- **Fetch all documents (`get_all_documents`)**

```python
from MongoDBConnector import MongoDBConnector
import asyncio

mongo = MongoDBConnector("remote") # or "local"
docs = asyncio.run(mongo.get_all_documents("{coll_name}"))

# If running from .ipynb:
# docs = await mongo.get_all_documents("{coll_name}")
```

### SQLDBConnector

Provides reliable **MySQL over SSH** access:

- **SSH tunnel wrapper**
- **SQLAlchemy engine wrapper**
- **Query to DataFrame**

```python
from SQLDBConnector import SQLDBConnector

sql = SQLDBConnector()
df  = sql.query_to_dataframe(query="SELECT ...")
```

---
