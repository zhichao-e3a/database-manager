import pandas as pd
from contextlib import contextmanager

from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

class SQLDBConnector:

    def __init__(self, cfg):

        self._ssh_host  = cfg["SSH_HOST"]
        self._ssh_port  = cfg["SSH_PORT"]
        self._ssh_user  = cfg["SSH_USER"]
        self._ssh_pkey  = cfg["SSH_PKEY"]
        self._db_host   = cfg["DB_HOST"]
        self._db_port   = cfg["DB_PORT"]
        self._db_user   = cfg["DB_USER"]
        self._db_pass   = cfg["DB_PASS"]
        self._db_name   = cfg["DB_NAME"]

    @contextmanager
    def ssh_tunnel(self):

        tunnel = SSHTunnelForwarder(
            ssh_address_or_host=(self._ssh_host, self._ssh_port),
            ssh_username=self._ssh_user,
            ssh_pkey=self._ssh_pkey,
            remote_bind_address=(self._db_host, self._db_port)
        )

        try:
            tunnel.start()
            local_bind_port = tunnel.local_bind_port
            print(f"SSH TUNNEL STARTED ON PORT {local_bind_port}")
            yield local_bind_port

        finally:
            tunnel.close()

    @contextmanager
    def connect(self):

        with self.ssh_tunnel() as local_bind_port:

            url = URL.create(
                drivername="mysql+pymysql",
                username=self._db_user,
                password=self._db_pass,
                host="127.0.0.1",
                port=local_bind_port,
                database=self._db_name,
                query={"charset": "utf8mb4"},
            )

            engine = create_engine(
                url,
                pool_size=8,
                max_overflow=8,
                pool_recycle=3600,
                pool_pre_ping=True,
                future=True,
                connect_args={"connect_timeout": 10}
            )

            try:
                yield engine
            finally:
                engine.dispose()

    def query_to_dataframe(self, query, chunksize=None):
        with self.connect() as engine:
            return pd.read_sql(query, engine, chunksize=chunksize)