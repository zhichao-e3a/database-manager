import json
import hashlib
from datetime import datetime
from contextlib import asynccontextmanager
from typing import Dict, Any, List, AsyncIterator, Optional, Tuple

import asyncio
from motor.motor_asyncio import AsyncIOMotorClient

from pymongo import UpdateOne
from pymongo.errors import AutoReconnect, BulkWriteError

class MongoDBConnector:

    def __init__(self, cfg):

        self._host   = cfg["DB_HOST"]
        self._db     = cfg["DB_NAME"]
        self._client = AsyncIOMotorClient(self._host, minPoolSize=5, maxPoolSize=50)

    @asynccontextmanager
    async def resource(self, coll_name):

        await self._client.admin.command('ping')
        coll = self._client[self._db][coll_name]

        yield coll

    async def stream_all_documents(

            self,
            coll_name   : str,
            query       : Optional[Dict[str, Any]] = {},
            projection  : Optional[Dict[str, int]] = None,
            sort        : Optional[List[Tuple[str, int]]] = None,
            batch_size  : Optional[int] = 1000

    ) -> AsyncIterator[List[Dict[str, Any]]]:

        async with self.resource(coll_name) as coll:

            sort = sort or [("utime", 1), ("_id", 1)]

            def make_cursor(base_q: Dict[str, Any], after_id=None):

                q = dict(base_q)

                if after_id is not None:

                    if "_id" in q and isinstance(q["_id"], dict):
                        q["_id"] = {**q["_id"], "$gt": after_id}
                    else:
                        q["_id"] = {"$gt": after_id}

                return coll.find(filter=q, projection=projection, sort=sort, batch_size=batch_size, no_cursor_timeout=True)

            cursor = make_cursor(query)
            buf: List[Dict[str, Any]] = []
            last_id = None
            retried = False

            try:
                while True:

                    try:
                        doc = await cursor.next()

                    except StopAsyncIteration:
                        break

                    except AutoReconnect:
                        if retried: raise
                        await cursor.close()
                        await asyncio.sleep(0.5)
                        cursor = make_cursor(query, after_id=last_id)
                        retried = True
                        continue

                    buf.append(doc)

                    if "_id" in doc: last_id = doc["_id"]

                    if len(buf) >= batch_size:
                        yield buf
                        buf = []

                if buf: yield buf

            finally:
                await cursor.close()

    async def get_all_documents(

            self,
            coll_name   : str,
            query       : Optional[Dict[str, Any]] = {},
            projection  : Optional[Dict[str, Any]] = None,
            sort        : Optional[List[Tuple[str, int]]] = None,
            batch_size  : Optional[int] = 1000

    ):

        async with self.resource(coll_name) as coll:

            try:
                cursor = coll.find(filter=query, projection=projection, sort=sort, batch_size=batch_size, no_cursor_timeout=True)
                return [doc async for doc in cursor]

            except AutoReconnect:
                await asyncio.sleep(0.5)
                cursor = coll.find(filter=query, projection=projection, sort=sort, batch_size=batch_size, no_cursor_timeout=True)
                if batch_size: cursor = cursor.batch_size(batch_size)
                return [doc async for doc in cursor]

    @staticmethod
    async def _flush(coll, ops):

        try:
            await coll.bulk_write(ops, ordered=False)

        except BulkWriteError as bwe:
            codes = {e.get("code") for e in (bwe.details or {}).get("writeErrors", [])}
            if codes & {6, 7, 89, 91, 189, 9001}:
                await asyncio.sleep(0.5)
                await coll.bulk_write(ops, ordered=False)

        except AutoReconnect:
            await asyncio.sleep(0.5)
            await coll.bulk_write(ops, ordered=False)

    @staticmethod
    def _fingerprint(obj, fields: Optional[List[str]]=None):

        if fields is None:
            return None

        clean = {k:v for k,v in obj.items() if k in fields}

        blob = json.dumps(
            clean, sort_keys=True, indent=4, separators=(',', ': '), default=str
        ).encode()

        return hashlib.sha1(blob).hexdigest()

    async def upsert_documents_hashed(

            self,
            coll_name   : str,
            records     : List[Dict[str, Any]],
            id_fields   : Optional[List[str]]=None,
            fields      : Optional[List[str]] = None,
            batch_size  : Optional[int] = 500

    ) -> None:

        async with self.resource(coll_name) as coll:

            ops = []

            for item in records:

                to_insert = dict(item)

                if id_fields is None:
                    _id = item.get("_id")
                    to_insert.pop("_id")

                else:
                    _id = ""
                    for f in id_fields:
                        _id += item.get(f)

                try:
                    to_insert.pop("doc_hash"); to_insert.pop('utime') ; to_insert.pop('ctime')
                except KeyError:
                    pass

                h = await asyncio.to_thread(self._fingerprint, to_insert, fields)

                op = UpdateOne(
                    {
                        "_id": _id,
                        "$or" : [
                            {"doc_hash": {"$ne": h}},
                            {"doc_hash" : {"$exists" : False}}
                        ]

                    },
                    {
                        "$set" : {**to_insert, "doc_hash" : h, "utime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")},
                        "$setOnInsert": {"ctime": datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
                    },
                    upsert=True
                )

                ops.append(op)

                if len(ops) >= batch_size:
                    await self._flush(coll, ops)
                    ops = []

            if ops: await self._flush(coll, ops)

    async def delete_document(self, coll_name: str, query: Optional[Dict[str, Any]]={}):

        async with self.resource(coll_name) as coll:

            try:
                res = await coll.delete_one(query)
                return res.deleted_count

            except AutoReconnect:
                await asyncio.sleep(0.5)
                res = await coll.delete_one(query)
                return res.deleted_count

    async def delete_all_documents(self, coll_name: str, query: Optional[Dict[str, Any]]={}) -> None:

        async with self.resource(coll_name) as coll:

            try:
                res = await coll.delete_many(query)
                return res.deleted_count

            except AutoReconnect:
                await asyncio.sleep(0.5)
                res = await coll.delete_many(query)
                return res.deleted_count
