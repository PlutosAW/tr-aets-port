import asyncio, os
from typing import List
from motor.motor_asyncio import AsyncIOMotorClient

try: 
    url_mgdb = os.environ['MONGO_URL']
except: 
    url_mgdb = input("Enter MONGO_URL: ")


async def get_mongo(url: str = url_mgdb) -> AsyncIOMotorClient:
    """
    example:
    url: 'mongodb://host1,host2/?replicaSet=my-replicaset-name'
    url: 'mongodb://localhost:27017'
    """
    return AsyncIOMotorClient(url)


class DBInsert:
    def __init__(self, mongo: AsyncIOMotorClient) -> None:
        self.mongo = mongo

    async def insert_perp_position(self, exchange: str, account: str, doc: List[dict]):
        pass

    async def insert_spot_position(self, exchange: str, account: str, doc: List[dict]):
        pass


class DBQuery:
    def __init__(self, mongo: AsyncIOMotorClient) -> None:
        self.mongo = mongo

    async def query_historical_position(self, exchange: str, account: str, contract_type: str, start_time: int, end_time: int):
        pass

    async def query_db(self, db: str, collection: str, condition: dict):
        return await self.mongo[db][collection].find(condition)


if __name__ == "__main__":
    pass
    # async def do_find_one():
    #     cli = await get_mongo("mongodb://amber:amber@10.136.5.217:8017")
    #     await cli.test_db.test_collection.insert_one({"aaa": 123})
    #     async for doc in cli.test_db.test_collection.find():
    #         print(doc)
    # asyncio.run(do_find_one())







