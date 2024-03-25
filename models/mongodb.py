import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from typing import List
from config import mongo_url

async def mongodb() -> AsyncIOMotorDatabase:
    db_url = mongo_url
    db_name = "mangabot"
    client = AsyncIOMotorClient(db_url, 27017)
    db = client[db_name]
    return db

async def add(db, collection_name, data):
    if "_id" not in data:
        result = await db[collection_name].insert_one(data)
    else:
        # If you want to update only chapter_url and keep url same
        if 'url' in data and 'chapter_url' in data:
            result = await db[collection_name].update_one(
                {"url": data["url"]},  # query
                {"$set": {"chapter_url": data["chapter_url"]}}  # new value
            )
        else:
            result = await db[collection_name].update_one({"_id": data["_id"]}, {"$set": data}, upsert=True)
    return result

async def get(db: AsyncIOMotorDatabase, collection_name: str, query: dict):
    return await db[collection_name].find_one(query)

async def get_all(db: AsyncIOMotorDatabase, collection_name: str) -> List[dict]:
    cursor = db[collection_name].find()
    return [doc async for doc in cursor]

async def delete(db: AsyncIOMotorDatabase, collection_name: str, id):
    await db[collection_name].delete_one({"_id": id})

async def erase_subs(db: AsyncIOMotorDatabase, user_id: str):
    await db["subscriptions"].delete_many({"user_id": user_id})
