import asyncio
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorDatabase, AsyncIOMotorCollection
from typing import Type, List, TypeVar, Optional
from config import mongo_url

T = TypeVar("T")

async def mongodb() -> AsyncIOMotorDatabase:
    db_url = mongo_url
    db_name = "mangabot"
    client = AsyncIOMotorClient(db_url, 27017)
    db = client[db_name]
    return db

async def add(db: AsyncIOMotorDatabase, collection_name: str, data: dict):
    await db[collection_name].update_one({"_id": data["_id"]}, {"$set": data}, upsert=True)

async def get(db: AsyncIOMotorDatabase, collection_name: str, query: dict):
    return await db[collection_name].find_one(query)

async def get_all(db: AsyncIOMotorDatabase, collection_name: str) -> List[dict]:
    cursor = db[collection_name].find()
    return [doc async for doc in cursor]

async def delete(db: AsyncIOMotorDatabase, collection_name: str, id):
    await db[collection_name].delete_one({"_id": id})

async def get_chapter_file_by_id(db: AsyncIOMotorDatabase, id: str) -> Optional[dict]:
    return await db["chapter_files"].find_one(
        {
            "$or": [
                {"file_unique_id": id},
                {"cbz_unique_id": id},
                {"telegraph_url": id},
            ]
        }
    )

async def get_subs(db: AsyncIOMotorDatabase, user_id: str, filters=None) -> List[dict]:
    filters = filters or []
    pipeline = [
        {"$match": {"user_id": user_id}},
        {
            "$lookup": {
                "from": "manga_names",
                "localField": "url",
                "foreignField": "url",
                "as": "manga_name",
            }
        },
        {"$unwind": "$manga_name"},
        {
            "$match": {
                "$or": [
                    {"manga_name.name": {"$regex": filter_, "$options": "i"}}
                    for filter_ in filters
                ]
            }
        },
        {"$project": {"_id": 0, "manga_name": 1}},
    ]
    cursor = db["subscriptions"].aggregate(pipeline)
    return [doc["manga_name"] async for doc in cursor]

async def get_subs_by_url(db: AsyncIOMotorDatabase, url: str) -> List[dict]:
    pipeline = [
        {"$match": {"url": url}},
        {
            "$lookup": {
                "from": "manga_names",
                "localField": "url",
                "foreignField": "url",
                "as": "manga_name",
            }
        },
        {"$unwind": "$manga_name"},
        {"$project": {"_id": 0, "manga_name": 1}},
    ]
    cursor = db["subscriptions"].aggregate(pipeline)
    return [doc["manga_name"] async for doc in cursor]

async def erase_subs(db: AsyncIOMotorDatabase, user_id: str):
    await db["subscriptions"].delete_many({"user_id": user_id})
