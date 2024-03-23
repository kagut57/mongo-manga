import os
from typing import List, Optional, Type, TypeVar
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import ServerSelectionTimeoutError
from pymongo import MongoClient

T = TypeVar("T")

class ChapterFile:
    def __init__(self, url: str, file_id: Optional[str] = None, file_unique_id: Optional[str] = None,
                 cbz_id: Optional[str] = None, cbz_unique_id: Optional[str] = None,
                 telegraph_url: Optional[str] = None, _id=None):
        self.url = url
        self.file_id = file_id
        self.file_unique_id = file_unique_id
        self.cbz_id = cbz_id
        self.cbz_unique_id = cbz_unique_id
        self.telegraph_url = telegraph_url
        self._id = _id

class MangaOutput:
    def __init__(self, user_id: str, output: int, _id=None):
        self.user_id = user_id
        self.output = output
        self._id = _id

class Subscription:
    def __init__(self, url: str, user_id: str, custom_caption: Optional[str] = None,
                 custom_filename: Optional[str] = None, _id=None):
        self.url = url
        self.user_id = user_id
        self.custom_caption = custom_caption
        self.custom_filename = custom_filename
        self._id = _id

class LastChapter:
    def __init__(self, url: str, chapter_url: str, _id=None):
        self.url = url
        self.chapter_url = chapter_url
        self._id = _id

class MangaName:
    def __init__(self, url: str, name: str, _id=None):
        self.url = url
        self.name = name
        self._id = _id

class MangaPicture:
    def __init__(self, manga_url: str, url: str, _id=None):
        self.manga_url = manga_url
        self.url = url
        self._id = _id

class DB:
    def __init__(self, mongo_url: str):
        self.client = AsyncIOMotorClient(mongo_url)
        self.db = self.client['manga_db']
        self.chapter_files = self.db['chapter_files']
        self.manga_outputs = self.db['manga_outputs']
        self.subscriptions = self.db['subscriptions']
        self.last_chapters = self.db['last_chapters']
        self.manga_names = self.db['manga_names']
        self.manga_pictures = self.db['manga_pictures']

    async def connect(self):
        pass

    async def add(self, other):
        if isinstance(other, ChapterFile):
            await self.chapter_files.update_one(
                {"_id": other._id},
                {"$set": other.__dict__},
                upsert=True 
            )
        elif isinstance(other, MangaOutput):
            await self.manga_outputs.update_one(
                {"_id": other._id},
                {"$set": other.__dict__},
                upsert=True 
            )
        elif isinstance(other, Subscription):
            await self.subscriptions.update_one(
                {"_id": other._id},
                {"$set": other.__dict__},
                upsert=True 
            )
        elif isinstance(other, LastChapter):
            await self.last_chapters.update_one(
                {"_id": other._id},
                {"$set": other.__dict__},
                upsert=True 
            )
        elif isinstance(other, MangaName):
            await self.manga_names.update_one(
                {"_id": other._id},
                {"$set": other.__dict__},
                upsert=True 
            )
        elif isinstance(other, MangaPicture):
            await self.manga_pictures.update_one(
                {"_id": other._id},
                {"$set": other.__dict__},
                upsert=True 
            )

    async def get(self, table: Type[T], _id):
        if table == ChapterFile:
            result = await self.chapter_files.find_one({"$or": [{"file_unique_id": _id}, {"cbz_unique_id": _id}, {"telegraph_url": _id}]})
            return ChapterFile(**result) if result else None
        elif table == MangaOutput:
            result = await self.manga_outputs.find_one({"user_id": _id})
            return MangaOutput(**result) if result else None
        elif table == Subscription:
            result = await self.subscriptions.find_one({"url": _id, "user_id": _id})
            return Subscription(**result) if result else None
        elif table == LastChapter:
            result = await self.last_chapters.find_one({"url": _id})
            return LastChapter(**result) if result else None
        elif table == MangaName:
            result = await self.manga_names.find_one({"url": _id})
            return MangaName(**result) if result else None
        elif table == MangaPicture:
            result = await self.manga_pictures.find_one({"manga_url": _id})
            return MangaPicture(**result) if result else None

    async def get_all(self, table: Type[T]):
        if table == ChapterFile:
            return [ChapterFile(**result) for result in await self.chapter_files.find().to_list(None)]
        elif table == MangaOutput:
            return [MangaOutput(**result) for result in await self.manga_outputs.find().to_list(None)]
        elif table == Subscription:
            return [Subscription(**result) for result in await self.subscriptions.find().to_list(None)]
        elif table == LastChapter:
            return [LastChapter(**result) for result in await self.last_chapters.find().to_list(None)]
        elif table == MangaName:
            return [MangaName(**result) for result in await self.manga_names.find().to_list(None)]
        elif table == MangaPicture:
            return [MangaPicture(**result) for result in await self.manga_pictures.find().to_list(None)]

    async def erase(self, other):
        if isinstance(other, ChapterFile):
            await self.chapter_files.delete_one({"url": other.url})
        elif isinstance(other, MangaOutput):
            await self.manga_outputs.delete_one({"user_id": other.user_id})
        elif isinstance(other, Subscription):
            await self.subscriptions.delete_one({"url": other.url, "user_id": other.user_id})
        elif isinstance(other, LastChapter):
            await self.last_chapters.delete_one({"url": other.url})
        elif isinstance(other, MangaName):
            await self.manga_names.delete_one({"url": other.url})
        elif isinstance(other, MangaPicture):
            await self.manga_pictures.delete_one({"manga_url": other.manga_url})

    async def get_chapter_file_by_id(self, id: str):
        result = await self.chapter_files.find_one({"$or": [{"file_unique_id": id}, {"cbz_unique_id": id}, {"telegraph_url": id}]})
        return ChapterFile(**result) if result else None

    async def get_subs(self, user_id: str, filters=None):
        filters = filters or []
        query = {"user_id": user_id}
        if filters:
            query["$or"] = [{"name": {"$regex": f".*{filter_}.*", "$options": "i"}}, {"url": {"$regex": f".*{filter_}.*", "$options": "i"}}]
            subs = await self.subscriptions.find(query).to_list(None)
            urls = [sub["url"] for sub in subs]
            manga_names = await self.manga_names.find({"url": {"$in": urls}}).to_list(None)
            return [MangaName(**name) for name in manga_names]

    async def get_subs_by_url(self, url: str):
        subs = await self.subscriptions.find({"url": url}).to_list(None)
        urls = [sub["url"] for sub in subs]
        manga_names = await self.manga_names.find({"url": {"$in": urls}}).to_list(None)
        return [MangaName(**name) for name in manga_names]

    async def erase_subs(self, user_id: str):
        await self.subscriptions.delete_many({"user_id": user_id})
