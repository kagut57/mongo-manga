import asyncio as aio
import os
from logger import logger
from bot import bot, manga_updater, chapter_creation
from models import DB
from config import mongo_url

if __name__ == '__main__':
    loop = aio.get_event_loop_policy().get_event_loop()
    loop.create_task(manga_updater())
    for i in range(10):
        loop.create_task(chapter_creation(i + 1))
    bot.run()
