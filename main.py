import asyncio as aio
import os
from logger import logger
from bot import bot, manga_updater, chapter_creation
from models import mongodb

async def async_main():
    try:
        db = mongodb()
        await db.connect()
    except Exception as e:
        logger.error(f"Error connecting to the database: {e}")

if __name__ == '__main__':
    loop = aio.get_event_loop_policy().get_event_loop()
    loop.run_until_complete(async_main())
    loop.create_task(manga_updater())
    for i in range(10):
        loop.create_task(chapter_creation(i + 1))
    bot.run()
