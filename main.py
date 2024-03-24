import asyncio as aio
import os
import uvloop

uvloop.install()
from logger import logger
from bot import bot, manga_updater, chapter_creation
from models import mongodb

if __name__ == '__main__':
    loop = aio.get_event_loop_policy().get_event_loop()
    loop.run_until_complete(mongodb())
    loop.create_task(manga_updater())
    for i in range(10):
        loop.create_task(chapter_creation(i + 1))
    bot.run()
