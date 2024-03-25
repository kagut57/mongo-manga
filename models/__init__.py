from .mongodb import mongodb
from collections import namedtuple

#LastChapter = namedtuple("LastChapter", ["url", "chapter_url"])
class LastChapter:
    def __init__(self, url: str, chapter_url: str):
        self.url = url
        self.chapter_url = chapter_url

