import json
import re
from typing import List, AsyncIterable
from urllib.parse import urlparse, urljoin, quote_plus

from plugins.client import MangaClient, MangaCard, MangaChapter, LastChapter
from .search_engine import search

class HentaifoxClient(MangaClient):
    base_url = urlparse("https://hentaifox.com/")
    search_url = urljoin(base_url.geturl(), "search/")
    gallery_url = urljoin(base_url.geturl(), "gallery")
    cover_url = "https://hentaifox.com/images/logo.png"

    pre_headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:97.0) Gecko/20100101 Firefox/97.0'
    }

    image_formats = {
        'j': 'jpg',
        'p': 'png',
        'b': 'bmp',
        'g': 'gif'
    }

    def __init__(self, *args, name="Hentaifox", **kwargs):
        super().__init__(*args, name=name, headers=self.pre_headers, **kwargs)

    def hentais_from_page(self, documents):
        titles = [doc.find('div', {'class': 'caption'}).find('a').get_text() for doc in documents]
        urls = [urljoin(self.gallery_url, doc.find('div', {'class': 'caption'}).find('a')['href'].split('/')[-2]) for doc in documents]
        thumbnails = [doc.find('img')['src'] for doc in documents]

        hentais = [MangaCard(self, *tup) for tup in zip(titles, urls, thumbnails)]

        return hentais

    def chapters_from_page(self, page: bytes, hentai: MangaCard = None):
        soup = BeautifulSoup(page, 'html.parser')
        path = soup.find('div', {'class': 'gallery_thumb'}).find('img')['data-src'].rsplit('/', 1)[0]
        script = soup.find(lambda tag: tag.name == 'script' and 'var g_th' in tag.text).text
        images = json.loads(script.replace("var g_th = $.parseJSON('", '')[:-4])
        image_urls = [f'{path}/{image}.{self.image_formats[images[image][0]]}' for image in images]

        return [MangaChapter(self, hentai.title, hentai.url, hentai, image_urls)]

    def updates_from_page(self, page: bytes):
        soup = BeautifulSoup(page, 'html.parser')
        latest_updates = soup.find('div', {'class': 'latest_updates'})
        if not latest_updates:
            return []

        update_items = latest_updates.find_all('div', {'class': 'thumb'})
        urls = []
        for item in update_items:
            caption = item.find('div', {'class': 'caption'})
            url = urljoin(self.gallery_url, caption.find('a')['href'].split('/')[-2])
            urls.append(url)

        return urls

    async def check_updated_urls(self, last_chapters: List[LastChapter]):
        content = await self.get_url(self.base_url.geturl())
        updates = self.updates_from_page(content)

        updated = [lc.url for lc in last_chapters if lc.url in updates]
        not_updated = [lc.url for lc in last_chapters if lc.url not in updates]

        return updated, not_updated

    async def search(self, query: str = "", page: int = 1) -> List[MangaCard]:
        request_url = urljoin(self.search_url, f"?q={quote_plus(query)}&page={page}")
        content = await self.get_url(request_url)
        soup = BeautifulSoup(content, 'html.parser')
        documents = soup.find_all('div', {'class': 'thumb'})

        return self.hentais_from_page(documents)

    async def get_chapters(self, hentai_card: MangaCard) -> List[MangaChapter]:
        request_url = hentai_card.url
        content = await self.get_url(request_url)

        return self.chapters_from_page(content, hentai_card)

    async def iter_chapters(self, hentai_url: str, hentai_name) -> AsyncIterable[MangaChapter]:
        hentai_card = MangaCard(self, hentai_name, hentai_url, '')

        request_url = hentai_card.url
        content = await self.get_url(request_url)

        for ch in self.chapters_from_page(content, hentai_card):
            yield ch

    async def contains_url(self, url: str):
        return url.startswith(self.base_url.geturl())
