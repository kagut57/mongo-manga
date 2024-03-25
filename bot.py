import enum
import shutil
from ast import arg
import asyncio
import re
from dataclasses import dataclass
import datetime as dt
import json

import pyrogram.errors
from pyrogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery, InputMediaDocument

from config import env_vars, mongo_url
from models import LastChapter
from img2cbz.core import fld2cbz
from img2pdf.core import fld2pdf, fld2thumb
from img2tph.core import img2tph
from plugins import MangaClient, ManhuaKoClient, MangaCard, MangaChapter, ManhuaPlusClient, TMOClient, MangaDexClient, \
    MangaSeeClient, MangasInClient, McReaderClient, MangaKakalotClient, ManganeloClient, ManganatoClient, \
    KissMangaClient, MangatigreClient, MangaHasuClient, MangaBuddyClient, AsuraScansClient, NineMangaClient
import os

from pyrogram import Client, filters
from typing import Dict, Tuple, List, TypedDict
from loguru import logger

from models.mongodb import mongodb, add, get, get_all, delete, erase_subs
from pagination import Pagination
from plugins.client import clean
from tools.aqueue import AQueue
from tools.flood import retry_on_flood


mangas: Dict[str, MangaCard] = dict()
chapters: Dict[str, MangaChapter] = dict()
pdfs: Dict[str, str] = dict()
paginations: Dict[int, Pagination] = dict()
queries: Dict[str, Tuple[MangaClient, str]] = dict()
full_pages: Dict[str, List[str]] = dict()
favourites: Dict[str, MangaCard] = dict()
language_query: Dict[str, Tuple[str, str]] = dict()
users_in_channel: Dict[int, dt.datetime] = dict()
locks: Dict[int, asyncio.Lock] = dict()

plugin_dicts: Dict[str, Dict[str, MangaClient]] = {
    "🇬🇧 EN": {
        "MangaDex": MangaDexClient(),
        "Manhuaplus": ManhuaPlusClient(),
        "Mangasee": MangaSeeClient(),
        "McReader": McReaderClient(),
        "MagaKakalot": MangaKakalotClient(),
        "Manganelo": ManganeloClient(),
        "Manganato": ManganatoClient(),
        "KissManga": KissMangaClient(),
        "MangaHasu": MangaHasuClient(),
        "MangaBuddy": MangaBuddyClient(),
        "AsuraScans": AsuraScansClient(),
        "NineManga": NineMangaClient(),
    },
    "🇪🇸 ES": {
        "MangaDex": MangaDexClient(language=("es-la", "es")),
        "ManhuaKo": ManhuaKoClient(),
        "TMO": TMOClient(),
        "Mangatigre": MangatigreClient(),
        "NineManga": NineMangaClient(language='es'),
        "MangasIn": MangasInClient(),
    }
}

cache_dir = "cache"
if os.path.exists(cache_dir):
    shutil.rmtree(cache_dir)
with open("tools/help_message.txt", "r") as f:
    help_msg = f.read()


class OutputOptions(enum.IntEnum):
    PDF = 1
    CBZ = 2
    Telegraph = 4

    def __and__(self, other):
        return self.value & other

    def __xor__(self, other):
        return self.value ^ other

    def __or__(self, other):
        return self.value | other


disabled = ["[🇬🇧 EN] McReader", "[🇬🇧 EN] Manhuaplus", "[🇪🇸 ES] MangasIn"]

plugins = dict()
for lang, plugin_dict in plugin_dicts.items():
    for name, plugin in plugin_dict.items():
        identifier = f'[{lang}] {name}'
        if identifier in disabled:
            continue
        plugins[identifier] = plugin

# subsPaused = ["[🇪🇸 ES] TMO"]
subsPaused = disabled + []


def split_list(li):
    return [li[x: x + 2] for x in range(0, len(li), 2)]


bot = Client('bot',
             api_id=int(env_vars.get('API_ID')),
             api_hash=env_vars.get('API_HASH'),
             bot_token=env_vars.get('BOT_TOKEN'),
             max_concurrent_transmissions=3)

pdf_queue = AQueue()

def get_buttons_for_options(user_options: int):
    buttons = []
    for option in OutputOptions:
        checked = "✅" if option & user_options else "❌"
        text = f'{checked} {option.name}'
        buttons.append([InlineKeyboardButton(text, f"options_{option.value}")])
    return InlineKeyboardMarkup(buttons)
    
@bot.on_message(filters=filters.command(['options']))
async def on_options_command(client: Client, message: Message):
    db = await mongodb()
    user_options = await get(db, "manga_output", str(message.from_user.id))
    if user_options:
        user_options = user_options.get("output", (1 << 30) - 1)
    else:
        user_options = (1 << 30) - 1
    buttons = get_buttons_for_options(user_options)
    return await message.reply("Select the desired output format.", reply_markup=buttons)

async def options_click(client, callback: CallbackQuery):
    db = await mongodb()
    user_id = str(callback.from_user.id)
    option = int(callback.data.split('_')[-1])
    
    user_options = await get(db, "manga_output", user_id)
    
    if user_options is None:
        user_options = {
            "_id": user_id,
            "output": (1 << 30) - 1
        }
    else:
        user_options = user_options.get("output", (1 << 30) - 1)
    
    user_options ^= option
    
    await add(db, "manga_output", {"_id": user_id, "output": user_options})
    
    buttons = get_buttons_for_options(user_options)
    
    await callback.message.edit_reply_markup(reply_markup=buttons)


@bot.on_message(filters=~(filters.private & filters.incoming))
async def on_chat_or_channel_message(client: Client, message: Message):
    pass


@bot.on_message()
async def on_private_message(client: Client, message: Message):
    channel = env_vars.get('CHANNEL')
    if not channel:
        return message.continue_propagation()
    if in_channel_cached := users_in_channel.get(message.from_user.id):
        if dt.datetime.now() - in_channel_cached < dt.timedelta(days=1):
            return message.continue_propagation()
    try:
        if await client.get_chat_member(channel, message.from_user.id):
            users_in_channel[message.from_user.id] = dt.datetime.now()
            return message.continue_propagation()
    except pyrogram.errors.UsernameNotOccupied:
        logger.debug("Channel does not exist, therefore bot will continue to operate normally")
        return message.continue_propagation()
    except pyrogram.errors.ChatAdminRequired:
        logger.debug("Bot is not admin of the channel, therefore bot will continue to operate normally")
        return message.continue_propagation()
    except pyrogram.errors.UserNotParticipant:
        await message.reply("In order to use the bot you must join it's update channel.",
                            reply_markup=InlineKeyboardMarkup(
                                [[InlineKeyboardButton('Join!', url=f't.me/{channel}')]]
                            ))
    except pyrogram.ContinuePropagation:
        raise
    except pyrogram.StopPropagation:
        raise
    except BaseException as e:
        logger.exception(e)


@bot.on_message(filters=filters.command(['start']))
async def on_start(client: Client, message: Message):
    logger.info(f"User {message.from_user.id} started the bot")
    await message.reply("Welcome to the best manga pdf bot in telegram!!\n"
                        "\n"
                        "How to use? Just type the name of some manga you want to keep up to date.\n"
                        "\n"
                        "For example:\n"
                        "`Fire Force`\n"
                        "\n"
                        "Check /help for more information.")
    logger.info(f"User {message.from_user.id} finished the start command")


@bot.on_message(filters=filters.command(['help']))
async def on_help(client: Client, message: Message):
    await message.reply(help_msg)


@bot.on_message(filters=filters.command(['queue']))
async def on_help(client: Client, message: Message):
    await message.reply(f'Queue size: {pdf_queue.qsize()}')


@bot.on_message(filters=filters.command(['refresh']))
async def on_refresh(client: Client, message: Message):
    db = await mongodb()
    text = message.reply_to_message.text or message.reply_to_message.caption
    if text:
        regex = re.compile(r'\[Read on telegraph]\((.*)\)')
        match = regex.search(text.markdown)
    else:
        match = None
    document = message.reply_to_message.document
    if not (message.reply_to_message and message.reply_to_message.outgoing and
            ((document and document.file_name[-4:].lower() in ['.pdf', '.cbz']) or match)):
        return await message.reply("This command only works when it replies to a manga file that bot sent to you")
    if document:
        chapter = await db.get_chapter_file_by_id(document.file_unique_id)
    else:
        chapter = await db.get_chapter_file_by_id(match.group(1))
    if not chapter:
        return await message.reply("This file was already refreshed")
    await delete(db, "chapter_files", chapter)
    return await message.reply("File refreshed successfully!")


@bot.on_message(filters=filters.command(['subs']))
async def on_subs(client: Client, message: Message):
    db = await mongodb()
    user_id = str(message.from_user.id)
    filter_string = message.text.split(maxsplit=1)[1] if message.text.split(maxsplit=1)[1:] else ''
    filter_list = [filter_.strip() for filter_ in filter_string.split(' ') if filter_.strip()]

    subs = await get_all(db, "subscriptions")
    manga_names = await get_all(db, "manga_names")

    filtered_subs = [sub for sub in subs if sub.get("user_id") == user_id]

    if filter_string:
        filtered_subs = [sub for sub in filtered_subs if any(filter_ in sub.get("url") for filter_ in filter_list)]

    lines = []
    for sub in filtered_subs[:10]:
        matching_manga = next((manga for manga in manga_names if manga.get("url") == sub.get("url")), None)
        if matching_manga:
            lines.append(f'<a href="{sub.get("url")}">{matching_manga.get("name")}</a>')
            lines.append(f'`/cancel {sub.get("url")}`')
            lines.append('')

    if not lines:
        if filter_string:
            return await message.reply("You have no subscriptions with that filter.")
        return await message.reply("You have no subscriptions yet.")

    text = "\n".join(lines)
    await message.reply(f'Your subscriptions:\n\n{text}\nTo see more subscriptions use `/subs filter`', disable_web_page_preview=True)


@bot.on_message(filters=filters.regex(r'^/cancel ([^ ]+)$'))
async def on_cancel_command(client: Client, message: Message):
    db = await mongodb()
    manga_url = message.matches[0].group(1)
    user_id = str(message.from_user.id)
    query = {"url": manga_url, "user_id": user_id}
    sub = await get(db, "subscriptions", query) 
    if not sub:
        return await message.reply("You were not subscribed to that manga.")
    await delete(db, "subscriptions", sub["_id"])  # Pass the _id field for deletion
    return await message.reply("You will no longer receive updates for that manga.")

@bot.on_message(filters=filters.regex(r'^/'))
async def on_unknown_command(client: Client, message: Message):
    await message.reply("Unknown command")


@bot.on_message(filters=filters.text)
async def on_message(client, message: Message):
    language_query[f"lang_None_{hash(message.text)}"] = (None, message.text)
    for language in plugin_dicts.keys():
        language_query[f"lang_{language}_{hash(message.text)}"] = (language, message.text)
    await bot.send_message(message.chat.id, "Select search languages.", reply_markup=InlineKeyboardMarkup(
        split_list([InlineKeyboardButton(language, callback_data=f"lang_{language}_{hash(message.text)}")
                    for language in plugin_dicts.keys()])
    ))

async def language_click(client, callback: CallbackQuery):
    lang, query = language_query[callback.data]
    if not lang:
        return await callback.message.edit("Select search languages.", reply_markup=InlineKeyboardMarkup(
            split_list([InlineKeyboardButton(language, callback_data=f"lang_{language}_{hash(query)}")
                        for language in plugin_dicts.keys()])
        ))
    for identifier, manga_client in plugin_dicts[lang].items():
        queries[f"query_{lang}_{identifier}_{hash(query)}"] = (manga_client, query)
    await callback.message.edit(f"Language: {lang}\n\nSelect search plugin.", reply_markup=InlineKeyboardMarkup(
        split_list([InlineKeyboardButton(identifier, callback_data=f"query_{lang}_{identifier}_{hash(query)}")
                    for identifier in plugin_dicts[lang].keys() if f'[{lang}] {identifier}' not in disabled]) + [
            [InlineKeyboardButton("◀️ Back", callback_data=f"lang_None_{hash(query)}")]]
    ))


async def plugin_click(client, callback: CallbackQuery):
    db = await mongodb()
    manga_client, query = queries[callback.data]
    results = await manga_client.search(query)
    if not results:
        await bot.send_message(callback.from_user.id, "No manga found for given query.")
        return
    for result in results:
        mangas[result.unique()] = result
    await bot.send_message(callback.from_user.id,
                           "This is the result of your search",
                           reply_markup=InlineKeyboardMarkup([
                               [InlineKeyboardButton(result.name, callback_data=result.unique())] for result in results
                           ]))


async def manga_click(client, callback: CallbackQuery, pagination: Pagination = None):
    db = await mongodb()
    if pagination is None:
        pagination = Pagination()
        paginations[pagination.id] = pagination

    if pagination.manga is None:
        manga = mangas[callback.data]
        pagination.manga = manga

    results = await pagination.manga.client.get_chapters(pagination.manga, pagination.page)

    if not results:
        await callback.answer("Ups, no chapters there.", show_alert=True)
        return

    full_page_key = f'full_page_{hash("".join([result.unique() for result in results]))}'
    full_pages[full_page_key] = []
    for result in results:
        chapters[result.unique()] = result
        full_pages[full_page_key].append(result.unique())
        
    query = {"url": pagination.manga.url, "user_id": str(callback.from_user.id)}
    subs = await get(db, "subscriptions", query)

    prev = [InlineKeyboardButton('<<', f'{pagination.id}_{pagination.page - 1}')]
    next_ = [InlineKeyboardButton('>>', f'{pagination.id}_{pagination.page + 1}')]
    footer = [prev + next_] if pagination.page > 1 else [next_]

    fav = [[InlineKeyboardButton(
        "Unsubscribe" if subs else "Subscribe",
        f"{'unfav' if subs else 'fav'}_{pagination.manga.unique()}"
    )]]
    favourites[f"fav_{pagination.manga.unique()}"] = pagination.manga
    favourites[f"unfav_{pagination.manga.unique()}"] = pagination.manga

    full_page = [[InlineKeyboardButton('Full Page', full_page_key)]]

    buttons = InlineKeyboardMarkup(fav + footer + [
        [InlineKeyboardButton(result.name, result.unique())] for result in results
    ] + full_page + footer)

    if pagination.message is None:
        try:
            message = await bot.send_photo(callback.from_user.id,
                                           pagination.manga.picture_url,
                                           f'{pagination.manga.name}\n'
                                           f'{pagination.manga.get_url()}', reply_markup=buttons)
            pagination.message = message
        except pyrogram.errors.BadRequest as e:
            file_name = f'pictures/{pagination.manga.unique()}.jpg'
            await pagination.manga.client.get_cover(pagination.manga, cache=True, file_name=file_name)
            message = await bot.send_photo(callback.from_user.id,
                                           f'./cache/{pagination.manga.client.name}/{file_name}',
                                           f'{pagination.manga.name}\n'
                                           f'{pagination.manga.get_url()}', reply_markup=buttons)
            pagination.message = message
    else:
        await bot.edit_message_reply_markup(
            callback.from_user.id,
            pagination.message.id,
            reply_markup=buttons
        )

users_lock = asyncio.Lock()


async def get_user_lock(chat_id: int):
    async with users_lock:
        lock = locks.get(chat_id)
        if not lock:
            locks[chat_id] = asyncio.Lock()
        return locks[chat_id]


async def chapter_click(client, data, chat_id):
    await pdf_queue.put(chapters[data], int(chat_id))
    logger.debug(f"Put chapter {chapters[data].name} to queue for user {chat_id} - queue size: {pdf_queue.qsize()}")


async def send_manga_chapter(client: Client, chapter, chat_id):
    db = await mongodb()
    chapter_file = await get(db, "chapter_files", {"_id": chapter.url})
    options = await get(db, "manga_output", str(chat_id))
    options = options.get("output", (1 << 30) - 1) if options else (1 << 30) - 1
    if chapter_file:
        file_id = chapter_file.get("file_id")
        file_unique_id = chapter_file.get("file_unique_id")
        cbz_id = chapter_file.get("cbz_id")
        cbz_unique_id = chapter_file.get("cbz_unique_id")
        telegraph_url = chapter_file.get("telegraph_url")
    else:
        file_id = file_unique_id = cbz_id = cbz_unique_id = telegraph_url = None

        

    error_caption = '\n'.join([
        f'{chapter.manga.name} - {chapter.name}',
        f'{chapter.get_url()}'
    ])

    success_caption = f'{chapter.manga.name} - {chapter.name}\n'

    download = not chapter_file
    download = download or options & OutputOptions.PDF and not file_id
    download = download or options & OutputOptions.CBZ and not cbz_id
    download = download or options & OutputOptions.Telegraph and not telegraph_url
    download = download and options & ((1 << len(OutputOptions)) - 1) != 0

    if download:
        pictures_folder = await chapter.client.download_pictures(chapter)
        if not chapter.pictures:
            return await client.send_message(chat_id,
                                          f'There was an error parsing this chapter or chapter is missing' +
                                          f', please check the chapter at the web\n\n{error_caption}')
        thumb_path = fld2thumb(pictures_folder)

    if download and not telegraph_url:
        telegraph_url = await img2tph(chapter, clean(f'{chapter.manga.name} {chapter.name}'))

    if options & OutputOptions.Telegraph:
        success_caption += f'[Read on telegraph]({telegraph_url})\n'
    success_caption += f'[Read on website]({chapter.get_url()})'

    ch_name = clean(f'{clean(chapter.manga.name, 25)} - {chapter.name}', 45)

    media_docs = []

    if options & OutputOptions.PDF:
        if file_id:
            media_docs.append(InputMediaDocument(file_id))
        else:
            try:
                pdf = await asyncio.get_running_loop().run_in_executor(None, fld2pdf, pictures_folder, ch_name)
            except Exception as e:
                logger.exception(f'Error creating pdf for {chapter.name} - {chapter.manga.name}\n{e}')
                return await client.send_message(chat_id, f'There was an error making the pdf for this chapter. '
                                                       f'Forward this message to the bot group to report the '
                                                       f'error.\n\n{error_caption}')
            media_docs.append(InputMediaDocument(pdf, thumb=thumb_path))

    if options & OutputOptions.CBZ:
        if cbz_id:
            media_docs.append(InputMediaDocument(cbz_id))
        else:
            try:
                cbz = await asyncio.get_running_loop().run_in_executor(None, fld2cbz, pictures_folder, ch_name)
            except Exception as e:
                logger.exception(f'Error creating cbz for {chapter.name} - {chapter.manga.name}\n{e}')
                return await client.send_message(chat_id, f'There was an error making the cbz for this chapter. '
                                                       f'Forward this message to the bot group to report the '
                                                       f'error.\n\n{error_caption}')
            media_docs.append(InputMediaDocument(cbz, thumb=thumb_path))

    if len(media_docs) == 0:
        messages: list[Message] = await retry_on_flood(client.send_message)(chat_id, success_caption)
    else:
        media_docs[-1].caption = success_caption
        messages: list[Message] = await retry_on_flood(client.send_media_group)(chat_id, media_docs)

    # Save file ids
    if download and media_docs:
        for message in [x for x in messages if x.document]:
            if message.document.file_name.endswith('.pdf'):
                file_id = message.document.file_id
                file_unique_id = message.document.file_unique_id
            elif message.document.file_name.endswith('.cbz'):
                cbz_id = message.document.file_id
                cbz_unique_id = message.document.file_unique_id

    chapter_file_dict = {
        "_id": chapter.url,
        "file_id": file_id,
        "file_unique_id": file_unique_id,
        "cbz_id": cbz_id,
        "cbz_unique_id": cbz_unique_id,
        "telegraph_url": telegraph_url
    }

    if download:
        shutil.rmtree(pictures_folder, ignore_errors=True)
        await add(db, "chapter_files", chapter_file_dict)


async def pagination_click(client: Client, callback: CallbackQuery):
    pagination_id, page = map(int, callback.data.split('_'))
    pagination = paginations[pagination_id]
    pagination.page = page
    await manga_click(client, callback, pagination)


async def full_page_click(client: Client, callback: CallbackQuery):
    chapters_data = full_pages[callback.data]
    for chapter_data in reversed(chapters_data):
        try:
            await chapter_click(client, chapter_data, callback.from_user.id)
        except Exception as e:
            logger.exception(e)


async def favourite_click(client: Client, callback: CallbackQuery):
    db = await mongodb()
    action, data = callback.data.split('_')
    fav = action == 'fav'
    manga = favourites[callback.data]
    
    sub_dict = {
        "url": manga.url,
        "user_id": str(callback.from_user.id),
        "custom_caption": None,
        "custom_filename": None
    }
    
    manga_name_dict = {
        "url": manga.url,
        "name": manga.name
    }

    subs = await get(db, "subscriptions", {"url": manga.url, "user_id": str(callback.from_user.id)})
    
    if not subs and fav:
        await add(db, "subscriptions", sub_dict)
        
    if subs and not fav:
        await delete(db, "subscriptions", subs["_id"])
        
    reply_markup = callback.message.reply_markup
    keyboard = reply_markup.inline_keyboard
    keyboard[0] = [InlineKeyboardButton(
        "Unsubscribe" if fav else "Subscribe",
        f"{'unfav' if fav else 'fav'}_{data}"
    )]
    await bot.edit_message_reply_markup(callback.from_user.id, callback.message.id,
                                        InlineKeyboardMarkup(keyboard))
    
    db_manga = await get(db, "manga_names", {"url": manga.url})
    if not db_manga:
        await add(db, "manga_names", manga_name_dict)

def is_pagination_data(callback: CallbackQuery):
    data = callback.data
    match = re.match(r'\d+_\d+', data)
    if not match:
        return False
    pagination_id = int(data.split('_')[0])
    if pagination_id not in paginations:
        return False
    pagination = paginations[pagination_id]
    if not pagination.message:
        return False
    if pagination.message.chat.id != callback.from_user.id:
        return False
    if pagination.message.id != callback.message.id:
        return False
    return True


@bot.on_callback_query()
async def on_callback_query(client, callback: CallbackQuery):
    if callback.data in queries:
        await plugin_click(client, callback)
    elif callback.data in mangas:
        await manga_click(client, callback)
    elif callback.data in chapters:
        await chapter_click(client, callback.data, callback.from_user.id)
    elif callback.data in full_pages:
        await full_page_click(client, callback)
    elif callback.data in favourites:
        await favourite_click(client, callback)
    elif is_pagination_data(callback):
        await pagination_click(client, callback)
    elif callback.data in language_query:
        await language_click(client, callback)
    elif callback.data.startswith('options'):
        await options_click(client, callback)
    else:
        await bot.answer_callback_query(callback.id, 'This is an old button, please redo the search', show_alert=True)
        return
    try:
        await callback.answer()
    except BaseException as e:
        logger.warning(e)


async def remove_subscriptions(sub: str):
    db = await mongodb()
    await db.erase_subs(sub)


async def update_mangas():
    db = await mongodb()
    logger.debug("Updating mangas")
    subscriptions = await get_all(db, "subscriptions")
    last_chapters = await get_all(db, "last_chapters")
    manga_names = await get_all(db, "manga_names")

    subs_dictionary = dict()
    chapters_dictionary = dict()
    url_client_dictionary = dict()
    client_url_dictionary = {client: set() for client in plugins.values()}
    manga_dict = dict()

    for subscription in subscriptions:
        if subscription.get("url") not in subs_dictionary:
            subs_dictionary[subscription.get("url")] = []
        subs_dictionary[subscription.get("url")].append(subscription.get("user_id"))

    for last_chapter in last_chapters:
        chapters_dictionary[last_chapter.get("url")] = last_chapter

    for manga in manga_names:
        manga_dict[manga.get("url")] = manga

    for url in subs_dictionary:
        for ident, client in plugins.items():
            if ident in subsPaused:
                continue
            if await client.contains_url(url):
                url_client_dictionary[url] = client
                client_url_dictionary[client].add(url)

    for client, urls in client_url_dictionary.items():
        logger.debug(f'Updating {client.name}')
        logger.debug(f'Urls:\t{list(urls)}')
        new_urls = [url for url in urls if not chapters_dictionary.get(url)]
        logger.debug(f'New Urls:\t{new_urls}')
        to_check = [chapters_dictionary[url] for url in urls if chapters_dictionary.get("url")]
        if len(to_check) == 0:
            continue
        try:
            updated, not_updated = await client.check_updated_urls(to_check)
        except BaseException as e:
            logger.exception(f"Error while checking updates for site: {client.name}, err: {e}")
            updated = []
            not_updated = list(urls)
        for url in not_updated:
            del url_client_dictionary[url]
        logger.debug(f'Updated:\t{list(updated)}')
        logger.debug(f'Not Updated:\t{list(not_updated)}')

    updated = dict()

    for url, client in url_client_dictionary.items():
        try:
            if url not in manga_dict:
                continue
            manga_name = manga_dict[url]["name"]
            if url not in chapters_dictionary:
                agen = client.iter_chapters(url, manga_name)
                last_chapter = await anext(agen)
                last_chapter_dict = {
                    "url": url,
                    "chapter_url": last_chapter.url
                }
                await add(db, "last_chapters", last_chapter_dict)
                await asyncio.sleep(10)
            else:
                last_chapter = chapters_dictionary[url]
                new_chapters: List[MangaChapter] = []
                counter = 0
                async for chapter in client.iter_chapters(url, manga_name):
                    if chapter.url == last_chapter.chapter_url:
                        break
                    new_chapters.append(chapter)
                    counter += 1
                    if counter == 20:
                        break
                if new_chapters:
                    last_chapter.chapter_url = new_chapters[0].url
                    last_chapter_dict = {
                        "url": url,
                        "chapter_url": new_chapters[0].url
                    }
                    await add(db, "last_chapters", last_chapter_dict)
                    updated[url] = list(reversed(new_chapters))
                    for chapter in new_chapters:
                        if chapter.unique() not in chapters:
                            chapters[chapter.unique()] = chapter
                await asyncio.sleep(1)
        except BaseException as e:
            logger.exception(f'An exception occurred getting new chapters for url {url}: {e}')

    blocked = set()
    for url, chapter_list in updated.items():
        for chapter in chapter_list:
            logger.debug(f'Updating {chapter.manga.name} - {chapter.name}')
            for sub in subs_dictionary[url]:
                if sub in blocked:
                    continue
                try:
                    await pdf_queue.put(chapter, int(sub))
                    logger.debug(f"Put chapter {chapter} to queue for user {sub} - queue size: {pdf_queue.qsize()}")
                except pyrogram.errors.UserIsBlocked:
                    logger.info(f'User {sub} blocked the bot')
                    await remove_subscriptions(sub)
                    blocked.add(sub)
                except BaseException as e:
                    logger.exception(f'An exception occurred sending new chapter: {e}')


async def manga_updater():
    minutes = 5
    while True:
        wait_time = minutes * 60
        try:
            start = dt.datetime.now()
            await update_mangas()
            elapsed = dt.datetime.now() - start
            wait_time = max((dt.timedelta(seconds=wait_time) - elapsed).total_seconds(), 0)
            logger.debug(f'Time elapsed updating mangas: {elapsed}, waiting for {wait_time}')
        except BaseException as e:
            logger.exception(f'An exception occurred during chapters update: {e}')
        if wait_time:
            await asyncio.sleep(wait_time)


async def chapter_creation(worker_id: int = 0):
    """
    This function will always run in the background
    It will be listening for a channel which notifies whether there is a new request in the request queue
    :return:
    """
    logger.debug(f"Worker {worker_id}: Starting worker")
    while True:
        chapter, chat_id = await pdf_queue.get(worker_id)
        logger.debug(f"Worker {worker_id}: Got chapter '{chapter.name}' from queue for user '{chat_id}'")
        try:
            await send_manga_chapter(bot, chapter, chat_id)
        except:
            logger.exception(f"Error sending chapter {chapter.name} to user {chat_id}")
        finally:
            pdf_queue.release(chat_id)
