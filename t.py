import os
from urllib.error import HTTPError
from urllib.parse import quote, unquote
from urllib.request import urlopen
import sqlite3
import sys
import re
from html.parser import HTMLParser
from concurrent.futures import ThreadPoolExecutor

if os.path.exists("links.db"):
    os.remove("links.db")

conn = sqlite3.connect("links.db")
cur = conn.cursor()

global_links = set()

class HTMLP(HTMLParser):
    def __init__(self):
        super().__init__()
        self.local_links = set()
        self.link_pattern = re.compile(r'^/wiki/(?!.*\.(?:png|jpg|gif|.*)).*$') # парсим конкретно ссылки(а не бутстрап условный или картинки) на другие статьи (у википедии так устроено)

    def handle_starttag(self, tag: str, attrs: str) -> None:
        if tag == 'a':
            for attr, value in attrs:
                if attr == "href" and value not in global_links:
                    if self.link_pattern.match(value) and ":" not in value: # не парсим лишние ссылки ":" (не на статьи)
                        self.local_links.add(value)
                        global_links.add(value)


def get_start_url() -> list:
    if len(sys.argv) < 3:
        print("Пожалуйста, введите изначальную ссылку для парсинга, а затем глубину поиска")
        exit()
    else:
        return [sys.argv[1], int(sys.argv[2])]


def configure_db() -> None:
    cur.execute("CREATE TABLE IF NOT EXISTS Links(id INTEGER PRIMARY KEY, link TEXT NOT NULL, cycle INTEGER)")
    conn.commit()


def add_links_bulk(links: set, deep: int) -> None:
    final_links = [
        (("https://ru.wikipedia.org" + unquote(link)) if not link.startswith("https://ru.wikipedia.org") else unquote(link), deep)
        for link in links
    ]
    cur.executemany("INSERT INTO Links(link, cycle) VALUES (?, ?)", final_links)


def fetch_content(url: str) -> str:
    try:
        encoded_url = quote(url.encode('utf-8'), safe=":/?&=")
        with urlopen(encoded_url) as response:
            return response.read().decode('utf-8')
    except HTTPError:
        return ''


def get_urls(deep) -> list:
    cur.execute("SELECT link FROM Links WHERE cycle = ?", (deep,))
    res = cur.fetchall()
    return [row[0] for row in res]


def main() -> None:
    start_url, max_depth = get_start_url()
    configure_db()

    count = 1
    add_links_bulk({start_url}, count)  # Initial link insertion
    conn.commit()

    while count < max_depth:
        m = get_urls(count)
        count += 1

        parser = HTMLP()

        with ThreadPoolExecutor(max_workers=500) as executor:
            results = list(executor.map(fetch_content, m))

        with ThreadPoolExecutor(max_workers=500) as executor:
            executor.map(parser.feed, filter(None, results))

        add_links_bulk(parser.local_links, count)
        conn.commit()

# Не очень понимаю как можно добиться глубины 6, скорость не позволяет быстрее... тем не менее код рабочий
if __name__ == "__main__":
    main()
