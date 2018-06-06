import re
import timeit
import os
import requests
from urllib.request import urlretrieve, quote
from urllib.parse import urlparse, unquote, urldefrag

from multiprocessing.dummy import Pool
from bs4 import BeautifulSoup

import time


def benchmark(func):
    def wrapped(*args, **kwargs):
        print("--- Function \"%s\" called." % func.__name__)
        s = timeit.default_timer()
        ret = func(*args, **kwargs)
        e = timeit.default_timer()
        print("--- Function \"%s\" completed with %.10fs elapsed." % (func.__name__, e - s))
        return ret

    return wrapped


class WebpageCrawler(object):
    """
    Web elements will be downloaded and stored on the disk
    """
    web_encoding = "utf-8"
    max_page_count = 16
    interval = 4

    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) " \
                "Chrome/66.0.3359.181 Safari/537.36 "

    def __init__(self, *urls):
        """

        :param urls: iterable
        """
        self.pending = list(urls)
        self.visited = set()
        self.url = None
        self.page = None

    def open(self):

        response = requests.get(self.url, headers={"User-Agent": self.user_agent})

        # Redirect is common and the final url might have been visited
        # TODO should it get de-fragmented here?

        self.url = urldefrag(response.url)[0]

        if self.url not in self.visited:
            page = response.content.decode("utf-8", "ignore")
            self.visited.add(self.url)
            return page

    def get_next_full_urls(self):
        """

        :return: list of full urls to be crawled in next turn
        """
        raise NotImplementedError

    def keep_crawling(self):
        """

        :return: whether we should stop crawling
        """
        return self.pending and len(self.visited) < self.max_page_count

    def crawl_next_page(self):
        """
        Pop and open a url from the pending url list.

        :return: true if a new page is crawled
        """
        self.url = urldefrag(self.pending.pop(0))[0]
        if self.url in self.visited:
            return False

        page = self.open()
        if not page:
            return False
        self.page = page

        new = self.get_next_full_urls()
        for link in new:
            link = urldefrag(link)[0]
            if link not in self.visited:
                self.pending.append(link)

        return True

    def handle_page(self):
        """
        Do something with the current page
        :return:
        """
        raise NotImplementedError

    def finish(self):
        pass

    @benchmark
    def start(self):
        while self.keep_crawling():
            if self.crawl_next_page():
                self.handle_page()
                time.sleep(self.interval)
        self.finish()


class WebpageElemDownloader(WebpageCrawler):
    thread_count = 4
    max_elem_count = 1024
    download_dir = "./%s/" % __name__
    prefix = ""
    suffix = ".dat"

    def __init__(self, *urls):
        super().__init__(*urls)
        self.elem_count = 0

        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

    def extract_elem_urls(self):
        """
        Extract full urls of elements from current page.

        :return: list of full urls, each of which refers to a web page element
        """
        raise NotImplementedError

    def keep_crawling(self):
        return super().keep_crawling() and self.elem_count < self.max_elem_count

    @benchmark
    def _retrieveElems(self, urls, filenames):
        pool = Pool(self.thread_count)
        pool.starmap(urlretrieve, zip(urls, filenames))

    def handle_page(self):
        remain = self.max_elem_count - self.elem_count
        prefix = self.download_dir + self.prefix
        urls = self.extract_elem_urls()[:remain]
        filenames = ["%s%d%s" % (prefix, self.elem_count + i, self.suffix) for i in range(len(urls))]

        self._retrieveElems(urls, filenames)
        self.elem_count += len(urls)

        print(f"[-] current page [{len(self.visited)}]; {len(urls)} downloaded; {self.elem_count} in total.")


class TaobaoSearchResultThumbnails(WebpageElemDownloader):
    elem_url_regex = re.compile('"pic_url":"//(.*?)"')
    max_elem_count = 64
    download_dir = "./test/data/taobao/"
    suffix = ".jpg"

    def __init__(self, keyword, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.query = "https://s.taobao.com/search?q=" + quote(keyword)
        self.prefix = keyword
        super().__init__(self.query)

    def get_next_full_urls(self):
        no = len(self.visited)
        return [self.query + "&s=" + str(no * 44)]

    def extract_elem_urls(self):
        urls = self.elem_url_regex.findall(self.page)
        return [f"http://{url}_180x180.jpg" for url in urls]


class JdSearchResultThumbnails(WebpageElemDownloader):
    elem_url_regex = re.compile('width="220" height="220".*source-data-lazy-img="//(.+\.jpg)"')
    max_elem_count = 64
    suffix = ".jpg"
    download_dir = "./test/data/jd/"

    def __init__(self, keyword, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

        self.query = "https://search.jd.com/Search?keyword=" + quote(keyword) + "&enc=utf-8"
        self.prefix = keyword
        super().__init__(self.query)

    def extract_elem_urls(self):
        img_sources = self.elem_url_regex.findall(self.page)
        return ["http://" + src for src in img_sources]

    def get_next_full_urls(self):
        no = len(self.visited)
        return [self.query + "&page=" + str(no * 2 - 1)]

# The "Html fragments" feature appears in at least two ways.
# Firstly, the link referring to a different part its original webpage contains a "#".
# Secondly, the link referring to another webpage might be a redirection to a part of destination page.


class ENWikiText(WebpageCrawler):
    parenthesis_regex = re.compile('\(.+?\)')  # to remove parenthesis content
    citations_regex = re.compile('\[.+?\]')  # to remove citations, e.g. [1]

    download_dir = "./test/data/ENWiki/"
    basic_url = "https://en.wikipedia.org/wiki/"

    def __init__(self, keyword, **kwargs):
        for k, v in kwargs.items():
            setattr(self, k, v)

        super().__init__(self.basic_url + quote(keyword))

        if not os.path.exists(self.download_dir):
            os.makedirs(self.download_dir)

        self.step = (self.max_page_count // 10) or self.max_page_count

    def get_next_full_urls(self):
        soup = BeautifulSoup(self.page, "html.parser")
        content = soup.find("div", {"id": "mw-content-text"})

        urls = []
        for a in content.find_all("a"):
            href = a.get("href")
            if not href:
                continue
            elif not href.startswith("/wiki/"):  # allow only article pages
                continue
            elif href[-4:] in ".png .jpg .jpeg .svg":  # ignore image files inside articles
                continue
            urls.append(self.basic_url + href[6:])

        self.main_content_text = content
        return urls

    def handle_page(self):
        _, _, path, _, _, _ = urlparse(self.url)
        keyword = unquote(path.rsplit("/", 1)[1])

        paragraphs = self.main_content_text.find_all("p")

        file_output = self.download_dir + keyword + ".txt"

        with open(file_output, mode="w", encoding="utf-8") as f:
            for p in paragraphs:
                text = p.get_text().strip()
                text = self.parenthesis_regex.sub("", text)
                text = self.citations_regex.sub("", text)
                if text:
                    f.write(text + "\n\n")

        count = len(self.visited)
        if count % self.step == 0:
            print("[-] %d/%d crawled." % (count, self.max_page_count))



@benchmark
def test_downloader():
    keyword = "女雪地靴"
    crawler = JdSearchResultThumbnails(keyword,
                                       download_dir="./data/jd/",
                                       max_elem_count=1024,
                                       max_page_count=50
                                       )
    crawler.start()


@benchmark
def test_crawler():
    keyword = "Arya_Stark"
    crawler = ENWikiText(keyword, max_page_count=50)
    crawler.start()


if __name__ == '__main__':
    # test_downloader()
    test_crawler()
