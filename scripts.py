import re
import timeit
from multiprocessing.dummy import Pool
from urllib import request

import time


def benchmark(func):
    def wrapped(*args, **kwargs):
        print("function %s: " % func.__name__, end="")
        s = timeit.default_timer()
        ret = func(*args, **kwargs)
        e = timeit.default_timer()
        print("%.10fs elapsed." % (e - s))
        return ret

    return wrapped


class WebPageElemCrawler(object):
    """
    Web elements will be downloaded and stored on the disk
    """

    threadCount = 5  # thread count
    fprefix = "./" + __name__ + "/file"  # prefix of storing path
    fsuffix = ".dat"  # suffix of a file
    seconds = 2  # how long the time interval should be between two page crawling

    # Caller should provide a pattern to find his desired url in web page.
    # The intended url must be placed inside the closing parentheses in regex.
    patElemUrl = None
    # If proto is None, extractElemUrls() returns original urls matched by patElemUrl.
    # Otherwise, a matched url is expected to start with "//" or proto. If it starts with "//"
    # then a proto string is appended to its front end; if it starts with proto, nothing more will be added.
    proto = "http"

    maxPageCount = 16
    maxElemCount = 1024

    def __init__(self, *urls):
        """

        :param urls: iterable
        """

        self._currPage = 0
        self._elemCount = 0
        self._pageUrls = list(urls)

    @benchmark
    def readUrl(self, url):
        return request.urlopen(url).read().decode("utf-8", "ignore")

    def extractPageUrls(self, page, no, url):
        """

        :param page: current page data
        :param no: current page number
        :param url: from which urls will be obtained and returned
        :return: list of urls to be crawled in next turn
        """
        raise NotImplementedError

    def extractElemUrls(self, page):
        """

        :param page: page data in string
        :return: list of urls, each of which refers to a web page element
        """
        urls = self.patElemUrl.findall(page) if self.patElemUrl else []
        return [(self.proto + ":" + url) if self.proto and not url.startswith(self.proto) else url for url in urls]

    def keepCrawling(self):
        """

        :return: whether we should stop crawling
        """
        return self._pageUrls and self._currPage < self.maxPageCount and self._elemCount < self.maxElemCount

    def crawlNextPage(self):
        url = self._pageUrls.pop(0)
        data = self.readUrl(url)

        self._pageUrls += self.extractPageUrls(data, self._currPage, url)
        self._data = data
        self._currPage += 1

    @benchmark
    def _retrieveElems(self, urls, filenames):
        pool = Pool(self.threadCount)
        pool.starmap(request.urlretrieve, zip(urls, filenames))

    def retrieveElems(self):
        remain = self.maxElemCount - self._elemCount
        urls = self.extractElemUrls(self._data)[:remain]
        filenames = ["%s%d%s" % (self.fprefix, self._elemCount + i, self.fsuffix) for i in range(len(urls))]

        self._retrieveElems(urls, filenames)
        self._elemCount += len(urls)

    @benchmark
    def startToEnd(self):
        while self.keepCrawling():
            self.crawlNextPage()
            self.retrieveElems()
            time.sleep(self.seconds)


class TaobaoGetThumbnails(WebPageElemCrawler):
    patElemUrl = re.compile(r'"pic_url":"(//.*?)"')
    maxElemCount = 64
    fsuffix = ".jpg"

    def __init__(self, keyword):
        self.firstUrl = "https://s.taobao.com/search?q=" + request.quote(keyword)
        self.keyword = keyword
        super().__init__(self.firstUrl)

        self.fprefix = "./test/data/taobao/" + keyword

    def extractPageUrls(self, page, no, url):
        return [self.firstUrl + "&s=" + str(no * 44)]

    def extractElemUrls(self, page):
        urls = super().extractElemUrls(page)
        return [url + "_180x180.jpg" for url in urls]


class JdGetThumbnails(WebPageElemCrawler):
    patElemUrl = re.compile(r'width="220" height="220".*src="(//.*?)"')
    maxElemCount = 64
    fsuffix = ".jpg"

    def __init__(self, keyword):
        self.firstUrl = "https://search.jd.com/Search?keyword=" + request.quote(keyword) + "&enc=utf-8"
        self.keyword = keyword
        super().__init__(self.firstUrl)

        self.fprefix = "./test/data/jd/" + keyword


    def extractPageUrls(self, page, no, url):
        return [self.firstUrl + "&page=" + str(no * 2 - 1)]


@benchmark
def test():
    keyword = "女雪地靴"
    JdGetThumbnails(keyword).startToEnd()
    TaobaoGetThumbnails(keyword).startToEnd()


if __name__ == '__main__':
    test()
