import logging
import coloredlogs
import os
import threading
from collections import Counter, defaultdict
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import repeat
from typing import *
from urllib.parse import urlparse

import dragnet
import metadata_parser
import requests
from bs4 import BeautifulSoup

from domains import Domains
from gdeltIO import GIO
from gutils import GUtils

'''
create a class that represents a separate thread for each download. 
Call the start() method to kick off subclass-ed run()

n.b. see:
https://stackoverflow.com/questions/21492424/python-multithreading-for-downloads

TODO : we need a Producer/Consumer pattern to avoid creating too many threads at once
    n.b. This uses up too many resources and may also hit the same sites too often for politeness 
    n.b. keep in mind that creating and destroying threads is expensive
    check out Queue class
    see: https://stackoverflow.com/questions/19369724/the-right-way-to-limit-maximum-number-of-threads-running-at-once

    Use a semaphore as a temporary fix

'''

# define an upper limit on the maximum value of threads. i.e. max number of concurrent downloads
maximumNumberOfThreads = 20
threadLimiter = threading.BoundedSemaphore(maximumNumberOfThreads)


class ArticleDownloader():
    def __init__(self, basedir, status_dir, ymd):
        self.basedir = basedir
        self.status_dir = status_dir
        self.ymd = ymd
        self.metadata_dict = defaultdict(list)

        print("ArticleDownloader")
        print("\tymd:        {}".format(self.ymd))
        print("\tbasedir:    {}".format(self.basedir))
        print("\tstatus_dir: {}".format(self.status_dir))
        print("\tpage domain dir: {}".format(GIO.define_page_domain_dir(self.basedir, self.ymd)))
        print("\thtml meta dir:   {}".format(GIO.define_html_meta_dir(self.basedir, self.ymd)))
        print("\tlinks dir:       {}".format(GIO.define_page_links_dir(self.basedir, self.ymd)))
        print("-"*80)

        coloredlogs.install()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Downloader')


    def download_articles(self, url_ids, gdelt_ymd):
        """
        Create a thread pool and download specified urls
        """
        self.logger.info("Downloading {} articles and resaving as gzipped files to : {}".format(len(url_ids), self.basedir))
        with ThreadPoolExecutor(max_workers=maximumNumberOfThreads) as executor:
            article_results = list(executor.map(self.execute_article_download, url_ids))

        # save stats collected in a global dict by `execute_article_download` method
        GIO.save_article_stats(self.metadata_dict, GIO.define_stat_dir(self.basedir, self.ymd), gdelt_ymd)

        num_downloads = [x[0] for x in article_results]
        total_bytes = sum([x[1] for x in article_results])

        print("\t>>> downloaded {} of {} urls".format(sum(num_downloads), len(num_downloads)))
        return sum(num_downloads), len(num_downloads), total_bytes


    def execute_article_download(self, url_id):
        url, eid, gid = url_id
        filename = eid if eid else "g_{}".format(gid)
        file_basename = os.path.basename(url)
        success = 0
        file_size_bytes = 0

        # if self.article_exists(filename):
        articlefile = GIO.define_article_file(self.basedir, self.ymd, filename, False)
        if os.path.exists(articlefile):
            self.logger.info("\t=> article exists {} for url {}".format(articlefile, url))
        elif os.path.exists("{}.gz".format(articlefile)):
            self.logger.info("\t=> article exists {} for url {}".format("{}.gz".format(articlefile), url))
        else:
            status, html = self._get_web_page(url)
            if status == 200:
                content = self._get_content(html)
                file_size_bytes = len(html)
                self.logger.info("Downloading {} bytes from {} to {}".format(file_size_bytes, url, articlefile))

                meta = self._extract_meta(html)
                page_links, page_domain_dict = self._extract_all_links(html, url)
                # TODO : domain_counts above is not used - probably should be saved to `page_domain_dir` below!

                # save list of links found on page to csv file
                GIO.save_links(page_links, GIO.define_page_links_dir(self.basedir, self.ymd), filename)
                # save html meta as json file
                GIO.save_dict(meta, GIO.define_html_meta_dir(self.basedir, self.ymd), filename)
                # save list of domains found on page to csv file
                # TODO : isn't this the exact same as above? Should be saving a list of domains, but is creating json
                GIO.save_dict(page_domain_dict, GIO.define_page_domain_dir(self.basedir, self.ymd), filename)
                # save content (body text) to txt file
                GIO.save_html_content(content, self.basedir, self.ymd, filename, False)

                title, author, site_name, keywords, description = self._parse_metadata(html)
                metadata = [url, Domains.get_domain(url), file_size_bytes, eid, gid, status, self.ymd, title,
                            site_name, author, keywords, description]
                self.metadata_dict[url] = metadata
                success = 1
            else:
                GIO.save_html_content("", self.basedir, self.ymd, filename, False)
                self.logger.info("status = {} for url: {} ".format(status, url))
        return success, file_size_bytes


    def article_exists(self, filename):
        html_articlefile = GIO.define_article_file(self.basedir, self.ymd, filename, True)
        content_articlefile = GIO.define_article_file(self.basedir, self.ymd, filename, False)
        if os.path.exists(html_articlefile) or os.path.exists(content_articlefile) or os.path.exists(html_articlefile + ".gz") or os.path.exists(content_articlefile + ".gz"):
            return True
        return False


    # -----------------------------------------------------------------------------------------------------------------
    #   Web-getter methods
    # -----------------------------------------------------------------------------------------------------------------
    def _get_web_page(self, url: str):
        '''return (status, text) -> (int, str)'''
        try:
            r = requests.get(url, timeout=25)
            status_code = r.status_code
            text = r.text
            html = text.replace(u'\xa0', u' ') if status_code == 200 else ""
            return status_code, html
        except requests.exceptions.RequestException as e:
            self.logger.warning("ERROR: RequestException exception for url: {}".format(url))
            # self.logger.error(e)
            return -1, ""


    def _get_content(self, html: str) -> str:
        ''' Extract & return just body text from HTML-formatted text '''
        try:
            content = dragnet.extract_content(html).replace(u'\xa0', u' ')
            return content
        except dragnet.blocks.BlockifyError as e:
            self.logger.error("content could not be parsed by lxml and extracted by dragnet ")
            self.logger.error(e)
            return ""


    # -----------------------------------------------------------------------------------------------------------------
    #   Content parsing methods
    # -----------------------------------------------------------------------------------------------------------------
    def _extract_meta(self, html_content: str) :
        page = metadata_parser.MetadataParser(html=html_content)
        metadata: Dict = {x: page.metadata[x] for x in page.metadata if
                    page.metadata[x] and isinstance(page.metadata[x], dict)}
        return metadata


    def _extract_all_links(self, html_content: str, url: str):
        '''
        Parse HTML and Return all URLs found on the page
        :param html_content: str of html-formatted content
        :return: (list of urls, dictionary of {domain -> count})
        '''
        soup = BeautifulSoup(html_content, 'lxml')
        links = [a.get('href') for a in soup.find_all('a', href=True) if a.get('href').startswith("http")]
        domains = [Domains.get_domain(a) for a in links]
        c = Counter([x for x in domains if x])
        page_domain_dict = {"url": url, "basedomain": Domains.get_domain(url), "domains": c }
        return links, page_domain_dict


    def _parse_metadata(self, HTML_str):
        page = metadata_parser.MetadataParser(html=HTML_str)
        # TODO : deleteme?
        # metadata = {x: page.metadata[x] for x in page.metadata if
        #             page.metadata[x] and isinstance(page.metadata[x], dict)}
        title = page.get_metadatas('title')
        author = page.get_metadatas('author')
        site_name = page.get_metadatas('site_name')
        description = page.get_metadatas('description')
        keywords = page.get_metadatas('news_keywords')
        metas = [title, author, site_name, keywords, description]
        return [GUtils.clean_text(x[0]) if isinstance(x, list) else "" for x in metas]


    # def _get_domain(self, url: str) -> str:
    #     ''' Return the base domain of a ULR (e.g. `http://www.cnn.com` )'''
    #     try:
    #         parsed_url = urlparse(url)
    #         return '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_url)
    #     except ValueError as e:
    #         self.logger.error("Received ValueError for url: {} :".format(url))
    #         self.logger.error(e)
    #         return ""

