import gzip
import json
import logging
import os
import re
from collections import Counter
from datetime import datetime
from threading import Thread
from typing import *
from urllib.parse import urlparse

import dragnet
import metadata_parser
import requests



from bs4 import BeautifulSoup

'''
create a class that represents a separate thread for each download. 
Call the start() method to kick off subclass-ed run()

n.b. see:
https://stackoverflow.com/questions/21492424/python-multithreading-for-downloads
'''


class Downloader(Thread):

    def __init__(self, url, eid, gid, metadata_dict, filename,
                 article_dir,
                 status_dir,
                 metadata_dict_dir,
                 page_link_dir,
                 domain_dir,
                 ymd=datetime.now().strftime("%Y-%m-%d")
                 ):
        super().__init__()
        self.url = url
        self.eid = eid
        self.gid = gid
        self.metadata_dict : Dict[List] = metadata_dict

        self.filename = filename
        self.article_dir = article_dir
        self.ymd = ymd
        self.status_dir = status_dir

        self.metadata_dict_dir = metadata_dict_dir
        self.page_link_dir = page_link_dir
        self.domain_dir = domain_dir

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Downloader')


    ####################################################################################################################

    #  override the run() function of Thread class to start the thread.
    def run(self):
        self.logger.info("Downloading {}".format(self.url))
        status, html = self._get_web_page(self.url)
        content = self._get_content(html) if status == 200 else ""
        file_size_bytes = len(html)

        meta = self._extract_meta(html)

        page_links, domain_counts = self._extract_all_links(html)
        self._save_links(page_links, self.page_link_dir, self.filename)
        self._save_dict(meta, self.metadata_dict_dir, self.filename)
        self._save_dict(domain_counts, self.domain_dir, self.filename)
        self.save_html_content(content, self.filename, False)

        title, site_name = self._parse_metadata(html)
        metadata = [self.url, self._get_domain(self.url), file_size_bytes, self.eid, self.gid, status, self.ymd, title, site_name]
        self.metadata_dict[self.url] = metadata



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
            print("RequestException exception for url: {}".format(url))
            print(e)
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
    def _parse_metadata(self, HTML_str):
        page = metadata_parser.MetadataParser(html=HTML_str)
        metadata = {x: page.metadata[x] for x in page.metadata if
                    page.metadata[x] and isinstance(page.metadata[x], dict)}
        title = page.get_metadatas('title')
        # author = page.get_metadatas('author')
        site_name = page.get_metadatas('site_name')
        # description = page.get_metadatas('description')
        # keywords = page.get_metadatas('news_keywords')
        metas = [title, site_name]
        return [self._clean_metadata(x[0]) if isinstance(x, list) else "" for x in metas]


    def _extract_meta(self, html_content: str) :
        page = metadata_parser.MetadataParser(html=html_content)
        metadata: Dict = {x: page.metadata[x] for x in page.metadata if
                    page.metadata[x] and isinstance(page.metadata[x], dict)}
        return metadata


    def _get_domain(self, url: str) -> str:
        ''' Return the base domain of a ULR (e.g. `http://www.cnn.com` )'''
        parsed_url = urlparse(url)
        return '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_url)


    def _extract_all_links(self, html_content: str):
        '''
        Parse HTML and Return all URLs found on the page
        :param html_content: str of html-formatted content
        :return: (list of urls, dictionary of {domain -> count})
        '''
        soup = BeautifulSoup(html_content, 'lxml')
        links = [a.get('href') for a in soup.find_all('a', href=True) if a.get('href').startswith("http")]
        domains = [self._get_domain(a) for a in links]
        c = Counter(domains)
        return links, c


    # -----------------------------------------------------------------------------------------------------------------
    #   Save file methods
    # -----------------------------------------------------------------------------------------------------------------
    def save_html_content(self, content, filename, raw=False, compress=True):
        file = self.html_content_filepath(self.article_dir, self.ymd, filename,  raw)
        if compress:
            # write content as gzip-ed text
            with gzip.open("{}.gz".format(file), "wt") as fo:
                fo.write(content)
        else:
            with open(file, "wt") as fo:
                fo.write(content)


    def _save_links(self, links, dir, filename):
        ''' save a list of URLS as a gzipped text file'''
        file = self._build_filepath(dir, filename, "txt")
        with gzip.open("{}.gz".format(file), "wt") as fo:
            fo.write("\n".join(links))


    def _save_dict(self, d, dir, filename):
        ''' save a dict to dir/filename as a gzipped json file'''
        file = self._build_filepath(dir, filename, "json")
        with gzip.open("{}.gz".format(file), "wt") as fo:
            json.dump(d, fo, indent=4)


    # -----------------------------------------------------------------------------------------------------------------
    #   Path define methods
    # -----------------------------------------------------------------------------------------------------------------
    def html_content_filepath(self, article_dir, ymd, filename, raw=False):
        '''
        Define an absolute path to which to save HTML (raw or content)
        :param filename:    name of the file
        :param article_dir: article directory
        :param ymd:         Year-Month-Date
        :param raw:         Boolean flag for whether or not to specify 'raw' article directory
        :return:            a path to which to save HTML content
        '''
        if raw:
            path = os.path.join("{}_raw".format(article_dir), "{}".format(ymd), "{}.html".format(filename))
        else:
            path = os.path.join(article_dir, "{}".format(ymd), "{}.text".format(filename))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path


    def _build_filepath(self, dir, filename, extension = "text"):
        ''' create a full path to a file by joining dir, ymd and filename'''
        path = os.path.join(dir, "{}".format(self.ymd), "{}.{}".format(filename, extension))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return path


    # -----------------------------------------------------------------------------------------------------------------
    #   Text processing methods
    # -----------------------------------------------------------------------------------------------------------------
    def _clean_metadata(self, s: str) -> str:
        ''' Replace tabs, newlines with spaces '''
        return re.sub("[\n\r\t]", " ", str(s))

