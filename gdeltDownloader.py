import gzip
from datetime import datetime
import logging
import re
from threading import Thread

import os

import dragnet
import metadata_parser
import requests

from typing import Dict, List

import sys


class Downloader(Thread):

    def __init__(self, url, eid, gid, metadata_dict, filename, article_dir="articles", metadata_dir="metadata", ymd=datetime.now().strftime("%Y-%m-%d")):
        super().__init__()
        self.url = url
        self.eid = eid
        self.gid = gid
        self.metadata_dict : Dict[List] = metadata_dict

        # print("type(self.metadata_dict):  {} ".format(type(self.metadata_dict)))

        self.filename = filename
        self.article_dir = article_dir
        self.ymd = ymd
        self.metadata_dir = metadata_dir

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Gdelter')


    def run(self):
        self.logger.info("Downloading {}".format(self.url))
        status, html = self.get_web_page(self.url)
        content = self.get_content(html) if status == 200 else ""

        title, author, site_name, description, keywords = self._parse_metadata(html)
        self.save_html_content(html, self.filename, True)
        self.save_html_content(content, self.filename, False)

        metadata = [self.url, self.eid, self.gid, status, self.ymd, title, author, site_name, description, keywords]
        # u = self.url
        # print("self.url:  {}".format(self.url))
        self.metadata_dict[self.url] = metadata



    def get_web_page(self, url):
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


    # TODO : should have a flat to optionally gzip the saved articles
    def save_html_content(self, content, filename, raw=False):
        file = self.html_content_filepath(self.article_dir, self.ymd, filename,  raw)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        # write content as gzip-ed text
        with gzip.open(file, "wt") as fo:
            fo.write(content)


    def get_content(self, html):
        try:
            content = dragnet.extract_content(html).replace(u'\xa0', u' ')
            return content
        except dragnet.blocks.BlockifyError as e:
            self.logger.error("content could not be parsed by lxml and extracted by dragnet ")
            self.logger.error(e)
            return ""



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
            return os.path.join("{}_raw".format(article_dir), "{}".format(ymd), "{}.html".format(filename))
        else:
            return os.path.join(article_dir, "{}".format(ymd), "{}.text".format(filename))



    def _clean_metadata(self, s):
        # return s.replace("\n", " ").replace("\t", " ")
        return re.sub("[\n\r\t]", " ", str(s))

    def _parse_metadata(self, HTML_str):
        page = metadata_parser.MetadataParser(html=HTML_str)
        metadata = {x: page.metadata[x] for x in page.metadata if
                    page.metadata[x] and isinstance(page.metadata[x], dict)}
        title = page.get_metadatas('title')
        author = page.get_metadatas('author')
        site_name = page.get_metadatas('site_name')
        description = page.get_metadatas('description')
        keywords = page.get_metadatas('news_keywords')
        metas = [title, author, site_name, description, keywords]
        return [self._clean_metadata(x[0]) if isinstance(x, list) else "" for x in metas]





