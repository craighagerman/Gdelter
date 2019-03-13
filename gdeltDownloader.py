import gzip
import json
import logging
import os
import re
from collections import Counter
from datetime import datetime
from threading import Thread
from typing import Dict, List
from urllib.parse import urlparse

import dragnet
import metadata_parser
import requests



from bs4 import BeautifulSoup


class Downloader(Thread):

    def __init__(self, url, eid, gid, metadata_dict, filename,
                 article_dir="articles",
                 metadata_dir="metadata",
                 ymd=datetime.now().strftime("%Y-%m-%d"),
                 metadata_dict_dir="metadata_dict",
                 page_link_dir="page_links",
                 domain_dir="domains"
                 ):
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

        self.metadata_dict_dir = metadata_dict_dir
        self.page_link_dir = page_link_dir
        self.domain_dir = domain_dir

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Gdelter')


    def run(self):
        self.logger.info("Downloading {}".format(self.url))
        status, html = self.get_web_page(self.url)
        content = self.get_content(html) if status == 200 else ""

        file_size_bytes = len(html)


        # WIP -------------------------------------------------------------------------------------------   start   ----
        meta = self._extract_meta(html)
        # TODO : define a metadata path
        meta_dictionary_filename = ""
        self.save_meta_dictionary(meta, meta_dictionary_filename)

        page_links_filename = ""
        domain_filename = ""
        page_links, domain_counts = self._extract_all_links(html)
        self._save_links(page_links, page_links_filename)
        self._save_domains(domain_counts, domain_filename)

        # WIP ---------------------------------------------------------------------------------------------   end   ----



        # self.save_html_content(html, self.filename, True)
        self.save_html_content(content, self.filename, False)

        title, site_name = self._parse_metadata(html)
        metadata = [self.url, self._get_domain(self.url), file_size_bytes, self.eid, self.gid, status, self.ymd, title, site_name]
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
        with gzip.open("{}.gz".format(file), "wt") as fo:
            fo.write(content)


    def save_meta_dictionary(self, metadata, filename):
        file = self.build_filepath(self.metadata_dict_dir, filename)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with gzip.open("{}.gz".format(file), "wt") as fo:
            json.dump(metadata, fo, indent=4)


    def _save_links(self, links, filename):
        file = self.build_filepath(self.page_link_dir, filename)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with gzip.open("{}.gz".format(file), "wt") as fo:
            fo.write("\n".join(links))

    def _save_domains(self, counter, filename):
        file = self.build_filepath(self.domain_dir, filename)
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with gzip.open("{}.gz".format(file), "wt") as fo:
            json.dump(counter, fo, indent=4)





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


    def build_filepath(self, dir, filename):
        return os.path.join(dir, "{}".format(self.ymd), "{}.text".format(filename))


    def _clean_metadata(self, s):
        # return s.replace("\n", " ").replace("\t", " ")
        return re.sub("[\n\r\t]", " ", str(s))

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


    def _extract_meta(self, html_content):
        page = metadata_parser.MetadataParser(html=html_content)
        metadata : Dict = {x: page.metadata[x] for x in page.metadata if
                    page.metadata[x] and isinstance(page.metadata[x], dict)}
        return metadata



    def _get_domain(self, url):
        parsed_url = urlparse(url)
        return '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_url)


    def _extract_all_links(self, html_content):
        soup = BeautifulSoup(html_content, 'lxml')
        links = [a.get('href') for a in soup.find_all('a', href=True) if a.get('href').startswith("http")]

        domains = [self._get_domain(a) for a in links]
        c = Counter(domains)
        return links, c


