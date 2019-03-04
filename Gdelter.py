#!/home/chagerman/.pyenv/shims/python

import logging
import os
import re
from datetime import datetime
from typing import List

import click
import metadata_parser
import pandas as pd
import requests
from collections import defaultdict
from tqdm import tqdm

import GdeltParameters
from downloader import Downloader
from gdelt_decorators import *
from gdeltutils import GdeltUtils
from webio import WebIO


'''


(1) cron job
    every 15 min download last update
    unzip, parse & merge for article urls

(2) historical (master list)
    specify a date
    download both events and gkg files for that date

(1 & 2) given a merged list of article urls to download: 
    unzip, parse & merge for article urls
    download article from url
    save article, save metadata

-----------------------------------------------------------------------------------------------------------------------

WebIO
    get html
    get content






-----------------------------------------------------------------------------------------------------------------------

parse lastupdate 15-min update
    download events, gkg zip files
        save to local file
        collect id, urls from events file
        collect id, urls from gtk file
        merge the above into a dictionary {url -> [event_id, gtk_id, status_code, other_metadata]}
    (from above list of urls...)
    download articles from list of urls
        save raw html
        extract and save content
        parse html metadata, add a row of metadata to metadata file (title, author etc)



parse master file lists:
    decide on a date to process
    get the relevant events file
    get the relevant gkg file
    download events, gkg zip files
        ... same as above


'''


class Gdelter:

    def __init__(self, url=None, file=None, basedir="./"):
        self.url = url
        self.file = file
        self.basedir = basedir

        # define output directory paths
        self.article_dir = os.path.join(self.basedir, "articles")
        self.metadata_dir = os.path.join(self.basedir, "metadata")

        self.ymd = datetime.now().strftime("%Y-%m-%d")
        # self.gdelt_ymd =
        self.webio = WebIO(self.article_dir, self.metadata_dir, self.ymd)

        self.MetadataFileKey = "url_metadata.csv"


        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Gdelter')
        # self.logger.setLevel(logging.INFO)

        # TODO : delete - moved to GdeltParameters
        # # define key for "events" and "gkg"
        # self.EventsKey = "events"
        # self.GkgKey = "gkg"

        # define column names from Gdelt event and gkg extract files
        self.EventIDColumnKey = "GLOBALEVENTID"
        self.EventUrlColumnKey = "SOURCEURL"
        self.GkgIDColumnKey = "GKGRECORDID"
        self.GkgUrlColumnKey = "DocumentIdentifier"
        self.GkgSourceColumnKey = "SourceCommonName"



    @timeit
    def process_last_update(self):
        # urls: List[str]
        self.logger.info("Getting lastupdate urls ... ")
        urls = self._get_lastupdate()
        # TODO : delete gdelt_ymd
        gdelt_ymd = self._get_gdelt_date(urls)
        self.ymd = gdelt_ymd

        self.logger.info("GDELT Year-Month-Date: {}".format(gdelt_ymd))
        self.webio.setymd(gdelt_ymd)

        self.logger.info("Creating a dictionary of gdelt type -> lastupdate urls... ")
        # TODO : should the urls just be passes in as a dict rather than a list?
        lastupdate_dict = self._get_lastupdate_dict(urls)

        # get the Events (export) and GKG urls from `lastupdate`
        self.logger.info("Decompressing and saving GDELT Event and GKG zip files... ")
        gutils = GdeltUtils()
        events_url = lastupdate_dict['export']
        events_basedir = os.path.join(self.basedir, GdeltParameters.EventsKey, gdelt_ymd)
        events_file = gutils.decompress_zip_url(events_url, events_basedir)
        gkg_url = lastupdate_dict[GdeltParameters.GkgKey]
        gkg_basedir = os.path.join(self.basedir, GdeltParameters.GkgKey, gdelt_ymd)
        gkg_file = gutils.decompress_zip_url(gkg_url, gkg_basedir)

        # parse Events & GKG files; create a master list of [url, event_id, gkg_id]
        self.logger.info("Parsing event and gkg extract files ...")
        event_id_urls = self._parse_gdelt_file(events_file, GdeltParameters.EventsKey)
        gkg_id_urls = self._parse_gdelt_file(gkg_file, GdeltParameters.GkgKey)
        self.logger.info("creating a single list of article urls ...")
        url_ids = self._create_event_gkg_url_dict(event_id_urls, gkg_id_urls)

        # iterate through all urls, downloading articles & web page metadata, save results
        self.logger.info("Downloading and saving articles & metadata from GDELT news articles ...")
        self._download_and_save_articles(url_ids, gdelt_ymd)

        # finish
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} urls".format(len(url_ids)))
        self.logger.info("-" * 50)



    @timeit
    def process_master(self):
        print("Not implemented yet")








    ####################################################################################################################



    def _get_lastupdate(self) -> List[str]:
        ''' Get the `lastupdate` from either a given url or a local file
                url => get web page, split into lines
                file => open file, read lines
            Parse last update and return a list of urls
        '''
        if (self.url):
            urls: List[str]     # a list of urls extracted from lastupdate file
            try:
                response = requests.get(self.url, timeout=25)
                status_code = response.status_code
                if status_code != 200:
                    self.logger.warning("Received status: {} for url: {}".format(status_code, self.url))
                else:
                    urls = [x.split()[-1] for x in response.text.split("\n") if x]
            except requests.exceptions.Timeout as e:
                self.logger.error("Timeout exception for url: {}".format(self.url))
        elif (self.file):
            try:
                urls = [x.strip().split()[-1] for x in open(self.file)]
            except FileNotFoundError as e:
                self.logger.error("File {} cannot be found: {}".format(self.file, e))
        return urls  # type: List[str]




    def _get_lastupdate_dict(self, urls):
        return {u.split(".")[-3]: u.strip() for u in tqdm(urls)}

    def _get_gdelt_date(self, urls):
        gdelt_timestamp = os.path.basename(urls[0]).split(".")[0]
        return gdelt_timestamp[:8]  # only return the year-month-date part


    def _parse_gdelt_file(self, file, gdelt_type):
        self.logger.info("parsing gdelt type {} for extract file {}".format(gdelt_type, file))
        df = pd.read_csv(file, sep="\t", header=None)
        columns = GdeltParameters.event_columns if gdelt_type == GdeltParameters.EventsKey else GdeltParameters.gkg_columns
        df.columns = columns
        id_url_columns = GdeltParameters.event_id_url_columns if gdelt_type == GdeltParameters.EventsKey else GdeltParameters.gkg_id_url_columns
        id_url = df[id_url_columns].values.tolist()
        return id_url


    def _create_event_gkg_url_dict(self, event_id_urls, gkg_id_urls):
        # check for duplicates
        # TODO : the following is an unneeded expense. Remove after getting enough stats on duplicates
        def check_dups(event_id_urls, gkg_id_urls):
            event_urls = [url for gid, url in event_id_urls]
            gkg_urls = [url for gid, url in gkg_id_urls]
            self.logger.info("Number of Event urls: {}".format(len(event_urls)))
            self.logger.info("Number of GKG  urls: {}".format(len(gkg_urls)))
            if len(event_urls) != len(set(event_urls)): self.logger.warning("*\tEvent urls contain {} duplicates".format(len(event_urls) - len(set(event_urls))))
            if len(gkg_urls) != len(set(gkg_urls)): self.logger.warning("*\tGKG urls contain {} duplicates".format(len(gkg_urls) - len(set(gkg_urls))))

        self.logger.info("Checking event & gkg urls for duplicates...")
        check_dups(event_id_urls, gkg_id_urls)

        event_url_dict = {url: gid for gid, url in event_id_urls}
        gkg_url_dict = {url: gid for gid, url in gkg_id_urls}
        # create a list of [url, eventid, gkgid]
        url_ids = [[url, eid, gkg_url_dict.get(url, "")] for url, eid in event_url_dict.items()]
        return url_ids


    def _download_and_save_articles(self, url_ids, gdelt_ymd):
        # download & save article; collect list of metadata about each
        metadata_dict = defaultdict(list)
        for ui in url_ids:
            self._process_url_list(ui, metadata_dict, gdelt_ymd)
        self.webio.save_url_metadata(metadata_dict, self.MetadataFileKey)




    # -----------------------------------------------------------------------------------------------------------------
    #
    #   Helper Methods
    #
    # -----------------------------------------------------------------------------------------------------------------
    def _already_exists(self, filename, article_dir, gdelt_ymd):
        gutils = GdeltUtils()
        html_file = gutils.html_content_filepath(article_dir, gdelt_ymd, filename, True)
        content_file = gutils.html_content_filepath(article_dir, gdelt_ymd, filename)
        if os.path.exists(html_file) or os.path.exists(content_file):
            return True
        return False


    def _clean_metadata(selfself, s):
        # return s.replace("\n", " ").replace("\t", " ")
        return re.sub("[\n\r\t]", " ", s)

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

    def _process_url_list(self, url_ids, metadata_dict, gdelt_ymd):
        url, eid, gid = url_ids
        filename = eid if eid else  "g_{}".format(gid)

        if not self._already_exists(filename, self.article_dir, gdelt_ymd):
            Downloader(url, eid, gid, metadata_dict, filename, self.article_dir, self.metadata_dir, self.ymd).start()

            # self.logger.info("Downloading {}".format(url))
            # status, html = self.webio.get_web_page(url)
            # content = self.webio.get_content(html) if status == 200 else ""
            # title, author, site_name, description, keywords = self._parse_metadata(html)
            # self.webio.save_html_content(html, filename, True)
            # self.webio.save_html_content(content, filename, False)

        else:
            self.logger.info("{} already exists".format(filename))











    #
    #
    #

    #
    #
    #






LastupdateKey = "lastupdate"
MasterKey = "master"
LastupdateUrlKey = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

@click.command()
@click.option('--online/--local/', '-o/-l')
@click.option('--kind', '-k', type=click.Choice([LastupdateKey, MasterKey]))
@click.option('--url', '-u', default=LastupdateUrlKey)
@click.option('--file', '-f')
@click.option('--basedir', '-d')
def main(online, kind, url, file, basedir):
    # basedir must be specified
    if not basedir:
        raise Exception("Base directory was not specified")

    print("-"*80)
    print("Input Parameters:")
    print("\tonline: \t{}".format(online))
    print("\tkind:   \t{}".format(kind))
    print("\turl:    \t{}".format(url))
    print("\tfile:   \t{}".format(file))
    print("\tbasedir:\t{}".format(basedir))
    print("\tstart time:\t{}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("-" * 80)

    # lastupdate_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
    # gdelt_event_masterlist = "http://data.gdeltproject.org/events/index.html"
    # gdelt_gkg_masterlist = "http://data.gdeltproject.org/gkg/index.html"

    if online:
        g = Gdelter(url=url, file=None, basedir=basedir)
    else:
        g = Gdelter(url=None, file=file, basedir=basedir)

    if kind == LastupdateKey:
        g.process_last_update()
    if kind == MasterKey:
        g.process_master()

    print("\tend time:\t{}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


# def warn(*args, **kwargs):
#     pass
# import warnings
# warnings.warn = warn


if __name__ == '__main__':
    main()


'''
USAGE EXAMPLES:

    to get lastupdate from GDELT and parse:
python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -k lastupdate

    to parse locally saved lastupdate file
python3 Gdelter.py -l -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -f /Users/chagerman/Projects/2019_News/Data/GDELT/file_list/lastupdate.txt







'''
