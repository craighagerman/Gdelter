#!/home/chagerman/.pyenv/shims/python
import gzip
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

import gdeltParameters
from gdeltDownloader import Downloader
from gdeltDecorators import *
from gdeltutils import GdeltUtils
from gdeltIO import GIO


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
        self.status_dir = os.path.join(self.basedir, "articles_status")
        # self.metadata_dict_dir = os.path.join(self.basedir, "metadata_dict")
        # self.page_link_dir = os.path.join(self.basedir, "page_links")
        # self.domain_dir = os.path.join(self.basedir, "domains")

        self.ymd = datetime.now().strftime("%Y-%m-%d")
        self.gdelt_ymd = self.ymd

        # define output paths
        self.events_basedir = os.path.join(self.basedir, gdeltParameters.EventsKey, self.ymd)
        self.gkg_basedir = os.path.join(self.basedir, gdeltParameters.GkgKey, self.ymd)

        self.MetadataFileKey = "url_metadata.csv"

        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Gdelter')
        self.logger.setLevel(logging.INFO)

        # define column names from Gdelt event and gkg extract files
        self.EventIDColumnKey = "GLOBALEVENTID"
        self.EventUrlColumnKey = "SOURCEURL"
        self.GkgIDColumnKey = "GKGRECORDID"
        self.GkgUrlColumnKey = "DocumentIdentifier"
        self.GkgSourceColumnKey = "SourceCommonName"


    # -----------------------------------------------------------------------------------------------------------------
    #   Main Methods
    # -----------------------------------------------------------------------------------------------------------------
    @timeit
    def process_last_update(self):
        # urls: List[str]
        start_time = time.time()
        self.logger.info("Getting lastupdate urls ... ")
        urls = self._get_lastupdate()
        self.ymd, self.gdelt_ymd = self._get_gdelt_date(urls)
        self.logger.info("GDELT Year-Month-Date: {}".format(self.ymd))

        # # define output paths
        # events_basedir = os.path.join(self.basedir, gdeltParameters.EventsKey, self.ymd)
        # gkg_basedir = os.path.join(self.basedir, gdeltParameters.GkgKey, self.ymd)

        # create a dictionary of {gdelt_extract_type -> url}
        self.logger.info("Creating a dictionary of gdelt type -> lastupdate urls... ")
        lastupdate_dict = self._create_extract_dict(urls)

        # get the Events (export) and GKG urls from `lastupdate`
        self.logger.info("Decompressing and saving GDELT Event and GKG zip files... ")


        narticles = self.process_extracts(lastupdate_dict)

        # # TODO : think about making GdeltUtils into a staticmethod class
        # # download, decompress and save the events and gkg update file(s)
        # gutils = GdeltUtils()
        # events_url = lastupdate_dict['export']
        # events_files = gutils.decompress_zip_url(events_url, self.events_basedir)
        # gkg_url = lastupdate_dict[gdeltParameters.GkgKey]
        # gkg_files = gutils.decompress_zip_url(gkg_url, self.gkg_basedir)
        #
        # # parse Events & GKG files; create a master list of [url, event_id, gkg_id]
        # self.logger.info("Parsing event and gkg extract files ...")
        # event_id_urls = self._parse_gdelt_files(events_files, gdeltParameters.EventsKey)
        # gkg_id_urls = self._parse_gdelt_files(gkg_files, gdeltParameters.GkgKey)
        #
        # # compress/re-save events_file, gkg_file as gzip
        # GIO.resave_compressed_files(events_files)
        # GIO.resave_compressed_files(gkg_files)
        #
        #
        # # merge event and gkg urls from event_id_urls, gkg_id_urls lists
        # self.logger.info("creating a single list of article urls ...")
        # url_ids = self._create_event_gkg_url_dict(event_id_urls, gkg_id_urls)
        #
        #
        # # iterate through all urls, downloading articles & web page metadata, save results
        # self.logger.info("Downloading and saving articles & metadata from GDELT news articles ...")
        # self._download_and_save_articles(url_ids, self.ymd)

        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} news article urls in {} minutes".format(narticles, duration))
        print("\n")
        print("Output Locations:")
        print("\tarticles:\t{}".format(self.article_dir))
        print("\tstatus:\t{}".format(self.status_dir))
        print("\tEvents:\t{}".format(self.events_basedir))
        print("\tGKG:\t{}".format(self.gkg_basedir))
        print("\n")
        self.logger.info("-" * 50)



    @timeit
    def process_master(self):
        print("Not implemented yet")





    def process_extracts(self, extract_dict):
        # TODO : think about making GdeltUtils into a staticmethod class
        # download, decompress and save the events and gkg update file(s)
        gutils = GdeltUtils()
        events_url = extract_dict['export']
        events_files = gutils.decompress_zip_url(events_url, self.events_basedir)
        gkg_url = extract_dict[gdeltParameters.GkgKey]
        gkg_files = gutils.decompress_zip_url(gkg_url, self.gkg_basedir)

        # parse Events & GKG files; create a master list of [url, event_id, gkg_id]
        self.logger.info("Parsing event and gkg extract files ...")
        event_id_urls = self._parse_gdelt_files(events_files, gdeltParameters.EventsKey)
        gkg_id_urls = self._parse_gdelt_files(gkg_files, gdeltParameters.GkgKey)

        # compress/re-save events_file, gkg_file as gzip
        GIO.resave_compressed_files(events_files)
        GIO.resave_compressed_files(gkg_files)


        # merge event and gkg urls from event_id_urls, gkg_id_urls lists
        self.logger.info("creating a single list of article urls ...")
        url_ids = self._create_event_gkg_url_dict(event_id_urls, gkg_id_urls)


        # iterate through all urls, downloading articles & web page metadata, save results
        self.logger.info("Downloading and saving articles & metadata from GDELT news articles ...")
        self._download_and_save_articles(url_ids, self.ymd)
        return len(url_ids)






    # -----------------------------------------------------------------------------------------------------------------
    #   lastupdate Methods
    # -----------------------------------------------------------------------------------------------------------------
    def _get_lastupdate(self) -> List[str]:
        ''' Get the `lastupdate` from either a given url or a local file
                url => get web page, split into lines
                file => open file, read lines
            Parse last update and return a list of urls

            Example:
                retrieve latest update and return:
                    ['http://data.gdeltproject.org/gdeltv2/20190311214500.export.CSV.zip',
                    'http://data.gdeltproject.org/gdeltv2/20190311214500.mentions.CSV.zip',
                    'http://data.gdeltproject.org/gdeltv2/20190311214500.gkg.csv.zip']
        '''
        if self.url:
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
        elif self.file:
            try:
                urls = [x.strip().split()[-1] for x in open(self.file)]
            except FileNotFoundError as e:
                self.logger.error("File {} cannot be found: {}".format(self.file, e))
        return urls  # type: List[str]




    # -----------------------------------------------------------------------------------------------------------------
    #   Gdelt Event & GKG 15-minute extract Methods
    # -----------------------------------------------------------------------------------------------------------------
    def _create_extract_dict(self, urls):
        '''
        :param urls: List[str] of urls
        :return:      Dictionary[file_type, url]

        Example:
            return a dictionary containing:
                {'export': 'http://data.gdeltproject.org/gdeltv2/20190311214500.export.CSV.zip',
                'mentions': 'http://data.gdeltproject.org/gdeltv2/20190311214500.mentions.CSV.zip',
                'gkg': 'http://data.gdeltproject.org/gdeltv2/20190311214500.gkg.csv.zip'}
        '''
        return {u.split(".")[-3]: u.strip() for u in tqdm(urls)}



    # -----------------------------------------------------------------------------------------------------------------
    #   Gdelt Event & GKG File Methods
    # -----------------------------------------------------------------------------------------------------------------
    '''  list version of `_parse_gdelt_file()`  '''
    def _parse_gdelt_files(self, files, gdelt_type):
        id_url = []
        for file in files:
            id_url.extend(self._parse_gdelt_file(file, gdelt_type))
        return id_url


    def _parse_gdelt_file(self, file, gdelt_type):
        '''
        Read the local (event or gkg) update file (just un-zipped and saved locally)
        and parse to extract all article ids and urls

        :param file:   path to event or gkg file
        :param gdelt_type: One of gdeltParameters.EventsKey  or gdeltParameters.GkgKey
        :return:        list of (id, url)

        Example:
            Return a (long) list of (event_id, url) pairs for all of the events/urls in a given event (or gkg) update file

            [[829989575, 'http://theparisnews.com/free/article_dde86092-441c-11e9-98b3-dfbd58a5d0d4.html'],
            [829989576, 'https://www.vox.com/policy-and-politics/2019/3/11/18260152/trump-budget-opioid-epidemic-2019'],
            [829989577, 'http://theparisnews.com/free/article_dde86092-441c-11e9-98b3-dfbd58a5d0d4.html'],
            [829989578, 'https://www.milforddailynews.com/news/20190311/blackstone-valley-tech-to-pay-for-own-roof-repair-project'],
        '''
        self.logger.info("parsing gdelt type {} for extract file {}".format(gdelt_type, file))
        df = pd.read_csv(file, sep="\t", header=None)
        columns = gdeltParameters.event_columns if gdelt_type == gdeltParameters.EventsKey else gdeltParameters.gkg_columns
        df.columns = columns
        id_url_columns = gdeltParameters.event_id_url_columns if gdelt_type == gdeltParameters.EventsKey else gdeltParameters.gkg_id_url_columns
        id_url = df[id_url_columns].values.tolist()
        return id_url


    # -----------------------------------------------------------------------------------------------------------------
    #   Article Methods
    # -----------------------------------------------------------------------------------------------------------------
    def _create_event_gkg_url_dict(self, event_id_urls, gkg_id_urls):
        '''
        Merge event and gkg urls (i.e. from event_id_urls & gkg_id_urls), dropping duplicates

        :param event_id_urls:
        :param gkg_id_urls:
        :return:

        Example:
            Return a (long) list of (url, event_id, gkg_id)

            [['http://theparisnews.com/free/article_dde86092-441c-11e9-98b3-dfbd58a5d0d4.html',829990672, '20190311214500-2318'],
            ['https://www.vox.com/policy-and-politics/2019/3/11/18260152/trump-budget-opioid-epidemic-2019', 829990601, '20190311214500-1355'],
            ['https://www.milforddailynews.com/news/20190311/blackstone-valley-tech-to-pay-for-own-roof-repair-project', 829991140, '20190311214500-1473'],
        '''
        # check for duplicates
        # TODO : the following is an unneeded expense. Remove after getting enough stats on duplicates
        def log_dups(event_id_urls, gkg_id_urls):
            event_urls = [url for gid, url in event_id_urls]
            gkg_urls = [url for gid, url in gkg_id_urls]
            self.logger.info("Number of Event urls: {}".format(len(event_urls)))
            self.logger.info("Number of GKG  urls: {}".format(len(gkg_urls)))
            if len(event_urls) != len(set(event_urls)): self.logger.warning("*\tEvent urls contain {} duplicates".format(len(event_urls) - len(set(event_urls))))
            if len(gkg_urls) != len(set(gkg_urls)): self.logger.warning("*\tGKG urls contain {} duplicates".format(len(gkg_urls) - len(set(gkg_urls))))

        self.logger.info("Checking event & gkg urls for duplicates...")
        log_dups(event_id_urls, gkg_id_urls)

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
        self._save_url_metadata(metadata_dict, self.MetadataFileKey)




    # -----------------------------------------------------------------------------------------------------------------
    #   Helper Methods
    # -----------------------------------------------------------------------------------------------------------------
    def _get_gdelt_date(self, urls):
        gdelt_timestamp = os.path.basename(urls[0]).split(".")[0]
        return gdelt_timestamp[:8], gdelt_timestamp   # only return the year-month-date part


    def _already_exists(self, filename, article_dir, gdelt_ymd):
        gutils = GdeltUtils()
        html_file = gutils.html_content_filepath(article_dir, gdelt_ymd, filename, True)
        content_file = gutils.html_content_filepath(article_dir, gdelt_ymd, filename)
        if os.path.exists(html_file) or os.path.exists(content_file) or os.path.exists(html_file + ".gz") or os.path.exists(content_file + ".gz"):
            return True
        return False

    def _process_url_list(self, url_ids, metadata_dict, gdelt_ymd):
        url, eid, gid = url_ids
        filename = eid if eid else  "g_{}".format(gid)

        if not self._already_exists(filename, self.article_dir, gdelt_ymd):
            Downloader(url, eid, gid, metadata_dict, filename, self.article_dir, self.status_dir, self.ymd).start()
        else:
            self.logger.info("{} already exists".format(filename))


    def _save_url_metadata(self, metadata_dict, name):
        url_metadata = list(metadata_dict.values())
        file = os.path.join(self.status_dir, self.ymd, "{}_{}".format(self.gdelt_ymd, name))
        self.logger.info("saving latest results to {}".format(file))
        os.makedirs(os.path.dirname(file), exist_ok=True)
        df = pd.DataFrame(url_metadata, columns=gdeltParameters.metadata_columns)
        # save metadata as a gzip-ed CSV file to `metadata` directory
        df.to_csv(file+".gz", sep="\t", index=False, compression="gzip")






########################################################################################################################






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
