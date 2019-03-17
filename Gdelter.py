#!/home/chagerman/.pyenv/shims/python
import gzip
import logging
import os
import re
import warnings
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
from gdeltExtractDownloader import ExtractDownloader
from gdeltMasterlist import MasterList
from gutils import GUtils
from gdeltIO import GIO

import pprint


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

    def __init__(self, url=None, localfile=None, basedir="./", local = True):
        self.url = url
        self.localfile = localfile
        self.basedir = basedir
        self.local = local

        # define output directory paths
        self.article_dir = os.path.join(self.basedir, "articles")
        self.status_dir = os.path.join(self.basedir, "articles_stats")
        self.stats_filename = "stats"

        self.metadata_dict_dir = os.path.join(self.basedir, "html_metadata")
        self.page_link_dir = os.path.join(self.basedir, "page_links")
        self.domain_dir = os.path.join(self.basedir, "page_domains")

        self.ymd = datetime.now().strftime("%Y-%m-%d")
        self.gdelt_ymd = self.ymd

        # define output paths
        self.events_basedir = os.path.join(self.basedir, gdeltParameters.EventsKey)
        self.gkg_basedir = os.path.join(self.basedir, gdeltParameters.GkgKey)

        # define column names from Gdelt event and gkg extract files
        self.EventIDColumnKey = "GLOBALEVENTID"
        self.EventUrlColumnKey = "SOURCEURL"
        self.GkgIDColumnKey = "GKGRECORDID"
        self.GkgUrlColumnKey = "DocumentIdentifier"
        self.GkgSourceColumnKey = "SourceCommonName"

        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Gdelter')
        self.logger.setLevel(logging.INFO)

        # TODO : base directory for each dir should be basedir/Year/month/YMD/filename
        # TODO : i.e. need a Year/Month hierarchy

    # -----------------------------------------------------------------------------------------------------------------
    #   Main Methods
    # -----------------------------------------------------------------------------------------------------------------
    @timeit
    def process_last_update(self):
        # urls: List[str]
        start_time = time.time()
        self.logger.info("Getting lastupdate urls ... ")
        extract_urls = self._get_lastupdate()
        self.ymd, self.gdelt_ymd = self._get_gdelt_date(extract_urls)
        self.logger.info("GDELT Year-Month-Date: {}".format(self.ymd))

        # create a dictionary of {gdelt_extract_type -> url}
        self.logger.info("Creating a dictionary of gdelt type -> lastupdate urls... ")
        # lastupdate_dict = self._create_extract_dict(extract_urls)
        lastupdate_dict = self._create_extract_dict_lists(extract_urls)

        # get the Events (export) and GKG urls from `lastupdate`
        self.logger.info("Decompressing and saving GDELT Event and GKG zip files... ")

        # WIP --------------------------------------------------------------------------------------------------------------
        # narticles_processed = self.process_extracts(lastupdate_dict)
        # events_files, gkg_files = self.downloadAndResaveZipFiles(lastupdate_dict)
        # events_files, gkg_files = self.process_zip_files(lastupdate_dict)

        # TODO : the following lines are duplicated. Put into a single function
        # TODO : make one function that first checks for local file and then downloads from web if no exists
        ed = ExtractDownloader()
        events_files = ed.starter(lastupdate_dict['export'],
                                  GIO.define_output_directory(self.events_basedir, self.ymd))
        gkg_files = ed.starter(lastupdate_dict[gdeltParameters.GkgKey],
                               GIO.define_output_directory(self.events_basedir, self.ymd))

        self.logger.info("finished downloading {} event and {} gkg extract urls".format(len(events_files), len(gkg_files)) )

        narticles_processed = self.process_extract_files(events_files, gkg_files)

        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} news article urls in {} minutes".format(narticles_processed, duration))
        self.logger.info("-" * 50)



    @timeit
    def process_extracts_from_masterlist(self, local, date):
        self._validate_date(date)
        start_time = time.time()
        self.ymd = date

        masterlist_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
        gm = MasterList(masterlist_url, self.localfile, self.basedir)

        # get links from masterlist file - either downloadedfrom GDELT web page or local file
        date2extract_dict = gm.load_masterlist(local)
        extract_urls = gm._get_extracts_for_date(date2extract_dict, date)
        extract_dict = self._create_extract_dict_lists(extract_urls)
        # TODO : make extract downloader subclass Thread and run half a dozen downloads at the same time
        # events_files, gkg_files = self.downloadAndResaveZipFiles(extract_dict)
        # events_files, gkg_files = self.process_zip_files(extract_dict)

        ed = ExtractDownloader()
        events_files = ed.starter(extract_dict['export'],
                                  GIO.define_output_directory(self.events_basedir, self.ymd))
        gkg_files = ed.starter(extract_dict[gdeltParameters.GkgKey],
                               GIO.define_output_directory(self.events_basedir, self.ymd))


        print("events_files: \n{}".format("\n".join(events_files[:10])))
        print("gkg_files: \n{}".format("\n".join(gkg_files[:10])))

        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} event and {} gkg urls in {} minutes".format(len(events_files), len(gkg_files),
                                                                                    duration))


    @timeit
    def process_backfill_date(self, local, date):
        ''' For a given date, parse (local) event/gkg extract files for urls, download articles  '''
        print("\nprocess_backfill_date()...\n")
        self._validate_date(date)
        start_time = time.time()

        events_files, gkg_files = self.get_local_event_gkg_extracts(date)

        self.logger.info("Retrieved {} event and {} gkg extract urls".format(len(events_files), len(gkg_files)))

        print( "\n".join(events_files[:20]))
        print("\n\n")
        print("\n".join(gkg_files[:20]))

        narticles_processed = self.process_extract_files(events_files, gkg_files)

        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} news article urls in {} minutes".format(narticles_processed, duration))
        self.logger.info("-" * 50)


    ###################################################################################################################













    def downloadAndResaveZipFiles(self, extract_dict):
        self.logger.info("Downloading zipped extract files and re-saving as gzipped files...")
        # download, decompress and save the events and gkg update file(s)
        gutils = GUtils()
        events_urls = extract_dict['export']
        event_files = gutils.decompress_zip_urls(events_urls, self.events_basedir, self.ymd)
        gkg_urls = extract_dict[gdeltParameters.GkgKey]
        gkg_files = gutils.decompress_zip_urls(gkg_urls, self.gkg_basedir, self.ymd)
        # compress/re-save events_file, gkg_file as gzip


        return event_files, gkg_files




    # def process_zip_files(self, extract_dict):
    #     events_urls = extract_dict['export']
    #     gkg_urls = extract_dict[gdeltParameters.GkgKey]
    #     print("events_urls:  {}".format(len(events_urls)))
    #     print("gkg_urls:  {}".format(len(gkg_urls)))
    #
    #     self.logger.info("Downloading {} Event extract files and resaving as gzipped files".format(len(events_urls)))
    #     events_files = self._download_and_resave_zip_files(events_urls, self.events_basedir)
    #     self.logger.info("Downloading {} GKG extract files and resaving as gzipped files".format(len(gkg_urls)))
    #     gkg_files = self._download_and_resave_zip_files(gkg_urls, self.gkg_basedir)
    #     return events_files, gkg_files











    def process_extracts(self, extract_dict):
        events_files, gkg_files = self.downloadAndResaveZipFiles(extract_dict)

        # parse Events & GKG files; create a master list of [url, event_id, gkg_id]
        self.logger.info("Parsing event and gkg extract files ...")
        event_id_urls = self._parse_gdelt_files(events_files, gdeltParameters.EventsKey)
        gkg_id_urls = self._parse_gdelt_files(gkg_files, gdeltParameters.GkgKey)

        # merge event and gkg urls from event_id_urls, gkg_id_urls lists
        self.logger.info("creating a single list of article urls ...")
        url_ids = self._create_event_gkg_url_dict(event_id_urls, gkg_id_urls)

        # iterate through all urls, downloading articles & web page metadata, save results
        self.logger.info("Downloading and saving articles & metadata from GDELT news articles ...")
        self._download_and_save_articles(url_ids, self.ymd)
        return len(url_ids)


    def process_extract_files(self, events_files, gkg_files):
        self.logger.info("processing event & gkg extract files...")
        # parse Events & GKG files; create a master list of [url, event_id, gkg_id]
        self.logger.info("Parsing event and gkg extract files ...")
        event_id_urls = self._parse_gdelt_files(events_files, gdeltParameters.EventsKey)
        gkg_id_urls = self._parse_gdelt_files(gkg_files, gdeltParameters.GkgKey)

        # merge event and gkg urls from event_id_urls, gkg_id_urls lists
        self.logger.info("creating a single list of article urls ...")
        url_ids = self._create_event_gkg_url_dict(event_id_urls, gkg_id_urls)

        print("url_ids: {}".format(len(url_ids)))

        # iterate through all urls, downloading articles & web page metadata, save results
        self.logger.info("Downloading and saving articles & metadata from GDELT news articles ...")
        self._download_and_save_articles(url_ids, self.ymd)
        return len(url_ids)






    #
    # def _download_and_resave_zip_files(self, urls, base_dir):
    #     extract_url_dict = defaultdict(list)
    #     # example:   d['event']['20190123001500']
    #     # extract_url_dict = defaultdict(lambda: defaultdict(list))
    #     for url in urls:
    #         gdelt_date = os.path.basename(url).split(".")[0]
    #         ymd = gdelt_date[:8]
    #         dest_dir = GIO.define_output_directory(base_dir, ymd)
    #         ExtractDownloader(url, extract_url_dict, gdelt_date, dest_dir).start()
    #
    #     print("pprint.pprint(extract_url_dict):")
    #     pprint.pprint(extract_url_dict)
    #     return GUtils.flatten(extract_url_dict.values())



    def get_local_event_gkg_extracts(self, date):
        events_ymd = os.path.join(self.events_basedir, date)
        gkg_ymd = os.path.join(self.gkg_basedir, date)
        if os.path.exists(events_ymd) and os.path.exists((gkg_ymd)):
            event_files = [os.path.join(events_ymd, x) for x in os.listdir(events_ymd)]
            gkg_files = [os.path.join(gkg_ymd, x) for x in os.listdir(gkg_ymd)]
            return event_files, gkg_files
        return [], []


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
        elif self.localfile:
            try:
                urls = [x.strip().split()[-1] for x in open(self.localfile)]
            except FileNotFoundError as e:
                self.logger.error("File {} cannot be found: {}".format(self.localfile, e))
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


    def _create_extract_dict_lists(self, urls):
        '''
        :param urls: List[str] of urls
        :return:      Dictionary[file_type, url]
            # TODO : deleteme:
            'http://data.gdeltproject.org/gdeltv2/http://data.gdeltproject.org/gdeltv220190301000000'
        Example:
            return a dictionary containing:
                {'export': 'http://data.gdeltproject.org/gdeltv2/20190311214500.export.CSV.zip',
                'mentions': 'http://data.gdeltproject.org/gdeltv2/20190311214500.mentions.CSV.zip',
                'gkg': 'http://data.gdeltproject.org/gdeltv2/20190311214500.gkg.csv.zip'}
        '''
        dd = defaultdict(list)
        for url in tqdm(urls):
            # TODO : deleteme:
            # get the basename starting with gdelt date
            # basename = re.sub("gdeltv2", "", os.path.basename(url), re.IGNORECASE)

            dd[url.split(".")[-3]].append(url)
        return dd


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
        print()
        print("_parse_gdelt_file   type(file):\t{}".format(type(file)))
        print("_parse_gdelt_file   file:\t{}".format(file))
        print()
        df = pd.read_csv(file, sep="\t", header=None, encoding='latin-1')
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
            self._download_article_url(ui, metadata_dict, gdelt_ymd)
        self._save_article_stats(metadata_dict, self.stats_filename)


    # -----------------------------------------------------------------------------------------------------------------
    #   Helper Methods
    # -----------------------------------------------------------------------------------------------------------------
    def _get_gdelt_date(self, urls):
        gdelt_timestamp = os.path.basename(urls[0]).split(".")[0]
        return gdelt_timestamp[:8], gdelt_timestamp   # only return the year-month-date part


    def _already_exists(self, filename, article_dir, gdelt_ymd):
        gutils = GUtils()
        html_file = gutils.html_content_filepath(article_dir, gdelt_ymd, filename, True)
        content_file = gutils.html_content_filepath(article_dir, gdelt_ymd, filename)
        if os.path.exists(html_file) or os.path.exists(content_file) or os.path.exists(html_file + ".gz") or os.path.exists(content_file + ".gz"):
            return True
        return False


    def _download_article_url(self, url_ids, metadata_dict, gdelt_ymd):
        url, eid, gid = url_ids
        filename = eid if eid else  "g_{}".format(gid)

        if not self._already_exists(filename, self.article_dir, gdelt_ymd):
            Downloader(url, eid, gid, metadata_dict, filename,
                       self.article_dir,
                       self.status_dir,
                       self.metadata_dict_dir,
                       self.page_link_dir,
                       self.domain_dir,
                       self.ymd).start()
        else:
            self.logger.info("{} already exists".format(filename))


    def _save_article_stats(self, metadata_dict, name):
        url_metadata = list(metadata_dict.values())
        file = os.path.join(self.status_dir, self.ymd, "{}_{}.csv".format(self.gdelt_ymd, name))
        self.logger.info("saving latest results to {}".format(file))
        os.makedirs(os.path.dirname(file), exist_ok=True)
        df = pd.DataFrame(url_metadata, columns=gdeltParameters.metadata_columns)
        # n.b. to avoid UnicodeEncodeError errors encode a problematic series first and then decode back to utf-8
        df['title'] = df['title'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))
        # save metadata as a gzip-ed CSV file to `metadata` directory
        df.to_csv("{}.gz".format(file), sep="\t", index=False, compression="gzip", encoding='utf-8')


    def _validate_date(self, date_text):
        # TODO : validate that date is a valid range -> not in the future or before Gdelt began
        try:
            datetime.strptime(date_text, '%Y%m%d')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYYMMDD")



########################################################################################################################





# parse lastupdate (local or online); download and save all articles
LastupdateKey = "lastupdate"
# download or load masterlist file, parse for given YMD date, download and save event & gkg files
MasterKey = "master"
# parse (local) event & gkg extract files (downloaded with `MasterKey`); download and save all articles
dateExtractKey = "dateextract"



LastupdateUrlKey = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

@click.command()
@click.option('--local/--online/', '-l/-o')
@click.option('--kind', '-k', type=click.Choice([LastupdateKey, MasterKey]))
@click.option('--url', '-u', default=LastupdateUrlKey)
@click.option('--file', '-f')
@click.option('--basedir', '-d')
@click.option('--date', '-y')
def main(local, kind, url, file, basedir, date):
    # basedir must be specified
    if not basedir:
        raise Exception("Base directory was not specified")

    print("-"*80)
    print("Input Parameters:")
    print("\tlocal:  \t{}".format(local))
    print("\tkind:   \t{}".format(kind))
    print("\turl:    \t{}".format(url))
    print("\tfile:   \t{}".format(file))
    print("\tbasedir:\t{}".format(basedir))
    print("\tdate:   \t{}".format(date))
    print("\tstart time:\t{}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    print("-" * 80)

    # from sklearn.exceptions import UserWarning
    warnings.filterwarnings(action='ignore', category=UserWarning)
    warnings.filterwarnings(action='ignore', category=FutureWarning)

    # if online:
    #     g = Gdelter(url=url, localfile=None, basedir=basedir)
    # else:
    #     g = Gdelter(url=None, localfile=file, basedir=basedir)
    #



    g = Gdelter(url=url, localfile=file, basedir=basedir, local=local)

    if kind == LastupdateKey:
        g.process_last_update()
    if kind == MasterKey:
        g.process_extracts_from_masterlist(local, date)
        # g.process_backfill_date(local, date)




    print("\tend time:\t{}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


# def warn(*args, **kwargs):
#     pass
# import warnings
# warnings.warn = warn


if __name__ == '__main__':
    main()


'''
            USAGE EXAMPLES:

# ----------------------------------------------------------------------------------------------------------------------
#   last update
# ----------------------------------------------------------------------------------------------------------------------

    >>> web url
python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -k lastupdate

    >>> locally saved lastupdate file
python3 Gdelter.py  -l -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -f /Users/chagerman/Projects/2019_News/Data/GDELT/file_list/lastupdate.txt -k lastupdate




# ----------------------------------------------------------------------------------------------------------------------
#   masterlist
# ----------------------------------------------------------------------------------------------------------------------

python3 Gdelter.py  -l -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -f /Users/chagerman/Projects/2019_News/Data/GDELT/file_list/masterfilelist.txt -y 20190312 -k master






'''
