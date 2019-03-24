#!/home/chagerman/.pyenv/shims/python
import logging
import os
import pprint
import warnings
from collections import defaultdict
from datetime import datetime
from typing import List

import click
import pandas as pd
import requests
from tqdm import tqdm

import gdeltParameters
from gdeltDecorators import *
from gdeltDownloader import ArticleDownloader
from gdeltExtractDownloader import ExtractDownloader
from gdeltIO import GIO
from gdeltMasterlist import MasterList

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

    def __init__(self, url=None, basedir="./", local = True):
        self.url = url
        self.basedir = basedir
        self.masterlist_file = os.path.join(self.basedir, "masterlist", "masterfilelist.txt")
        self.local = local

        # define output directory paths
        self.article_dir = os.path.join(self.basedir, "articles")
        self.status_dir = os.path.join(self.basedir, "articles_stats")
        self.stats_filename = "stats"

        self.metadata_dict_dir = os.path.join(self.basedir, "html_metadata")
        self.page_link_dir = os.path.join(self.basedir, "page_links")
        self.domain_dir = os.path.join(self.basedir, "page_domains")

        self.ymd = datetime.now().strftime("%Y%m%d")
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

        self.masterlist_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Gdelter')
        self.logger.setLevel(logging.INFO)

        # TODO : base directory for each dir should be basedir/Year/month/YMD/filename
        # TODO : i.e. need a Year/Month hierarchy


    # -----------------------------------------------------------------------------------------------------------------
    #   Main Methods
    # -----------------------------------------------------------------------------------------------------------------
    # @good
    @timeit
    def process_last_update(self):
        '''
        A main method: Get lastupdate extrats & articles
            - download `lastupdate` from GDELT
            - get events & gkg extract urls from above
            - download article urls from above, save to local article_dir
        '''
        self.logger.info("running process_last_update() ...")
        start_time = time.time()
        self.logger.info("Getting lastupdate urls ... ")
        extract_urls : List[str]= self._get_lastupdate()

        self.ymd, self.gdelt_ymd = self._get_gdelt_date(extract_urls)
        self.logger.info("GDELT Year-Month-Date: {}".format(self.ymd))

        # create a dictionary of {gdelt_extract_type -> url}
        self.logger.info("Creating a dictionary of gdelt type -> lastupdate urls... ")
        lastupdate_dict = self._create_extract_dict_lists(extract_urls)

        # get the Events (export) and GKG urls from `lastupdate`
        self.logger.info("Decompressing and saving GDELT Event and GKG zip files... ")

        # load extract (event, gkg) files from lastupdate url (or local fallback)
        events_files, gkg_files = self.load_extract_files(lastupdate_dict)
        self.logger.info("finished downloading {} event and {} gkg extract urls".format(len(events_files), len(gkg_files)) )

        # download each article url found in extracts above
        narticles_processed = self.process_extract_files(events_files, gkg_files)

        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} news article urls in {} minutes".format(narticles_processed, duration))
        self.logger.info("-" * 50)



    @timeit
    def process_gdelt_extract_files(self, local, date):
        self.logger.info("running process_gdelt_extract_files() ...")
        self._validate_date(date)
        start_time = time.time()
        self.ymd = date

        # masterlist_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"

        gm = MasterList(self.masterlist_url, self.masterlist_file)

        # get links from masterlist file - either downloaded from GDELT web page or local file

        # date2extract_dict = gm.load_masterlist(local)
        # extract_urls = gm.get_extracts_for_date(date2extract_dict, date)



        # extract_dict = self._create_extract_dict_lists(extract_urls)
        # events_files, gkg_files = self.load_extract_files(extract_dict)

        events_files, gkg_files = self.get_extracts(date)
        print("number of events_files: {}   number of gkg_files : {}".format(len(events_files), len(gkg_files) ))
        # exit(0)


        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} event and {} gkg urls in {} minutes".format(len(events_files), len(gkg_files),
                                                                                    duration))


    # @timeit
    def backfill_articles(self, local, date):
        self.logger.info("running backfill_articlesx() ...")
        ''' For a given date, parse (local) event/gkg extract files for urls, download articles  '''
        print("\nprocess_backfill_date()... {}\n".format(date))
        self._validate_date(date)
        self.ymd = date
        start_time = time.time()

        # events_files, gkg_files = self.get_local_event_gkg_extracts(date)
        events_files, gkg_files = self.get_extracts(date)

        self.logger.info("Retrieved {} event and {} gkg extract urls".format(len(events_files), len(gkg_files)))

        print("events_files")
        print("\n\t".join(events_files[:10]))
        print("gkg_files")
        print("\n\t".join(gkg_files[:10]))
        # exit(0)
        narticles_processed = self.process_extract_files(events_files, gkg_files)

        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} news article urls in {} minutes".format(narticles_processed, duration))
        self.logger.info("-" * 50)






    #
    #
    #
    #
    #
    #
    #
    #

    ###################################################################################################################
    #   Helper Methods
    ###################################################################################################################

    #
    #
    #
    #
    #
    #
    #
    #



    def process_extract_files(self, events_files, gkg_files):
        url_ids = self.get_article_urls_from_extracts(events_files, gkg_files)
        # iterate through all urls, downloading articles & web page metadata, save results
        self.logger.info("Downloading and saving articles & metadata from GDELT news articles ...")
        self._download_and_save_articles(url_ids, self.ymd)
        return len(url_ids)



    def _download_and_save_articles(self, url_ids, ymd):
        # download & save article; collect list of metadata about each
        ad = ArticleDownloader(self.basedir, self.status_dir, ymd)
        num_downloads, num_urls, total_bytes = ad.download_articles(url_ids)
        self.logger.info("Successfully downloaded {} of {} urls. Total bytes: {}".format(num_downloads, num_urls, total_bytes))



    def get_article_urls_from_extracts(self, events_files, gkg_files):
        self.logger.info("processing event & gkg extract files...")
        # parse Events & GKG files; create a master list of [url, event_id, gkg_id]
        self.logger.info("Parsing event and gkg extract files ...")
        event_id_urls = self._parse_gdelt_files(events_files, gdeltParameters.EventsKey)
        gkg_id_urls = self._parse_gdelt_files(gkg_files, gdeltParameters.GkgKey)

        # merge event and gkg urls from event_id_urls, gkg_id_urls lists
        self.logger.info("creating a single list of article urls ...")
        url_ids = self._create_event_gkg_url_dict(event_id_urls, gkg_id_urls)
        return url_ids


    # WIP ------------------------------------------------------------------------------------------
    def get_local_event_gkg_extracts(self, date):
        print("...get_local_event_gkg_extracts...")
        events_dir = GIO.define_output_directory(self.basedir, gdeltParameters.EventsKey, date)
        gkg_dir = GIO.define_output_directory(self.basedir, gdeltParameters.GkgKey, date)

        print("events_dir : {}".format(events_dir))
        print("gkg_dir :    {}".format(gkg_dir))

        # events_ymd = os.path.join(self.events_basedir, date)
        # gkg_ymd = os.path.join(self.gkg_basedir, date)
        if os.path.exists(events_dir) and os.path.exists((gkg_dir)):
            event_files = [os.path.join(events_dir, x) for x in os.listdir(events_dir)]
            gkg_files = [os.path.join(gkg_dir, x) for x in os.listdir(gkg_dir)]
            return event_files, gkg_files
        return [], []


    def load_extract_files(self, extract_dict):
        '''
        Return two lists (paths to local files) for event extracts and gkg extracts
        :param extract_dict: dictionary mapping {extract_type -> list_of_local_paths
        :return:
        '''
        # TODO : check if extract file exists; if so, load local and continue
        ed = ExtractDownloader()
        events_files = ed.download_extracts(extract_dict['export'],
                                            GIO.define_output_directory(self.basedir, gdeltParameters.EventsKey,
                                                                        self.ymd))
        gkg_files = ed.download_extracts(extract_dict['gkg'],
                                         GIO.define_output_directory(self.basedir, gdeltParameters.GkgKey, self.ymd))
        return events_files, gkg_files


    def get_extracts(self, date):
        # check if extracts are local
        events_dir = GIO.define_output_directory(self.basedir, gdeltParameters.EventsKey, date)
        gkg_dir = GIO.define_output_directory(self.basedir, gdeltParameters.GkgKey, date)
        if os.path.exists(events_dir) and os.path.exists((gkg_dir)) and len(os.listdir(events_dir)) > 90 and len(os.listdir(gkg_dir)) > 90 :
            self.logger.info("extract files exist locally")
            print("events_dir : {}".format(events_dir))
            print("gkg_dir    : {}".format(gkg_dir))
            event_files = [os.path.join(events_dir, x) for x in os.listdir(events_dir)]
            gkg_files = [os.path.join(gkg_dir, x) for x in os.listdir(gkg_dir)]
        else:
            # else download extracts from Gdelt
            self.logger.info("extract files will be downloaded from GDELT")
            gm = MasterList(self.masterlist_url, self.masterlist_file)
            # get links from masterlist file - either downloaded from GDELT web page or local file
            date2extract_dict = gm.load_masterlist(self.local)
            extract_urls = gm.get_extracts_for_date(date2extract_dict, date)
            extract_dict = self._create_extract_dict_lists(extract_urls)
            event_files, gkg_files = self.load_extract_files(extract_dict)
        return event_files, gkg_files





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
        elif self.masterlist_file:
            try:
                urls = [x.strip().split()[-1] for x in open(self.masterlist_file)]
            except FileNotFoundError as e:
                self.logger.error("File {} cannot be found: {}".format(self.masterlist_file, e))
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

        Example:
            return a dictionary containing:
                {'export': 'http://data.gdeltproject.org/gdeltv2/20190311214500.export.CSV.zip',
                'mentions': 'http://data.gdeltproject.org/gdeltv2/20190311214500.mentions.CSV.zip',
                'gkg': 'http://data.gdeltproject.org/gdeltv2/20190311214500.gkg.csv.zip'}
        '''
        dd = defaultdict(list)
        for url in tqdm(urls):
            basename = os.path.basename(url)
            extract_kind = basename.split(".")[1]
            dd[extract_kind].append(url)
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


    # -----------------------------------------------------------------------------------------------------------------
    #   Helper Methods
    # -----------------------------------------------------------------------------------------------------------------
    def _get_gdelt_date(self, urls):
        gdelt_timestamp = os.path.basename(urls[0]).split(".")[0]
        return gdelt_timestamp[:8], gdelt_timestamp   # only return the year-month-date part


    # def _already_exists(self, filename, article_dir, gdelt_ymd):
    #     gutils = GUtils()
    #     html_file = GIO.html_content_filepath(article_dir, gdelt_ymd, filename, True)
    #     content_file = GIO.html_content_filepath(article_dir, gdelt_ymd, filename)
    #     if os.path.exists(html_file) or os.path.exists(content_file) or os.path.exists(html_file + ".gz") or os.path.exists(content_file + ".gz"):
    #         return True
    #     return False


    # def _download_article_url(self, url_ids, metadata_dict, gdelt_ymd):
    #     url, eid, gid = url_ids
    #     filename = eid if eid else  "g_{}".format(gid)
    #
    #     if not self._already_exists(filename, self.article_dir, gdelt_ymd):
    #         Downloader(url, eid, gid, metadata_dict, filename,
    #                    self.article_dir,
    #                    self.status_dir,
    #                    self.metadata_dict_dir,
    #                    self.page_link_dir,
    #                    self.domain_dir,
    #                    self.ymd).start()
    #     else:
    #         self.logger.info("{} already exists".format(filename))


    # def _save_article_stats(self, metadata_dict, name):
    #     url_metadata = list(metadata_dict.values())
    #     file = os.path.join(self.status_dir, self.ymd, "{}_{}.csv".format(self.gdelt_ymd, name))
    #     self.logger.info("saving latest results to {}".format(file))
    #     os.makedirs(os.path.dirname(file), exist_ok=True)
    #     df = pd.DataFrame(url_metadata, columns=gdeltParameters.metadata_columns)
    #     # n.b. to avoid UnicodeEncodeError errors encode a problematic series first and then decode back to utf-8
    #     df['title'] = df['title'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))
    #     # save metadata as a gzip-ed CSV file to `metadata` directory
    #     df.to_csv("{}.gz".format(file), sep="\t", index=False, compression="gzip", encoding='utf-8')
    #

    def _validate_date(self, date_text):
        # TODO : validate that date is a valid range -> not in the future or before Gdelt began
        try:
            datetime.strptime(date_text, '%Y%m%d')
        except ValueError:
            raise ValueError("Incorrect data format, should be YYYYMMDD")



########################################################################################################################
'''
Four run options

    1. process lastupdate
        - download last update file from Gdelt
        - get list of event and gkg files (from above)
        - download extract files (above) from Gdelt
        - parse extract files (above) for (1) metadata and (2) article urls
        - download all articles from urls (above)
    2. get all extract files for a given date
    3. get (backfill) all articles for a given date (assumed extract files above are already downloaded)


-------------
File Types
-------------
    GDELT
        masterlist
        lastupdate
        event extract
        gkg extract
    Other
        News article

-------------
Functionality
-------------
  Extract Files
    1. download lastupdate (i.e. 1 event, 1 gkg file)
    2. download all extracts for a given date (i.e. multiple event & gkg files)
    3. download all extracts for a date range
    4. download master list (not really necessary)
  Articles
    5. download all articles for lastupdate (i.e. all articles in 1 event, 1 gkg file)
    6. download all articles for a given date (assumes extracts have been downloaded by #2 above)
    7. download all articles for a given date range (assumes #3 above)
    
'''

# parse lastupdate (local or online); download and save all articles
LastupdateKey = "lastupdate"
# download or load masterlist file, parse for given YMD date, download and save event & gkg files
MasterKey = "master"
# parse (local) event & gkg extract files (downloaded with `MasterKey`); download and save all articles
BackfillKey = "backfill"
ExtractKey = "extract"

LastupdateUrlKey = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

@click.command()
@click.option('--local/--online/', '-l/-o')
@click.option('--kind', '-k', type=click.Choice([LastupdateKey, MasterKey, BackfillKey, ExtractKey]))
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

    g = Gdelter(url=url, basedir=basedir, local=local)

    if kind == LastupdateKey:
        g.process_last_update()
    if kind == MasterKey:
        g.process_gdelt_extract_files(local, date)
    if kind == BackfillKey:
        g.backfill_articles(local, date)
    if kind==ExtractKey:
        g.process_gdelt_extract_files(local, date)


    print("\tend time:\t{}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))


if __name__ == '__main__':
    main()


'''
            USAGE EXAMPLES:

# ----------------------------------------------------------------------------------------------------------------------
#   last update
# ----------------------------------------------------------------------------------------------------------------------

    >>> web url
python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -k lastupdate

python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Gdelt_Data -k lastupdate



    >>> locally saved lastupdate file
python3 Gdelter.py  -l -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -f /Users/chagerman/Projects/2019_News/Data/GDELT/file_list/lastupdate.txt -k lastupdate



# ----------------------------------------------------------------------------------------------------------------------
#   masterlist
# ----------------------------------------------------------------------------------------------------------------------

python3 Gdelter.py  -l -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -f /Users/chagerman/Projects/2019_News/Data/GDELT/file_list/masterfilelist.txt -y 20190312 -k master
python3 Gdelter.py  -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -y 20190228 -k master



# ----------------------------------------------------------------------------------------------------------------------
#   backfill by date
# ----------------------------------------------------------------------------------------------------------------------

python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -k backfill -y 20190228
python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -k backfill -y 20190208



# ----------------------------------------------------------------------------------------------------------------------
#   Download extracts by date
# ----------------------------------------------------------------------------------------------------------------------

python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -k extract -y 20190228
python3 Gdelter.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out -k extract -y 20190208






'''
