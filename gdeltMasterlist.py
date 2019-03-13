import gzip
import logging
import os
import re

import click
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from typing import List

from collections import defaultdict

import gdeltParameters
import gdeltutils
from gdeltutils import GdeltUtils

'''

GDELT 2.0 Events, Mentions & GKG

masterlist_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
masterlist_dir = "/Users/chagerman/Projects/2019_News/Data/GDELT/file_list"
masterlist_filename = "masterfilelist.txt"

gm = MasterList(masterlist_dir, masterlist_filename, masterlist_url )

'''

class MasterList:

    def __init__(self, masterlist_dir, masterlist_filename, masterlist_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"):
        self.masterlist_dir = masterlist_dir
        self.masterlist_file = os.path.join(self.masterlist_dir, masterlist_filename)
        self.masterlist_url = masterlist_url

        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Masterlist')



    ################################################################################
    # get & save master list
    ################################################################################
    def run(self, ymd, local = False):
        print("local: {}".format(local))

        # define output paths
        # TODO : basedir is from Gdelter - should be defined in one place
        events_basedir = os.path.join(self.basedir, gdeltParameters.EventsKey, ymd)
        gkg_basedir = os.path.join(self.basedir, gdeltParameters.GkgKey, ymd)


        # download masterlist from GDELT web page
        masterlist_links = self.load_masterlist(local)


        # n.b. master list contains everything - have to filter for what we want to process
        # filter by date and export / gkg files
        masterlist = self.filter_masterlist(masterlist_links, ymd)
        masterlist_dict = self.create_extract_dict(masterlist)


        # TODO : refactor - same as Gdelter!
        gutils = GdeltUtils()
        events_url = masterlist_dict['export']
        events_files = gutils.decompress_zip_url(events_url, events_basedir)
        gkg_url = masterlist_dict[gdeltParameters.GkgKey]
        gkg_files = gutils.decompress_zip_url(gkg_url, gkg_basedir)

        # TODO : refactor - same as Gdelter!
        # parse Events & GKG files; create a master list of [url, event_id, gkg_id]
        self.logger.info("Parsing event and gkg extract files ...")
        event_id_urls = self._parse_gdelt_files(events_files, gdeltParameters.EventsKey)
        gkg_id_urls = self._parse_gdelt_files(gkg_files, gdeltParameters.GkgKey)

        # TODO : refactor - same as Gdelter!
        # compress/re-save events_file, gkg_file as gzip
        self._resave_compressed_files(events_files)
        self._resave_compressed_files(gkg_files)

        # TODO : refactor - same as Gdelter!
        # merge event and gkg urls from event_id_urls, gkg_id_urls lists
        self.logger.info("creating a single list of article urls ...")
        url_ids = self._create_event_gkg_url_dict(event_id_urls, gkg_id_urls)

        # TODO : refactor - same as Gdelter!
        # iterate through all urls, downloading articles & web page metadata, save results
        self.logger.info("Downloading and saving articles & metadata from GDELT news articles ...")
        self._download_and_save_articles(url_ids, self.ymd)

        # TODO : refactor - same as Gdelter!
        # finish
        end_time = time.time()
        duration = int((end_time - start_time) / 60)
        self.logger.info("-" * 50)
        self.logger.info("finished processing {} urls in {} minutes".format(len(url_ids), duration))
        print("\n")
        print("self.article_dir :	 {}".format(self.article_dir))
        print("self.status_dir :	 {}".format(self.status_dir))
        print("self.metadata_dict_dir :	 {}".format(self.metadata_dict_dir))
        print("self.page_link_dir :	 {}".format(self.page_link_dir))
        print("self.domain_dir :	 {}".format(self.domain_dir))
        print("\n")
        self.logger.info("-" * 50)










    # WIP ------------------------------------------------------------------------------------------    start    -----
    '''
        QUESTION: are there two masterlists? (event & gkg) or just one?
        FUNCTIONALITY:
            get masterlist (from local file or url)
            get (event, gkg) urls for a given date
            get (event, gkg) urls for a given date range
            create a queue of extract zip files to process (and download articles in each)
            get all urls in masterlist file (be polite - use sleep between urls)
            parse, process files (above) and download articles (using Gdelter.ph and/or gdeltDownloader.py)
            for a given date, check if exists in local file, if not there get from url
        TODO :
            find a way to measure resource use
                network traffic
                average running time to download # articles from an extract file
                average size per article of saved gzipped data for each article
                extrapolate above per hour, per day, per week, per month etc.
                
    '''

    #
    # def get_date_from_masterlist(self, local=false):
    #
    #     event_r, gkg_r = self._get_masterlists(event_masterlist_url, gkg_masterlist_url)





    ''' load (event, gkg) masterlist from url or local file. Return dict of {date -> [events, mentions, gkgs]} '''
    def load_masterlist(self, local=False):
        masterlist_source = self.masterlist_file if local else self.masterlist_url
        masterlist_links = defaultdict(list)
        if local:
            masterlist = self._get_masterlist_from_file(masterlist_source)
        else:
            masterlist = self._get_masterlist_from_url(masterlist_source)
            # save masterlist each time we download it
            self._save_masterlist_urls(masterlist, self.masterlist_file)

        urls = (os.path.basename(url) for url in masterlist)

        # create a dictionary of {YMD -> [all_event/mention/gkg_urls]}
        for url in urls:
            masterlist_links[GdeltUtils.ymd_from_gdelt_date(url.split(".")[0])].append(url)

        return masterlist_links




    def filter_masterlist(self, masterlist_links, date):
        print("masterlist_links.keys()")
        # print("\n".join(masterlist_links.keys()))
        masterlist = masterlist_links[date]
        print("type(masterlist): {}".format(type(masterlist)) )

        tmp = [x for x in masterlist if re.search("(export|gkg)", x)]
        print("tmp: {}".format(len(tmp)))
        # print("tmp: {}".format( "\n".join(tmp)))

        return (url for url in masterlist if url.startswith(date))




    # TODO : refactor. This is the exact same as Gdelter._get_lastupdate_dict()
    def create_extract_dict(self, urls):
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





    def _get_request(self, url):
        try:
            r = requests.get(url)
            status_code = r.status_code
            return status_code, r
        except requests.exceptions.Timeout:
            self.logger.error("Timeout exception for url: {}".format(url))



    def _get_masterlist_from_url(self, masterlist_url) -> List[str]:
        ''' Return a list of lines from masterlist file loaded from url'''
        status, r = self._get_request(masterlist_url)
        if status == 200:
            return r.text.split("\n")
        return []


    def _get_masterlist_from_file(self, masterlist_file) -> List[str]:
        ''' Return a list of lines from masterlist file loaded from local file'''
        lines = (x.strip()for x in open(masterlist_file))
        return [x.split()[-1] for x in lines]







    # def _get_masterlists(self, event_masterlist_url, gkg_masterlist_url):
    #     status_e, event_r = self._get_request(event_masterlist_url)
    #     status_g, gkg_r = self._get_request(gkg_masterlist_url)
    #     # TODO : throw an exception here
    #     if status_e == 200 and status_g == 200:
    #         return event_r, gkg_r


    def _get_all_links(self, response, url):
        dirname = os.path.dirname(url)

        soup = BeautifulSoup(response.text, 'lxml')
        links = soup.find_all('a')
        return [os.path.join(dirname, x['href']) for x in links if x['href'].endswith(".zip")]


    # TODO : make saving a method of a static class so that makedirs, gzip etc is done in one place
    def _save_masterlist_urls(self, masterlist, file):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with gzip.open("{}.gz".format(file), "wt") as fo:
            fo.write("\n".join(masterlist))







    # TODO : ----- STUBS -----
    def _get_ymd_from_url(self, url):
        gdelt_timestamp = os.path.basename(url).split(".")[0]


    # read masterlist file in an iterator, get the url, parse for date, return just files with date searched for
    def wip(self, masterlist_file, gdate):
        masterlist_urls = (x.strip().split()[-1] for x in open(masterlist_file))
        urls = [u for u in masterlist_urls if os.path.basename(u).startswith(gdate) ]












@click.command()
@click.option('--online/--local/', '-o/-l/')
@click.option('--masterlist_dir', '-m', default="/Users/chagerman/Projects/2019_News/Data/GDELT/file_list")
@click.option('--masterlist_filename', '-f', default="masterfilelist.txt")
@click.option('--date', '-d')
def main(online, masterlist_dir, masterlist_filename, date):
    masterlist_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"


    gm = MasterList(masterlist_dir, masterlist_filename, masterlist_url)

    if (online):
        print("online")
        gm.run(date, False)
    else:
        print("local")
        gm.run(date, True)

    print("\tend time:\t{}".format(datetime.now().strftime("%Y-%m-%d %H:%M:%S")))








if __name__ == '__main__':
  main()





''' 
python3 gdeltMasterlist.py -l -d 20190301


'''