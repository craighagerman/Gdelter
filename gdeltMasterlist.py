import gzip
import logging
import os

import requests
from bs4 import BeautifulSoup
from datetime import datetime

import gdeltParameters

'''
  gdelt_event_masterlist = "http://data.gdeltproject.org/events/index.html"
  gdelt_gkg_masterlist = "http://data.gdeltproject.org/gkg/index.html"
'''

class MasterList:


    def __init__(self, basedir):

        self.basedir = basedir
        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Masterlist')



    ################################################################################
    # get & save master list
    ################################################################################
    def get_and_save_masterlist(self, event_masterlist_url, gkg_masterlist_url ):
        event_r, gkg_r = self._get_masterlists(event_masterlist_url, gkg_masterlist_url)

        evenk_links = self._get_all_links(event_r, event_masterlist_url)
        gkg_links = self._get_all_links(gkg_r, gkg_masterlist_url)

        event_file = "{}_{}.txt".format(gdeltParameters.EventsKey, datetime.now().strftime("%Y-%m-%d"))
        self._save_masterlist_urls(evenk_links, os.path.join(self.basedir, "masterlist", event_file))

        gkg_file = "{}_{}.txt".format(gdeltParameters.GkgKey, datetime.now().strftime("%Y-%m-%d"))
        self._save_masterlist_urls(gkg_links, os.path.join(self.basedir, "masterlist", gkg_file))




    def _get_request(self, url):
        try:
            r = requests.get(url)
            status_code = r.status_code
            return status_code, r
        except requests.exceptions.Timeout:
            self.logger.error("Timeout exception for url: {}".format(url))


    def _get_masterlists(self, event_masterlist_url, gkg_masterlist_url):
        status_e, event_r = self._get_request(event_masterlist_url)
        status_g, gkg_r = self._get_request(gkg_masterlist_url)
        # TODO : throw an exception here
        if status_e == 200 and status_g == 200:
            return event_r, gkg_r


    def _get_all_links(self, response, url):
        dirname = os.path.dirname(url)

        soup = BeautifulSoup(response.text, 'lxml')
        links = soup.find_all('a')
        return [os.path.join(dirname, x['href']) for x in links if x['href'].endswith(".zip")]


    def _save_masterlist_urls(self, masterlist, file):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with gzip.open(file, "wt") as fo:
            fo.write("\n".join(masterlist))







    # TODO : ----- STUBS -----
    def _get_ymd_from_url(self, url):
        gdelt_timestamp = os.path.basename(url).split(".")[0]


    # read masterlist file in an iterator, get the url, parse for date, return just files with date searched for
    def wip(self, masterlist_file, gdate):
        masterlist_urls = (x.strip().split()[-1] for x in open(masterlist_file))
        urls = [u for u in masterlist_urls if os.path.basename(u).startswith(gdate) ]













