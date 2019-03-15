
import logging
import os
from collections import defaultdict
from datetime import datetime
from typing import List

import click
import requests

from gdeltIO import GIO
from gdeltutils import GdeltUtils

'''

GDELT 2.0 Events, Mentions & GKG

masterlist_url = "http://data.gdeltproject.org/gdeltv2/masterfilelist.txt"
masterlist_dir = "/Users/chagerman/Projects/2019_News/Data/GDELT/file_list"
masterlist_filename = "masterfilelist.txt"

gm = MasterList(masterlist_dir, masterlist_filename, masterlist_url )

'''

class MasterList:
    def __init__(self, url = None, localfile = None, basedir ="./"):
        self.masterlist_dir = os.path.join(basedir, "masterlist")
        # self.masterlist_file = localfile if localfile else os.path.join(self.masterlist_dir, "masterlist.txt")
        self.masterlist_file = os.path.join(self.masterlist_dir, localfile)
        self.masterlist_url = url

        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Masterlist')



    def load_masterlist(self, local=False):
        ''' load (event, gkg) masterlist from url or local file.
            Return dict of {date -> [event_urls, mention_urls, gkg_urls]}
        '''
        masterlist_source = self.masterlist_file if local else self.masterlist_url
        masterlist_links = defaultdict(list)

        if local:
            masterlist_urls = self._get_masterlist_from_file(masterlist_source)
        else:
            masterlist_urls = self._get_masterlist_from_url(masterlist_source)
            # save masterlist each time we download it
            GIO.save_masterlist_urls(masterlist_urls, self.masterlist_file)

        # create a dictionary of {YMD -> [all_event/mention/gkg_urls]}
        for url in masterlist_urls:
            gdelt_date = os.path.basename(url).split(".")[0]
            date = GdeltUtils.ymd_from_gdelt_date(gdelt_date)
            masterlist_links[ date ].append(url)
        return masterlist_links



    def filter_date2extract_dict(self, date2extract_dict, date):
        ''' Return a list of urls '''
        if not isinstance(date2extract_dict, dict) :
            raise Exception("date2extract_dict")
        if not (isinstance(date, str) or (len(date) == 8) or (date.startswith("20"))):
            raise Exception("date is not a valid string like `20190321`")
        if not date in date2extract_dict:
            self.logger.error("YMD date {} is not in masterlist_links".format(date))
            return None
        else:
            self.logger.info("filtering masterlist for YMD date {} ...".format(date))
            extract_urls = date2extract_dict[date]
            return (url for url in extract_urls if os.path.basename(url).startswith(date))


    def _get_masterlist_from_url(self, masterlist_url) -> List[str]:
        ''' Return a list of lines from masterlist file loaded from url'''
        if not (isinstance(masterlist_url, str) and (masterlist_url.startswith("http"))):
            raise Exception("masterlist_url is not a valid URL")
        self.logger.innfo("_getting masterlist from url {} ...".format(masterlist_url))
        try:
            r = requests.get(masterlist_url)
            status= r.status_code
        except requests.exceptions.Timeout:
            status = -1
            self.logger.error("Timeout exception for url: {}".format(masterlist_url))
        if status == 200:
            return r.text.split("\n")
        return []


    def _get_masterlist_from_file(self, masterlist_file) -> List[str]:
        ''' Return a list of lines from masterlist file loaded from local file'''
        if not isinstance(masterlist_file, str):
            raise Exception("masterlist_file is not a valid string")
        print("_get_masterlist_from_file()  {}...".format(masterlist_file))
        lines = (x.strip().split() for x in open(masterlist_file))
        return [x[-1] for x in lines]












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