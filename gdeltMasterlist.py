
import logging
import os
import threading
from collections import defaultdict
from datetime import datetime
from itertools import chain
from typing import List, Dict

import click
import requests

from gdeltIO import GIO
from gutils import GUtils
import gdeltParameters

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


    def load_masterlist(self, local=False) -> Dict[str, str]:
        ''' load (event, gkg) masterlist from url or local file.
            Return list of urls
        '''
        masterlist_source = self.masterlist_file if local else self.masterlist_url

        # load masterlist file from local or online
        if local:
            masterlist_urls = self._get_masterlist_from_file(masterlist_source)
        else:
            masterlist_urls = self._get_masterlist_from_url(masterlist_source)
            # save masterlist each time we download it
            GIO.save_masterlist_urls(masterlist_urls, self.masterlist_file)

        # create a dictionary of {YMD -> [all_event/mention/gkg_urls]}
        date2extract_dict = self._create_date_extract_url_dict(masterlist_urls)
        return date2extract_dict


    ###################################################################################################################


    def _get_extracts_for_date(self, date2extract_dict, ymd):
        #  http://data.gdeltproject.org/gdeltv2/20150218230000.export.CSV.zip
        # 'http://data.gdeltproject.org/gdeltv2/20190201010000.export.CSV.zip'
        def make_url(ymd, hms, kind):
            head = "http://data.gdeltproject.org/gdeltv2"
            if kind == "export":
                tail = ".{}.CSV.zip".format(kind)
            elif kind == "gkg":
                tail = ".{}.csv.zip".format(kind)
            return os.path.join(head, "{}{}{}".format(ymd, hms, tail))

        if ymd in date2extract_dict:
            self.logger.info("filtering masterlist file for for YMD date {} ...".format(ymd))
            extract_urls = date2extract_dict[ymd]
            # TODO : check if `if os.path.basename(url).startswith(ymd)` is needed here:
            extract_urls = [url for url in extract_urls if os.path.basename(url).startswith(ymd)]
        else:
            self.logger.info("creating url lists for for YMD date {} ...".format(ymd))
            event_extracts = map(lambda x: make_url(ymd, x, "export"),  gdeltParameters.hms)
            gkg_extracts = map(lambda x: make_url(ymd, x, "gkg"), gdeltParameters.hms)
            # extract_urls = chain(event_extracts, gkg_extracts)
            extract_urls = list(event_extracts) + list(gkg_extracts)
            # filter out 'mentions' files
        return [x for x in extract_urls if "mentions" not in x]


    def _create_date_extract_url_dict(self, masterlist_urls):
        '''
            Return dict of {date -> [event_urls, mention_urls, gkg_urls]}
        '''
        # create a dictionary of {YMD -> [all_event/mention/gkg_urls]}
        date2extract_dict = defaultdict(list)
        for url in masterlist_urls:
            gdelt_date = os.path.basename(url).split(".")[0]
            date = GUtils.ymd_from_gdelt_date(gdelt_date)
            date2extract_dict[ date ].append(url)
        return date2extract_dict


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



    ###################################################################################################################



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