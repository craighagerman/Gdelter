

import gzip
import json
import os
from collections import namedtuple

import pandas as pd

from gdeltData import Data


class Ingest:

    def __init__(self):
        # Point = namedtuple('Point', ['x', 'y'])
        GdeltFiles = namedtuple('GdeltFiles', ["article_stats", "articles", "events", "gkg", "html_meta", "page_domains", "page_links"])
        # STEPS
        #   - get path to date directory
        #   - get list of files in date directory
        #   - read txt/json/csv files into data structure

        gdelt_data_dir = "/Users/chagerman/Projects/NewsAggregator/Gdelt_Data"
        data = Data(gdelt_data_dir)
        self.files = GdeltFiles(data.article_stats,
                                data.articles,
                                data.events,
                                data.gkg,
                                data.html_meta,
                                data.page_domains,
                                data.page_links)



    def get_data_for_date(self, year, month, date):
        pass



    # -----------
    def get_data_paths(self,year, month, date):
        article_stats_dir = Data.article_stats
        articles_dir = Data.articles
        events_dir = Data.events
        gkg_dir = Data.gkg
        html_meta_dir = Data.html_meta
        page_domains_dir = Data.page_domains
        page_links_dir = Data.page_links



    def get_path_to_date(self, adir, year, month, date):
        return os.path.join(adir, year, month, "{}{}{}".format(year, month, date))

    def get_date_files(self, adir, year, month, date):
        bdir = self.get_path_to_date(adir, year, month, date)
        return [os.path.join(bdir, f) for f in  os.listdir(bdir)]

    def get_first_file(self, adir, year, month, date):
        paths = self.get_date_files(adir, year, month, date)
        return paths[0]


    # -----------


    def get_header(self, path):
        return open(path).read().split(",")


    ###################################################################################################################
    #  Load Gzipped GDELT files
    ###################################################################################################################

    # NOTE: moved to GIO
    # def read_gzipped_csv_file(self, path, columnNames=None):
    #     if columnNames:
    #         df = pd.read_csv(path, compression="gzip", sep="\t", names=columnNames)
    #     else:
    #         df = pd.read_csv(path, compression="gzip", sep="\t")
    #     return df
    #
    # def read_gzipped_json_file(self, path):
    #     return json.loads(path)
    #
    # def read_gzipped_txt_file(self, path):
    #     return gzip.open(path, "rt")
    #
    # def read_gzipped_txt_list_file(self, path):
    #     return self.read_gzipped_txt_file(path).split("\n")





