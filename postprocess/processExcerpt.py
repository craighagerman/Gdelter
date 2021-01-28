
import os
from gdeltIO import GIO
import re
from elasticsearch import Elasticsearch
from gdeltIO import GIO
import requests
import json
from typing import Dict

from postprocess.postprocessutils import PPUtils

'''
Option 1:
    start at basedir - iterate through all article_stats
        process all dates

Option 2:



'''


class processExcerpt:

    def __init__(self, basedir):
        self.basedir = basedir
        res = requests.get('http://localhost:9200')
        # connect to ES
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])


    def process_ymd_extract(self, ymd):
        article_stats_extracts = PPUtils.get_article_stats_generator(self.basedir, ymd)
        for extract_path in article_stats_extracts:
            self.process_article_stats_extract(extract_path, ymd)



    def process_article_stats_extract(self, extract_path,  ymd):
        article_dir = GIO.define_article_dir(self.basedir, ymd)

        df = GIO.load_article_stats(extract_path)
        df.fillna("", inplace=True)
        # index, row = list(df.iterrows())[0]
        for index, row in df.iterrows():
            eventid = row["eventid"]
            # gkgid = row["gkgid"]
            # accessdate = row["accessdate"]
            # title = row["title"]
            # TODO : author, keywords etc can be Nan (why?!) deal with it, convert to empty string
            # author = row["author"]
            # keywords = row["keywords"]
            # description = row["description"]

            article_path = os.path.join(article_dir, "{}.text.gz".format(eventid))
            article = GIO.load_article(article_path)
            row["article"] = article
            # convert row to a content dict
            content = self.row_to_es_doc(row)
            # ingest all of the above into ES
            self.add_doc_to_ES(content)

    def row_to_es_doc(self, row) -> Dict:
        d = {"title":       row["title"],
             "accessdate":  row["accessdate"],
             "article":     row["article"],
             "author":      row["author"],
             "keywords":    row["keywords"],
             "description": row["description"],
             "eventid":     row["eventid"],
             "gkgid":         row["gkgid"]}
        return d


    def add_doc_to_ES(self, content):
        response = self.es.index(index="test_index", doc_type='people', body=json.dumps(content))

    def bulk_add_to_ES(self, bulk_content):
        response = self.es.bulk(index="test_index", body=json.loads(bulk_content))


def main(ymd):
    pe = processExcerpt()
    pe.process_date(ymd)



