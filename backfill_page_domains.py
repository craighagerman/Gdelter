
import logging
import os
import pprint
from collections import Counter
from urllib.parse import urlparse

from domains import Domains
from gdeltIO import GIO
from postprocess.postprocessutils import PPUtils


class BackfillPageDomains:
    def __init__(self, basedir):
        self.basedir = basedir
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('BackfillPageDomains')

    def backfill(self, ymd):
        print("processing {}".format(ymd))
        article_stats_extracts = PPUtils.get_article_stats_generator(self.basedir, ymd)
        for extract in article_stats_extracts:
            self.process_eventid(extract, ymd)

    def process_eventid(self, extract_path, ymd):
        pp = pprint.PrettyPrinter(indent=4)
        df = GIO.load_article_stats(extract_path)
        if not df.empty:
            df.fillna("", inplace=True)
            # index, row = list(df.iterrows())[0]
            for index, row in df.iterrows():
                eventid = row["eventid"]
                url = row["url"]
                links = self.get_links(ymd, eventid)
                domain_dict = self.get_domains(url, links)
                # print("---------- {} - {} ----------".format(ymd, eventid))
                # pp.pprint(domain_dict)
                GIO.save_dict(domain_dict, GIO.define_page_domain_dir(self.basedir, ymd), eventid)


    def get_links(self, ymd, eventid):
        page_links_dir = GIO.define_page_links_dir(self.basedir, ymd)
        links = GIO.load_page_links(os.path.join(page_links_dir, "{}.txt.gz".format(eventid)))
        return links

    def get_domains(self, url, links):
        domains = [Domains.get_domain(a) for a in links]
        c = Counter([x for x in domains if x])
        domain_dict = {"url": url, "basedomain": "{uri.scheme}://{uri.netloc}/".format(uri=urlparse(url)), "domains": c}
        return domain_dict


########################################################################################################################
#       Script functions
########################################################################################################################
def walk_article_stats(basedir):
    stats_dir = os.path.join(basedir, "article_stats")
    for year in [x for x in os.listdir(stats_dir) if not x.startswith(".")]:
        year_dir = os.path.join(stats_dir, year)
        for month in [x for x in os.listdir(year_dir) if not x.startswith(".")]:
            month_dir = os.path.join(year_dir, month)
            ymds = [x for x in os.listdir(month_dir) if not x.startswith(".")]
            for ymd in ymds:
                yield  ymd


def main():
    print("Starting...")
    basedir = "/Volumes/MyPassport/Data/Gdelt_Data"
    backfill = BackfillPageDomains(basedir)
    ymds = walk_article_stats(basedir)
    for ymd in ymds:
        if ( int(ymd) >= 20191004 ):
            print("..........\t\tprocessing {}".format(ymd))
            backfill.backfill(ymd)


if __name__ == "__main__":
    main()