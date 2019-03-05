import logging
import os
from datetime import datetime


import pandas as pd



class WebIO:

    def __init__(self, article_dir="articles", metadata_dir="metadata", ymd=datetime.now().strftime("%Y-%m-%d")):
        self.article_dir = article_dir
        self.metadata_dir = metadata_dir
        self.ymd = ymd

        # define header names for url_metadata so that it can be (1) convereted to a Pandas Dataframe and saved to CSV
        self.metadata_columns = ['url', 'eventid', 'gkgid', 'status', 'accessdate', 'title', 'author', 'site_name',
                                   'description', 'keywords']
        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('WebIO')
        self.logger.setLevel(logging.INFO)

    def setymd(self, ymd):
        self.ymd = ymd
        self.logger.info("WebIO Year-Month-Date has been set to {}".format(self.ymd))


    ################################################################################
    # get web page
    ################################################################################

    # TODO: not used? delete
    # def get_response(self, url):
    #     try:
    #         r = requests.get(url)
    #         status_code = r.status_code
    #         return r
    #     except requests.exceptions.Timeout:
    #         print("Timeout exception for url: {}".format(url))


    # def get_web_page(self, url):
    #     '''return (status, text) -> (int, str)'''
    #     try:
    #         r = requests.get(url, timeout=25)
    #         status_code = r.status_code
    #         text = r.text
    #         html = text.replace(u'\xa0', u' ') if status_code == 200 else ""
    #         return status_code, html
    #     except requests.exceptions.RequestException as e:
    #         print("RequestException exception for url: {}".format(url))
    #         print(e)
    #         return -1, ""


    # def get_content(self, html):
    #     try:
    #         content = dragnet.extract_content(html).replace(u'\xa0', u' ')
    #         return content
    #     except dragnet.blocks.BlockifyError as e:
    #         self.logger.error("content could not be parsed by lxml and extracted by dragnet ")
    #         self.logger.error(e)
    #         return ""
    #

    # # TODO : should have a flat to optionally gzip the saved articles
    # def save_html_content(self, content, filename, raw=False):
    #     file = self.html_content_filepath(self.article_dir, self.ymd, filename,  raw)
    #     os.makedirs(os.path.dirname(file), exist_ok=True)
    #     with open(file, "w") as fo:
    #         fo.write(content)


    # os.path.join(metadata_dir, now.strftime("%Y-%m-%d"), "url_event_gtk_status.csv"))
    # name = "url_event_gtk_status.csv"
    def save_url_metadata(self, metadata_dict, name):
        url_metadata = list(metadata_dict.values())
        file = os.path.join(self.metadata_dir, self.ymd, name)
        self.logger.info("saving latest results to {}".format(file))
        os.makedirs(os.path.dirname(file), exist_ok=True)
        df = pd.DataFrame(url_metadata, columns=self.metadata_columns)
        df.to_csv(file, sep="\t", index=False)


    # def html_content_filepath(self, article_dir, ymd, filename, raw=False):
    #     '''
    #     Define an absolute path to which to save HTML (raw or content)
    #     :param filename:    name of the file
    #     :param article_dir: article directory
    #     :param ymd:         Year-Month-Date
    #     :param raw:         Boolean flag for whether or not to specify 'raw' article directory
    #     :return:            a path to which to save HTML content
    #     '''
    #     if raw:
    #         return os.path.join("{}_raw".format(article_dir), "{}".format(ymd), "{}.html".format(filename))
    #     else:
    #         return os.path.join(article_dir, "{}".format(ymd), "{}.text".format(filename))


    # def _already_exists(self, filename, article_dir, ymd):
    #     html_file = self.html_content_filepath(article_dir, ymd, filename, True)
    #     content_file = self.html_content_filepath(article_dir, filename, ymd)
    #     if os.path.exists(html_file) or os.path.exists(content_file):
    #         return True
    #     return False
    #




