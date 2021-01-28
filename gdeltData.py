
import os


class Data:

    article_stats_dir = "article_stats"
    articles_dir = "articles"
    events_dir = "events"
    gkg_dir = "gkg"
    html_meta_dir = "html_meta"
    page_domains_dir = "page_domains"
    page_links_dir = "page_links"

    # TODO : gdelt_data_dir should not be static. Make a class parameter?
    def __init__(self, gdelt_data_dir):
        # self.gdelt_data_dir = "/Users/chagerman/Projects/NewsAggregator/Gdelt_Data"
        self.gdelt_data_dir = gdelt_data_dir


    # @staticmethod
    def article_stats(self):
        date_dir = os.path.join(self.gdelt_data_dir, self.article_stats_dir)
        return os.listdir(date_dir)

    # @staticmethod
    def articles(self):
        date_dir = os.path.join(self.gdelt_data_dir, self.articles_dir)
        return os.listdir(date_dir)

    # @staticmethod
    def events(self):
        date_dir = os.path.join(self.gdelt_data_dir, self.events_dir )
        return os.listdir(date_dir)

    # @staticmethod
    def gkg(self):
        date_dir = os.path.join(self.gdelt_data_dir, self.gkg_dir)
        return os.listdir(date_dir)

    # @staticmethod
    def html_meta(self):
        date_dir = os.path.join(self.gdelt_data_dir, self.html_meta_dir)
        return os.listdir(date_dir)

    # @staticmethod
    def page_domains(self):
        date_dir = os.path.join(self.gdelt_data_dir, self.page_domains_dir)
        return os.listdir(date_dir)

    # @staticmethod
    def page_links(self):
        date_dir = os.path.join(self.gdelt_data_dir, self.page_links_dir)
        return os.listdir(date_dir)



    # article_stats_dir = os.path.join(self.gdelt_data_dir, "article_stats")
    # articles_dir = os.path.join(self.gdelt_data_dir, "articles")
    # events_dir = os.path.join(self.gdelt_data_dir, "events")
    # gkg_dir = os.path.join(self.gdelt_data_dir, "gkg")
    # html_meta_dir = os.path.join(self.gdelt_data_dir, "html_meta")
    # page_domains_dir = os.path.join(self.gdelt_data_dir, "page_domains")
    # page_links_dir = os.path.join(self.gdelt_data_dir, "page_links")