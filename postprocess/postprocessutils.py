

import os

from gdeltIO import GIO


class PPUtils:

    @staticmethod
    def get_article_stats_generator(basedir, ymd):
        article_stats_path = GIO.define_stat_dir(basedir , ymd)
        article_stats_extracts = (os.path.join(article_stats_path, x) for x in os.listdir(article_stats_path) if not x.startswith(".") )
        return article_stats_extracts

