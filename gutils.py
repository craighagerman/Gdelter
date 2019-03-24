import logging
import re


class GUtils:

    def __init__(self):
        # create logger with 'spam_application'
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Gdelter')
        self.logger.setLevel(logging.INFO)


    @staticmethod
    def flatten(lst):
        return [item for sublist in lst for item in sublist]


    @staticmethod
    def ymd_from_gdelt_date(gdelt_date):
        return gdelt_date[:8]   # only return the year-month-date part


    # -----------------------------------------------------------------------------------------------------------------
    #   Text processing methods
    # -----------------------------------------------------------------------------------------------------------------
    @staticmethod
    def clean_text(s: str) -> str:
        ''' Replace tabs, newlines with spaces '''
        return re.sub("[\n\r\t]", " ", str(s))

