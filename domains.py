

from urllib.parse import urlparse


class Domains:
    @staticmethod
    def get_domain(url: str) -> str:
        ''' Return the base domain of a ULR (e.g. `http://www.cnn.com` )'''
        try:
            parsed_url = urlparse(url)
            return '{uri.scheme}://{uri.netloc}/'.format(uri=parsed_url)
        except ValueError as e:
            print("Received ValueError for url: {} :".format(url))
            print(e)
            return ""

