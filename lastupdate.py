import os
from typing import List

class LastUpdate:

    def __init__(self):
        pass

    ################################################################################
    # get & parse GDELT page
    ################################################################################
    def parse_lastupdate_file(self, file) -> list:
        ''' Read lastupdate.txt file, extract and return the urls (last column) as a list[str] '''
        return [x.strip().split()[-1] for x in open(file)]


    def parse_lastupdate(self, response) -> List:
        ''' Get lastupdate html response, extract and return the urls (last column) as a list[str] '''
        urls : List[str] = [x.split()[-1] for x in response.text.split("\n") if x]
        return urls


    def get_gdelt_date(self, urls):
        return os.path.basename(urls[0]).split(".")[0]


    def save_last_update(urls, file):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with open(file, "w") as fo:
            fo.write("\n".join(urls))


    def get_lastupdate_dict(self, urls):
        return {u.split(".")[-3]: u.strip() for u in tqdm(urls)}


