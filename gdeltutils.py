
import io
import os
import zipfile
from typing import List

import requests


class GdeltUtils:

    def __init__(self):
        pass

    def decompress_zip_url(self, url, dest_dir) -> List[str]:
        '''
        Download the given url (a zip file), decompress and return a path to the local file.

        :parm  url:         url of the zipfile to download
        :param dest_dir:    path to destination directory (where zip file is expanded)
        :return:            list[str] containing path to (one?!) decompressed file

        Example:
            Return a path to a local file:
                '/Users/chagerman/Projects/2019_News/Data/tmp_out/events/20190311/20190311214500.export.CSV'
        '''
        z = self._download_zipfile(url)
        files = self._extract_zipfile(z, dest_dir)
        # n.b. export and gkg zip files seem to only contain one file.
        # TODO : confirm/deny if zip files can contain more than one. Otherwise, return just the one.
        # return files[0]
        return files


    def decompress_zip_urls(self, urls, dest_dir) -> List[str]:
        files = []
        for url in urls:
            files.append(self.decompress_zip_url(url, dest_dir))
        return files



    def _download_zipfile(self, url):
        '''
        :param url:       url of the zipfile to download
        :return:    a ZipFile object
        '''
        r = requests.get(url, timeout=25)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        return z    # type: zipfile.ZipFile


    def _extract_zipfile(self, z, dest_dir) -> List[str]:
        '''
        :param z:   a ZipFile object (downloaded & returned by _download_zipfile())
        :return:    list[str] containing path to (one?!) decompressed file
        '''
        extracted_files = z.namelist()
        # extract zipfile into `dest_dir`
        z.extractall(dest_dir)
        return [os.path.join(dest_dir, f) for f in extracted_files]



    def html_content_filepath(self, article_dir, ymd, filename, raw=False):
        '''
        Define an absolute path to which to save HTML (raw or content)
        :param filename:    name of the file
        :param article_dir: article directory
        :param ymd:         Year-Month-Date
        :param raw:         Boolean flag for whether or not to specify 'raw' article directory
        :return:            a path to which to save HTML content
        '''
        if raw:
            return os.path.join("{}_raw".format(article_dir), "{}".format(ymd), "{}.html".format(filename))
        else:
            return os.path.join(article_dir, "{}".format(ymd), "{}.text".format(filename))



    @staticmethod
    def ymd_from_gdelt_date(gdelt_date):
        return gdelt_date[:8]   # only return the year-month-date part




