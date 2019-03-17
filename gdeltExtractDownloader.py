import io
import logging
import os
import re
import threading
import zipfile
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import repeat
from typing import *

import requests

from gdeltIO import GIO
from gutils import GUtils

maximumNumberOfThreads = 5
# threadLimiter = threading.BoundedSemaphore(maximumNumberOfThreads)


class ExtractDownloader:
    def __init__(self):
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Downloader')


    def starter(self, urls, dest_dir):
        """
        Create a thread pool and download specified urls
        """
        self.logger.info("Downloading {} extract files and resaving as gzipped files to :".format(len(urls), dest_dir))
        with ThreadPoolExecutor(max_workers=maximumNumberOfThreads) as executor:
            file_list = list(executor.map(self.executeExtractDownload, urls, repeat(dest_dir), timeout=60))
        return GUtils.flatten(file_list)


    # TODO : move to staticmethod - this is duplicated in Gdelter.py
    # def _flatten(self, lst):
    #     return [item for sublist in lst for item in sublist]


    def executeExtractDownload(self, url, dest_dir):
        self.logger.info("Downloading {}".format(url))
        file_basename = os.path.basename(url)

        if self._extract_file_exists(dest_dir, file_basename):
            self.logger.info("path {}/.gz  => exists".format(os.path.join(dest_dir, file_basename)))
            files = []
        else:
            files = self.decompress_zip_url(url, dest_dir)
            gzfiles = GIO.resave_compressed_files(files)
        return gzfiles


    ###################################################################################################################

    def _extract_file_exists(self, outdir, basename):
        extension = "gz"
        path = os.path.join(outdir, basename)
        if os.path.exists(path) or os.path.exists(re.sub("zip", extension, path)) or os.path.exists(
                re.sub("\.zip", "", path)):
            return True
        return False


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
        # n.b. export and gkg zip files seem to only contain one file.
        return files



    def _download_zipfile(self, url):
        '''
        :param url:       url of the zipfile to download
        :return:    a ZipFile object
        '''
        r = requests.get(url, timeout=25)
        z = zipfile.ZipFile(io.BytesIO(r.content))
        return z  # type: zipfile.ZipFile



    def _extract_zipfile(self, z, dest_dir) -> List[str]:
        '''
        :param z:   a ZipFile object (downloaded & returned by _download_zipfile())
        :return:    list[str] containing path to (one?!) decompressed file
        '''
        extracted_files = z.namelist()
        # extract zipfile into `dest_dir`
        z.extractall(dest_dir)
        return [os.path.join(dest_dir, f) for f in extracted_files]
