
import gzip
import io
import json
import logging
import os
import re
import threading
import zipfile
from collections import Counter
from datetime import datetime
from threading import Thread
from typing import *

import requests

from gdeltIO import GIO

maximumNumberOfThreads = 4
threadLimiter = threading.BoundedSemaphore(maximumNumberOfThreads)


class ExtractDownloader(threading.Thread):

    def __init__(self, url, extract_url_dict, gdelt_date, dest_dir):
        super().__init__()
        self.url = url
        self.extract_url_dict = extract_url_dict
        # self.extract_kind = extract_kind
        self.gdelt_date = gdelt_date
        self.dest_dir = dest_dir

        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Downloader')


    def run(self):
        threadLimiter.acquire()
        try:
            self.executeExtractDownload()
        finally:
            threadLimiter.release()


    def executeExtractDownload(self):
        self.logger.info("Downloading {}".format(self.url))
        nfiles = 0
        file_basename = os.path.basename(self.url)
        if self._extract_file_exists(self.dest_dir, file_basename):
            self.logger.info("path {}/.gz  => exists".format(os.path.join(self.dest_dir, file_basename)))
        else:
            files = self.decompress_zip_url(self.url, self.dest_dir)
            # self.extract_url_dict[self.extract_kind][self.gdelt_date].extend(files)
            # self.extract_url_dict[self.gdelt_date].extend(files)
            self.extract_url_dict[self.gdelt_date] = files
            GIO.resave_compressed_files(files)
            nfiles += 1
            # filelist.extend(files)
        # return nfiles


    ###################################################################################################################


    def _extract_file_exists(self, outdir, basename):
        extension = "gz"
        path = os.path.join(outdir, basename)
        if os.path.exists(path) or os.path.exists(re.sub("zip", extension, path )) or os.path.exists(re.sub("\.zip", "", path )):
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
        # TODO : confirm/deny if zip files can contain more than one. Otherwise, return just the one.
        # return files[0]
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

