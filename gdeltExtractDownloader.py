import io
import coloredlogs
import logging
import os
import re
import zipfile
from concurrent.futures.thread import ThreadPoolExecutor
from itertools import repeat
from typing import *

import requests

from gdeltIO import GIO
from gutils import GUtils

maximumNumberOfThreads = 5


class ExtractDownloader:
    def __init__(self, max_workers=4):
        # self.dest_dir = dest_dir
        self.dest_dir = ""
        self.workers = max_workers
        coloredlogs.install()
        logging.basicConfig(level=logging.INFO)
        self.logger = logging.getLogger('Downloader')
        self.logger.info("\tmax_workers: {}".format(self.workers))


    def download_extracts(self, urls, dest_dir):
        """
        Create a thread pool and download specified urls
        """
        self.dest_dir = dest_dir
        self.logger.info("Downloading {} extract files and resaving as gzipped files to : {}".format(len(urls), self.dest_dir))
        # TODO : move dest_dir into map function as repeat(dest_dir)
        with ThreadPoolExecutor(max_workers=self.workers) as executor:
            # file_list = list(executor.map(self.execute_extract_download, urls, timeout=600))
            file_list = list(executor.map(self.execute_extract_download, urls))
        return GUtils.flatten(file_list)


    def execute_extract_download(self, url):
        self.logger.info("Downloading {}".format(url))
        file_basename = os.path.basename(url)

        filelist = []
        if self._extract_file_exists(self.dest_dir, file_basename):
            self.logger.info("path {}/.gz  => exists".format(os.path.join(self.dest_dir, file_basename)))
        else:
            files = self.decompress_zip_url(url, self.dest_dir)
            gzfiles = GIO.resave_compressed_files(files)
            filelist = gzfiles
        return filelist


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
        files = []
        if z:
            files = self._extract_zipfile(z, dest_dir)
        # n.b. export and gkg zip files seem to only contain one file.
        return files


    def _download_zipfile(self, url):
        '''
        :param url:       url of the zipfile to download
        :return:    a ZipFile object
        '''
        z = None
        r = requests.get(url, timeout=25)
        if r.status_code == 404:
            return z
        try:
            z = zipfile.ZipFile(io.BytesIO(r.content))
        except zipfile.BadZipFile as e:
            self.logger.error("***\tBadZipFile for url: {}".format(url))
            self.logger.error(e)
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
