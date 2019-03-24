
import errno
import gzip
import json
import os
import pandas as pd
from datetime import datetime

import gdeltParameters


class GIO:

    @staticmethod
    def resave_compressed_files(files):
        gzfiles = []
        for file in files:
            gzfiles.append(GIO.resave_compressed(file))
        return gzfiles

    @staticmethod
    def resave_compressed(file):
        gzfile = file + ".gz"
        print("\tsaving {}".format(gzfile))

        data = open(file, encoding='latin1').read()
        with gzip.open(gzfile, 'wt') as fo:
            fo.write(data)

        if os.path.isfile(file) and os.path.isfile(gzfile):
            os.remove(file)
        else:  ## Show an error ##
            raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), file)
        return gzfile


    @staticmethod
    def save_masterlist_urls(masterlist, file):
        os.makedirs(os.path.dirname(file), exist_ok=True)
        with gzip.open("{}.gz".format(file), "wt") as fo:
            fo.write("\n".join(masterlist))


    @staticmethod
    def path_exists(path, extension):
        if os.path.exists(path):
            return True
        if extension and os.path.exists("{}.{}".format(path, extension)):
            return True
        return False




    @staticmethod
    def define_output_directory(basedir, dir_kind, ymd):
        ''' Define standarized output paths that include year and month.

            Example: define_output_directory('/Gdelt_data', 'events', '20190316')
                input:   ('/Gdelt_data', 'events', '20190316')
                output:  '/Gdelt_data/events/2019/03/20190316'
        '''
        GDELT_YMD_FORMAT = '%Y%m%d'
        datetime_obj = datetime.strptime(ymd, GDELT_YMD_FORMAT)
        output_directory = os.path.join(basedir, dir_kind, datetime_obj.strftime("%Y"), datetime_obj.strftime("%m"), ymd)
        os.makedirs(output_directory, exist_ok=True)
        return output_directory


    @staticmethod
    def define_extract_dir(basedir, extract_kind, ymd):
        return GIO.define_output_directory(basedir, extract_kind, ymd)


    @staticmethod
    def define_page_links_dir(basedir, ymd):
        return GIO.define_output_directory(basedir, "page_links", ymd)


    @staticmethod
    def define_page_domain_dir(basedir, ymd):
        return GIO.define_output_directory(basedir, "page_domains", ymd)

    @staticmethod
    def define_html_meta_dir(basedir, ymd):
        return GIO.define_output_directory(basedir, "html_meta", ymd)


    @staticmethod
    def define_article_dir(basedir, ymd, raw=False):
        article_kind = "articles_raw" if raw else "articles"
        return GIO.define_output_directory(basedir, article_kind, ymd)


    @staticmethod
    def define_stat_dir(basedir, ymd):
        return GIO.define_output_directory(basedir, "article_stats", ymd)






    @staticmethod
    def html_content_filepath(basedir, ymd, filename, raw=False):
        '''
        Define an absolute path to which to save HTML (raw or content)
        :param filename:    name of the file
        :param article_dir: article directory
        :param ymd:         Year-Month-Date
        :param raw:         Boolean flag for whether or not to specify 'raw' article directory
        :return:            a path to which to save HTML content
        '''
        basedir = GIO.define_article_dir(basedir, ymd, raw)
        file = os.path.join(basedir, "{}.html".format(filename) if raw else "{}.text".format(filename))
        os.makedirs(os.path.dirname(file), exist_ok=True)
        return file



    @staticmethod
    def save_html_content(content, dest_dir, ymd, filename, raw=False, compress=True):
        basedir = GIO.define_article_dir(dest_dir, ymd, raw)
        file = os.path.join(basedir, "{}.html".format(filename) if raw else "{}.text".format(filename))
        if compress:
            # write content as gzip-ed text
            with gzip.open("{}.gz".format(file), "wt") as fo:
                fo.write(content)
        else:
            with open(file, "wt") as fo:
                fo.write(content)


    @staticmethod
    def save_links(links, dest_dir, filename):
        ''' save a list of URLS as a gzipped text file'''
        gzfile = os.path.join(dest_dir, "{}.txt.gz".format(filename))
        with gzip.open(gzfile, "wt") as fo:
            fo.write("\n".join(links))


    @staticmethod
    def save_dict(data, dest_dir, filename):
        ''' save a dict to dir/filename as a gzipped json file'''
        gzfile = os.path.join(dest_dir, "{}.json.gz".format(filename))
        with gzip.open(gzfile, "wt") as fo:
            json.dump(data, fo, indent=4)


    # -----------------------------------------------------------------------------------------------------------------
    #   Save metadata methods
    # -----------------------------------------------------------------------------------------------------------------
    @staticmethod
    def save_article_stats(metadata_dict, dest_dir, filename):
        gzfile = os.path.join(dest_dir, "{}.csv.gz".format(filename))
        os.makedirs(os.path.dirname(gzfile), exist_ok=True)
        print("saving latest stats to {}".format(gzfile))

        url_metadata = list(metadata_dict.values())
        df = pd.DataFrame(url_metadata, columns=gdeltParameters.metadata_columns)

        # n.b. to avoid UnicodeEncodeError errors encode a problematic series first and then decode back to utf-8
        df['title'] = df['title'].map(lambda x: x.encode('unicode-escape').decode('utf-8'))
        # save metadata as a gzip-ed CSV file to `metadata` directory
        df.to_csv(gzfile, sep="\t", index=False, compression="gzip", encoding='utf-8')


