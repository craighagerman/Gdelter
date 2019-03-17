
import errno
import gzip
import os
from datetime import datetime


class GIO:

    @staticmethod
    def resave_compressed_files(files):
        gzfiles = []
        for file in files:
            gzfiles.append(GIO.resave_compressed(file))
        return gzfiles

    @staticmethod
    def resave_compressed(file):
        # lines = (x for x in open(file, encoding='latin1'))
        gzfile = file + ".gz"
        print("\tsaving {}".format(gzfile))
        # with gzip.open(gzfile, 'wt') as fo:
        #     for line in lines:
        #         fo.write(line)

        data = open(file, encoding='latin1').read()
        with gzip.open(gzfile, 'wt') as fo:
            fo.write(data)

        if os.path.isfile(file) and os.path.isfile(gzfile):
            os.remove(file)
        else:  ## Show an error ##
            raise FileNotFoundError(
                    errno.ENOENT, os.strerror(errno.ENOENT), file)
        return gzfile


    # TODO : make saving a method of a static class so that makedirs, gzip etc is done in one place
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
    def define_output_directory(basedir, ymd):
        ''' Define standarized output paths that include year and month.

            Example: define_output_directory('/Gdelt_data/events', '20190316')
                input:   ('/Gdelt_data/events', '20190316')
                output:  '/Gdelt_data/events/2019/03/20190316'
        '''
        GDELT_YMD_FORMAT = '%Y%m%d'
        datetime_obj= datetime.strptime(ymd, GDELT_YMD_FORMAT )
        return os.path.join(basedir, datetime_obj.strftime("%Y"), datetime_obj.strftime("%m"), ymd)


