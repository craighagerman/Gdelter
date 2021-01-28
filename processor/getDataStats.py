#!/home/chagerman/.pyenv/shims/python

'''

Walk the Data/Gdelt_Data directory and gather stats
    - number of articles per day
    {year:
        {month:
            {date:
                {org

    year, month, date, org, # articles, # w. keywords, # w. descriptions, # w. keys & descrit


    parse article_stats for (if status==200):
        eventid, gkgid, year, month, date, newsorg, has_keys, has_description
    parse gkg for:
        gkgid(GKGRECORDID), Themes, Persons, Organizations, Locations (parsed)

    combine above (if eventid exists in both):
        eventid, gkgid, year, month, date, newsorg, has_keys, has_description, Themes, Persons, Organizations, Locations (parsed)

    parse above for :
         year, month, date, newsorg,


'''

import os
import re
from datetime import datetime
from pathlib import Path
from time import gmtime, mktime
import pandas as pd
import dask
import os

import numpy as np

from gdeltIO import GIO

########################################################################################################################
# Path Definitions
########################################################################################################################
# ROOTDIR = "/Users/chagerman/Data/Gdelt-sample"        // local sample of Gdelt data
# ROOTDIR = "/Volumes/Elements/Data/Gdelt_Data"
ROOTDIR = "/Volumes/MyPassport/Data/Gdelt_Data"
OUTDIR = "/Users/chagerman/Projects/NewsAggregator/Data/TopicModel2020"

YEAR_TO_PROCESS = '2020'
MONTH_TO_PROCESS = '09'
# input data
articles_dir = os.path.join(ROOTDIR, "articles", YEAR_TO_PROCESS)
article_stats_dir = os.path.join(ROOTDIR, "article_stats", YEAR_TO_PROCESS)
gkg_dir = os.path.join(ROOTDIR, "gkg", YEAR_TO_PROCESS)
# output data
article_stats_processed = os.path.join(OUTDIR, "article_stats_processed.csv")
gkg_processed = os.path.join(OUTDIR, "gkg_processed.csv")


########################################################################################################################
# Main methods
########################################################################################################################
# deleteme
def process(month):
    # article_stats_dir = os.path.join(ROOTDIR, "article_stats", YEAR_TO_PROCESS, month)
    # walkProcessAndSave(article_stats_dir, article_stats_processed, process_article_stats, as_output_columns, ".csv.gz")
    gkg_output_columns = ['gkgid', 'Themes', 'Persons', 'Organizations', 'Locations', 'year', 'month', 'day']
    gkg_dir = os.path.join(ROOTDIR, "gkg", YEAR_TO_PROCESS, str(month).zfill(2))
    print("\nprocessing gkg directory: {}\n".format(gkg_dir))
    walkProcessAndSave(gkg_dir, gkg_processed, process_gkg_extract, gkg_output_columns, ".gkg.csv.gz")
    # process(4)
    # process(6)
    # process(7)
    # process(8)
    # process(9)





def definitions():
    art_stats = "article_stats_processed.2020."
    gkg = "gkg_processed.2020."
    whitelist = [x.strip() for x in open("whitelist_domains.txt")]
    domain_pattern = '(' +  '|'.join(whitelist) + ')'


def getArtIds(domain_pattern, m):
    basedir = "/Volumes/Elements/Data/Gdelt_processed/2020"
    outdir = "/Users/chagerman/Projects/NewsAggregator/Data/TopicModel2020"
    month = str(m).zfill(2)
    art_file = os.path.join(basedir, "article_stats_processed.2020." + month + '.csv')
    artdf = pd.read_csv(art_file, sep='\t')
    artdf2 = artdf[artdf['domain'].apply(lambda x: bool(re.search(domain_pattern,  x))  )]
    artdf3 = artdf2['gkgid']
    outfile = os.path.join(outdir, "art_ids.2020." + month + '.csv')
    artdf3.to_csv(outfile, sep='\t', header=True, index=False)



import dask.dataframe as dd

def tmpfunc(m):
    month = str(m).zfill(2)
    art_file = "art_stats.2020." + month + '.csv'
    gkg_file = "gkg_processed.2020." + str(month).zfill(2) + '.csv'
    tmp_file = 'tmp.csv'

    artdf = pd.read_csv(art_file, sep='\t', error_bad_lines=False)
    artdf2 = artdf[artdf['domain'].apply(lambda x: bool(re.search(domain_pattern,  str(x))) )]
    artdf2.to_csv(tmp_file, sep='\t', header=True, index=False)

    # Read in the csv files.
    df1 = dd.read_csv(tmp_file, sep='\t', dtype={'gkgid': str})
    df2 = dd.read_csv(gkg_file, sep='\t', dtype={'gkgid': str})
    # Merge the csv files.
    df = dd.merge(df1, df2, how='left', on=['gkgid'])
    # Write the output.
    df.to_csv('art_gkg.2020.{}.csv'.format(month), index=False)
    os.remove(tmp_file)



# def resaver(df, outname, month):
#     df2 = df[df['month'] == month]
#     filename = '{}.2020.{}.csv'.format(outname, str(month).zfill(2))
#     df2.to_csv(filename, sep='\t', header=True, index=False)








def main():
    article_stats_columns = ['url', 'domain', 'nbytes', 'eventid', 'gkgid', 'status', 'accessdate', 'title', 'site_name', 'author', 'keywords', 'description']
    as_output_columns = ['domain', 'eventid', 'gkgid', 'year', 'month', 'day', 'hasKeywords', 'hasDescription']
    walkProcessAndSave(article_stats_dir, article_stats_processed, process_article_stats, as_output_columns, ".csv.gz")

    gkg_output_columns = ['gkgid', 'Themes', 'Persons', 'Organizations', 'Locations', 'year', 'month', 'day']
    art2_columns = ['gkgid', 'eventid', 'year', 'month', 'day', 'title', 'site_name', 'author', 'keywords', 'description', 'url', 'domain']
    # walkProcessAndSave(gkg_dir, gkg_processed, process_gkg_extract, gkg_output_columns, ".gkg.csv.gz")

    # walkProcessAndSaveArt2(article_stats_dir, process_article_stats_2, art2_columns, ".csv.gz")

    process_article_stats_3(article_stats_dir, ".csv.gz")




########################################################################################################################
# Helper functions
########################################################################################################################
def parseAccessDate(s):
    dt = datetime.strptime(str(s['accessdate']), '%Y%m%d')
    s['year'] = dt.year
    s['month'] = dt.month
    s['day'] = dt.day
    return s


def parseLocations(loc):
    # loc = s['Locations']
    return ';'.join(set([re.sub(';.*', '', x) for x in re.sub(r'-?[0-9]+\.?[0-9]?', '', loc).split('#') if x]))


def parseGkgDate(s):
    d = str(s['DATE'])
    try:
        dt = datetime.strptime(d[0:8], '%Y%m%d')
    except ValueError:
        dt = datetime(1, 1, 1)
    s['year'] = dt.year
    s['month'] = dt.month
    s['day'] = dt.day
    return s


########################################################################################################################
# Walk & process extract directories
########################################################################################################################
def walkProcessAndSaveArt2(root_dir, process_func, output_columns, extract_suffix):
    months = range(1,10)

    for m in months:
        month = str(m).zfill(2)
        id_file = os.path.join(OUTDIR, 'art_ids.2020.{}.csv'.format(month))
        gkgids = set([x.strip() for x in open(id_file)][1:])
        file_to_write = os.path.join(OUTDIR, "art_stats.2020.{}.csv".format(month))
        num_files_processed = 0

        with open(file_to_write, 'w') as fo:
            fo.write('\t'.join(output_columns) + '\n')
            for root, dirs, files in os.walk(root_dir):
                for name in files:
                    num_files_processed += 1
                    extract_path = os.path.join(root, name)
                    if num_files_processed % 100 == 0:
                        now = datetime.now()
                        print("* {}:{}:{} processed {} files\t current: {}".format(now.hour, now.minute, now.second,num_files_processed, extract_path))
                    if not name.startswith('.') and name.endswith(extract_suffix):
                        process_func(fo, gkgids, extract_path, output_columns)
        print('done walking directory {}\n'.format(root_dir))



def walkProcessAndSave(root_dir, outfile, process_func, output_columns, extract_suffix):
    print('\nstarting to walk directory {}'.format(root_dir))
    milli_now = int(mktime(gmtime()))
    file_to_write = outfile.replace('.csv', '.{}'.format(str(milli_now))) + ''.join(Path(outfile).suffixes)
    print("writing results to {}".format(file_to_write))
    num_files_processed = 0
    with open(file_to_write, 'w') as fo:
        fo.write('\t'.join(output_columns) + '\n')
        for root, dirs, files in os.walk(root_dir):
            for name in dirs:
                print("processing directory: {}/{}".format(root, name))
            for name in files:
                num_files_processed += 1
                extract_path = os.path.join(root, name)
                if num_files_processed % 100 == 0:
                    now = datetime.now()
                    print("* {}:{}:{} processed {} files\t current: {}".format(now.hour, now.minute, now.second,  num_files_processed, extract_path))
                if not name.startswith('.') and name.endswith(extract_suffix):
                    process_func(fo, extract_path, output_columns)
    print('done walking directory {}\n'.format(root_dir))


def process_article_stats_2(fo, gkgids, extract_path, output_columns):
    """ a walkProcessAndSave processor function
    :param fo: TextIO object for writing file
    :param extract_path:  path of extract file to open and parse
    :param output_columns:  desired dataframe columns to select for output
    :return:    unit
    """
    if extract_path.endswith('.csv.gz'):
        df = GIO.load_article_stats(extract_path)
        if not df.empty and 'accessdate' in df.columns:
            # when we load a CSV into Pandas empty rows become NaN - convert to '' for ease of processing
            df = df.replace(np.nan, '', regex=True)
            # filter for rows with status=200
            df = df[df['status'] == 200]
            df = df[df['gkgid'].isin(gkgids)]
            # create new columns `hasKeywords` and `hasDescription` with binary (0,1) truth values
            # df['hasKeywords'] = df['keywords'].apply(lambda x: 0 if len(x) == 0 else 1)
            # df['hasDescription'] = df['description'].apply(lambda x: 0 if len(x) == 0 else 1)
            # parse the `accessdate` column, create new columns for `year`, `month`, day`
            df = df.apply(parseAccessDate, axis=1)
            if df.shape[0] > 0:
                # select desired columns to output
                df = df[output_columns]
                csvdata = ['\t'.join(map(str, x)) for x in df.values.tolist()]
                fo.write('\n'.join(csvdata) + '\n')


def process_article_stats_3(root_dir, extract_suffix):
    months = range(1,10)
    local_dir = '/Users/chagerman/Projects/NewsAggregator/Data/TopicModel2020'
    whitelist_file = os.path.join(local_dir, "whitelist_domains.txt")
    whitelist = [x.strip() for x in open(whitelist_file)]
    domain_pattern = '(' +  '|'.join(whitelist) + ')'
    for m in months:
        month = str(m).zfill(2)
        outfile = os.path.join(OUTDIR, "article_stats.2020.{}.csv".format(month))
        num_files_processed = 0

        # with open(file_to_write, 'w') as fo:
        #     fo.write('\t'.join(output_columns) + '\n')
        for root, dirs, files in os.walk(os.path.join(root_dir, month)):
            for name in files:
                num_files_processed += 1
                extract_path = os.path.join(root, name)
                if num_files_processed % 100 == 0:
                    now = datetime.now()
                    print("* {}:{}:{} processed {} files\t current: {}".format(now.hour, now.minute, now.second, num_files_processed, extract_path))
                if not name.startswith('.') and name.endswith(extract_suffix):
                    # process_func(fo, gkgids, extract_path, output_columns)
                    df = GIO.load_article_stats(extract_path)
                    if not df.empty and 'accessdate' in df.columns:
                        # when we load a CSV into Pandas empty rows become NaN - convert to '' for ease of processing
                        df = df.replace(np.nan, '', regex=True)
                        # filter for rows with status=200
                        df = df[df['status'] == 200]
                        df2 = df[df['domain'].apply(lambda x: bool(re.search(domain_pattern, x)))]
                        df2.to_csv(outfile, sep='\t', mode='a', header=False)
        print('done walking directory {}\n'.format(root_dir))






def process_article_stats(fo, extract_path, output_columns):
    """ a walkProcessAndSave processor function
    :param fo: TextIO object for writing file
    :param extract_path:  path of extract file to open and parse
    :param output_columns:  desired dataframe columns to select for output
    :return:    unit
    """
    if extract_path.endswith('.csv.gz'):
        df = GIO.load_article_stats(extract_path)
        if not df.empty:
            # when we load a CSV into Pandas empty rows become NaN - convert to '' for ease of processing
            df = df.replace(np.nan, '', regex=True)
            # filter for rows with status=200
            df = df[df['status'] == 200]
            # create new columns `hasKeywords` and `hasDescription` with binary (0,1) truth values
            df['hasKeywords'] = df['keywords'].apply(lambda x: 0 if len(x) == 0 else 1)
            df['hasDescription'] = df['description'].apply(lambda x: 0 if len(x) == 0 else 1)
            # parse the `accessdate` column, create new columns for `year`, `month`, day`
            df = df.apply(parseAccessDate, axis=1)
            # select desired columns to output
            df = df[output_columns]
            csvdata = ['\t'.join(map(str, x)) for x in df.values.tolist()]
            fo.write('\n'.join(csvdata) + '\n')


def process_gkg_extract(fo, extract_path, output_columns):
    """ a walkProcessAndSave processor function
    :param fo: TextIO object for writing file
    :param extract_path:  path of extract file to open and parse
    :param output_columns:  desired dataframe columns to select for output
    :return:    unit
    """
    if extract_path.endswith('.gkg.csv.gz'):
        df = GIO.load_gkg(extract_path)
        if not df.empty:
            # when we load a CSV into Pandas empty rows become NaN - convert to '' for ease of processing
            df = df.replace(np.nan, '', regex=True)
            # rename id column to match article_stats file naming
            df['gkgid'] = df['GKGRECORDID']
            # extract human-readable locations, re-join ;-separated
            # df['Locations'] = df['Locations'].apply(parseLocations, axis=1)
            df['Locations'] = df['Locations'].apply(parseLocations)
            # parse the `accessdate` column, create new columns for `year`, `month`, day`
            df = df.apply(parseGkgDate, axis=1)
            # filter out rows with invalid dates
            df = df[df['year'] > 1]
            # select desired columns to output
            df = df[output_columns]
            csvdata = ['\t'.join(map(str, x)) for x in df.values.tolist()]
            fo.write('\n'.join(csvdata) + '\n')


########################################################################################################################


if __name__ == '__main__':
    main()
