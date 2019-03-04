#


##############################################################################################################
#
# USAGE EXAMPLES:
#   to download and save master lists
#     python3 masterlist_downloader.py -o -d /Users/chagerman/Projects/2019_News/Data/Gdelt_out
#
##############################################################################################################



from gdelt_decorators import *
from gdelt_downloader import *


import click
import requests
import zipfile
import os.path
from datetime import datetime

from bs4 import BeautifulSoup



@click.command()
@click.option('--online/--local/', '-o/-l/')
@click.option('--basedir', '-d', default="out")
def main(online, basedir):
  gdelt_event_masterlist = "http://data.gdeltproject.org/events/index.html"
  gdelt_gkg_masterlist = "http://data.gdeltproject.org/gkg/index.html"

  if (online):
    print("online")
    download_masterlist(gdelt_event_masterlist, gdelt_gkg_masterlist, basedir)
  else:
    print("local")



@timeit
def download_masterlist(gdelt_event_masterlist, gdelt_gkg_masterlist, basedir):
  get_and_save_masterlist(basedir, gdelt_event_masterlist, "events")
  get_and_save_masterlist(basedir, gdelt_gkg_masterlist, "gkg")



################################################################################
# get & save master list
################################################################################
def get_and_save_masterlist(basedir, gdelt_masterlist_url, gdelt_type):
  base_path = os.path.dirname(gdelt_masterlist_url)
  urls = get_all_links(get_response(gdelt_masterlist_url), base_path)
  print("saving {} masterlist to {}".format(gdelt_type, base_path))
  ymd = datetime.now().strftime("%Y-%m-%d")
  save_masterlist(urls, os.path.join(basedir, "masterlist", "{}_{}.txt".format(gdelt_type, ymd)))


def get_all_links(response, base_path):
  soup = BeautifulSoup(response.text, 'lxml')
  links = soup.find_all('a')
  return [os.path.join(base_path, x['href']) for x in links if x['href'].endswith(".zip")]


def save_masterlist(masterlist, file):
  os.makedirs(os.path.dirname(file), exist_ok=True)
  with open(file, "w") as fo:
    fo.write("\n".join(masterlist))



################################################################################

# basedir = "/Users/chagerman/Projects/2019_News/Data/Gdelt_out"

def parse_csv_file():

  basedir = "/Users/chagerman/Projects/2019_News/Data/Gdelt_out"
  event_url = "http://data.gdeltproject.org/events/20190228.export.CSV.zip"
  gkg_url = "http://data.gdeltproject.org/gkg/20190228.gkg.csv.zip"



  event_file = get_zip_urls(event_url, basedir, "events")[0]
  gkg_file = get_zip_urls(gkg_url, basedir, "gkg")[0]

  print("creating a single list of article urls ...")
  event_gkg_url_list = create_event_gkg_url_dict(event_file, gkg_file)

  article_dir = os.path.join(basedir, "articles")
  metadata_dir = os.path.join(basedir, "gdelt_metadata")

  download_and_save_articles(event_gkg_url_list, article_dir, metadata_dir)
  print("-"*50)
  print("finished processing {} urls".format(len(event_gkg_url_list)))
  print("-"*50)







def get_zip_urls(url, basedir, filetype):
  gdelt_date = get_gdelt_date([url])

  file = os.path.join(basedir, filetype, gdelt_date)

  print("decompressing and saving zip file to {} ...".format(file))
  export_file_list = decompress_zip_url(url, file)
  return export_file_list





################################################################################

# TODO : this is from gdelt_downloader
#   put into a separate importable file
def get_response(url):
  try:
    r = requests.get(url)
    status_code = r.status_code
    return r
  except requests.exceptions.Timeout:
    println("Timeout exception for url: {}".format(url))







if __name__ == '__main__':
  main()


