

import os







def read_article_stats(path: str):
    ''' read article_stats csv  file (in Pandas?), open and add in article text
        article_stats header:
        url	domain	nbytes	eventid	gkgid	status	accessdate	title	site_name	author	keywords	description

    '''
    pass





def read_article():
    ''' n.b. article file name is eventid .text
    
    '''
    pass







# /Volumes/MyPassport/Data_sample/Gdelt_Data/page_links/2019/10/20191001/876420440.txt.gz


def get_gdelt_file(basedir, dirname, year, month, day ):
    os.path.join(basedir, dirname, year, month, "{}{}{}".join(year, month, day) )


def get_article_path_from_eventid(eventid):
    pass



