#!/bin/bash

SRC_DIR="/Users/chagerman/Projects/2019_News/Code/Gdelter"

TITANX="chagerman@titanx:~/Projects/NewsAggregator/"
MPAT=""
DEST_DIR=$TITANX

EXCLUDEFILE="scripts/rsync_exclude.txt"

rsync -ravp -e ssh  --exclude-from=$EXCLUDEFILE $SRC_DIR $DEST_DIR
