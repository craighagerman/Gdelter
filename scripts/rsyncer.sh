#!/bin/bash


USAGE="Usage: sh ryncher.sh [vps]"

TITANX="chagerman@titanx:~/Projects/NewsAggregator/"
MPAT="chagerman@mpat:~/Projects/NewsAggregator/"



# n.b. you must enter a VPS argument
if [ $# -eq 0 ]
  then
    echo "Error: No arguments supplied."
    echo $USAGE
    exit 1
fi


VPS=$1
if [[ $VPS == "titanx" ]]; then
    DEST=$TITANX
elif [[ $VPS == "mpat" ]]; then
    DEST=$MPAT
else
    echo "Errors: VPS arguments is not recognized."
    echo $USAGE
    exit 1
fi

SCRIPTSDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
EXCLUDEFILE="${SCRIPTSDIR}/rsync_exclude.txt"
PROJECTDIR="$(dirname $SCRIPTSDIR)"


echo "\tVPS:               $VPS"
echo "\tdestination:       $DEST"
echo "\tproject directory: $PROJECTDIR"
echo "\tscripts directory: $SCRIPTSDIR"
echo "\texclude file:      $EXCLUDEFILE"


rsync -ravp -e ssh  --exclude-from=$EXCLUDEFILE $PROJECTDIR $DEST
