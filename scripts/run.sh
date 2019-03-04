#!/usr/bin/env bash


cd /home/chagerman/Projects/NewsAggregator/Gdelter

#DataDir="/home/chagerman/Projects/NewsAggregator/Gdelter/Data"
#RunCmd="/home/chagerman/Projects/NewsAggregator/Gdelter/Gdelter.py"
DataDir="/Users/chagerman/Projects/2019_News/Data/Gdelt_out"
RunCmd="/Users/chagerman/Projects/2019_News/Code/Gdelter/Gdelter.py"

python3 $RunCmd -o -d $DataDir -k lastupdate


