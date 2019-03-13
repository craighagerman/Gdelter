#!/usr/bin/env bash


#cd ./Gdelter
#cd /home/chagerman/Projects/NewsAggregator/Gdelter

#   ___titanx___
#DataDir="/Gdelter/Data"
#RunCmd="/Gdelter/Gdelter.py"

#   ___macbook___
DataDir="/Users/chagerman/Projects/2019_News/Gdelt_Data"
RunCmd="/Users/chagerman/Projects/2019_News/Code/Gdelter/Gdelter.py"


python3 $RunCmd -o -d $DataDir -k lastupdate


#  /home/chagerman/Projects/NewsAggregator/Gdelter/scripts/docker_run_titanx.sh