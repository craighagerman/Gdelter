#!/bin/bash


#docker run --name=python-gdelt -d -v /home/chagerman/Projects/NewsAggregator/Gdelter/Data:/Gdelter/Data python-gdelt

#docker run -v /home/chagerman/Projects/NewsAggregator/Gdelter/Data:/Gdelter/Data python-gdelt

docker run -v /home/chagerman/Gdelt_Data:/Gdelt_Data python-gdelt

# docker stop python-gdelt

