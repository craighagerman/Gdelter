#!/usr/bin/env bash


#docker run --name=python-gdelt -d -v "${$HOME}/Projects/2019_News/Code/Gdelter/Data":/Gdelter/Data python-gdelt

#docker run --name=python-gdelt -v "${HOME}/Projects/2019_News/Gdelt_Data":/Gdelt_Data python-gdelt

docker run -v /Users/chagerman/Projects/2019_News/Gdelt_Data:/Gdelt_Data py-gdelt



# docker stop --time=30 python-gdelt
