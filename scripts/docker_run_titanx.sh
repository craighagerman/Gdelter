#!/bin/bash


#docker run --name=python-gdelt -d -v /home/chagerman/Gdelt_Data:/Gdelt_Data python-gdelt


docker run -v /home/chagerman/Gdelt_Data:/Gdelt_Data py-gdelt

# docker stop --time=30 python-gdelt

