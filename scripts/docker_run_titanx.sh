#!/bin/bash


# CMD version
#
# docker run -v /home/chagerman/Data/Gdelt_Data:/Gdelt_Data cloud.canister.io:5000/chagerman/py-gdelt:0.1.2e


# EXEC version
# ------------
docker run -v /home/chagerman/Data/Gdelt_Data:/Gdelt_Data cloud.canister.io:5000/chagerman/py-gdelt:0.1.3 -o -d /Gdelt_Data -k lastupdate


# docker stop --time=30 python-gdelt

