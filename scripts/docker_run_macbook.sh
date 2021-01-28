#!/usr/bin/env bash



# CMD version
# ------------
# docker run -v /Users/chagerman/Projects/2019_News/Gdelt_Data:/Gdelt_Data py-gdelt


# EXEC version
# ------------
docker run -v /Users/chagerman/Projects/2019_News/Gdelt_Data:/Gdelt_Data cloud.canister.io:5000/chagerman/py-gdelt:0.1.3 -o -d /Gdelt_Data -k lastupdate



# docker stop --time=30 python-gdelt
