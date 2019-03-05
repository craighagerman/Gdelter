#!/usr/bin/env bash


docker run --name=python-gdelt -d -v /home/chagerman/Projects/NewsAggregator/Gdelter/Data:/Gdelter/Data python-gdelt
