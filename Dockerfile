
# Download base image Python 3.7.2
FROM python:3.7.2

ADD ./ /Gdelter

# Make sure timezone is set up correctly
ENV TZ=America/Toronto
RUN apt-get install tzdata

# Install Python Dependencies
RUN pip install beautifulsoup4
RUN pip install Click
RUN pip install Cython
RUN pip install lxml
RUN pip install numpy
RUN pip install dragnet
RUN pip install metadata-parser
RUN pip install pandas
RUN pip install requests
RUN pip install tqdm
RUN pip install coloredlogs

# Volume configuration
VOLUME ["/Gdelt_Data"]

#CMD [ "python", "/Gdelter/Gdelter.py", "-o", "-d", "/Gdelt_Data", "-k", "lastupdate"]
ENTRYPOINT [ "python", "/Gdelter/Gdelter.py"]



# USAGE EXAMPLES
# --------------
# docker run -v /Users/chagerman/Projects/2019_News/Gdelt_Data:/Gdelt_Data py-gdelt:0.1.2f -o -d /Gdelt_Data -k lastupdate
#
# docker run -v /Users/chagerman/Projects/2019_News/Gdelt_Data:/Gdelt_Data py-gdelt:0.1.2f -o -d /Gdelt_Data -k extract -y 20190228
#
# docker run -v /Users/chagerman/Projects/2019_News/Gdelt_Data:/Gdelt_Data py-gdelt:0.1.2f -o -d /Gdelt_Data -k backfill -y 20190208
#
