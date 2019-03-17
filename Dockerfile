
# Download base image Python 3.7.2
FROM python:3.7.2

ADD ./ /Gdelter

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


# Volume configuration
VOLUME ["/Gdelt_Data"]


CMD [ "python", "/Gdelter/Gdelter.py", "-o", "-d", "/Gdelt_Data", "-k", "lastupdate"]
