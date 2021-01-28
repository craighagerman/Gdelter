
import gzip


def load_page_domains_file(gzfile):
    content = gzip.open(gzfile, "rt").read()
    json.loads(content)

