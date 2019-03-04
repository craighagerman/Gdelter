#!/home/chagerman/.pyenv/shims/python


from datetime import datetime


def main():
    now = gettime()
    print(now)
    writer(now)


def gettime() -> str:
    now : str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return now


def writer(s):
    with open("/home/chagerman/Projects/NewsAggregator/Gdelter/tester.txt", "a") as fo:
        fo.write("{}\n".format(s))

if __name__ == '__main__':
    main()

