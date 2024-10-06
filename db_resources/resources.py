import datetime, time, csv
import numpy as np
import pandas as pd

jan1_2018 = 1514764800
jul25_2018 = 1532476800
oct11_2017 = 1507680000
jan1_2017 = 1483228800
jul10_2018 = 1531180800
jan1_2016 = 1451606400
sep25_2018 = 1537833600
oct1_2018 = 1538352000

period_id_to_ts = {
    '2h': 2*60*60,
    '1h': 60*60,
    '15m': 15*60,
}


def dict_to_list(dct):
    lst = []
    for key in dct: lst.append([key, dct[key]])
    lst = sorted(lst, key=lambda x: x[0])
    return lst


def parse_time(time_str):
    # print(time_str)
    if isinstance(time_str, int): return time_str
    try: date = time_str.split('.')[0]
    except Exception: date = time_str
    try: dt = datetime.datetime.strptime(date, "%Y-%m-%d %H:%M:%S")
    except Exception: dt = datetime.datetime.strptime(date, "%Y-%m-%d")
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp())


def timestamp_to_date(timestamp):
    return datetime.datetime.utcfromtimestamp(timestamp)


def split_df(df, sections):
    # print(np.array_split(df, sections))
    splits = np.array_split(df, sections)
    # print(splits)
    return splits


if __name__ == "__main__":
    pass
