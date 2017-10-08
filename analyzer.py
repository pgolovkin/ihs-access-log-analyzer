import collections

import pandas as p

data = p.read_table(filepath_or_buffer="C:\\work\\bsc\\rc\\rc-ibs\\DA-logs\\2017-10-05-IHS-accesslog\\access_log",
                    sep='\s+',
                    names=['ip', 'dash1', 'dash2', 'date', 'timezone', 'url', 'httpVersion', 'status', 'time'],
                    skiprows=1235991)
data_to_count = data[['url', 'time']]
data_to_count['time'] = data_to_count['time'] / 1_000_000
data_groups = data_to_count.groupby(['url'])
quantile_dict = collections.defaultdict(list)

for name, group in data_groups:
    quantile_dict[name] = group['time'].quantile(.9)
quantile_df = p.DataFrame.from_dict(data=quantile_dict, orient='index')
quantile_df.columns = ['time']
quantile_df = quantile_df.sort_values(['time'], ascending=False)
quantile_df.to_csv("result.csv", sep=';')
