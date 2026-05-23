import pandas as pd

path = "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/皇家贝蒂斯VS莱万特(亚盘).xls"
df = pd.read_excel(path)
print(df.iloc[15:].to_string())
