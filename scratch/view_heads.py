import pandas as pd

files = {
    "asian": "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/皇家贝蒂斯VS莱万特(亚盘).xls",
    "total": "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/皇家贝蒂斯VS莱万特(大小).xls",
    "euro": "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/皇家贝蒂斯VS莱万特(西甲)欧洲数据.xls"
}

for name, path in files.items():
    print(f"\n==================== {name} ====================")
    df = pd.read_excel(path)
    print("Columns:", list(df.columns))
    print(df.head(10).to_string())
