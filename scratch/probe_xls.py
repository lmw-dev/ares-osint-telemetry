import os
import pandas as pd

path = "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/皇家贝蒂斯VS莱万特(亚盘).xls"

print("File size:", os.path.getsize(path))

# Let's try reading with different methods to see what it is
try:
    with open(path, "r", encoding="utf-8", errors="ignore") as f:
        head = f.read(500)
        print("First 500 chars raw text:")
        print(head)
except Exception as e:
    print("Failed raw read:", e)

try:
    # Try reading as html first since many sports sites export HTML tables as .xls
    dfs = pd.read_html(path)
    print("Successfully read as HTML tables! Number of tables found:", len(dfs))
    for i, df in enumerate(dfs):
        print(f"\nTable {i} head:")
        print(df.head(5))
except Exception as e:
    print("Failed read_html:", e)

try:
    # Try standard excel read (which might need xlrd for xls)
    df = pd.read_excel(path)
    print("Successfully read with read_excel! Shape:", df.shape)
    print(df.head(5))
except Exception as e:
    print("Failed read_excel:", e)
