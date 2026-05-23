import os

path = "/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/tmp/odds/皇家贝蒂斯VS莱万特(亚盘).xls"

print("File size:", os.path.getsize(path))

with open(path, "rb") as f:
    raw = f.read(1000)
    print("First 1000 bytes:")
    print(raw[:100])
    # Try decode
    try:
        print("Decoded text:")
        print(raw[:1000].decode("gbk", errors="ignore"))
    except Exception as e:
        print("GBK decode failed:", e)
