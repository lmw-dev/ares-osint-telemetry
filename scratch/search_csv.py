import glob
import os

print("--- Searching for CSV files in project ---")
for f in glob.glob("/Users/liumingwei/01-project/12-liumw/21-ares-osint-telemetry/**/*.csv", recursive=True):
    print(f, os.path.getsize(f))

print("\n--- Searching for CSV files in vaults ---")
for f in glob.glob("/Users/liumingwei/vaults/AresVault/**/*.csv", recursive=True):
    print(f, os.path.getsize(f))
