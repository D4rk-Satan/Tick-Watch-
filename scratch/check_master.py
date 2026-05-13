import pandas as pd
import requests

url = "https://public.fyers.in/sym_details/NSE_FO.csv"
print(f"Downloading {url}...")
df = pd.read_csv(url, header=None, nrows=10)
print("\n--- FIRST 10 ROWS ---")
print(df.to_string())

print("\n--- COLUMN ANALYSIS ---")
for i, val in enumerate(df.iloc[0]):
    print(f"Col {i}: {val}")
