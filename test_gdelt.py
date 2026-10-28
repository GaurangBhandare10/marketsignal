# test_gdelt.py  — save this in marketsignal/ and run: py test_gdelt.py
from urllib.request import urlopen
import pandas as pd

with urlopen("http://data.gdeltproject.org/gdeltv2/lastupdate.txt", timeout=30) as r:
    lines = r.read().decode().strip().splitlines()

gkg_url = [l for l in lines if ".gkg." in l][0].split()[-1]
print("Latest GKG file:", gkg_url)

df = pd.read_csv(gkg_url, sep="\t", header=None, nrows=20, on_bad_lines="skip")
print("Shape:", df.shape)
print("Sample (col 3 = source, col 7 = themes):")
print(df[[3, 7]].head(5).to_string())
print("✅ GDELT working!")