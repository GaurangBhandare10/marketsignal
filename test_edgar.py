"""
Quick smoke test for SEC EDGAR APIs — run from marketsignal/ root.
Usage: py test_edgar.py
"""
import requests

HEADERS = {"User-Agent": "MarketSignal/1.0 gnbhandare20@gmail.com"}

print("=" * 50)
print("TEST 1: CIK lookup via company_tickers_exchange")
print("=" * 50)
r = requests.get(
    "https://www.sec.gov/files/company_tickers.json",
    headers=HEADERS, timeout=30
)
print("Status:", r.status_code)
data = r.json()
cik_map = {v["ticker"]: (str(v["cik_str"]), v["title"]) for v in data.values()}

for ticker in ["AAPL", "MSFT", "TSLA"]:
    cik, name = cik_map[ticker]
    print(f"  {ticker} → CIK: {cik} | Name: {name}")

print()
print("=" * 50)
print("TEST 2: Submissions API (AAPL recent 8-Ks)")
print("=" * 50)
aapl_cik = cik_map["AAPL"][0].zfill(10)
r2 = requests.get(
    f"https://data.sec.gov/submissions/CIK{aapl_cik}.json",
    headers=HEADERS, timeout=30
)
print("Status:", r2.status_code)
data2 = r2.json()
print("Entity:", data2.get("name"))
forms = data2["filings"]["recent"]["form"]
dates = data2["filings"]["recent"]["filingDate"]
eightk = [(d, f) for f, d in zip(forms, dates) if f == "8-K"][:5]
print("Recent 8-Ks:")
for date, form in eightk:
    print(f"  📄 {date} | {form}")

print()
print("=" * 50)
print("TEST 3: EFTS full-text search (8-K filings)")
print("=" * 50)
r3 = requests.get(
    "https://efts.sec.gov/LATEST/search-index?q=%228-K%22&forms=8-K&entity=APPLE&dateRange=custom&startdt=2024-01-01",
    headers=HEADERS, timeout=30
)
print("Status:", r3.status_code)
hits = r3.json().get("hits", {}).get("hits", [])
print(f"Found {len(hits)} filings via EFTS search")
for h in hits[:3]:
    src = h["_source"]
    print(f"  📄 {src.get('file_date')} | {src.get('display_names')}")

print()
print("✅ All EDGAR tests passed!")
