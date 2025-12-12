import requests
import datetime
import pandas as pd

url = "https://query1.finance.yahoo.com/v8/finance/chart/ALV.DE?interval=1d&range=1mo"
response = requests.get(url)
data = response.json()

timestamps = data["chart"]["result"][0]["timestamp"]
closes = data["chart"]["result"][0]["indicators"]["quote"][0]["close"]

# Tarih formatına çevir
dates = [datetime.datetime.fromtimestamp(ts).date() for ts in timestamps]
df = pd.DataFrame({"date": dates, "close": closes})
print(df.tail(10))
