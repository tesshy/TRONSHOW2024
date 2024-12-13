# %%
import duckdb
import pandas as pd
from google.transit import gtfs_realtime_pb2
from google.protobuf import json_format
import requests

# %% GTFS-RTを取得してパースする
feed = gtfs_realtime_pb2.FeedMessage()
response = requests.get('https://api-public.odpt.org/api/v4/gtfs/realtime/ToeiBus')
feed.ParseFromString(response.content)

# %% パースしたデータをPython Dictにした上でDataFrameに変換
f = json_format.MessageToDict(feed)
# pd.json_normalizeでentityに含まれるデータのKeyをFlattingした上でDataFrameに変換
df_feed = pd.json_normalize(f["entity"])
# vehicle.timestampをUnixTimeからDateTimeに変換、この際UTCとして扱うようにしておくことで日本時間で扱いやすくなる
df_feed['vehicle.timestamp'] = pd.to_datetime(df_feed['vehicle.timestamp'], unit='s', utc=True)

# %% DuckDBに保存
con = duckdb.connect("gtfsrt.duckdb")

# %%
# 初回だけこちらを実行してください
# con.execute("CREATE SCHEMA IF NOT EXISTS toeibus")
# con.execute("CREATE TABLE IF NOT EXISTS toeibus.vehicles AS SELECT * FROM df_feed")

# 2回目以降はこちら
con.execute("INSERT INTO toeibus.vehicles SELECT * FROM df_feed")

con.close()