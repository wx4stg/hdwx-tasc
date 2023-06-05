#!/usr/bin/env python3
# Fetches TASC location from APRS
# Created 24 October 2022 by Sam Gardner <stgardner4@tamu.edu>

import aprslib
import pandas as pd
from os import path, system, remove
from datetime import datetime as dt, timedelta
import atexit


basePath = path.dirname(path.abspath(__file__))


def callback(packet):
    if b"WX5AGS-9" in packet:
        packet = aprslib.parse(packet)
        print(packet["path"])
        if packet["from"] == "WX5AGS-9":
            newData = pd.DataFrame({"lat": [packet["latitude"]], "lon": [packet["longitude"]], "deltaLat": [0], "deltaLon": [0], "type": ["APRS"]}, index=[dt.utcnow()])
            if path.exists(path.join(basePath, "tascLoc.csv")):
                oldData = pd.read_csv(path.join(basePath, "tascLoc.csv"), index_col=0)
                oldData.index = pd.to_datetime(oldData.index)
                newData = pd.concat([oldData, newData])
            newData = newData[~newData.index.duplicated(keep="first")]
            newData = newData.sort_index()
            cutOffTime = dt.utcnow() - timedelta(days=1)
            newData = newData[newData.index > cutOffTime]
            newData.to_csv(path.join(basePath, "tascLoc.csv"))
            exit()
            

if __name__ == "__main__":
    AIS = aprslib.IS("WX5AGS")
    AIS.connect()
    AIS.consumer(callback, raw=True)