#!/usr/bin/env python3
# Fetches TASC location from WSPR
# Created 24 October 2022 by Sam Gardner <stgardner4@tamu.edu>

import pandas as pd
import requests
import maidenhead as mh
from os import path, system, remove
from datetime import datetime as dt, timedelta
import pytz
from time import sleep
import atexit
import json

basePath = path.dirname(path.realpath(__file__))
updateWait = 1800

@atexit.register
def exitFunc():
    print(f"waiting {updateWait} seconds before exiting")
    sleep(updateWait)
    if path.exists("wspr-lock.txt"):
        remove("wspr-lock.txt")
    system("bash generate.sh &")


def fetchWSPR():
    wsprSession = requests.Session()
    passwd = open(path.join(basePath, "wsprPasswd.txt"), 'r').read().strip()
    login = wsprSession.post("https://www.wsprnet.org/drupal/rest/user/login", json={"name": "wx4stg", "pass" : passwd})
    spotsRes = wsprSession.post("https://www.wsprnet.org/drupal/wsprnet/spots/json/", cookies=login.cookies, data={"callsign": "WX5AGS", "band" : "All", "minutes": 45})
    spots = json.loads(spotsRes.text)
    if len(spots) == 0:
        return
    global updateWait
    updateWait = 150
    datetimes = []
    lats = []
    lons = []
    deltaLats = []
    deltaLons = []
    types = []
    for spot in spots:
        thisDate = dt.fromtimestamp(int(spot["Date"]))
        thisLat, thisLon = mh.to_location(spot["Grid"])
        thisLatC, thisLonC = mh.to_location(spot["Grid"], center=True)
        thisLatDelta = 2*(thisLatC - thisLat)
        thisLonDelta = 2*(thisLonC - thisLon)
        print("====")
        print(f"{thisLat}, {thisLon}")
        print(f"{thisLatC}, {thisLonC}")
        print(f"{thisLat+thisLatDelta}, {thisLon+thisLonDelta}")
        print("====")
        datetimes.append(thisDate)
        lats.append(thisLat)
        lons.append(thisLon)
        deltaLats.append(thisLatDelta)
        deltaLons.append(thisLonDelta)
        types.append(f"{len(spot['Grid'])}-character maidenhead")
    newData = pd.DataFrame({"lat": lats, "lon": lons, "deltaLat": deltaLats, "deltaLon": deltaLons, "type": types}, index=datetimes)
    if path.exists(path.join(basePath, "tascLoc.csv")):
        oldData = pd.read_csv(path.join(basePath, "tascLoc.csv"), index_col=0)
        oldData.index = pd.to_datetime(oldData.index)
        newData = pd.concat([oldData, newData])
    newData = newData[~newData.index.duplicated(keep="first")]
    cutOffTime = dt.utcnow() - timedelta(days=1)
    newData = newData[newData.index > cutOffTime]
    newData = newData.sort_index()
    newData.to_csv(path.join(basePath, "tascLoc.csv"))

if __name__ == "__main__":
    if path.exists(path.join(basePath, "tascLoc.csv")):
        oldData = pd.read_csv(path.join(basePath, "tascLoc.csv"), index_col=0)
        oldData.index = pd.to_datetime(oldData.index)
        lastDt = oldData.index[-1]
        print(lastDt)
        if lastDt > dt.utcnow() - timedelta(hours=2):
            updateWait = 150
            print("Data is up to date")
    fetchWSPR()