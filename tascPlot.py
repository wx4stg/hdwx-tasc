#!/usr/bin/env python3
# Plots TASC location data
# Created 24 October 2022 by Sam Gardner <stgardner4@tamu.edu>

from os import path, remove, listdir
from pathlib import Path
import shutil
import gzip
import pandas as pd
from matplotlib import pyplot as plt
from matplotlib import colors as pltcolors
from matplotlib.patches import Rectangle
from cartopy import crs as ccrs
from cartopy import feature as cfeat
from metpy import plots as mpplots
import numpy as np
import xarray as xr
import pyart
from datetime import datetime as dt, timedelta
import requests

basePath = path.dirname(path.abspath(__file__))
hasHelpers = False
if path.exists(path.join(basePath, "HDWX_helpers.py")):
    import HDWX_helpers
    hasHelpers = True

def addMRMSToFig(fig, ax, mrmsGribName, axExtent, time, targetLat, targetLon):
    datasetFilePath = path.join(basePath, "radarInput", mrmsGribName)
    radarDS = xr.open_dataset(datasetFilePath)
    radarDS = radarDS.sel(latitude=slice(axExtent[3], axExtent[2]), longitude=slice(axExtent[0]+360, axExtent[1]+360))
    radarData = np.ma.masked_array(radarDS.unknown.data, mask=np.where(radarDS.unknown.data > 10, 0, 1))
    cmap = "pyart_ChaseSpectral"
    vmin=-10
    vmax=80
    rdr = ax.pcolormesh(radarDS.longitude, radarDS.latitude, radarData, cmap=cmap, vmin=vmin, vmax=vmax, transform=ccrs.PlateCarree(), zorder=5, alpha=0.5)
    runPathExtension = path.join(time.strftime("%Y"), time.strftime("%m"), time.strftime("%d"), "0000")
    Path(path.join(basePath, "output", "products", "tasc", "rala", runPathExtension)).mkdir(parents=True, exist_ok=True)
    if hasHelpers:
        HDWX_helpers.dressImage(fig, ax, f"TASC Location: {targetLat:.2f}, {targetLon:.2f} + MRMS Reflectivity", time, notice="MRMS data provided by NOAA/NSSL. WSPR data courtesy of wsprnet.org", plotHandle=rdr, colorbarLabel="Reflectivity (dBZ)")
        HDWX_helpers.saveImage(fig, path.join(basePath, "output", "products", "tasc", "rala", runPathExtension, time.strftime("%H%M.png")))
        HDWX_helpers.writeJson(basePath, 191, time.replace(hour=0), time.strftime("%H%M.png"), time, ["0,0", "0,0"], 60)
    else:
        fig.savefig(path.join(basePath, "output", "products", "tasc", "rala", runPathExtension, time.strftime("%H%M.png")))
    plt.close(fig)

def plotTASC(ax, timeToPlot, data):
    infoToPlot = data.loc[timeToPlot]
    targetLat = infoToPlot["lat"] + infoToPlot["deltaLat"]/2
    targetLon = infoToPlot["lon"] + infoToPlot["deltaLon"]/2
    ax.scatter(targetLon, targetLat, s=15, linewidths=0.75, transform=ccrs.PlateCarree(), color="yellow", edgecolor="black", zorder=10)
    if infoToPlot["deltaLat"] != 0 and infoToPlot["deltaLon"] != 0:
        rect = Rectangle((infoToPlot["lon"], infoToPlot["lat"]), infoToPlot["deltaLon"], infoToPlot["deltaLat"], facecolor="None", edgecolor="black", transform=ccrs.PlateCarree(), zorder=9)
        rect2 = Rectangle((infoToPlot["lon"], infoToPlot["lat"]), infoToPlot["deltaLon"], infoToPlot["deltaLat"], facecolor="black", edgecolor="black", transform=ccrs.PlateCarree(), zorder=9, alpha=0.2)
        ax.add_patch(rect)
        ax.add_patch(rect2)
    return targetLon, targetLat

def plotTrail(ax, lats, lons):
    totalIdx = len(lats)-1
    for i in range(totalIdx):
        startLat = lats[i]
        startLon = lons[i]
        endLat = lats[i+1]
        endLon = lons[i+1]
        latsToPlot = np.linspace(startLat, endLat, 1000)
        lonsToPlot = np.linspace(startLon, endLon, 1000)
        colors = np.linspace((i/totalIdx), ((i+1)/totalIdx), 1000)
        ax.scatter(lonsToPlot, latsToPlot, s=1, c=colors, vmin=0, vmax=1, cmap="plasma_r", edgecolor=None, transform=ccrs.PlateCarree(), zorder=7)

if __name__ == "__main__":
    if not path.exists(path.join(basePath, "tascLoc.csv")):
        exit()
    tascLoc = pd.read_csv(path.join(basePath, "tascLoc.csv"), index_col=0)
    tascLoc.index = pd.to_datetime(tascLoc.index)
    lastTime = tascLoc.index[-1]
    filenameToSave = lastTime.strftime("%H%M") + ".png"
    if path.exists(path.join(basePath, "output", "gisproducts", "tasc", lastTime.strftime("%Y"), lastTime.strftime("%m"), lastTime.strftime("%d"), lastTime.strftime("0000"), filenameToSave)):
        exit()
    fig = plt.figure()
    ax = plt.axes(projection=ccrs.epsg(3857))
    targetLon = 0
    targetLat = 0
    targetLon, targetLat = plotTASC(ax, lastTime, tascLoc)
    trailLats = []
    trailLons = []
    cutOffTime = dt.utcnow() - timedelta(minutes=15)
    for time, data in tascLoc.iloc[::-1].iterrows():
        if time > cutOffTime:
            if data["deltaLat"] == 0:
                trailLats.append(data["lat"])
                trailLons.append(data["lon"])
        else:
            break
    plotTrail(ax, trailLats, trailLons)
    ax.set_extent([targetLon - 0.5, targetLon + 0.5, targetLat - 0.5, targetLat + 0.5], crs=ccrs.PlateCarree())
    ax.set_box_aspect(9/16)
    roads = cfeat.NaturalEarthFeature("cultural", "roads_north_america", "10m", facecolor="none")
    ax.add_feature(roads, edgecolor="red", linewidth=0.25, zorder=3)
    ax.add_feature(mpplots.USCOUNTIES.with_scale("5m"), edgecolor="green", linewidth=0.25, zorder=2)
    px = 1/plt.rcParams["figure.dpi"]
    fig.set_size_inches(1920*px, 1080*px)
    pathToSave = path.join(basePath, "output", "gisproducts", "tasc", lastTime.strftime("%Y"), lastTime.strftime("%m"), lastTime.strftime("%d"), lastTime.strftime("0000"), filenameToSave)
    extent = ax.get_tightbbox(fig.canvas.get_renderer()).transformed(fig.dpi_scale_trans.inverted())
    Path(path.dirname(pathToSave)).mkdir(exist_ok=True, parents=True)
    point1 = ccrs.PlateCarree().transform_point(ax.get_extent()[0], ax.get_extent()[2], ccrs.epsg(3857))
    point2 = ccrs.PlateCarree().transform_point(ax.get_extent()[1], ax.get_extent()[3], ccrs.epsg(3857))
    gisInfo = [str(point1[1])+","+str(point1[0]), str(point2[1])+","+str(point2[0])]
    if hasHelpers:
        HDWX_helpers.saveImage(fig, pathToSave, transparent=True, bbox_inches=extent)
        HDWX_helpers.writeJson(basePath, 190, lastTime.replace(hour=0), filenameToSave, lastTime, gisInfo, 60)
    else:
        fig.savefig(pathToSave, transparent=True, bbox_inches=extent)

    gribList = pd.read_html("https://mrms.ncep.noaa.gov/data/2D/ReflectivityAtLowestAltitude/")[0].dropna(how="any")
    gribList = gribList[~gribList.Name.str.contains("latest") == True].reset_index()
    gribList["pyDateTimes"] = [dt.strptime(filename, "MRMS_ReflectivityAtLowestAltitude_00.50_%Y%m%d-%H%M%S.grib2.gz") for filename in gribList["Name"]]
    gribList = gribList.set_index(["pyDateTimes"])
    latestRadarGrib = gribList["Name"].iloc[-1]
    if not path.exists(path.join(basePath, latestRadarGrib.replace(".gz", ""))):
        [remove(path.join(basePath, oldRadarFile)) for oldRadarFile in listdir(basePath) if oldRadarFile.endswith(".grib2.gz")]
        [remove(path.join(basePath, oldRadarFile)) for oldRadarFile in listdir(basePath) if oldRadarFile.endswith(".grib2")]
        urlToFetch = "https://mrms.ncep.noaa.gov/data/2D/ReflectivityAtLowestAltitude/"+latestRadarGrib
        mrmsData = requests.get(urlToFetch)
        output = path.join(basePath, latestRadarGrib)
        if mrmsData.status_code == 200:
            with open(output, "wb") as fileWrite:
                fileWrite.write(mrmsData.content)
            with gzip.open(output, "rb") as f_in:
                with open(output.replace(".gz", ""), "wb") as f_out:
                    shutil.copyfileobj(f_in, f_out)
            remove(output)
    addMRMSToFig(fig, ax, path.join(basePath, latestRadarGrib.replace(".gz", "")), [point1[0], point2[0], point1[1], point2[1]], lastTime, targetLat, targetLon)
    [remove(path.join(basePath, oldRadarFile)) for oldRadarFile in listdir(basePath) if oldRadarFile.endswith(".idx")]
    