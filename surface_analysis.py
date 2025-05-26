#!/usr/bin/env python3
# Surface Observations+vis satellite product generation for python HDWX
# Created 23 May 2024 by Sam Gardner <samuel.gardner@ttu.edu>


import xarray as xr
from matplotlib import pyplot as plt
import numpy as np
from cartopy import crs as ccrs
from cartopy import feature as cfeat
import metpy
from metpy.units import pandas_dataframe_to_unit_arrays
from metpy.io import parse_metar_file
from metpy import calc as mpcalc
from metpy import plots as mpplots
from metpy.units import units
from metpy.cbook import get_test_data
from matplotlib.patheffects import withStroke
from os import path
import pandas as pd
from siphon.catalog import TDSCatalog
from os import path, remove
from pathlib import Path
from datetime import datetime as dt, timedelta, UTC
import json
import sys
import pyart
import pysolar


basePath = path.dirname(path.abspath(__file__))
if path.exists(path.join(basePath, "HDWX_helpers.py")):
    import HDWX_helpers
    hasHelpers = True
else:
    hasHelpers = False

def gamma_correct(data, channel):
    if channel == 2:
        data = (data * np.pi * 0.3)/663.274497
        data = np.clip(data, 0, 1)
    return data**0.5

def addMRMSToFig(ax, time, data="ReflectivityAtLowestAltitude"):
    if path.exists("../hdwx-hlma/radarDataFetch.py"):
        sys.path.append("../hdwx-hlma/")
        import radarDataFetch
        mrmsGribName = radarDataFetch.fetchRadarClosestToTime(time, data)
        if ".grib" in mrmsGribName:
            datasetFilePath = path.join(basePath, "radarInput", mrmsGribName)
            radarDS = xr.open_dataset(datasetFilePath)
            radarDS = radarDS.sel(latitude=slice(axExtent[3], axExtent[2]), longitude=slice(axExtent[0]+360, axExtent[1]+360))
            if data == "ReflectivityAtLowestAltitude":
                radarData = np.ma.masked_array(radarDS.unknown.data, mask=np.where(radarDS.unknown.data > 5, 0, 1))
                cmap = "pyart_ChaseSpectral"
                vmin=-10
                vmax=80
            elif data == "RadarOnly_QPE_01H":
                radarData = np.ma.masked_array(radarDS.unknown.data, mask=np.where(radarDS.unknown.data > 0, 0, 1))/25.4
                cmap = "viridis"
                vmin=0
                vmax=10
                labels = ax.contour(radarDS.longitude, radarDS.latitude, radarData, levels=range(1, 99, 1), cmap="viridis", vmin=0, vmax=10, linewidths=0.5, transform=ccrs.PlateCarree(), zorder=5)
            rdr = ax.pcolormesh(radarDS.longitude, radarDS.latitude, radarData, cmap=cmap, vmin=vmin, vmax=vmax, transform=ccrs.PlateCarree(), zorder=5, alpha=0.5)
            return rdr
    else:
        return None

def addStationPlot(ax, validTime):
    metarTime = validTime.replace(minute=0, second=0, microsecond=0)
    stationCatalog = TDSCatalog("https://thredds.ucar.edu/thredds/catalog/noaaport/text/metar/catalog.xml")
    airports = pd.read_csv(get_test_data("airport-codes.csv"))
    airports = airports[(airports["type"] == "large_airport") | (airports["type"] == "medium_airport") | (airports["type"] == "small_airport")]
    try:
        dataset = stationCatalog.datasets.filter_time_nearest(metarTime)
        dataset.download()
        [remove(file) for file in sorted(listdir()) if "metar_" in file and file != dataset.name]
    except Exception as e:
        print(stationCatalog.datasets.filter_time_nearest(metarTime).remote_open().read())
    if path.exists(dataset.name):
        metarData = parse_metar_file(dataset.name, year=metarTime.year, month=metarTime.month)
    else:
        return
    metarUnits = metarData.units

    metarDataFilt = metarData[metarData["station_id"].isin(airports["ident"])]
    metarDataFilt = metarDataFilt.dropna(how="any", subset=["longitude", "latitude", "station_id", "wind_speed", "wind_direction", "air_temperature", "dew_point_temperature", "air_pressure_at_sea_level", "current_wx1_symbol", "cloud_coverage"])
    metarDataFilt = metarDataFilt.drop_duplicates(subset=["station_id"], keep="last")
    metarData = pandas_dataframe_to_unit_arrays(metarDataFilt, metarUnits)
    metarData["u"], metarData["v"] = mpcalc.wind_components(metarData["wind_speed"], metarData["wind_direction"])
    locationsInMeters = ccrs.epsg(3857).transform_points(ccrs.PlateCarree(), metarData["longitude"].m, metarData["latitude"].m)
    overlap_prevent = mpcalc.reduce_point_density(locationsInMeters[:, 0:2], 50000)
    stations = mpplots.StationPlot(ax, metarData["longitude"][overlap_prevent], metarData["latitude"][overlap_prevent], clip_on=True, transform=ccrs.PlateCarree(), fontsize=6)
    stations.plot_parameter("NW", metarData["air_temperature"][overlap_prevent].to(units.degF), path_effects=[withStroke(linewidth=1, foreground="white")])
    stations.plot_parameter("SW", metarData["dew_point_temperature"][overlap_prevent].to(units.degF), path_effects=[withStroke(linewidth=1, foreground="white")])
    stations.plot_parameter("NE", metarData["air_pressure_at_sea_level"][overlap_prevent].to(units.hPa), formatter=lambda v: format(10 * v, '.0f')[-3:], path_effects=[withStroke(linewidth=1, foreground="white")])
    stations.plot_symbol((-1.5, 0), metarData['current_wx1_symbol'][overlap_prevent], mpplots.current_weather, path_effects=[withStroke(linewidth=1, foreground="white")], fontsize=9)
    if validTime.minute == 1 or validTime.minute == 21 or validTime.minute == 41:
        stations.plot_text((2, 0), metarData["station_id"][overlap_prevent], path_effects=[withStroke(linewidth=2, foreground="white")])
    stations.plot_symbol("C", metarData["cloud_coverage"][overlap_prevent], mpplots.sky_cover)
    stations.plot_barb(metarData["u"][overlap_prevent], metarData["v"][overlap_prevent], sizes={"emptybarb" : 0})
    return ax

def plotSat():
    # Get the satellite data
    dataAvail = TDSCatalog("https://thredds.ucar.edu/thredds/catalog/satellite/goes/east/products/CloudAndMoistureImagery/CONUS/Channel02/current/catalog.xml").datasets[-1]
    latestTimeAvailable = dt.strptime(dataAvail.name.split("_")[3][:-3], "s%Y%j%H%M")
    outputMetadataPath = path.join(basePath, "output", "metadata", "products", "6", latestTimeAvailable.strftime("%Y%m%d%H00") + ".json")
    if path.exists(outputMetadataPath):
        with open(outputMetadataPath, "r") as f:
            currentRunMetadata = json.load(f)
        lastPlottedTime = dt.strptime(currentRunMetadata["productFrames"][-1]["valid"], "%Y%m%d%H%M")
        if lastPlottedTime >= latestTimeAvailable:
            exit()
    vis_dataset = dataAvail.remote_access(use_xarray=True)
    dataset_transformed = vis_dataset.metpy.parse_cf("Sectorized_CMI").metpy.assign_latitude_longitude()
    lons_to_plot = dataset_transformed.longitude.data
    lats_to_plot = dataset_transformed.latitude.data
    data_mask = np.ones_like(lons_to_plot, dtype=bool)
    data_mask[(lons_to_plot > axExtent[1]) | (lons_to_plot < axExtent[0])] = False
    data_mask[(lats_to_plot > axExtent[3]) | (lats_to_plot < axExtent[2])] = False
    data_mask[np.isnan(dataset_transformed.data)] = False
    
    step = 50
    for i in range(step, lons_to_plot.shape[0], step):
        for j in range(step, lons_to_plot.shape[1], step):
            lon, lat = np.max(lons_to_plot[i-step:i, j-step:j]), np.mean(lats_to_plot[i-step:i, j-step:j])
            solar_zenith_angle = 90 - pysolar.solar.get_altitude(lat, lon, latestTimeAvailable.replace(tzinfo=UTC))
            if solar_zenith_angle <= 89:
                continue
            else:
                data_mask[i-step:i, j-step:j] = False
    
    data_to_plot = np.where(data_mask, dataset_transformed.data, np.nan)
    valid_rows = ~np.isnan(data_to_plot).all(axis=1)
    valid_cols = ~np.isnan(data_to_plot).all(axis=0)
    lons_to_plot = lons_to_plot[valid_rows, :][:, valid_cols]
    lats_to_plot = lats_to_plot[valid_rows, :][:, valid_cols]
    data_to_plot = data_to_plot[valid_rows, :][:, valid_cols]
    validTime = pd.to_datetime(dataset_transformed.time.data)

    fig = plt.figure()
    ax = plt.axes(projection=ccrs.LambertConformal())
    if len(lons_to_plot) > 0:
        ax.pcolormesh(lons_to_plot, lats_to_plot, gamma_correct(data_to_plot, 2), transform=ccrs.PlateCarree(), cmap='Greys_r')
    return fig, ax, validTime
    


if __name__ == "__main__":
    fig, ax, validTime = plotSat()
    ax.add_feature(cfeat.COASTLINE.with_scale("50m"), linewidth=1, edgecolor="black", zorder=10)
    ax.add_feature(cfeat.STATES.with_scale("50m"), linewidth=0.5, edgecolor="black", zorder=9)
    ax.set_extent(axExtent)
    rdr = addMRMSToFig(ax, validTime)
    if rdr is not None:
        notice = "MRMS data provided by NOAA/NSSL"
    else:
        notice = None
    ax = addStationPlot(ax, validTime)
    
    outputPath = path.join(basePath, "output", "products", "satellite", "goes16", "sfcobs", validTime.strftime("%Y"), validTime.strftime("%m"), validTime.strftime("%d"), validTime.strftime("%H00"), validTime.strftime("%M.png"))
    Path(path.dirname(outputPath)).mkdir(parents=True, exist_ok=True)
    if hasHelpers:
        HDWX_helpers.dressImage(fig, ax, "Surface Obs + GOES-16 visible", validTime, plotHandle=rdr, colorbarLabel="MRMS Reflectivity at Lowest Altitude (dBZ)", notice=notice, width=3840, height=2160)
    if hasHelpers:
        HDWX_helpers.saveImage(fig, outputPath)
        HDWX_helpers.writeJson(path.abspath(path.dirname(__file__)), 6, runTime=(validTime - timedelta(minutes=validTime.minute)), fileName=validTime.strftime("%M.png"), validTime=validTime, gisInfo=["0,0", "0,0"], reloadInterval=270)
    else:
        fig.savefig(outputPath)
    plt.close(fig)