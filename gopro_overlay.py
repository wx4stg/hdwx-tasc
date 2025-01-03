#!/usr/bin/env python3
# Generate transparent radar/satellite/location overlays for GoPro
# Create 31 December 2024 by Sam Gardner <sam@wx4stg.com>

from os import path
from pathlib import Path
import pandas as pd
import numpy as np
from cartopy import crs as ccrs
from cartopy import feature as cfeat
from metpy.plots import USCOUNTIES
from matplotlib import pyplot as plt
from datetime import datetime as dt, timedelta
import nexradaws
import xradar as xd
import cmweather
from dask.distributed import LocalCluster
from matplotlib import use as mpl_use


INPUT_FILEPATH = 'tascLoc.csv'
CPUS_TO_USE = 12
MEM_PER_CPU = '5GB'


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
        ax.scatter(lonsToPlot, latsToPlot, s=1, c=colors, vmin=0, vmax=1, cmap="plasma", edgecolor=None, transform=ccrs.PlateCarree(), zorder=7)


def plot_time(time, locations, row, j):
    mpl_use('agg')
    roads = cfeat.NaturalEarthFeature("cultural", "roads_north_america", "10m", facecolor="none")
    counties = USCOUNTIES.with_scale('5m')
    fig, (reflax, velax) = plt.subplots(1, 2, subplot_kw={'projection': ccrs.PlateCarree()})
    cutoff_time = time - timedelta(minutes=10)
    locations_for_trail = locations[(locations.index > cutoff_time) & (locations.index <= time)]
    this_lat = row['lat']
    this_lon = row['lon']
    try:
        radar = xd.io.open_nexradlevel2_datatree(row['closest_nexrad_path'], sweep=[0, 1]).xradar.georeference()
    except OSError as e:
        print(f'Error reading file: {row["closest_nexrad_path"]}')
        return 1
    refl_crs = xd.georeference.get_crs(radar['sweep_0'].ds)
    refl_crs = ccrs.Projection(refl_crs)
    refl_data = np.ravel(radar['sweep_0'].DBZH.data)
    refl_data[refl_data < -10] = np.nan
    refl_data[refl_data > 80] = np.nan
    vel_crs = xd.georeference.get_crs(radar['sweep_1'].ds)
    vel_crs = ccrs.Projection(vel_crs)
    vel_data = np.ravel(radar['sweep_1'].VRADH.data)
    vel_data[np.ravel(radar['sweep_1'].DBZH.data) < 20] = np.nan
    reflax.pcolormesh(radar['sweep_0'].x, radar['sweep_0'].y, radar['sweep_0'].DBZH, cmap=cmweather.cm_colorblind.ChaseSpectral, vmin=-10, vmax=80, transform=refl_crs)
    velax.pcolormesh(radar['sweep_1'].x, radar['sweep_1'].y, radar['sweep_1'].VRADH, cmap=cmweather.cm_colorblind.balance, vmin=-25, vmax=25, transform=vel_crs)

    for ax in [reflax, velax]:
        ax.scatter(this_lon, this_lat, s=15, linewidths=0.75, transform=ccrs.PlateCarree(),
                    color="yellow", edgecolor="black", zorder=10)
        plotTrail(ax, locations_for_trail['lat'].values, locations_for_trail['lon'].values)
        ax.set_extent([this_lon-0.5, this_lon+0.5, this_lat-0.5, this_lat+0.5])
        ax.add_feature(roads, edgecolor="white", linewidth=0.25, zorder=3)
        ax.add_feature(counties, edgecolor="gray", linewidth=0.25, zorder=2)
        ax.axis('off')
    px = 1/plt.rcParams['figure.dpi']
    fig.set_size_inches(600*px, 270*px)
    fig.tight_layout()
    reflax.set_position([0, 0, 0.48, 0.98])
    velax.set_position([0.51, 0.01, 0.48, 0.98])
    fig.savefig(f'out/{str(j).zfill(5)}.png', transparent=True)
    plt.close(fig)
    del radar
    del refl_data
    del vel_data
    del roads
    del counties
    del refl_crs
    del vel_crs
    return 0


def get_nexrad_df():
    nexrad_station_file = 'https://www.ncei.noaa.gov/access/homr/file/nexrad-stations.txt'
    # Define the column specifications
    colspecs = [
        (0, 8),   # NCDCID
        (9, 13),  # ICAO
        (14, 19), # WBAN
        (20, 50), # NAME
        (51, 71), # COUNTRY
        (72, 74), # ST
        (75, 105),# COUNTY
        (106, 115),# LAT
        (116, 126),# LON
        (127, 133),# ELEV
        (134, 139),# UTC
        (140, 190) # STNTYPE
    ]

    # Define the column names
    colnames = [
        'NCDCID', 'ICAO', 'WBAN', 'NAME', 'COUNTRY', 'ST', 'COUNTY', 
        'LAT', 'LON', 'ELEV', 'UTC', 'STNTYPE'
    ]

    # Read the fixed-width file
    df_nexrad_loc = pd.read_fwf(nexrad_station_file, colspecs=colspecs, names=colnames, skiprows=6)
    return df_nexrad_loc


if __name__ == '__main__':
    cluster = LocalCluster(n_workers=CPUS_TO_USE, threads_per_worker=1, memory_limit=MEM_PER_CPU)
    client = cluster.get_client()
    locations = pd.read_csv(INPUT_FILEPATH, index_col=0)
    locations.index = pd.to_datetime(locations.index)
    locations = locations.groupby(locations.index.floor('10S')).first()
    nexrads = get_nexrad_df()
    nexrad_lats = nexrads['LAT'].values.reshape(-1, 1)
    nexrad_lons = nexrads['LON'].values.reshape(-1, 1)
    location_lats = locations['lat'].values
    location_lons = locations['lon'].values
    distances = ((location_lats - nexrad_lats)**2 + (location_lons - nexrad_lons)**2)**0.5
    closest_nexrad_indices = np.argmin(distances, axis=0)
    closest_nexrad_icaos = nexrads.iloc[closest_nexrad_indices]['ICAO']
    locations['closest_nexrad_ICAO'] = closest_nexrad_icaos.values
    locations['closest_nexrad_path'] = ''
    locations['group'] = (locations['closest_nexrad_ICAO'] != locations['closest_nexrad_ICAO'].shift()).cumsum()

    # Aggregate to find start and end times for each group
    nex_start_stops = locations.groupby(['group', 'closest_nexrad_ICAO']).apply(
        lambda x: pd.Series({'start_time': x.index[0], 'end_time': x.index[-1]})
    ).reset_index()

    # Drop the group column for a cleaner result
    nex_start_stops = nex_start_stops.drop(columns='group')
    locations = locations.drop(columns='group')
    locations.to_csv('grouped_locations.csv')
    nex_start_stops.to_csv('nexrad_groups.csv')
    all_scans = {}
    for i, row in nex_start_stops.iterrows():
        conn = nexradaws.NexradAwsInterface()
        scans = conn.get_avail_scans_in_range(row['start_time']-timedelta(minutes=15), row['end_time'], row['closest_nexrad_ICAO'])
        for scan in scans:
            if 'MDM' in scan.filename:
                continue
            print(f'Finding closest NEXRAD at: {scan.scan_time}')
            locations.loc[(locations.index >= np.datetime64(scan.scan_time)), 'closest_nexrad_path'] = scan.filename
            if path.exists(f'./radar-dls/{scan.filename}'):
                continue
            all_scans[scan.filename] = scan
    dl_res = conn.download(list(all_scans.values()), './radar-dls', threads=CPUS_TO_USE)
    locations['closest_nexrad_path'] = locations['closest_nexrad_path'].apply(lambda x: f'./radar-dls/{x}')
    all_res = []
    Path('./out').mkdir(exist_ok=True)
    print('#'*80)
    print('VIEW PROGRESS AT: ', client.dashboard_link)
    print('#'*80)
    for j, (time, row) in enumerate(locations.iterrows()):
        if path.exists(f'./out/{str(j).zfill(5)}.png'):
            continue
        # plot_time(time, locations, row, j)
        this_res = client.submit(plot_time, time, locations, row, j)
        all_res.append(this_res)
    client.gather(all_res)
    
