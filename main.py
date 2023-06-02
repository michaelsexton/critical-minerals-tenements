from datetime import datetime

import geopandas as gpd
import pandas as pd
import requests
from fiona import BytesCollection
from requests import Request

# Columns we wish to retain from the tenements services
KEEP_COLUMNS = ["NAME", "TENEMENTTYPE", "OWNER", "STATUS"]

# Key value pairs of states and their WFS endpoints
ENDPOINTS = {
    "NSW": "https://gs.geoscience.nsw.gov.au/geoserver/wfs",
    "NT": "https://geology.data.nt.gov.au/geoserver/wfs",
    "QLD": "https://gisservices.information.qld.gov.au/arcgis/services/Economy/MineralTenement/MapServer/WFSServer",
    "SA": "https://sarigdata.pir.sa.gov.au/geoserver/wfs",
    "TAS": "https://www.mrt.tas.gov.au/web-services/wfs",
    "VIC": "http://geology.data.vic.gov.au/nvcl/wfs",
    "WA": "http://geossdi.dmp.wa.gov.au/services/wfs"
}

if __name__ == '__main__':
    # Uses data from the Australian Critical Minerals Map 2022
    # https://pid.geoscience.gov.au/dataset/ga/147741
    file = "147741_01_0.xlsx"
    # Specific sheet in the spreadsheet
    sheet_name = "CM Mines and Deposits 2022"
    # Import data from Excel, removing header rows
    df = pd.read_excel(file, sheet_name=sheet_name, engine="openpyxl", skiprows=12)

    # Turn dataframe into geodataframe to permit spatial operations
    critical_minerals = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude),
                                         crs="EPSG:4283")
    # Create empty geodataframe to append resultant frames
    tenements_all = gpd.GeoDataFrame()

    # Iterate through key-value pairsin the ENDPOINT dictionary
    for state, url in ENDPOINTS.items():

        # The Queensland web service behaves slightly differently to the others, uses a non-namespaced layer name
        if state == "QLD":
            layer_name = "MineralTenement"
        else:
            layer_name = "mt:MineralTenement"

        # Query parameters for the URL to make a GetFeature request
        params = dict(service='WFS', version="1.1.0", request='GetFeature',
                      typeName=layer_name, outputFormat='gml3', srsName="EPSG:4283")

        # Build the URL
        wfs_request_url = Request('GET', url, params=params).prepare().url

        # Import the data into a BytesCollection and read into a geodataframe
        # NOTE: there are other methods to do this, but this was the most reliable. The fiona package by default will
        # use the /vsicurl/ connecting for WFS urls, this did not work for all services.
        with BytesCollection(requests.get(wfs_request_url).content) as f:
            tenements = gpd.GeoDataFrame.from_features(f, crs="EPSG:4283")

        # Concatenate downloaded tenements into the empty tenements geodataframe
        tenements_all = pd.concat([tenements_all, tenements])

    # Create a dictionary of renamed columns in the geodataframe to be uppercase, except the geometry column
    replace_columns = dict(zip([c for c in tenements_all.columns if c != "geometry"],
                               [c.upper() for c in tenements_all.columns if c != "geometry"]))

    # Rename the columns
    tenements_all.rename(columns=replace_columns, inplace=True)

    # Spatially join the critical minerals geodataframe with the tenements geodataframe using and intersect
    join = gpd.sjoin(critical_minerals, tenements_all, how="left", predicate="intersects")

    # Create a set of columns to retain in the output dataset
    all_columns = list(critical_minerals.columns) + KEEP_COLUMNS

    # Drop the unnecessary columns
    join.drop([c for c in join.columns if c not in all_columns], inplace=True, axis=1)

    # Export to Excel
    join.to_excel("critical-mineral-deposits-with-tenements-{0}.xlsx".format(datetime.today().strftime("%Y%m%d")),
                  index=False)
