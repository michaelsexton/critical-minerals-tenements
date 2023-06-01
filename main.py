from datetime import datetime

import geopandas as gpd
import pandas as pd
import requests
from fiona import BytesCollection
from requests import Request

KEEP_COLUMNS = ["NAME", "TENEMENTTYPE",
                "OWNER", "STATUS"
                ]

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
    sheet_name = "CM Mines and Deposits 2022"
    df = pd.read_excel("147741_01_0.xlsx", sheet_name=sheet_name, engine="openpyxl", skiprows=12)

    critical_minerals = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude),
                                         crs="EPSG:4283")
    tenements_all = gpd.GeoDataFrame()
    for state, url in ENDPOINTS.items():

        if state == "QLD":
            layer_name = "MineralTenement"
        else:
            layer_name = "mt:MineralTenement"
        params = dict(service='WFS', version="1.1.0", request='GetFeature',
                      typeName=layer_name, outputFormat='gml3', srsName="EPSG:4283")

        wfs_request_url = Request('GET', url, params=params).prepare().url
        print(wfs_request_url)
        with BytesCollection(requests.get(wfs_request_url).content) as f:
            tenements = gpd.GeoDataFrame.from_features(f, crs="EPSG:4283")

        tenements_all = pd.concat([tenements_all, tenements])

    replace_columns = dict(zip([c for c in tenements_all.columns if c != "geometry"],
                               [c.upper() for c in tenements_all.columns if c != "geometry"]))

    tenements_all.rename(columns=replace_columns, inplace=True)
    join = gpd.sjoin(critical_minerals, tenements_all, how="left", predicate="intersects")

    all_columns = list(critical_minerals.columns) + KEEP_COLUMNS

    join.drop([c for c in join.columns if c not in all_columns], inplace=True, axis=1)

    join.to_excel("critical-mineral-deposits-with-tenements-{0}.xlsx".format(datetime.today().strftime("%Y%m%d")),
                  index=False)
