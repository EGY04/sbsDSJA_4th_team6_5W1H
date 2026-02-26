import geopandas as gpd
import os

# 🔥 스크립트 기준 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

shp = os.path.join(
    BASE_DIR,
    "..",
    "data_raw",
    "zones_shp",
    "11_서울_zones_with_id",
    "11_서울_zones_with_id.shp"
)

print("shp 경로:", shp)

gdf = gpd.read_file(shp)

print("CRS:", gdf.crs)
print("컬럼:", gdf.columns.tolist())
print(gdf.head())