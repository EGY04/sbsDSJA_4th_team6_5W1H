############################################################
# 전국 보호구역 통합 (대구 프로토타입 방식 유지)
############################################################

import os
import geopandas as gpd
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ZONES_ROOT = os.path.join(BASE_DIR, "..", "data_raw", "zones_shp")
OUTPUT_PATH = os.path.join(BASE_DIR, "..", "data_processed", "geojson", "zones_all.geojson")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

gdf_list = []

for root, _, files in os.walk(ZONES_ROOT):
    for file in files:
        if file.lower().endswith(".shp"):
            shp_path = os.path.join(root, file)
            print("[INFO] 로딩:", shp_path)

            gdf = gpd.read_file(shp_path)

            # CRS → 4326 변환 (대구와 동일)
            if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
                gdf = gdf.to_crs(epsg=4326)

            # zone_id + geometry만 유지
            gdf = gdf[["zone_id", "geometry"]].copy()

            gdf_list.append(gdf)

# 전국 통합
zones_all = pd.concat(gdf_list, ignore_index=True)
zones_all = gpd.GeoDataFrame(zones_all, geometry="geometry", crs="EPSG:4326")

print("전국 보호구역 수:", len(zones_all))

zones_all.to_file(OUTPUT_PATH, driver="GeoJSON")

print("저장 완료:", OUTPUT_PATH)