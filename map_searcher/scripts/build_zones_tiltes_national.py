############################################################
# build_zones_tiles_national.py
# 전국 보호구역 shp → 경량화 → 타일용 GeoJSON 생성
############################################################

import os
import geopandas as gpd
import pandas as pd
from shapely import wkt

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

ZONES_RAW_DIR = os.path.join(
    BASE_DIR, "..", "data_raw", "zones_shp"
)

OUTPUT_PATH = os.path.join(
    BASE_DIR, "..", "data_processed",
    "geojson", "zones_tiles.geojson"
)

print("1️⃣ 전국 zones shp 수집")

gdf_list = []

for root, dirs, files in os.walk(ZONES_RAW_DIR):
    for file in files:
        if file.endswith(".shp"):
            shp_path = os.path.join(root, file)
            print("   로드:", shp_path)
            gdf = gpd.read_file(shp_path)
            gdf_list.append(gdf)

zones = pd.concat(gdf_list, ignore_index=True)

zones = gpd.GeoDataFrame(zones, geometry="geometry")

print("   총 polygon 수:", len(zones))
print("   CRS:", zones.crs)

# ----------------------------------------------------------
# 2️⃣ simplify (5174 상태에서)
# ----------------------------------------------------------

print("2️⃣ simplify 시작")

if zones.crs is not None and zones.crs.to_epsg() == 5174:
    tolerance = 5  # meters 단위 (5~10 추천)
    zones["geometry"] = zones["geometry"].simplify(
        tolerance=tolerance,
        preserve_topology=True
    )
    print("   simplify 완료 (tolerance =", tolerance, ")")
else:
    print("   ⚠ CRS가 5174 아님. simplify 스킵")

# ----------------------------------------------------------
# 3️⃣ 필요한 컬럼만 유지
# ----------------------------------------------------------

print("3️⃣ 컬럼 최소화")

zones = zones[["zone_id", "geometry"]].copy()

# ----------------------------------------------------------
# 4️⃣ CRS → 4326 변환
# ----------------------------------------------------------

print("4️⃣ CRS 4326 변환")

zones = zones.to_crs(epsg=4326)

# ----------------------------------------------------------
# 5️⃣ 좌표 정밀도 축소 (파일 크기 감소)
# ----------------------------------------------------------

print("5️⃣ 좌표 정밀도 축소")

def round_geometry(geom, precision=6):
    return wkt.loads(wkt.dumps(geom, rounding_precision=precision))

zones["geometry"] = zones["geometry"].apply(
    lambda geom: round_geometry(geom, precision=6)
)

# ----------------------------------------------------------
# 6️⃣ 저장
# ----------------------------------------------------------

print("6️⃣ GeoJSON 저장")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

zones.to_file(OUTPUT_PATH, driver="GeoJSON")

print("✅ 저장 완료:", OUTPUT_PATH)
print("최종 polygon 수:", len(zones))