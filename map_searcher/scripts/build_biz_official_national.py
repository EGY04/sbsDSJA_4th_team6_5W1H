############################################################
# build_biz_official_national.py
# 전국 공식 유흥·단란업소 → school_id 매핑 → GeoJSON 생성
############################################################

import os
import pandas as pd
import geopandas as gpd

# ----------------------------------------------------------
# 0. 경로 설정
# ----------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BIZ_PATH = os.path.join(
    BASE_DIR, "..", "data_raw", "biz",
    "전국_유흥단란주점_with_ID_260226.csv"
)

ZONES_PATH = os.path.join(
    BASE_DIR, "..", "data_processed",
    "geojson", "zones_all.geojson"
)

SCHOOLS_PATH = os.path.join(
    BASE_DIR, "..", "data_processed",
    "csv_by_sido", "schools_all.csv"
)

OUTPUT_PATH = os.path.join(
    BASE_DIR, "..", "data_processed",
    "geojson", "biz_official_all.geojson"
)

# ----------------------------------------------------------
# 1. 안전한 CSV 로드 함수
# ----------------------------------------------------------

def safe_read_csv(path):
    for enc in ["utf-8-sig", "utf-8", "cp949"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    raise ValueError("파일 인코딩 판별 실패")

# ----------------------------------------------------------
# 2. biz 로드
# ----------------------------------------------------------

print("1️⃣ biz 로드 시작")

biz_df = safe_read_csv(BIZ_PATH)
print("   원본 biz 수:", len(biz_df))

# 컬럼 통일
biz_df = biz_df.rename(columns={
    "ID": "biz_id",
    "사업장명": "name"
})

# 좌표 컬럼 지정
lat_col = "위도(latitude)"
lng_col = "경도(longitude)"

# 좌표 결측 제거
biz_df = biz_df.dropna(subset=[lat_col, lng_col]).copy()
print("   좌표 유효 biz 수:", len(biz_df))

# GeoDataFrame 변환
biz_gdf = gpd.GeoDataFrame(
    biz_df,
    geometry=gpd.points_from_xy(
        biz_df[lng_col],
        biz_df[lat_col]
    ),
    crs="EPSG:4326"
)

print("   GeoDataFrame 변환 완료")

# ----------------------------------------------------------
# 3. zones 로드
# ----------------------------------------------------------

print("2️⃣ zones 로드 시작")

zones = gpd.read_file(ZONES_PATH)
print("   zones 수:", len(zones))

# spatial index 생성
print("   spatial index 생성 중...")
zones.sindex
biz_gdf.sindex
print("   spatial index 생성 완료")

# ----------------------------------------------------------
# 4. 공간조인
# ----------------------------------------------------------

print("3️⃣ 공간조인 시작...")

biz_joined = gpd.sjoin(
    biz_gdf,
    zones,
    predicate="within",
    how="left"
)

inside_count = biz_joined["zone_id"].notna().sum()
print("   보호구역 내부 업소 수:", inside_count)

# ----------------------------------------------------------
# 5. school_zone_map 생성
# ----------------------------------------------------------

print("4️⃣ school_zone_map 생성")

schools = safe_read_csv(SCHOOLS_PATH)

def explode_zone(df, col):
    tmp = df[["school_id", col]].copy()
    tmp[col] = tmp[col].fillna("")
    tmp = tmp[tmp[col] != ""]
    tmp["zone_id"] = tmp[col].str.split("|")
    tmp = tmp.explode("zone_id")
    tmp = tmp[["school_id", "zone_id"]]
    return tmp

abs_map = explode_zone(schools, "absolute_zone_ids")
rel_map = explode_zone(schools, "relative_zone_ids")

school_zone_map = pd.concat([abs_map, rel_map], ignore_index=True)
print("   zone → school 매핑 수:", len(school_zone_map))

# ----------------------------------------------------------
# 6. zone_id → school_id 매핑
# ----------------------------------------------------------

print("5️⃣ school_id 매핑")

biz_joined = biz_joined.merge(
    school_zone_map,
    on="zone_id",
    how="left"
)

print("   school_id 매핑 완료")

# ----------------------------------------------------------
# 7. 겹침 제거
# ----------------------------------------------------------

print("6️⃣ 중복 제거")

before = len(biz_joined)

biz_joined = biz_joined.drop_duplicates(
    subset=["biz_id", "school_id"]
)

after = len(biz_joined)

print("   제거된 행 수:", before - after)
print("   현재 행 수:", after)

# ----------------------------------------------------------
# 8. 최소 필드 유지
# ----------------------------------------------------------

print("7️⃣ 필드 정리")

biz_out = biz_joined[[
    "biz_id",
    "name",
    "school_id",
    "zone_id",
    "geometry"
]].copy()

biz_out = gpd.GeoDataFrame(
    biz_out,
    geometry="geometry",
    crs="EPSG:4326"
)

print("   school_id null 수:",
      biz_out["school_id"].isna().sum())

# ----------------------------------------------------------
# 9. 저장
# ----------------------------------------------------------

print("8️⃣ GeoJSON 저장")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

biz_out.to_file(OUTPUT_PATH, driver="GeoJSON")

print("✅ 저장 완료:", OUTPUT_PATH)
print("최종 업소 수:", len(biz_out))