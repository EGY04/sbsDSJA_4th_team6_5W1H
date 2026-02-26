############################################################
# build_biz_crawled_seoul.py
# 서울 크롤링 유해업소 → school_id 매핑 → GeoJSON 생성
############################################################

import os
import pandas as pd
import geopandas as gpd

# ----------------------------------------------------------
# 0. 경로 설정
# ----------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

CRAWL_PATH = os.path.join(
    BASE_DIR, "..", "data_raw", "biz",
    "crawling_data_true.csv"
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
    "geojson", "biz_crawled_seoul.geojson"
)

# ----------------------------------------------------------
# 1. 안전한 CSV 로드
# ----------------------------------------------------------

def safe_read_csv(path):
    for enc in ["utf-8-sig", "utf-8", "cp949"]:
        try:
            return pd.read_csv(path, encoding=enc)
        except Exception:
            continue
    raise ValueError("파일 인코딩 판별 실패")

print("1️⃣ 크롤링 데이터 로드")

crawl_df = safe_read_csv(CRAWL_PATH)
print("   원본 수:", len(crawl_df))

# ----------------------------------------------------------
# 2. 서울만 필터 (안전장치)
# ----------------------------------------------------------

crawl_df = crawl_df[crawl_df["sigu"].str.contains("서울", na=False)].copy()
print("   서울 필터 후:", len(crawl_df))

# ----------------------------------------------------------
# 3. biz_id 생성
# ----------------------------------------------------------

crawl_df = crawl_df.reset_index(drop=True)
crawl_df["biz_id"] = "C_" + crawl_df.index.astype(str)

# 이름 통일
crawl_df = crawl_df.rename(columns={
    "name": "name"
})

# ----------------------------------------------------------
# 4. GeoDataFrame 변환
# ----------------------------------------------------------

crawl_df = crawl_df.dropna(subset=["lat", "lng"]).copy()

crawl_gdf = gpd.GeoDataFrame(
    crawl_df,
    geometry=gpd.points_from_xy(
        crawl_df["lng"],
        crawl_df["lat"]
    ),
    crs="EPSG:4326"
)

print("   GeoDataFrame 변환 완료:", len(crawl_gdf))

# ----------------------------------------------------------
# 5. zones 로드
# ----------------------------------------------------------

print("2️⃣ zones 로드")

zones = gpd.read_file(ZONES_PATH)
print("   zones 수:", len(zones))

zones.sindex
crawl_gdf.sindex
print("   spatial index 완료")

# ----------------------------------------------------------
# 6. 공간조인
# ----------------------------------------------------------

print("3️⃣ 공간조인 시작")

crawl_joined = gpd.sjoin(
    crawl_gdf,
    zones,
    predicate="within",
    how="left"
)

inside_count = crawl_joined["zone_id"].notna().sum()
print("   보호구역 내부 크롤링 업소 수:", inside_count)

# ----------------------------------------------------------
# 7. school_zone_map 생성
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

print("   zone→school 매핑 수:", len(school_zone_map))

# ----------------------------------------------------------
# 8. school_id 매핑
# ----------------------------------------------------------

print("5️⃣ school_id 매핑")

crawl_joined = crawl_joined.merge(
    school_zone_map,
    on="zone_id",
    how="left"
)

# ----------------------------------------------------------
# 9. 겹침 제거
# ----------------------------------------------------------

print("6️⃣ 중복 제거")

before = len(crawl_joined)

crawl_joined = crawl_joined.drop_duplicates(
    subset=["biz_id", "school_id"]
)

after = len(crawl_joined)

print("   제거된 행 수:", before - after)
print("   현재 행 수:", after)

# ----------------------------------------------------------
# 10. 최소 필드 유지
# ----------------------------------------------------------

print("7️⃣ 필드 정리")

crawl_out = crawl_joined[[
    "biz_id",
    "name",
    "school_id",
    "zone_id",
    "geometry"
]].copy()

crawl_out = gpd.GeoDataFrame(
    crawl_out,
    geometry="geometry",
    crs="EPSG:4326"
)

print("   school_id null 수:",
      crawl_out["school_id"].isna().sum())

# ----------------------------------------------------------
# 11. 저장
# ----------------------------------------------------------

print("8️⃣ GeoJSON 저장")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

crawl_out.to_file(OUTPUT_PATH, driver="GeoJSON")

print("✅ 저장 완료:", OUTPUT_PATH)
print("최종 크롤링 업소 수:", len(crawl_out))