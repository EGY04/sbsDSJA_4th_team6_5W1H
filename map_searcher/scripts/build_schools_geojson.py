############################################################
# build_schools_geojson.py
############################################################

import os
import json
import pandas as pd

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_PATH = os.path.join(BASE_DIR, "..", "data_processed", "csv_by_sido", "schools_all.csv")
OUTPUT_PATH = os.path.join(BASE_DIR, "..", "data_processed", "geojson", "schools_all.geojson")

os.makedirs(os.path.dirname(OUTPUT_PATH), exist_ok=True)

df = pd.read_csv(INPUT_PATH, encoding="utf-8")

# 🔥 NaN 제거
df = df.fillna({
    "absolute_zone_ids": "",
    "relative_zone_ids": "",
    "count": 0,
    "추가확인필요": False
})

df["count"] = df["count"].astype(int)
df["추가확인필요"] = df["추가확인필요"].astype(bool)

# NaN 존재 여부 확인
print("NaN 전체 개수:")
print(df.isna().sum())

print("\nNaN 포함 행 예시:")
print(df[df.isna().any(axis=1)].head())

features = []

for _, row in df.iterrows():
    features.append({
        "type": "Feature",
        "geometry": {
            "type": "Point",
            "coordinates": [float(row["lng"]), float(row["lat"])]
        },
        "properties": {
            "school_id": row["school_id"],
            "school_name": row["school_name"],
            "school_type": row["school_type"],
            "sido": row["sido"],
            "absolute_zone_ids": row["absolute_zone_ids"],
            "relative_zone_ids": row["relative_zone_ids"],
            "추가확인필요": bool(row["추가확인필요"]),
            "count": int(row["count"])
        }
    })

geojson = {
    "type": "FeatureCollection",
    "features": features
}

with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(geojson, f, ensure_ascii=False)

print("[INFO] GeoJSON 생성 완료:", OUTPUT_PATH)