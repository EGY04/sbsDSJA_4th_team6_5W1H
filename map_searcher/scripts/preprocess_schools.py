############################################################
# build_schools_national.py
# ----------------------------------------------------------
# 전국 schools 통합 정제 스크립트
# - raw schools 통합
# - 스키마 표준화
# - 추가확인필요 TRUE 학교에만 count 매핑
# - 최종 csv 생성
############################################################

import os
import glob
import pandas as pd

# ----------------------------------------------------------
# 1. 경로 설정
# ----------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

RAW_PATH = os.path.join(BASE_DIR, "..", "data_raw", "schools")
TRUE_COUNT_PATH = os.path.join(RAW_PATH, "true_count_schools.csv")
SAVE_PATH = os.path.join(BASE_DIR, "..", "data_processed", "csv_by_sido", "schools_all.csv")

# ----------------------------------------------------------
# 2. raw schools 파일 로드
# ----------------------------------------------------------

files = glob.glob(os.path.join(RAW_PATH, "schools_*.csv"))

print(f"[INFO] RAW_PATH: {RAW_PATH}")
print(f"[INFO] schools 파일 수: {len(files)}")

if not files:
    raise FileNotFoundError(f"[ERROR] schools 파일을 찾을 수 없음: {RAW_PATH}")

def safe_read_csv(path):
    for enc in ["utf-8", "cp949"]:
        try:
            return pd.read_csv(
                path,
                encoding=enc,
                sep=None,
                engine="python"
            )
        except UnicodeDecodeError:
            continue
    raise ValueError(f"파일 인코딩 오류: {path}")

df_list = []
for file in files:
    df = safe_read_csv(file)
    # 🔥 BOM 제거 + 공백 제거
    df.columns = (
        df.columns
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    df_list.append(df)

schools = pd.concat(df_list, ignore_index=True)
print("[INFO] 통합 완료:", schools.shape)


# ----------------------------------------------------------
# 3. 컬럼 구조 점검 및 표준화
# ----------------------------------------------------------

schools = schools.rename(columns={
    "기관명": "school_name",
    "학교구분": "school_type",
    "위도": "lat",
    "경도": "lng",
    "절대보호구역": "absolute_zone_ids",
    "상대보호구역": "relative_zone_ids"
})

required_cols = [
    "school_name",
    "school_type",
    "sido",
    "lat",
    "lng",
    "school_id",
    "absolute_zone_ids",
    "relative_zone_ids",
    "추가확인필요"
]

missing = [c for c in required_cols if c not in schools.columns]
if missing:
    raise ValueError(f"[ERROR] 누락 컬럼: {missing}")


# ----------------------------------------------------------
# 4. 데이터 타입 정제 (좌표 안전 파싱 버전)
# ----------------------------------------------------------

import re

# 원본 문자열 보관
schools["lat_raw"] = schools["lat"].astype(str).str.strip()
schools["lng_raw"] = schools["lng"].astype(str).str.strip()

# 1차 숫자 변환
schools["lat"] = pd.to_numeric(schools["lat_raw"], errors="coerce")
schools["lng"] = pd.to_numeric(schools["lng_raw"], errors="coerce")

# 숫자만 추출하는 정규식
num_pattern = re.compile(r"-?\d+(?:\.\d+)?")

def extract_float(val):
    match = num_pattern.search(str(val))
    return float(match.group()) if match else None

# 변환 실패한 경우 복구 시도
fail_mask = schools["lat"].isna() | schools["lng"].isna()

schools.loc[fail_mask, "lat"] = schools.loc[fail_mask, "lat_raw"].apply(extract_float)
schools.loc[fail_mask, "lng"] = schools.loc[fail_mask, "lng_raw"].apply(extract_float)

# 최종 결측 제거
before = len(schools)
schools = schools.dropna(subset=["lat", "lng"])
after = len(schools)

print(f"[INFO] 좌표 제거된 행 수: {before - after}")

# 보호구역 컬럼 정리
schools["absolute_zone_ids"] = schools["absolute_zone_ids"].fillna("")
schools["relative_zone_ids"] = schools["relative_zone_ids"].fillna("")

# 추가확인필요 boolean 변환
schools["추가확인필요"] = (
    schools["추가확인필요"]
    .astype(str)
    .str.upper()
    .eq("TRUE")
)

# 임시 raw 컬럼 제거
schools = schools.drop(columns=["lat_raw", "lng_raw"])

# ----------------------------------------------------------
# 5. count 컬럼 생성
# ----------------------------------------------------------

schools["count"] = 0


# ----------------------------------------------------------
# 6. TRUE 학교에만 count 매핑 (school_id 기준)
# ----------------------------------------------------------

if os.path.exists(TRUE_COUNT_PATH):

    true_count = safe_read_csv(TRUE_COUNT_PATH)

    # 필수 컬럼 확인
    required_true_cols = ["school_id", "count"]
    missing_true = [c for c in required_true_cols if c not in true_count.columns]

    if missing_true:
        raise ValueError(f"[ERROR] true_count 파일에 필요한 컬럼 없음: {missing_true}")

    # count_value로 이름 통일
    true_count = true_count.rename(columns={
        "count": "count_value"
    })

    # school_id 기준 left join
    schools = schools.merge(
        true_count[["school_id", "count_value"]],
        on="school_id",
        how="left"
    )

    # TRUE 학교에만 count 할당
    mask = schools["추가확인필요"]

    schools.loc[mask, "count"] = (
        schools.loc[mask, "count_value"]
        .fillna(0)
        .astype(int)
    )

    # 임시 컬럼 제거
    schools = schools.drop(columns=["count_value"])

    print("[INFO] TRUE 학교 count 매핑 완료 (school_id 기준)")

else:
    print("[WARN] true_count 파일 없음")

# ----------------------------------------------------------
# 7. 최종 스키마 구성
# ----------------------------------------------------------

schools_final = schools[[
    "school_id",
    "school_name",
    "school_type",
    "sido",
    "lat",
    "lng",
    "absolute_zone_ids",
    "relative_zone_ids",
    "추가확인필요",
    "count"
]].copy()


# ----------------------------------------------------------
# 8. 저장
# ----------------------------------------------------------

os.makedirs(os.path.dirname(SAVE_PATH), exist_ok=True)
schools_final.to_csv(SAVE_PATH, index=False, encoding="utf-8-sig")

print("[INFO] 저장 완료:", SAVE_PATH)
print("[INFO] 최종 shape:", schools_final.shape)