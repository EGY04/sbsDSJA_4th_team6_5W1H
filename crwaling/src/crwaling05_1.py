import os
import re
import time
import random
import datetime
import pandas as pd
from seleniumbase import SB

BASE_DOMAIN = "https://opga037.com"
LOGIN_URL = BASE_DOMAIN + "/bbs/login.php"

LOGIN_ID = "ccl12345"
LOGIN_PW = "ccl12345"

AREA_NAME = "서울-강남"


# ======================
# 유틸
# ======================

def now_kst():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def extract_external_id(url):
    m = re.search(r"wr_id=(\d+)", url)
    return m.group(1) if m else None


def clean_name(name):
    name = re.sub(r"\s+", "", name)
    name = re.sub(r"[^\w가-힣]", "", name)
    return name.strip()


def build_area_url(area):
    return f"{BASE_DOMAIN}/bbs/board.php?bo_table=op_partner_posting&addrName={area}"


# ======================
# 스크롤
# ======================

def scroll_to_bottom(sb):
    print("스크롤 시작")
    click_count = 0

    while True:
        try:
            btn_text = sb.get_text("div.more_btn").strip()

            if "맨위로" in btn_text:
                print(f"마지막 도달 (총 {click_count}회 클릭)")
                break

            sb.click("div.more_btn")
            click_count += 1
            print(f"더보기 클릭 {click_count}회")
            sb.sleep(1.5)

        except Exception as e:
            print("더보기 버튼 종료:", e)
            break


def collect_detail_urls(sb):
    links = sb.find_elements("a[href*='wr_id=']")
    urls = []

    for a in links:
        href = a.get_attribute("href")
        if href and "wr_id=" in href:
            urls.append(href)

    unique = list(dict.fromkeys(urls))
    print(f"상세 URL {len(unique)}개 확보")
    return unique


# ======================
# 메인
# ======================

def main():

    results = []
    visited = set()
    crawled_at = now_kst()

    print("===== 로그인 시작 =====")

    with SB(uc=True, headed=True) as sb:

        sb.open(BASE_DOMAIN)
        sb.sleep(5)
        input("Cloudflare 확인 후 엔터")

        sb.open(LOGIN_URL)
        sb.type("#login_id", LOGIN_ID)
        sb.type("#login_pw", LOGIN_PW)
        sb.type("#login_pw", "enter")
        sb.sleep(4)

        # alert 처리
        try:
            sb.switch_to_alert()
            sb.accept_alert()
            sb.sleep(2)
        except:
            pass

        if "로그아웃" not in sb.get_page_source():
            print("로그인 실패")
            return

        print("로그인 성공")

        area_url = build_area_url(AREA_NAME)
        sb.open(area_url)
        sb.sleep(3)
        print(f"{AREA_NAME} 진입 완료")

        scroll_to_bottom(sb)

        detail_urls = collect_detail_urls(sb)
        total = len(detail_urls)

        print("===== 상세 페이지 수집 시작 =====")

        for idx, url in enumerate(detail_urls, start=1):

            if url in visited:
                continue

            visited.add(url)
            print(f"[{AREA_NAME}] {idx}/{total} 진행 중")

            try:
                sb.driver.get(url)
                sb.sleep(random.uniform(0.8, 1.5))

                page_text = sb.get_page_source()

                # 업소명
                try:
                    name = clean_name(sb.get_text("span.member"))
                except:
                    name = None

                # 업종
                try:
                    category = sb.get_text("#strBusinessName").strip()
                except:
                    category = None

                # 지역
                try:
                    address = sb.get_text("#strAreaName").strip()
                except:
                    address = AREA_NAME

                # 전화
                phone = None
                m = re.search(r"(0\d{1,2}-\d{3,4}-\d{4})", page_text)
                if m:
                    phone = m.group(1)

                results.append({
                    "source_site": BASE_DOMAIN,
                    "crawled_at": crawled_at,
                    "listing_url": area_url,
                    "detail_url": url,
                    "external_id": extract_external_id(url),
                    "name_raw": name,
                    "category_raw": category,
                    "address_raw": address,
                    "phone_number": phone,
                })

            except Exception as e:
                print("에러:", url)
                continue

    # 저장
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    ts = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    filename = f"opguide_gangnam_raw_{ts}.csv"
    save_path = os.path.join(data_dir, filename)

    pd.DataFrame(results).to_csv(save_path, index=False, encoding="utf-8-sig")

    print("수집 완료")
    print("총 건수:", len(results))
    print("저장:", save_path)


if __name__ == "__main__":
    main()
