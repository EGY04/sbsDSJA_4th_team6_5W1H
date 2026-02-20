import os
import re
import time
import random
import datetime
import pandas as pd
from seleniumbase import SB

BASE_DOMAIN = "https://opga037.com"
LOGIN_URL = BASE_DOMAIN + "/bbs/login.php"
BOARD_URL = BASE_DOMAIN + "/bbs/board.php?bo_table=op_partner_posting"

LOGIN_ID = "ccl12345"
LOGIN_PW = "ccl12345"

AREA_NAME = "서울-비강남"

def extract_external_id(url):
    m = re.search(r"wr_id=(\d+)", url)
    return m.group(1) if m else None

def clean_name(name):
    name = re.sub(r"\s+", "", name)
    name = re.sub(r"[^\w가-힣]", "", name)
    return name

def move_area(sb, area):

    if area == "서울-강남":
        url = BASE_DOMAIN + "/bbs/board.php?bo_table=op_partner_posting&addrName=서울-강남"
    elif area == "서울-비강남":
        url = BASE_DOMAIN + "/bbs/board.php?bo_table=op_partner_posting&addrName=서울-비강남"
    else:
        raise ValueError("Unknown area")

    sb.open(url)
    sb.sleep(2)


def scroll_to_bottom(sb):
    print("스크롤 시작")
    scroll_count = 0
    while True:
        try:
            btn = sb.find_element("css selector", "div.more_btn")
            txt = btn.text.strip()
            if "맨위로" in txt:
                print("마지막 페이지 도달")
                break
            btn.click()
            scroll_count += 1
            print(f"스크롤 {scroll_count}회 수행")
            sb.sleep(1.2)
        except:
            break

def collect_detail_urls(sb):
    links = sb.find_elements("css selector", "a[href*='wr_id=']")
    return list({l.get_attribute("href") for l in links})

def main():

    results = []
    visited = set()
    crawled_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    print("===== 로그인 시작 =====")

    with SB(uc=True, headed=True) as sb:

        sb.open(BASE_DOMAIN)
        sb.sleep(5)
        input("Cloudflare 확인 후 엔터")

        sb.open(LOGIN_URL)
        sb.type("#login_id", LOGIN_ID)
        sb.type("#login_pw", LOGIN_PW)
        sb.click("button[type='submit']")
        sb.sleep(3)

        print("로그인 성공")

        print(f"\n===== {AREA_NAME} 수집 시작 =====")

        move_area(sb, AREA_NAME)
        print(f"{AREA_NAME} 페이지 진입 완료")

        scroll_to_bottom(sb)

        detail_urls = collect_detail_urls(sb)
        total = len(detail_urls)

        print(f"상세 URL {total}개 확보")
        print("\n===== 상세 페이지 순회 시작 =====")

        for idx, url in enumerate(detail_urls, start=1):
            if url in visited:
                continue
            visited.add(url)

            print(f"[{AREA_NAME}] {idx}/{total} 수집 중...", end="\r")

            try:
                sb.open(url)
                sb.sleep(random.uniform(1.5, 2.5))

                try:
                    name = clean_name(sb.get_text("span.member"))
                except:
                    name = None

                try:
                    category = sb.get_text("#strBusinessName")
                except:
                    category = None

                try:
                    address = sb.get_text("#strAreaName")
                except:
                    address = AREA_NAME

                try:
                    phone = sb.get_text("a[href^='tel:']")
                except:
                    phone = None

                results.append({
                    "source_site": BASE_DOMAIN,
                    "crawled_at": crawled_at,
                    "listing_url": sb.get_current_url(),
                    "detail_url": url,
                    "external_id": extract_external_id(url),
                    "name_raw": name,
                    "category_raw": category,
                    "address_raw": address,
                    "phone_number": phone,
                })

            except:
                print("에러:", url)
                continue

    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    data_dir = os.path.join(project_root, "data")
    os.makedirs(data_dir, exist_ok=True)

    ts = datetime.datetime.now().strftime("%y%m%d_%H%M%S")
    filename = f"opguide_non_gangnam_raw_{ts}.csv"
    save_path = os.path.join(data_dir, filename)

    pd.DataFrame(results).to_csv(save_path, index=False, encoding="utf-8-sig")
    print("저장 완료:", save_path)

if __name__ == "__main__":
    main()
