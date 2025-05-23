import os
import json

# JSON에서 슬랙 사용자 매핑 불러오기
with open("slack_users.json", "r", encoding="utf-8") as f:
    slack_id_map = json.load(f)

import requests
import xml.etree.ElementTree as ET
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from datetime import datetime

# Slack Webhook
SLACK_WEBHOOK_URL = os.environ["SLACK_WEBHOOK_URL"]

# ✅ Google Sheets 인증 (환경변수 기반)
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
creds_dict = json.loads(os.environ["GOOGLE_CREDENTIALS"])
creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
client = gspread.authorize(creds)

# 구글 시트 연결
spreadsheet = client.open_by_key("1zC3C1lIwTUSUrfxPbuQrNur3auclc2k_CUq6RcfqLV8")
main_ws = spreadsheet.worksheet("상태조회")
log_ws = spreadsheet.worksheet("상태로그")

# 유니패스 API 키
API_KEY = "i270g245b044o067e040h090r0"

# 기존 로그 불러오기 (중복 방지용 + 조회제외 조건)
existing_logs = log_ws.get_all_values()[1:]
logged_set = set((row[0], row[1], row[3], row[4]) for row in existing_logs if len(row) >= 5)

# 🚫 '수입신고 수리 후 반출' 건은 조회 제외 대상
excluded_hbls = set(
    (row[0], row[1])
    for row in existing_logs
    if len(row) >= 6 and row[5] == "수입신고 수리 후 반출"
)

# 상태조회 시트 데이터 (3행부터)
data = main_ws.get_all_values()[2:]

def format_date(value):
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except:
        return value

def generate_slack_message(hbl_no, mbl_no, origin, destination, complete_time, slack_user_id):
    mention_block = f"<@{slack_user_id}>" if slack_user_id else ""
    message = f"""
🚨 *하선신고 수리 완료!*

*🕒 완료시각:* `{complete_time}`
*🗺️ 구간:* `{origin} → {destination}`  
*📦 HB/L:* `{hbl_no}`  
*📑 MB/L:* `{mbl_no}`

📣 {mention_block} *인보이스 작업 시작해주세요!*
    """.strip()
    return {"text": message}

for idx, row in enumerate(data, start=3):
    hbl_no = row[0].strip()
    bl_yy = row[1].strip()
    slack_user_id = row[2].strip()
    slack_user_id = slack_id_map.get(slack_user_id, slack_user_id)


    if not hbl_no or not bl_yy:
        continue

    # 🚫 제외 대상이면 건너뜀
    if (hbl_no, bl_yy) in excluded_hbls:
        continue

    try:
        url = (
            f"https://unipass.customs.go.kr:38010/ext/rest/cargCsclPrgsInfoQry/retrieveCargCsclPrgsInfo"
            f"?crkyCn={API_KEY}&hblNo={hbl_no}&blYy={bl_yy}"
        )
        response = requests.get(url)
        root = ET.fromstring(response.content)

        main = root.find("cargCsclPrgsInfoQryVo")
        details = root.findall("cargCsclPrgsInfoDtlQryVo")

        # 상태조회 정보 초기화
        etprDt = csclPrgsStts = prcsDttm_main = mtYn = ""
        tpcd = rlbrDttm = rlbrCn = shedNm = prcsDttm_detail = ""
        mblNo = ldprNm = dsprNm = ""

        # 기본값으로 update_row 초기화
        update_row = [""] * 11

        if main is not None:
            etprDt = main.findtext("etprDt", "")
            csclPrgsStts = main.findtext("csclPrgsStts", "")
            prcsDttm_main = format_date(main.findtext("prcsDttm", ""))
            mtYn = main.findtext("mtTrgtCargYnNm", "")
            mblNo = main.findtext("mblNo", "")
            ldprNm = main.findtext("ldprNm", "")
            dsprNm = main.findtext("dsprNm", "")

            if details:
                latest = details[0]
                tpcd = latest.findtext("cargTrcnRelaBsopTpcd", "")
                rlbrDttm = latest.findtext("rlbrDttm", "")
                rlbrCn = latest.findtext("rlbrCn", "")
                shedNm = latest.findtext("shedNm", "")
                prcsDttm_detail = format_date(latest.findtext("prcsDttm", ""))

            update_row = [
                etprDt, csclPrgsStts, prcsDttm_main, mtYn, tpcd,
                rlbrDttm, rlbrCn, shedNm, prcsDttm_detail, mblNo, ldprNm, dsprNm
            ]
            main_ws.update(f"D{idx}:O{idx}", [update_row])

        # 상태로그 + 슬랙 알림
        new_logs = []
        if details:
            for d in details:
                event_type = d.findtext("cargTrcnRelaBsopTpcd", "")
                log_time = format_date(d.findtext("prcsDttm", ""))
                release_time = format_date(d.findtext("rlbrDttm", ""))
                release_content = d.findtext("rlbrCn", "")
                key = (hbl_no, bl_yy, event_type, release_time)

                if event_type and key not in logged_set:
                    new_logs.append([hbl_no, bl_yy, log_time, event_type, release_time, release_content])
                    logged_set.add(key)

                    if event_type == "하선신고 수리":
                        payload = generate_slack_message(
                            hbl_no=hbl_no,
                            mbl_no=mblNo,
                            origin=ldprNm,
                            destination=dsprNm,
                            complete_time=release_time or log_time,
                            slack_user_id=slack_user_id
                        )
                        requests.post(SLACK_WEBHOOK_URL, json=payload)

        else:
            new_logs.append([hbl_no, bl_yy, "", "처리이력 없음", "", ""])

        if new_logs:
            log_ws.append_rows(new_logs)

    except Exception as e:
        print(f"❌ 오류 발생 (B/L: {hbl_no}, 연도: {bl_yy}): {e}")

print("🎉 최종 실행 완료: 상태조회 + 상태로그 + 슬랙알림 + 종료조건")
