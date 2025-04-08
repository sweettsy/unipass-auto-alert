import os
import json
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

# 시트 연결
spreadsheet = client.open_by_key("1zC3C1lIwTUSUrfxPbuQrNur3auclc2k_CUq6RcfqLV8")
main_ws = spreadsheet.worksheet("상태조회")
log_ws = spreadsheet.worksheet("상태로그")

# 유니패스 API 키
API_KEY = "i270g245b044o067e040h090r0"

# 이전 로그 및 조건 체크
existing_logs = log_ws.get_all_values()[1:]
logged_keys = set((row[0], row[1], row[3], row[4]) for row in existing_logs if len(row) >= 5)
excluded_hbls = set((row[0], row[1]) for row in existing_logs if len(row) >= 6 and row[5] == "수입신고 수리후 반출")
existing_unloads = set((row[0], row[1]) for row in existing_logs if row[3] == "하선신고 수리")

# 상태조회 대상 (3행부터)
data = main_ws.get_all_values()[2:]

def format_date(value):
    try:
        return datetime.strptime(value, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
    except:
        return value

def generate_slack_message(hbl_no, mbl_no, origin, destination, complete_time, slack_user_id):
    mention_block = f"<@{slack_user_id}>" if slack_user_id else ""
    return {
        "text": f"""
🚨 *하선신고 수리 완료!*

*🕒 완료시각:* `{complete_time}`
*🗺️ 구간:* `{origin} → {destination}`  
*📦 HB/L:* `{hbl_no}`  
*📑 MB/L:* `{mbl_no}`

📣 {mention_block} *인보이스 작업 시작해주세요!*
        """.strip()
    }

for idx, row in enumerate(data, start=3):
    hbl_no = row[0].strip()
    bl_yy = row[1].strip()
    slack_user_id = row[2].strip()

    if not hbl_no or not bl_yy or (hbl_no, bl_yy) in excluded_hbls:
        continue

    try:
        print(f"\n🔍 [{idx}] {hbl_no} - {bl_yy}")
        url = (
            f"https://unipass.customs.go.kr:38010/ext/rest/cargCsclPrgsInfoQry/retrieveCargCsclPrgsInfo"
            f"?crkyCn={API_KEY}&hblNo={hbl_no}&blYy={bl_yy}"
        )
        response = requests.get(url)
        root = ET.fromstring(response.content)

        main = root.find("cargCsclPrgsInfoQryVo")
        details = root.findall("cargCsclPrgsInfoDtlQryVo")

        etprDt = csclPrgsStts = prcsDttm_main = mtYn = ""
        tpcd = rlbrDttm = rlbrCn = shedNm = prcsDttm_detail = ""
        mblNo = ldprNm = dsprNm = ""

        if main is not None:
            etprDt = main.findtext("etprDt", "")
            csclPrgsStts = main.findtext("csclPrgsStts", "")
            prcsDttm_main = format_date(main.findtext("prcsDttm", ""))
            mtYn = main.findtext("mtTrgtCargYnNm", "")
            mblNo = main.findtext("mblNo", "")
            ldprNm = main.findtext("ldprNm", "")
            dsprNm = main.findtext("dsprNm", "")
            print(f"✅ 요약상태: {csclPrgsStts} / MBL: {mblNo}")

        if details:
            latest = details[0]
            tpcd = latest.findtext("cargTrcnRelaBsopTpcd", "")
            rlbrDttm = latest.findtext("rlbrDttm", "")
            rlbrCn = latest.findtext("rlbrCn", "")
            shedNm = latest.findtext("shedNm", "")
            prcsDttm_detail = format_date(latest.findtext("prcsDttm", ""))

        main_ws.update(range_name=f"D{idx}:O{idx}", values=[[
            etprDt, csclPrgsStts, prcsDttm_main, mtYn, tpcd,
            rlbrDttm, rlbrCn, shedNm, prcsDttm_detail, mblNo, ldprNm, dsprNm
        ]])

        new_logs = []
        for d in details:
            event_type = d.findtext("cargTrcnRelaBsopTpcd", "")
            log_time = format_date(d.findtext("prcsDttm", ""))
            release_time = format_date(d.findtext("rlbrDttm", ""))
            release_content = d.findtext("rlbrCn", "")
            print(f"🔍 검사 중 이벤트: {event_type}, 시각: {log_time}, 내용: {release_content}")
            key = (hbl_no, bl_yy, event_type, release_time)

            if key not in logged_keys:
                new_logs.append([hbl_no, bl_yy, log_time, event_type, release_time, release_content])
                logged_keys.add(key)

                if event_type == "하선신고 수리" and (hbl_no, bl_yy) not in existing_unloads:
                    payload = generate_slack_message(hbl_no, mblNo, ldprNm, dsprNm, release_time or log_time, slack_user_id)
                    requests.post(SLACK_WEBHOOK_URL, json=payload)
                    existing_unloads.add((hbl_no, bl_yy))

        if new_logs:
            log_ws.append_rows(new_logs)

    except Exception as e:
        print(f"❌ 오류 발생 (B/L: {hbl_no}, 연도: {bl_yy}): {e}")

print("✅ 전체 실행 완료")
