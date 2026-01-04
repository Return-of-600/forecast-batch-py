# src/kiwoom.py
import os
import re
import time
import random
import requests
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, Optional, Tuple, List

from token_store import RedisTokenStore

KST = timezone(timedelta(hours=9))

# ====== 키움 엔드포인트 ======
ENDPOINT_TOKEN = "/oauth2/token"         # au10001
ENDPOINT_REVOKE = "/oauth2/revoke"       # au10002
ENDPOINT_CHART = "/api/dostk/chart"      # ka10081 (백업용)
ENDPOINT_STKINFO = "/api/dostk/stkinfo"  # ka10001, ka10099
ENDPOINT_MRKCOND = "/api/dostk/mrkcond"  # ka10086

# ====== 운영 파라미터(레이트리밋/재시도) ======
MAX_RETRIES = 8
BASE_SLEEP = 0.6
MAX_SLEEP = 15.0
SUCCESS_SLEEP_SEC = 0.15  # 429 완화용(성공 시에도 약간 쉼)


def ttl_from_expires_dt(expires_dt: str, safety_margin: int = 30) -> int:
    """
    expires_dt: 'YYYYMMDDHHMMSS' (KST 기준)
    TTL = expires_dt - now(KST) - safety_margin
    """
    exp = datetime.strptime(expires_dt, "%Y%m%d%H%M%S").replace(tzinfo=KST)
    now = datetime.now(KST)
    ttl = int((exp - now).total_seconds()) - safety_margin
    return max(60, ttl)


def _normalize_int(x: Any) -> int:
    if x is None:
        return 0
    if isinstance(x, (int, float)):
        return int(x)
    s = str(x).replace(",", "").strip()
    if s == "":
        return 0
    return int(float(s))


# =========================
# 종목 필터 (insert.py에서 이식)
# =========================
ETF_BRANDS = [
    "TIGER", "KODEX", "KOSEF", "KBSTAR", "ARIRANG", "HANARO", "SOL",
    "ACE", "TIMEFOLIO", "TREX", "KINDEX", "RISE", "FOCUS", "PLUS",
    "UNICORN", "1Q", "QV", "KIWOOM", "KB발해"
]

NON_STOCK_TOKENS = [
    "ETF", "ETN", "ETC", "리츠", "REIT", "스팩", "SPAC", "우선",
    "선물", "인버스", "레버리지", "커버드콜", "합성",
    "(H)", "H)", "환헤지", "헤지",
    "S&P", "NASDAQ", "NIKKEI", "DAX", "EURO", "KOSPI200", "코스피200", "코스피", "200", "100", "미국", "액티브", "BNK", "부동산",
    "국채", "채권", "단기채", "중기채", "장기채",
    "원유", "WTI", "브렌트", "금", "은", "구리", "철강", "곡물",
    "달러", "USD", "엔", "JPY", "유로", "EUR", "위안", "CNY",
]

PREFERRED_REGEX = re.compile(r"(?:\d*우[B-C]?|우선|우선주)", re.IGNORECASE)
NON_STOCK_REGEX = re.compile(r"(?:\b[23]X\b|레버리지|인버스|선물|커버드콜|합성|\(H\)|ETF|ETN|REIT|리츠|SPAC|스팩|우선)", re.IGNORECASE)
KR_CODE_REGEX = re.compile(r"^\d{6}$")


def is_non_common_stock(name: str, company_class: str = "") -> bool:
    n = (name or "").upper()
    c = (company_class or "").upper()

    # 우선주 컷
    if PREFERRED_REGEX.search(n):
        return True

    # 회사분류 힌트 컷
    if any(x in c for x in ["스팩", "SPAC", "리츠", "REIT", "ETF", "ETN"]):
        return True

    # 브랜드 컷
    if any(b in n for b in ETF_BRANDS):
        return True

    # 토큰 컷
    if any(t.upper() in n for t in NON_STOCK_TOKENS):
        return True

    # 정규식 컷
    if NON_STOCK_REGEX.search(n):
        return True

    return False


def filter_stock_list(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    ka10099 list 결과에서:
    - 6자리 코드만
    - ETF/ETN/리츠/스팩/우선주/파생 등 제외
    """
    out: List[Dict[str, Any]] = []
    for item in items:
        code = str(item.get("code", "")).strip()
        name = str(item.get("name", "")).strip()
        company_class = str(item.get("companyClassName", "")).strip()

        if not KR_CODE_REGEX.fullmatch(code):
            continue

        if is_non_common_stock(name, company_class):
            continue

        out.append(item)
    return out


class KiwoomAPI:
    def __init__(self, token_store: RedisTokenStore):
        self.host = os.getenv("KIWOOM_HOST")
        if not self.host:
            raise RuntimeError("KIWOOM_HOST is not set. Check .env loading.")
        self.token_store = token_store

    # -------------------------
    # 1) 토큰
    # -------------------------
    def fn_au10001(self, data: Dict[str, Any]) -> dict:
        """접근토큰 발급 + Redis 저장"""
        url = self.host + ENDPOINT_TOKEN
        headers = {"Content-Type": "application/json;charset=UTF-8"}

        r = requests.post(url, headers=headers, json=data, timeout=10)
        r.raise_for_status()
        body = r.json()

        token = body.get("token")
        expires_dt = body.get("expires_dt")
        token_type = body.get("token_type", "Bearer")

        if not token or not expires_dt:
            raise RuntimeError(f"Token issuance failed. body={body}")

        ttl = ttl_from_expires_dt(expires_dt)
        self.token_store.set_token(token=token, ttl_seconds=ttl, token_type=token_type)

        # 민감정보(토큰) 출력 금지
        print(f"[KIWOOM] token saved to redis ttl={ttl} expires_dt={expires_dt}")
        return body

    def fn_au10002(self, data: Dict[str, Any]) -> dict:
        """접근토큰 폐기"""
        url = self.host + ENDPOINT_REVOKE
        headers = {"Content-Type": "application/json;charset=UTF-8"}

        r = requests.post(url, headers=headers, json=data, timeout=10)
        r.raise_for_status()
        return r.json()

    def get_access_token(self) -> str:
        """
        Redis에 토큰이 있으면 재사용.
        없으면 au10001로 발급 후 Redis 저장.
        """
        cached = self.token_store.get_token()
        if cached and cached.get("token"):
            return cached["token"]

        params = {
            "grant_type": "client_credentials",
            "appkey": os.getenv("KIWOOM_APP_KEY"),
            "secretkey": os.getenv("KIWOOM_SECRET_KEY"),
        }
        self.fn_au10001(params)

        cached = self.token_store.get_token()
        if not cached or not cached.get("token"):
            raise RuntimeError("Token issuance succeeded but token not found in Redis.")
        return cached["token"]

    # -------------------------
    # 2) 공통 요청 (POST + retry)
    # -------------------------
    def _post_tr(
        self,
        api_id: str,
        endpoint: str,
        payload: Dict[str, Any],
        cont_yn: str = "N",
        next_key: str = "",
    ) -> Tuple[Dict[str, Any], Dict[str, str]]:
        token = self.get_access_token()

        url = self.host + endpoint
        headers = {
            "Content-Type": "application/json;charset=UTF-8",
            "authorization": f"Bearer {token}",
            "cont-yn": cont_yn,
            "next-key": next_key,
            "api-id": api_id,
        }

        backoff = BASE_SLEEP
        for attempt in range(1, MAX_RETRIES + 1):
            r = requests.post(url, headers=headers, json=payload, timeout=20)

            if r.status_code == 200:
                return r.json(), dict(r.headers)

            # 레이트리밋/일시 오류 대응
            if r.status_code in (429, 500, 502, 503, 504):
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        sleep_s = float(ra)
                    except ValueError:
                        sleep_s = backoff
                else:
                    sleep_s = min(MAX_SLEEP, backoff) + random.uniform(0, 0.3)

                time.sleep(sleep_s)
                backoff = min(MAX_SLEEP, backoff * 2)
                continue

            raise RuntimeError(f"[{api_id}] HTTP {r.status_code} body={r.text[:800]}")

        raise RuntimeError(f"[{api_id}] retry exhausted")

    # -------------------------
    # 3) ka10099: 종목 리스트 조회 + 필터 적용
    # -------------------------
    def fn_ka10099_stock_list(self, markets: Tuple[str, ...] = ("0", "10")) -> List[Dict[str, Any]]:
        """
        markets:
          - "0": 코스피
          - "10": 코스닥
        반환: 필터된 종목 리스트
        """
        all_items: List[Dict[str, Any]] = []

        for mrkt in markets:
            payload = {"mrkt_tp": mrkt}
            body, _ = self._post_tr(api_id="ka10099", endpoint=ENDPOINT_STKINFO, payload=payload)
            lst = body.get("list") or []
            if isinstance(lst, list):
                all_items.extend(lst)

            time.sleep(SUCCESS_SLEEP_SEC)

        return filter_stock_list(all_items)

    # -------------------------
    # 4) ka10086: 일별 주가(날짜 기준 1건)  ✅ 배치 기본
    # -------------------------
    def fn_ka10086_daily(
        self,
        stk_cd: str,
        qry_dt: str,
        indc_tp: str = "0",  # 0: 수량, 1: 금액(백만)
    ) -> Optional[Dict[str, Any]]:
        """
        ka10086: 일별주가요청
        반환: 해당 일자의 row dict (없으면 None)

        응답:
          body["daly_stkpc"] = [ { date, open_pric, high_pric, low_pric, close_pric, trde_qty, ... } ]
        """
        payload = {
            "stk_cd": stk_cd,
            "qry_dt": qry_dt,
            "indc_tp": indc_tp,
        }

        body, _ = self._post_tr(api_id="ka10086", endpoint=ENDPOINT_MRKCOND, payload=payload)
        rows = body.get("daly_stkpc") or []
        if not rows:
            return None

        return rows[0]

    # -------------------------
    # 5) ka10081: 종목별 최신 1건(백업용)
    # -------------------------
    def fn_ka10081_latest(self, stk_cd: str, base_dt: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        ka10086 장애/제약 시 fallback 용도.
        """
        if base_dt is None:
            base_dt = datetime.now(KST).strftime("%Y%m%d")

        payload = {
            "stk_cd": stk_cd,
            "base_dt": base_dt,
            "upd_stkpc_tp": "1",
        }

        body, _ = self._post_tr(api_id="ka10081", endpoint=ENDPOINT_CHART, payload=payload)
        rows = body.get("stk_dt_pole_chart_qry") or []
        if not rows:
            return None
        return rows[0]

    # -------------------------
    # 6) ka10001: 기본정보(상장주식수/시총)
    # -------------------------
    def fn_ka10001_basic(self, stk_cd: str) -> dict:
        """
        ka10001 응답 기준 (단위 보정)
          - flo_stk: 천주 → 주
          - mac: 억원 → 원
        """
        payload = {"stk_cd": stk_cd}
        body, _ = self._post_tr(api_id="ka10001", endpoint=ENDPOINT_STKINFO, payload=payload)

        flo_stk_raw = _normalize_int(body.get("flo_stk"))  # 천주
        mac_raw = _normalize_int(body.get("mac"))  # 억원

        listed_shares = abs(flo_stk_raw) * 1_000  # 주
        market_cap = abs(mac_raw) * 100_000_000  # 원

        return {
            "listed_shares": listed_shares,
            "market_cap": market_cap,
            "stk_nm": body.get("stk_nm"),
        }

    # -------------------------
    # 7) 배치 스냅샷 생성(저장X) ✅ ka10086 기반
    # -------------------------
    def collect_today_snapshot(
        self,
        markets: Tuple[str, ...] = ("0", "10"),
        qry_dt: Optional[str] = None,
        indc_tp: str = "0",
        per_code_sleep: float = 0.12,
    ) -> List[Dict[str, Any]]:
        """
        최종 산출:
          - 종목필터된 code에 대해
          - ka10086 (qry_dt 하루 데이터)
          - ka10001 (flo_stk, mac)
          - 병합한 row 리스트를 반환 (DB 저장은 외부에서)

        반환 row:
          {
            code, dt(=qry_dt),
            open, high, low, close, volume,
            listed_shares, market_cap, name
          }
        """
        if qry_dt is None:
            qry_dt = datetime.now(KST).strftime("%Y%m%d")

        stocks = self.fn_ka10099_stock_list(markets=markets)
        codes = [s["code"] for s in stocks]
        print(f"[KIWOOM] filtered codes={len(codes)} qry_dt={qry_dt}")

        out: List[Dict[str, Any]] = []

        for idx, code in enumerate(codes, 1):
            try:
                daily = self.fn_ka10086_daily(code, qry_dt=qry_dt, indc_tp=indc_tp)
                if not daily:
                    continue

                basic = self.fn_ka10001_basic(code)

                row = {
                    "code": code,
                    "dt": qry_dt,  # YYYYMMDD (DB에서 date로 변환)
                    "open": abs(_normalize_int(daily.get("open_pric"))),
                    "high": abs(_normalize_int(daily.get("high_pric"))),
                    "low":abs(_normalize_int(daily.get("low_pric"))),
                    "close": abs(_normalize_int(daily.get("close_pric"))),
                    "volume": abs(_normalize_int(daily.get("trde_qty"))),
                    "listed_shares": int(basic["listed_shares"]),
                    "market_cap": int(basic["market_cap"]),
                    "name": basic.get("stk_nm") or "",
                }
                out.append(row)

                if idx % 100 == 0:
                    print(f"[KIWOOM] progress {idx}/{len(codes)} collected={len(out)}")

            except Exception as e:
                print(f"[KIWOOM][ERROR] code={code} {e}")

            time.sleep(per_code_sleep)

        return out


if __name__ == "__main__":
    # 로컬 단독 테스트용(운영 배치에서는 main.py에서 import 해서 호출)
    store = RedisTokenStore()
    api = KiwoomAPI(token_store=store)

    rows = api.collect_today_snapshot(
        markets=("0", "10"),
        qry_dt=None,       # None이면 오늘
        indc_tp="0",
        per_code_sleep=0.12,
    )

    print(f"snapshot rows={len(rows)}")
    if rows:
        print("sample:", {k: rows[0][k] for k in ["code", "dt", "close", "market_cap", "listed_shares", "name"]})
