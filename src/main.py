from pathlib import Path
from dotenv import load_dotenv
import time

BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

from token_store import RedisTokenStore
from kiwoom import KiwoomAPI
from pg_writer import PostgresWriter


def run():
    t0 = time.perf_counter()

    # 1) 수집
    t_collect_0 = time.perf_counter()
    api = KiwoomAPI(RedisTokenStore())
    rows = api.collect_today_snapshot(
        markets=("0", "10"),
        qry_dt=None,      # None이면 오늘
        indc_tp="0",
        per_code_sleep=0.12,
    )
    t_collect_1 = time.perf_counter()

    collect_sec = t_collect_1 - t_collect_0
    print(f"[MAIN] collected rows={len(rows)}  time={collect_sec:.2f}s"
          + (f"  ({len(rows)/collect_sec:.2f} rows/s)" if collect_sec > 0 and rows else ""))

    # 2) 업서트
    t_upsert_0 = time.perf_counter()
    writer = PostgresWriter()
    n = writer.upsert_kr_daily_price(rows, table="kr_daily_price")
    t_upsert_1 = time.perf_counter()

    upsert_sec = t_upsert_1 - t_upsert_0
    print(f"[MAIN] upserted rows={n}  time={upsert_sec:.2f}s"
          + (f"  ({n/upsert_sec:.2f} rows/s)" if upsert_sec > 0 and n else ""))

    # 3) 전체
    total_sec = time.perf_counter() - t0
    print(f"[MAIN] total time={total_sec:.2f}s")


if __name__ == "__main__":
    run()
