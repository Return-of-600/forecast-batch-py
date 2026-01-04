import os
from typing import List, Dict, Any
import psycopg2
from psycopg2.extras import execute_values


class PostgresWriter:
    def __init__(self):
        dsn = os.getenv("PG_DSN")
        if not dsn:
            raise RuntimeError("PG_DSN is not set in environment/.env")
        self.dsn = dsn

    def upsert_kr_daily_price(self, rows: List[Dict[str, Any]], table: str = "kr_daily_price") -> int:
        """
        rows format (from kiwoom.collect_today_snapshot()):
          code, dt(YYYYMMDD), open, high, low, close, volume, market_cap, listed_shares, name(optional)
        Upsert key: (code, ymd)
        """
        if not rows:
            return 0

        values = []
        for r in rows:
            values.append((
                r["code"],
                r["dt"],
                int(r["open"]),
                int(r["high"]),
                int(r["low"]),
                int(r["close"]),
                int(r["volume"]),
                int(r.get("market_cap", 0)),
                int(r.get("listed_shares", 0)),
                r['name']
            ))

        sql = f"""
        INSERT INTO {table}
            (code, ymd, open, high, low, close, volume, market_cap, listed_shares, name)
        SELECT
            v.code,
            to_date(v.dt, 'YYYYMMDD') AS ymd,
            v.open,
            v.high,
            v.low,
            v.close,
            v.volume,
            v.market_cap,
            v.listed_shares,
            v.name        
        FROM (VALUES %s) AS v(code, dt, open, high, low, close, volume, market_cap, listed_shares, name)
        ON CONFLICT (code, ymd) DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low  = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            market_cap = EXCLUDED.market_cap,
            listed_shares = EXCLUDED.listed_shares,
            name = EXCLUDED.name
        ;
        """

        with psycopg2.connect(self.dsn) as conn:
            with conn.cursor() as cur:
                execute_values(cur, sql, values, page_size=1000)
            conn.commit()

        return len(rows)
