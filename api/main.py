"""
API layer: read-only access to normalized data for any downstream consumer
(a dashboard, another service, a notebook, whatever).
"""
import os
from datetime import datetime

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from pydantic import BaseModel

app = FastAPI(title="Real-Time Data Pipeline API", version="0.1.0")


def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


class Trade(BaseModel):
    symbol: str
    price: float
    quantity: float
    trade_time: datetime
    is_buyer_maker: bool


@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/trades/{symbol}", response_model=list[Trade])
def get_trades(symbol: str, limit: int = Query(default=50, le=500)):
    """Latest trades for a symbol, most recent first."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT symbol, price, quantity, trade_time, is_buyer_maker
                FROM crypto_trades
                WHERE symbol = %s
                ORDER BY trade_time DESC
                LIMIT %s
                """,
                (symbol.upper(), limit),
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/trades/{symbol}/latest", response_model=Trade)
def get_latest_trade(symbol: str):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT symbol, price, quantity, trade_time, is_buyer_maker
                FROM crypto_trades
                WHERE symbol = %s
                ORDER BY trade_time DESC
                LIMIT 1
                """,
                (symbol.upper(),),
            )
            return cur.fetchone()
    finally:
        conn.close()


@app.get("/symbols")
def get_symbols():
    """Which symbols currently have data."""
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT DISTINCT symbol FROM crypto_trades ORDER BY symbol")
            return {"symbols": [row[0] for row in cur.fetchall()]}
    finally:
        conn.close()
