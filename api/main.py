"""
API layer: read-only access to normalized data for any downstream consumer
(a dashboard, another service, a notebook, whatever).
"""
import os
from datetime import datetime

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

app = FastAPI(title="Real-Time Data Pipeline API", version="0.1.0")

# Allow the React dev server (and containerized frontend) to call this API
# from the browser. In a real production deploy you'd lock allow_origins
# down to your actual frontend domain instead of "*".
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Auto-instruments every route: request count, latency, in-progress
# requests, all exposed at /metrics for Prometheus to scrape.
Instrumentator().instrument(app).expose(app)


def get_conn():
    return psycopg2.connect(os.environ["DATABASE_URL"])


class Trade(BaseModel):
    symbol: str
    price: float
    quantity: float
    trade_time: datetime
    is_buyer_maker: bool


class Article(BaseModel):
    article_id: str
    title: str
    summary: str | None = None
    link: str | None = None
    author: str | None = None
    received_at: datetime


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


@app.get("/ticker", response_model=list[Trade])
def get_ticker():
    """Latest trade for every symbol that has data - powers a price ticker bar."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT DISTINCT ON (symbol)
                    symbol, price, quantity, trade_time, is_buyer_maker
                FROM crypto_trades
                ORDER BY symbol, trade_time DESC
                """
            )
            return cur.fetchall()
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


@app.get("/news", response_model=list[Article])
def get_news(limit: int = Query(default=20, le=100)):
    """Latest news articles, most recent first - second source, same API shape as /trades."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT article_id, title, summary, link, author, received_at
                FROM news_articles
                ORDER BY received_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            return cur.fetchall()
    finally:
        conn.close()
