"""
API layer: read-only access to normalized data for any downstream consumer
(a dashboard, another service, a notebook, whatever).
"""
import os
from datetime import datetime

import psycopg2
import psycopg2.extras
from fastapi import Depends, FastAPI, HTTPException, Query, status
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator
from pydantic import BaseModel

from api.auth import CurrentUser, create_access_token, get_current_user, hash_password, verify_password

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
    feed_source: str | None = None
    received_at: datetime


class Transaction(BaseModel):
    product_name: str
    store_id: str | None = None
    quantity: int | None = None
    unit_price: float | None = None
    total_amount: float | None = None
    transaction_date: datetime | None = None
    payment_method: str | None = None
    source_file: str | None = None
    received_at: datetime


class GLAccount(BaseModel):
    id: int
    code: str
    name: str
    account_type: str


class CreateGLAccountRequest(BaseModel):
    code: str
    name: str
    account_type: str = "expense"


class GLMapping(BaseModel):
    id: int
    source_system: str
    source_category: str
    gl_account_id: int
    gl_account_code: str
    gl_account_name: str


class CreateGLMappingRequest(BaseModel):
    source_system: str
    source_category: str
    gl_account_id: int


class GLTransaction(BaseModel):
    source_system: str
    source_category: str
    canonical_gl_account_id: int | None = None
    canonical_gl_code: str | None = None
    canonical_gl_name: str | None = None
    description: str | None = None
    amount: float | None = None
    transaction_date: datetime | None = None
    source_file: str | None = None
    received_at: datetime


class UnmappedCategory(BaseModel):
    source_system: str
    source_category: str
    transaction_count: int
    total_amount: float | None = None


class SignupRequest(BaseModel):
    org_name: str
    email: str
    password: str
    industry: str = "general"  # 'retail' | 'crypto' | 'general' - drives which panels are relevant


class LoginRequest(BaseModel):
    email: str
    password: str


class TokenResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    org_name: str
    industry: str


class MeResponse(BaseModel):
    email: str
    org_id: int
    org_name: str
    industry: str


@app.get("/health")
def health():
    return {"status": "ok"}


def _slugify(name: str) -> str:
    import re
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "org"


@app.post("/auth/signup", response_model=TokenResponse)
def signup(payload: SignupRequest):
    """
    Creates a brand new organization AND its first user in one step - this
    is the "sign up your business" flow. Every org is created with a
    unique slug derived from its name; a numeric suffix is appended if
    that slug's already taken.
    """
    if payload.industry not in ("retail", "crypto", "general"):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid industry")

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT id FROM users WHERE email = %s", (payload.email,))
            if cur.fetchone():
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Email already registered")

            base_slug = _slugify(payload.org_name)
            slug = base_slug
            suffix = 1
            while True:
                cur.execute("SELECT id FROM organizations WHERE slug = %s", (slug,))
                if not cur.fetchone():
                    break
                suffix += 1
                slug = f"{base_slug}-{suffix}"

            cur.execute(
                "INSERT INTO organizations (name, slug, industry) VALUES (%s, %s, %s) RETURNING id",
                (payload.org_name, slug, payload.industry),
            )
            org_id = cur.fetchone()[0]

            password_hash = hash_password(payload.password)
            cur.execute(
                "INSERT INTO users (org_id, email, password_hash, role) VALUES (%s, %s, %s, 'owner') RETURNING id",
                (org_id, payload.email, password_hash),
            )
            user_id = cur.fetchone()[0]
            conn.commit()

        token = create_access_token(user_id=user_id, org_id=org_id, email=payload.email)
        return TokenResponse(access_token=token, org_name=payload.org_name, industry=payload.industry)
    finally:
        conn.close()


@app.post("/auth/login", response_model=TokenResponse)
def login(payload: LoginRequest):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT u.id, u.org_id, u.password_hash, o.name AS org_name, o.industry
                FROM users u JOIN organizations o ON o.id = u.org_id
                WHERE u.email = %s
                """,
                (payload.email,),
            )
            user = cur.fetchone()

        if not user or not verify_password(payload.password, user["password_hash"]):
            raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid email or password")

        token = create_access_token(user_id=user["id"], org_id=user["org_id"], email=payload.email)
        return TokenResponse(access_token=token, org_name=user["org_name"], industry=user["industry"])
    finally:
        conn.close()


@app.get("/auth/me", response_model=MeResponse)
def me(current_user: CurrentUser = Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT name, industry FROM organizations WHERE id = %s", (current_user.org_id,))
            org_name, industry = cur.fetchone()
        return MeResponse(email=current_user.email, org_id=current_user.org_id, org_name=org_name, industry=industry)
    finally:
        conn.close()


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
def get_news(limit: int = Query(default=20, le=100), feed: str | None = None):
    """
    Latest news articles, most recent first. Optionally filter to one feed
    (e.g. ?feed=world) now that coverage spans multiple global sources.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            if feed:
                cur.execute(
                    """
                    SELECT article_id, title, summary, link, author, feed_source, received_at
                    FROM news_articles
                    WHERE feed_source = %s
                    ORDER BY received_at DESC
                    LIMIT %s
                    """,
                    (feed, limit),
                )
            else:
                cur.execute(
                    """
                    SELECT article_id, title, summary, link, author, feed_source, received_at
                    FROM news_articles
                    ORDER BY received_at DESC
                    LIMIT %s
                    """,
                    (limit,),
                )
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/retail", response_model=list[Transaction])
def get_retail_transactions(
    limit: int = Query(default=50, le=500),
    current_user: CurrentUser = Depends(get_current_user),
):
    """
    Latest retail transactions for the CALLER'S organization only. This is
    the actual multi-tenant boundary: the query is filtered by
    current_user.org_id, which comes from the verified JWT, not from
    anything the client sends - a user can't pass a different org_id and
    see someone else's data.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT product_name, store_id, quantity, unit_price, total_amount,
                       transaction_date, payment_method, source_file, received_at
                FROM retail_transactions
                WHERE org_id = %s
                ORDER BY received_at DESC
                LIMIT %s
                """,
                (current_user.org_id, limit),
            )
            return cur.fetchall()
    finally:
        conn.close()


# ---- GL reconciliation: canonical chart of accounts + crosswalk ----------

@app.get("/gl/accounts", response_model=list[GLAccount])
def list_gl_accounts(current_user: CurrentUser = Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                "SELECT id, code, name, account_type FROM gl_accounts WHERE org_id = %s ORDER BY code",
                (current_user.org_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.post("/gl/accounts", response_model=GLAccount, status_code=status.HTTP_201_CREATED)
def create_gl_account(payload: CreateGLAccountRequest, current_user: CurrentUser = Depends(get_current_user)):
    """Add one account to this org's canonical chart of accounts (e.g. mirroring Xero's COA)."""
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO gl_accounts (org_id, code, name, account_type)
                VALUES (%s, %s, %s, %s)
                RETURNING id, code, name, account_type
                """,
                (current_user.org_id, payload.code, payload.name, payload.account_type),
            )
            conn.commit()
            return cur.fetchone()
    except psycopg2.errors.UniqueViolation:
        conn.rollback()
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=f"Account code '{payload.code}' already exists")
    finally:
        conn.close()


@app.get("/gl/mappings", response_model=list[GLMapping])
def list_gl_mappings(current_user: CurrentUser = Depends(get_current_user)):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT m.id, m.source_system, m.source_category, m.gl_account_id,
                       a.code AS gl_account_code, a.name AS gl_account_name
                FROM gl_mappings m JOIN gl_accounts a ON a.id = m.gl_account_id
                WHERE m.org_id = %s
                ORDER BY m.source_system, m.source_category
                """,
                (current_user.org_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.post("/gl/mappings", response_model=GLMapping, status_code=status.HTTP_201_CREATED)
def create_gl_mapping(payload: CreateGLMappingRequest, current_user: CurrentUser = Depends(get_current_user)):
    """
    Defines the crosswalk rule: 'this system's category = this canonical
    account'. Once created, every past AND future gl_transactions row with
    that (source_system, source_category) pair for this org resolves
    automatically - this is the whole point of the reconciliation engine.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("SELECT id FROM gl_accounts WHERE id = %s AND org_id = %s", (payload.gl_account_id, current_user.org_id))
            if not cur.fetchone():
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="GL account not found for this org")

            cur.execute(
                """
                INSERT INTO gl_mappings (org_id, source_system, source_category, gl_account_id)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (org_id, source_system, source_category)
                    DO UPDATE SET gl_account_id = EXCLUDED.gl_account_id
                RETURNING id, source_system, source_category, gl_account_id
                """,
                (current_user.org_id, payload.source_system.lower(), payload.source_category, payload.gl_account_id),
            )
            row = cur.fetchone()

            # Backfill: apply this new mapping to any transactions that
            # already arrived unmapped, so fixing a mapping once retroactively
            # reconciles everything already ingested, not just future rows.
            cur.execute(
                """
                UPDATE gl_transactions
                SET canonical_gl_account_id = %s
                WHERE org_id = %s AND source_system = %s AND source_category = %s
                  AND canonical_gl_account_id IS NULL
                """,
                (payload.gl_account_id, current_user.org_id, payload.source_system.lower(), payload.source_category),
            )
            conn.commit()

            cur.execute("SELECT code, name FROM gl_accounts WHERE id = %s", (payload.gl_account_id,))
            acc = cur.fetchone()
            return GLMapping(
                id=row["id"], source_system=row["source_system"], source_category=row["source_category"],
                gl_account_id=row["gl_account_id"], gl_account_code=acc["code"], gl_account_name=acc["name"],
            )
    finally:
        conn.close()


@app.get("/gl/transactions", response_model=list[GLTransaction])
def list_gl_transactions(
    limit: int = Query(default=50, le=500),
    unmapped_only: bool = False,
    current_user: CurrentUser = Depends(get_current_user),
):
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            where_unmapped = "AND t.canonical_gl_account_id IS NULL" if unmapped_only else ""
            cur.execute(
                f"""
                SELECT t.source_system, t.source_category, t.canonical_gl_account_id,
                       a.code AS canonical_gl_code, a.name AS canonical_gl_name,
                       t.description, t.amount, t.transaction_date, t.source_file, t.received_at
                FROM gl_transactions t
                LEFT JOIN gl_accounts a ON a.id = t.canonical_gl_account_id
                WHERE t.org_id = %s {where_unmapped}
                ORDER BY t.received_at DESC
                LIMIT %s
                """,
                (current_user.org_id, limit),
            )
            return cur.fetchall()
    finally:
        conn.close()


@app.get("/gl/unmapped-categories", response_model=list[UnmappedCategory])
def list_unmapped_categories(current_user: CurrentUser = Depends(get_current_user)):
    """
    The 'what still needs my attention' view: distinct (system, category)
    pairs with no mapping yet, grouped so you map once per category instead
    of once per transaction. This is the queue a bookkeeper would work
    through to fully reconcile a period.
    """
    conn = get_conn()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT source_system, source_category,
                       COUNT(*) AS transaction_count,
                       SUM(amount) AS total_amount
                FROM gl_transactions
                WHERE org_id = %s AND canonical_gl_account_id IS NULL
                GROUP BY source_system, source_category
                ORDER BY transaction_count DESC
                """,
                (current_user.org_id,),
            )
            return cur.fetchall()
    finally:
        conn.close()
