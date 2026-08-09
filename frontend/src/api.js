// Central place for talking to the FastAPI backend. If the API's shape
// changes, this is the only file that should need to change.

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";

async function get(path) {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    throw new Error(`API ${path} failed: ${res.status}`);
  }
  return res.json();
}

export function getTicker() {
  return get("/ticker");
}

export function getTrades(symbol, limit = 40) {
  return get(`/trades/${symbol}?limit=${limit}`);
}

export function getSymbols() {
  return get("/symbols");
}

export function getNews(limit = 15) {
  return get(`/news?limit=${limit}`);
}
