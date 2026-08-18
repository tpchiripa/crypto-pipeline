// Central place for talking to the FastAPI backend. If the API's shape
// changes, this is the only file that should need to change.

const API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const TOKEN_KEY = "pipeline_auth_token";

export function getToken() {
  return localStorage.getItem(TOKEN_KEY);
}

export function setToken(token) {
  localStorage.setItem(TOKEN_KEY, token);
}

export function clearToken() {
  localStorage.removeItem(TOKEN_KEY);
}

async function get(path, { auth = false } = {}) {
  const headers = {};
  if (auth) {
    const token = getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;
  }
  const res = await fetch(`${API_BASE}${path}`, { headers });
  if (!res.ok) {
    throw new Error(`API ${path} failed: ${res.status}`);
  }
  return res.json();
}

async function post(path, body, { auth = false } = {}) {
  const headers = { "Content-Type": "application/json" };
  if (auth) {
    const token = getToken();
    if (token) headers["Authorization"] = `Bearer ${token}`;
  }
  const res = await fetch(`${API_BASE}${path}`, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    throw new Error(detail.detail || `API ${path} failed: ${res.status}`);
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

export function getRetailTransactions(limit = 15) {
  // Tenant-scoped: the backend filters this to the logged-in user's org.
  return get(`/retail?limit=${limit}`, { auth: true });
}

export function getHospitalityItems(limit = 15) {
  return get(`/hospitality?limit=${limit}`, { auth: true });
}

export function signup(orgName, email, password, industry = "general") {
  return post("/auth/signup", { org_name: orgName, email, password, industry });
}

export function login(email, password) {
  return post("/auth/login", { email, password });
}

export function getMe() {
  return get("/auth/me", { auth: true });
}

// ---- GL reconciliation ----

export function getGLAccounts() {
  return get("/gl/accounts", { auth: true });
}

export function createGLAccount(code, name, accountType) {
  return post("/gl/accounts", { code, name, account_type: accountType }, { auth: true });
}

export function getGLMappings() {
  return get("/gl/mappings", { auth: true });
}

export function createGLMapping(sourceSystem, sourceCategory, glAccountId) {
  return post(
    "/gl/mappings",
    { source_system: sourceSystem, source_category: sourceCategory, gl_account_id: glAccountId },
    { auth: true }
  );
}

export function getUnmappedCategories() {
  return get("/gl/unmapped-categories", { auth: true });
}

export async function uploadGLFile(file, sourceSystem) {
  const formData = new FormData();
  formData.append("file", file);
  formData.append("source_system", sourceSystem);

  const token = getToken();
  const res = await fetch(`${API_BASE}/gl/upload`, {
    method: "POST",
    headers: token ? { Authorization: `Bearer ${token}` } : {},
    // NOTE: deliberately no Content-Type header - the browser sets the
    // multipart boundary itself. Setting it manually breaks the upload.
    body: formData,
  });
  if (!res.ok) {
    const detail = await res.json().catch(() => ({}));
    throw new Error(detail.detail || `Upload failed: ${res.status}`);
  }
  return res.json();
}
